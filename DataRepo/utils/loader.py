import re
from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple

from typing import Dict, Optional, Type

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Model, Q

from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    ConflictingValueErrors,
    DryRun,
    DuplicateValueErrors,
    DuplicateValues,
    ExcelSheetsNotFound,
    InfileDatabaseError,
    InvalidHeaderCrossReferenceError,
    RequiredColumnValue,
    RequiredColumnValues,
    RequiredHeadersError,
    RequiredValueError,
    RequiredValueErrors,
    UnknownHeadersError,
    generate_file_location_string,
)
from DataRepo.utils.file_utils import (
    get_column_dupes,
    get_sheet_names,
    is_excel,
    read_headers_from_file,
)


class TraceBaseLoader(ABC):
    """Class to be used as a superclass for defining a (derived) loader class used to load a (sheet of) an input file.

    Class Attributes:
        TableHeaders (namedtuple): Defines the header keys.
        DefaultHeaders (TableHeaders of strings): Default header names by header key.
        RequiredHeaders (TableHeaders of booleans): Whether a file column is required to be present in the input file,
            indexed by header key.
        RequiredValues (TableHeaders of booleans): Whether a value on a row in a file column is required to be present
            in the input file, indexed by header key.
        UniqueColumnConstraints (list of lists of strings): Sets of unique column name combinations defining what values
            must be unique in the file.
        FieldToHeaderKey (dict): Header keys by field name.
        ColumnTypes (Optional[dict]): Column value types by header key.
        DefaultValues (Optional[TableHeaders of objects]): Column default values by header key.  Auto-filled.
        Models (list of Models): List of model classes.

    Instance Attributes:
        headers (TableHeaders of strings): Customized header names by header key.
        defaults (TableHeaders of objects): Customized default values by header key.
        all_headers (list of strings): Customized header names.
        reqd_headers (TableHeaders of booleans): Required header booleans.
        FieldToHeader (dict of dicts of strings): Header names by model and field.
        unique_constraints (list of lists of strings): Header key combos whose columns must be unique.
        dry_run (boolean) [False]: Dry Run mode.
        defer_rollback (boolean) [False]: Defer rollback mode.
        sheet (str): Name of excel sheet to be loaded.
        file (str): Name of file to be loaded.
    """

    # Abstract required class attributes
    # Must be initialized in the derived class.
    # See TissuesLoader for a concrete example.
    @property
    @abstractmethod
    def TableHeaders(self):  # namedtuple spec
        pass

    @property
    @abstractmethod
    def DefaultHeaders(self):  # namedtuple of strings
        pass

    @property
    @abstractmethod
    def RequiredHeaders(self):  # namedtuple of booleans
        pass

    @property
    @abstractmethod
    def RequiredValues(self):  # namedtuple of booleans
        pass

    @property
    @abstractmethod
    def UniqueColumnConstraints(
        self,
    ):  # list of lists of header keys (e.g. the values in TableHeaders)
        pass

    @property
    @abstractmethod
    def Models(self):  # list of Model classes
        pass

    @property
    @abstractmethod
    def FieldToHeaderKey(self):  # dict of model dicts of field names and header keys
        pass

    @abstractmethod
    def load_data(self):
        """Derived classes must implement a load_data method that does the work of the load.
        Args:
            None
        Raises:
            TBD by the derived class
        Returns:
            Nothing
        """
        pass

    # DefaultValues is populated automatically (with Nones)
    DefaultValues: Optional[tuple] = None  # namedtuple

    # ColumnTypes is optional unless read_from_file needs a dtype argument
    # (converted to by-header-name in get_column_types)
    ColumnTypes: Optional[Dict[str, Type[str]]] = None  # dict of types by header key

    # The keys for the headers in the "Defaults" sheet.
    DefaultsSheetTuple = namedtuple(
        "TableHeaders",
        [
            "SHEET_NAME",
            "COLUMN_NAME",
            "DEFAULT_VALUE",
        ],
    )
    # These are the headers for the "Defaults" sheet.  These are not customizable.
    DefaultsSheetHeaders = DefaultsSheetTuple(
        SHEET_NAME="Sheet Name",
        COLUMN_NAME="Column Header",
        DEFAULT_VALUE="Default Value",
    )

    def __init__(
        self,
        df=None,
        headers=None,
        defaults=None,
        dry_run=False,
        defer_rollback=False,  # DO NOT USE MANUALLY - A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
        data_sheet=None,
        file=None,
        defaults_df=None,
        defaults_sheet=None,
        defaults_file=None,
    ):
        """Constructor.

        Args:
            df (pandas dataframe): Data, e.g. as parsed from a table-like file.
            headers (Optional[Tableheaders namedtuple]) [DefaultHeaders]: Header names by header key.
            defaults (Optional[Tableheaders namedtuple]) [DefaultValues]: Default values by header key.
            dry_run (Optional[boolean]) [False]: Dry run mode.
            defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT MUST
                HANDLE THE ROLLBACK.
            data_sheet (Optional[str]) [None]: Sheet name (for error reporting).
            defaults_sheet (Optional[str]) [None]: Sheet name (for error reporting).
            file (Optional[str]) [None]: File name (for error reporting).

        Raises:
            Nothing

        Returns:
            Nothing
        """
        # Check class attribute validity
        self.check_class_attributes()

        # Apply the loader decorator to the load_data method in the derived class
        self.apply_loader_wrapper()

        # File data
        self.df = df
        # TODO: Extract the defaults file data in the load_table script instead of in get_user_defaults
        self.defaults_df = defaults_df

        # Running Modes
        self.dry_run = dry_run
        self.defer_rollback = defer_rollback

        # Error tracking
        self.skip_row_indexes = []
        self.aggregated_errors_object = AggregatedErrors()

        # For error reporting
        self.file = file
        self.sheet = data_sheet
        self.defaults_file = defaults_file
        self.defaults_sheet = defaults_sheet

        # TODO: Check that defaults_file is None if file is an excel file (the 2 are mutually exclusive when file is excel)

        # Load stats
        self.record_counts = defaultdict(lambda: defaultdict(int))

        # Metadata
        self.initialize_metadata(headers, defaults)

    def apply_loader_wrapper(self):
        """This applies a decorator to the derived class's load_data method.

        See:

        https://stackoverflow.com/questions/72666230/wrapping-derived-class-method-from-base-class

        Args:
            None

        Raises:
            Nothing

        Returns:
            Nothing
        """
        # Apply the _loader decorator to the load_data method in the derived class
        decorated_derived_class_method = self._loader(getattr(self, "load_data"))
        # Get the binding for the decorated method
        bound = decorated_derived_class_method.__get__(self, None)
        # Apply the binding to the handle method in the object
        setattr(self, "load_data", bound)

    def set_row_index(self, index):
        """Sets row_index and rownum instance attributes.

        Args:
            index (int)

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.row_index = index
        self.rownum = index + 2

    def is_skip_row(self, index=None):
        """Determines if the current row is one that should be skipped.

        Various methods will append the current row index to self.skip_row_indexes, such as when errors occur on that
        row.  The current row is set whenever get_row_val is called.  Call this method after all of the row values have
        been obtained to see if loading of the data from this row should be skipped (in order to avoid unnecessary
        errors).  The ultimate goal here is to suppress repeating errors.  You can use add_skip_row_index to manually
        add row indexes that should be skipped.

        Args:
            index (Optional[int]): A manually supplied row index

        Raises:
            Nothing

        Returns:
            boolean: Whether the row should be skipped or not
        """
        check_index = index if index is not None else self.row_index
        return check_index in self.get_skip_row_indexes()

    @classmethod
    def get_class_headers(cls, custom_header_data=None):
        """Returns file headers.

        Note, this method calls check_class_attributes to ensure the derived class is completely defined since that
        check is only otherwise called during object instantiation.

        Args:
            custom_header_data (dict): Header names by header key.  This is expected to be obtained from the parsing of
                a yaml file.  The dict may contain a subset of header keys.  Missing header keys will fall back to the
                defaults defined in the class.

        Raises:
            ValueError if a header key is not in cls.DefaultHeaders

        Returns:
            headers (namedtuple of TableHeaders)
        """
        cls.check_class_attributes()

        extras = []
        if custom_header_data is not None:
            if type(custom_header_data) != dict:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise AggregatedErrors().buffer_error(
                    TypeError(
                        f"Invalid argument: [custom_header_data] dict required, {type(custom_header_data)} supplied."
                    )
                )

            new_dh_dict = cls.DefaultHeaders._asdict()
            for hk in custom_header_data.keys():
                if hk in new_dh_dict.keys():
                    # If None was sent in as a value, fall back to the default so that errors about this header (e.g.
                    # default values of required headers) reference *something*.
                    if custom_header_data[hk] is not None:
                        new_dh_dict[hk] = custom_header_data[hk]
                else:
                    extras.append(hk)

            # Raise programming errors immediately
            if len(extras) > 0:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise AggregatedErrors().buffer_error(
                    ValueError(f"Unexpected header keys: {extras}.")
                )

            headers = cls.DefaultHeaders._replace(**new_dh_dict)
        else:
            headers = cls.DefaultHeaders

        return headers

    def get_headers(self, custom_header_data=None):
        """Returns file headers.

        Note, this method calls check_class_attributes to ensure the derived class is completely defined since that
        check is only otherwise called during object instantiation.

        Args:
            custom_header_data (dict): Header names by header key.  This is expected to be obtained from the parsing of
                a yaml file.  The dict may contain a subset of header keys.  Missing header keys will fall back to the
                defaults defined in the class.

        Raises:
            ValueError if a header key is not in cls.DefaultHeaders

        Returns:
            headers (namedtuple of TableHeaders)
        """
        if hasattr(self, "headers") and self.isnamedtuple(self.headers):
            base_headers = self.headers
        else:
            base_headers = self.DefaultHeaders

        extras = []
        if custom_header_data is not None:
            if type(custom_header_data) != dict:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise AggregatedErrors().buffer_error(
                    TypeError(
                        f"Invalid argument: [custom_header_data] dict required, {type(custom_header_data)} supplied."
                    )
                )

            new_dh_dict = base_headers._asdict()
            for hk in custom_header_data.keys():
                if hk in new_dh_dict.keys():
                    # If None was sent in as a value, fall back to the default so that errors about this header (e.g.
                    # default values of required headers) reference *something*.
                    if custom_header_data[hk] is not None:
                        new_dh_dict[hk] = custom_header_data[hk]
                else:
                    extras.append(hk)

            # Raise programming errors immediately
            if len(extras) > 0:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise AggregatedErrors().buffer_error(
                    ValueError(f"Unexpected header keys: {extras}.")
                )

            headers = base_headers._replace(**new_dh_dict)
        else:
            headers = base_headers

        return headers

    @classmethod
    def get_pretty_default_headers(cls):
        """Generate a list of header strings, with appended asterisks if required, and a message about the asterisks.

        Note, this method calls check_class_attributes to ensure the derived class is completely defined since that
        check is only otherwise called during object instantiation.

        Args:
            None

        Raises:
            Nothing

        Returns:
            pretty_headers (list of string)
            msg (str)
        """
        cls.check_class_attributes()

        msg = "(* = Required)"
        pretty_headers = []
        for hk in list(cls.DefaultHeaders._asdict().keys()):
            reqd = getattr(cls.RequiredHeaders, hk)
            pretty_header = getattr(cls.DefaultHeaders, hk)
            if reqd:
                pretty_header += "*"
            pretty_headers.append(pretty_header)

        return pretty_headers, msg

    @classmethod
    def get_header_keys(cls):
        """Generate a list of header keys.

        Note, this method calls check_class_attributes to ensure the derived class is completely defined since that
        check is only otherwise called during object instantiation.

        Args:
            None

        Raises:
            Nothing

        Returns:
            keys (list of strings)
        """
        cls.check_class_attributes()

        keys = []
        for hk in list(cls.DefaultHeaders._asdict().keys()):
            keys.append(hk)

        return keys

    @classmethod
    def get_class_defaults(cls, custom_default_data=None):
        """Returns defaults tuple.

        Note, this method calls check_class_attributes to ensure the derived class is completely defined since that
        check is only otherwise called during object instantiation.

        Args:
            custom_default_data (dict): Default values by header key.  The dict may contain a subset of header keys.
                Missing header keys will fall back to the defaults defined in the class.

        Raises:
            TypeError is the argument is the wrong type
            ValueError if a header key is not in cls.DefaultValues

        Returns:
            defaults (Optional[namedtuple of TableHeaders])
        """
        cls.check_class_attributes()

        extras = []
        if custom_default_data is not None:
            if type(custom_default_data) != dict:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise AggregatedErrors().buffer_error(
                    TypeError(
                        f"Invalid argument: [custom_default_data] dict required, {type(custom_default_data)} supplied."
                    )
                )

            new_dv_dict = cls.DefaultValues._asdict()
            for hk in custom_default_data.keys():
                if hk in new_dv_dict.keys():
                    new_dv_dict[hk] = custom_default_data[hk]
                else:
                    extras.append(hk)

            # Raise programming errors immediately
            if len(extras) > 0:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise AggregatedErrors().buffer_error(
                    ValueError(f"Unexpected default keys: {extras}.")
                )

            defaults = cls.DefaultValues._replace(**new_dv_dict)
        else:
            defaults = cls.DefaultValues

        return defaults

    def get_defaults(self, custom_default_data=None):
        """Returns defaults tuple.

        Args:
            custom_default_data (dict): Default values by header key.  The dict may contain a subset of header keys.
                Missing header keys will fall back to the defaults defined in the class.

        Raises:
            TypeError is the argument is the wrong type
            ValueError if a header key is not in cls.DefaultValues

        Returns:
            defaults (Optional[namedtuple of TableHeaders])
        """
        if hasattr(self, "defaults") and self.isnamedtuple(self.defaults):
            base_defaults = self.defaults
        else:
            base_defaults = self.DefaultValues

        extras = []
        if custom_default_data is not None:
            if type(custom_default_data) != dict:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise AggregatedErrors().buffer_error(
                    TypeError(
                        f"Invalid argument: [custom_default_data] dict required, {type(custom_default_data)} supplied."
                    )
                )

            new_dv_dict = base_defaults._asdict()
            for hk in custom_default_data.keys():
                if hk in new_dv_dict.keys():
                    new_dv_dict[hk] = custom_default_data[hk]
                else:
                    extras.append(hk)

            # Raise programming errors immediately
            if len(extras) > 0:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise AggregatedErrors().buffer_error(
                    ValueError(f"Unexpected default keys: {extras}.")
                )

            defaults = base_defaults._replace(**new_dv_dict)
            print(f"CUSTOM DEFAULTS SET {defaults}")
        else:
            defaults = base_defaults
            print(f"DEFAULT DEFAULTS SET {defaults}")

        return defaults

    @classmethod
    def check_class_attributes(cls):
        """Checks that the class and instance attributes are properly defined and initialize optional ones.

        Checks the type of:
            DefaultHeaders (class attribute, namedtuple of TableHeaders of strings)
            RequiredHeaders (class attribute, namedtuple of TableHeaders of booleans)
            RequiredValues (class attribute, namedtuple of TableHeaders of booleans)
            UniqueColumnConstraints (class attribute, list of lists of strings): Sets of unique column combinations
            FieldToHeaderKey (class attribute, dict): Header keys by field name
            ColumnTypes (class attribute, Optional[dict]): Column value types by header key
            DefaultValues (Optional[namedtuple of TableHeaders of objects]): Column default values by header key.

        Fills in default None values for header keys in DefaultValues.

        Args:
            None

        Raises:
            AggregatedErrors
                ValueError
                TypeError

        Returns:
            Nothing
        """
        # We create an aggregated errors object in class methods because we may not have an instance with one
        aes = AggregatedErrors()
        # Error check the derived class for required attributes
        typeerrs = []

        try:
            if not cls.isnamedtuple(cls.DefaultHeaders):
                typeerrs.append(
                    f"attribute [{cls.__name__}.DefaultHeaders] namedtuple required, {type(cls.DefaultHeaders)} set"
                )

            if not cls.isnamedtuple(cls.DefaultHeaders):
                typeerrs.append(
                    f"attribute [{cls.__name__}.RequiredHeaders] namedtuple required, {type(cls.RequiredHeaders)} set"
                )

            if not cls.isnamedtuple(cls.RequiredValues):
                typeerrs.append(
                    f"attribute [{cls.__name__}.RequiredValues] namedtuple required, {type(cls.RequiredValues)} set"
                )

            if type(cls.UniqueColumnConstraints) != list:
                typeerrs.append(
                    f"attribute [{cls.__name__}.UniqueColumnConstraints] list required, "
                    f"{type(cls.UniqueColumnConstraints)} set"
                )

            if type(cls.FieldToHeaderKey) != dict:
                typeerrs.append(
                    f"attribute [{cls.__name__}.FieldToHeaderKey] dict required, {type(cls.FieldToHeaderKey)} set"
                )

            # ColumnTypes is optional.  Allow to be left as None.
            if cls.ColumnTypes is not None and type(cls.ColumnTypes) != dict:
                typeerrs.append(
                    f"attribute [{cls.__name__}.ColumnTypes] dict required, {type(cls.ColumnTypes)} set"
                )

            if cls.DefaultValues is None:
                # DefaultValues is optional (not often used/needed). Set all to None using DefaultHeaders
                if cls.DefaultHeaders is not None:
                    # Initialize the same "keys" as the DefaultHeaders, then set all values to None
                    dv_dict = cls.DefaultHeaders._asdict()
                    for hk in dv_dict.keys():
                        dv_dict[hk] = None
                    cls.DefaultValues = cls.DefaultHeaders._replace(**dv_dict)
            elif not cls.isnamedtuple(cls.DefaultValues):
                typeerrs.append(
                    f"attribute [{cls.__name__}.DefaultValues] namedtuple required, {type(cls.DefaultValues)} set"
                )
        except Exception as e:
            aes.buffer_error(e)
        finally:
            if len(typeerrs) > 0:
                nlt = "\n\t"
                aes.buffer_error(
                    TypeError(f"Invalid attributes:\n\t{nlt.join(typeerrs)}")
                )

        # Immediately raise programming related errors
        if aes.should_raise():
            raise aes

    def initialize_metadata(self, headers=None, defaults=None):
        """Initializes metadata.

        Metadata initialized:
            headers (TableHeaders namedtuple of strings): Customized header names by header key.
            defaults (TableHeaders namedtuple of objects): Customized default values by header key.
            all_headers (list of strings): Customized header names.
            reqd_headers (TableHeaders namedtuple of booleans): Required header booleans.
            FieldToHeader (dict of dicts of strings): Header names by model and field.
            unique_constraints (list of lists of strings): Header key combos whose columns must be unique.
            record_counts (dict of dicts of ints): Created, existed, and errored counts by model.

        Args:
            headers (TableHeaders namedtuple of strings): Customized header names by header key.
            defaults (TableHeaders namedtuple of objects): Customized default values by header key.

        Raises:
            AggregatedErrors
                TypeError

        Returns:
            Nothing
        """
        typeerrs = []

        try:
            self.set_headers(headers)
        except TypeError as te:
            typeerrs.append(str(te))

        try:
            self.set_defaults(defaults)
        except TypeError as te:
            typeerrs.append(str(te))

        self.defaults_by_header = self.get_defaults_dict_by_header_name()
        print(f"DBH: {self.defaults_by_header}")

        if self.Models is None or len(self.Models) == 0:
            # Raise programming-related errors immediately
            typeerrs.append("Models is required to have at least 1 Model class")
        else:
            mdlerrs = []
            for mdl in self.Models:
                if not issubclass(mdl, Model):
                    mdlerrs.append(
                        f"{type(mdl).__name__}: Not a subclass of a Django Model"
                    )
            if len(mdlerrs) > 0:
                nltt = "\n\t\t"
                typeerrs.append(
                    "Models must all be valid models, but the following errors were encountered:\n"
                    f"\t\t{nltt.join(mdlerrs)}"
                )

        if len(typeerrs) > 0:
            nlt = "\n\t"
            msg = f"Invalid arguments:\n\t{nlt.join(typeerrs)}"
            self.aggregated_errors_object.buffer_error(TypeError(msg))
            if self.aggregated_errors_object.should_raise():
                raise self.aggregated_errors_object

        # Create a list of the required header string values from a namedtuple of header key/value pairs
        self.reqd_headers = [
            getattr(self.headers, hk)
            for hk in list(self.headers._asdict().keys())
            if getattr(self.RequiredHeaders, hk)
        ]

        # Create a list of header string values for columns whose values are required, from a namedtuple of header key/
        # value pairs
        self.reqd_values = [
            getattr(self.headers, hk)
            for hk in list(self.headers._asdict().keys())
            if getattr(self.RequiredValues, hk)
        ]

        # Create a list lists of header string values whose combinations must be unique, from a list of lists of header
        # keys
        self.unique_constraints = []
        for header_list_combo in self.UniqueColumnConstraints:
            self.unique_constraints.append([])
            for header_key in header_list_combo:
                header_val = getattr(self.headers, header_key)
                self.unique_constraints[-1].append(header_val)

        if self.Models is not None:
            for mdl in self.Models:
                self.record_counts[mdl.__name__]["created"] = 0
                self.record_counts[mdl.__name__]["existed"] = 0
                self.record_counts[mdl.__name__]["errored"] = 0

    def set_headers(self, custom_headers=None):
        """Sets instance's header names.  If no custom headers are provided, it reverts to class defaults.

        This method also sets the following instance attributes because they involve header names (not header keys):

        - all_headers
        - FieldToHeader
        - defaults_by_header

        Args:
            custom_headers (namedtupe of TableHeaders): Header names by header key

        Raises:
            TypeError

        Returns:
            Nothing
        """
        if custom_headers is None:
            self.headers = self.DefaultHeaders
        elif self.isnamedtuple(custom_headers):
            self.headers = custom_headers
        else:
            # Immediately raise programming related errors
            raise TypeError(
                f"Invalid headers: namedtuple required, {type(custom_headers)} supplied"
            )

        # Create a list of all header string values from a namedtuple of header key/value pairs
        self.all_headers = list(self.headers._asdict().values())

        # Error-check the headers
        self.check_header_names()

        # Create a dict of database field keys to header names, from a dict of field name keys and header keys
        self.FieldToHeader = defaultdict(lambda: defaultdict(str))
        for mdl in self.FieldToHeaderKey.keys():
            for fld, hk in self.FieldToHeaderKey[mdl].items():
                self.FieldToHeader[mdl][fld] = getattr(self.headers, hk)

        # Now create a defaults by header name dict (for use by get_row_val)
        self.defaults_by_header = self.get_defaults_dict_by_header_name()

    def set_defaults(self, custom_defaults=None):
        """Sets instance's default values.  If no custom defaults are provided, it reverts to class defaults.

        This method also sets the following instance attributes because they involve header names (not header keys):

        - defaults_by_header

        Args:
            custom_defaults (namedtupe of TableHeaders): Default values by header key

        Raises:
            TypeError

        Returns:
            Nothing
        """
        if custom_defaults is None:
            self.defaults = self.DefaultValues
        elif self.isnamedtuple(custom_defaults):
            self.defaults = custom_defaults
        else:
            # Immediately raise programming related errors
            raise TypeError(
                f"Invalid defaults: namedtuple required, {type(custom_defaults)} supplied"
            )

        # Now create a defaults by header name dict (for use by get_row_val)
        self.defaults_by_header = self.get_defaults_dict_by_header_name()

    @staticmethod
    def isnamedtuple(obj) -> bool:
        """Determined if obj is a namedtuple.

        Based on: https://stackoverflow.com/a/62692640/2057516

        Args:
            obj (object): Any object.

        Raises:
            Nothing

        Returns:
            boolean
        """
        return (
            isinstance(obj, tuple)
            and hasattr(obj, "_asdict")
            and hasattr(obj, "_fields")
        )

    @classmethod
    def get_column_types(cls, headers=None):
        """Returns a dict of column types by header name (not header key).

        This class method is used to obtain a dtypes dict to be able to supply to read_from_file.  You can supply it
        "headers", which is a namedtuple that can be obtained from cls.get_headers.

        Args:
            headers (TableHeaders namedtuple of strings): Customized header names by header key.

        Raises:
            TypeError

        Returns:
            dtypes (dict): Types by header name (instead of by header key)
        """
        if cls.ColumnTypes is None:
            return None

        if headers is None:
            headers = cls.DefaultHeaders
        elif not cls.isnamedtuple(headers):
            # Immediately raise programming related errors
            # We create an aggregated errors object in class methods because we may not have an instance with one
            raise AggregatedErrors().buffer_error(
                TypeError(
                    f"Invalid headers. namedtuple required, {type(headers)} supplied"
                )
            )

        dtypes = {}
        for key in cls.ColumnTypes.keys():
            hdr = getattr(headers, key)
            dtypes[hdr] = cls.ColumnTypes[key]

        return dtypes

    @classmethod
    def header_key_to_name(cls, indict, headers=None):
        """Returns the supplied indict, but its keys are changed from header key to header name.

        This class method is used to obtain a dtypes dict to be able to supply to read_from_file.  You can supply it
        "headers", which is a namedtuple that can be obtained from cls.get_headers.

        Args:
            indict (dict of objects): Any objects by header key
            headers (TableHeaders namedtuple of strings): Customized header names by header key.

        Raises:
            TypeError

        Returns:
            outdict (dict): objects by header name (instead of by header key)
        """
        if indict is None:
            return None

        if headers is None:
            headers = cls.get_class_headers()
        elif not cls.isnamedtuple(headers):
            # Immediately raise programming related errors
            # We create an aggregated errors object in class methods because we may not have an instance with one
            raise AggregatedErrors().buffer_error(
                TypeError(
                    f"Invalid headers. namedtuple required, {type(headers)} supplied"
                )
            )

        outdict = {}
        for key in headers._asdict().keys():
            hdr = getattr(headers, key)
            outdict[hdr] = indict[key]

        return outdict

    def check_header_names(self):
        """Error-checks the header (custom) names set in self.all_headers.

        Args:
            None

        Raises:
            Nothing

        Exceptions buffered:
            ValueError

        Returns:
            Nothing
        """
        dupe_dict = {}
        name_dict = defaultdict(int)
        for hn in self.all_headers:
            name_dict[hn] += 1
        for hn in name_dict.keys():
            if name_dict[hn] > 1:
                dupe_dict[hn] = name_dict[hn]
        if len(dupe_dict.keys()) > 0:
            nlt = "\n\t"
            deets = "\n\t".join([f"{k} occurs {v} times" for k, v in dupe_dict.items()])
            self.aggregated_errors_object.buffer_error(
                ValueError(f"Duplicate Header names encountered:{nlt}{deets}")
            )

    def check_dataframe_headers(self):
        """Error-checks the headers in the dataframe.

        Args:
            None

        Raises:
            RequiredHeadersError

        Exceptions buffered:
            UnknownHeadersError

        Returns:
            Nothing
        """
        if self.df is None:
            # Raise programming errors immediately
            raise ValueError(
                "The df instance member must be defined before calling check_dataframe_headers."
            )

        if self.all_headers is not None:
            unknown_headers = []
            for file_header in self.df.columns:
                if file_header not in self.all_headers:
                    unknown_headers.append(file_header)
            if len(unknown_headers) > 0:
                self.aggregated_errors_object.buffer_error(
                    UnknownHeadersError(
                        unknown_headers, file=self.file, sheet=self.sheet
                    )
                )

        if self.reqd_headers is not None:
            missing_headers = []
            for rqd_header in self.reqd_headers:
                if rqd_header not in self.df.columns:
                    missing_headers.append(rqd_header)
            if len(missing_headers) > 0:
                # Cannot proceed, so not buffering
                raise self.aggregated_errors_object.buffer_error(
                    RequiredHeadersError(
                        missing_headers, file=self.file, sheet=self.sheet
                    )
                )

    def check_unique_constraints(self):
        """Check file column unique constraints.

        Handling unique constraints by catching IntegrityErrors lacks context.  Did the load encounter pre-existing data
        or was the data in the file not unique?  There's no way to tell the user from catching the IntegrityError where
        the duplicate is.  Handling the unique constraints at the file level allows the user to tell where all the
        duplicate values are.

        Args:
            None

        Raises:
            Nothing

        Exceptions Buffered:
            DuplicateValues

        Returns:
            Nothing
        """
        if self.df is None:
            # Raise programming errors immediately
            raise ValueError(
                "The df instance member must be defined before calling check_dataframe_headers."
            )

        if self.unique_constraints is None:
            return
        for unique_combo in self.unique_constraints:
            # A single field unique requirements is much cleaner to display than unique combos, so handle differently
            if len(unique_combo) == 1:
                dupes, row_idxs = self.get_one_column_dupes(self.df, unique_combo[0])
            else:
                dupes, row_idxs = get_column_dupes(self.df, unique_combo)
            self.add_skip_row_index(index_list=row_idxs)
            if len(dupes) > 0:
                self.aggregated_errors_object.buffer_error(
                    DuplicateValues(
                        dupes, unique_combo, sheet=self.sheet, file=self.file
                    )
                )

    def add_skip_row_index(
        self, index: Optional[int] = None, index_list: Optional[list] = None
    ):
        """Adds indexes to skip_row_indexes.

        Args:
            index (int): Row index.  Mutually exclusive with index_list.  Required if index_list is None.
            index_list (list of ints)L Row indexes.  Mutually exclusive with index.  Required if index is None.

        Raises:
            ValueError

        Returns:
            Nothing
        """
        if index is None and index_list is None:
            # Raise programming errors (data errors are buffered)
            raise ValueError("Either an index or index_list argument is required.")
        if index is not None and index not in self.skip_row_indexes:
            self.skip_row_indexes.append(index)
        if index_list is not None:
            for idx in index_list:
                if idx not in self.skip_row_indexes:
                    self.skip_row_indexes.append(idx)

    def get_skip_row_indexes(self):
        """Returns skip_row_indexes.

        Args:
            None

        Raises:
            Nothing

        Returns:
            Nothing
        """
        return self.skip_row_indexes

    def get_row_val(self, row, header, strip=True):
        """Returns value from the row (presumably from df) and column (identified by header).

        Converts empty strings and "nan"s to None.  Strips leading/trailing spaces.

        Args:
            row (row of a dataframe): Row of data.
            header (str): Column header name.
            strip (boolean) [True]: Whether to strip leading and trailing spaces.

        Raises:
            ValueError
            RequiredColumnValue

        Returns:
            val (object): Data from the row at the column (header)
        """
        # A pandas dataframe row object contains that row's index as an integer in the .name attribute
        # By setting the current row index in get_row_val, the derived class never needs to explicitly do it
        self.set_row_index(row.name)

        none_vals = ["", "nan"]
        val = None

        if header in row:
            val = row[header]
            if type(val) == str and strip is True:
                val = val.strip()
            if val in none_vals:
                val = None
        elif self.all_headers is not None and header not in self.all_headers:
            # Missing headers are addressed way before this. If we get here, it's a programming issue, so raise instead
            # of buffer
            raise ValueError(
                f"Incorrect header supplied: [{header}].  Must be one of: {self.all_headers}"
            )

        # If val is None
        if val is None:
            # Fill in a default value
            val = self.defaults_by_header.get(header, None)

            # If the val is still None and it is required
            if val is None and header in self.reqd_values:
                self.add_skip_row_index(self.row_index)
                # This raise was added to force the developer to not continue the loop. It's handled/caught in
                # handle_load_db_errors.
                raise RequiredColumnValue(
                    column=header,
                    sheet=self.sheet,
                    file=self.file,
                    rownum=self.rownum,
                )

        return val

    def get_user_defaults(self):
        # TODO: Make get_user_defaults in load_table do the file parsing and supply a dataframe, then re-write this method to not read the file.
        # Return value
        user_defaults = {}

        # Save the looked up headers (to avoid repeated lookups)
        headers_by_sheet = {}

        # Get all the sheet names in the current (assumed: excel) file
        all_sheet_names = None
        if self.file is not None and is_excel(self.file):
            all_sheet_names = get_sheet_names(self.file)

        # Get the column types by header name
        coltypes = self.get_column_types()

        # Error tracking
        unknown_sheets = defaultdict(list)
        unknown_headers = defaultdict(list)
        invalid_type_errs = []

        for _, row in self.df.iterrows():
            # Get the sheet from the row
            sheet_name = self.get_row_val(row, self.DefaultsSheetHeaders.SHEET_NAME)

            # If the sheet name was not found in the file
            if (
                all_sheet_names is not None
                and sheet_name not in all_sheet_names
                and sheet_name not in unknown_sheets
            ):
                unknown_sheets[sheet_name].append(self.rownum)
                continue

            # Skip sheets that are not the target load_sheet
            # TODO: Figure out how to handle when self.sheet is None (i.e. when processing a tsv file, if I decide to
            # support a separate defaults file)
            if self.sheet is not None and sheet_name != self.sheet:
                continue

            # Get the header from the row
            header_name = str(self.get_row_val(row, self.DefaultsSheetHeaders.COLUMN_NAME))

            # If we have not saved the headers yet for this sheet
            if sheet_name not in headers_by_sheet.keys():
                headers_by_sheet[sheet_name] = read_headers_from_file(self.file)

            # If the header name from the defaults sheet is not an actual header on the load_sheet
            if header_name not in headers_by_sheet[sheet_name]:
                unknown_headers[header_name].append(self.rownum)
                continue

            # Grab the default value
            default_val = self.get_row_val(row, self.DefaultsSheetHeaders.DEFAULT_VALUE)

            if (
                header_name in coltypes.keys()
                and default_val is not None
                and coltypes[header_name] is not None
                and type(default_val) != coltypes[header_name]
            ):
                invalid_type_errs.append(
                    f"Invalid default value: [{default_val}].  Value type should be [{coltypes[header_name]}] but the "
                    f"type encountered was [{type(default_val)}] on row {self.rownum}."
                )

            user_defaults[header_name] = default_val

        if len(unknown_sheets.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                ExcelSheetsNotFound(
                    unknown_sheets,
                    all_sheet_names,
                    file=self.file,
                    column=self.DefaultsSheetHeaders.SHEET_NAME,
                    source_sheet=self.defaults_sheet,
                )
            )

        if len(unknown_headers.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                InvalidHeaderCrossReferenceError(
                    source_file=self.file,
                    source_sheet=self.defaults_sheet,
                    column=self.DefaultsSheetHeaders.COLUMN_NAME,
                    unknown_headers=unknown_headers,
                    target_file=self.file,
                    target_sheet=self.sheet,
                    target_headers=headers_by_sheet[self.sheet],
                )
            )

        if len(invalid_type_errs) > 0:
            loc = generate_file_location_string(
                column=self.DefaultsSheetHeaders.DEFAULT_VALUE, sheet=self.defaults_sheet, file=self.file
            )
            deets = "\n\t".join(invalid_type_errs)
            self.aggregated_errors_object.buffer_error(
                TypeError(
                    f"Invalid default values encountered in {loc} on the indicated rows:\n\t{deets}"
                )
            )

        return user_defaults

    def tableheaders_to_dict_by_header_name(self, intuple):
        """Convert the intuple (a TableHeaders namedtuple) into a dict by (custom) header name.

        Args:
            intuple (TableHeaders namedtuple): objects by header key

        Raises:
            TypeError

        Returns:
            defdict (Optional[dict of objects]): objects by header name
        """
        print(f"INTUPLE: {intuple}")
        self.check_class_attributes()
        if not self.isnamedtuple(intuple):
            self.aggregated_errors_object.buffer_error(
                TypeError(
                    f"Invalid intuple argument: namedtuple required, {type(intuple)} supplied"
                )
            )
            return None
        outdict = {}
        for hk, hn in self.headers._asdict().items():
            v = getattr(intuple, hk)
            outdict[hn] = v
        return outdict

    def get_defaults_dict_by_header_name(self):
        """Convert the defaults namedtuple instance attribute (by header key) into a dict by (custom) header name.

        Args:
            None

        Raises:
            Nothing

        Returns:
            defdict (dict of objects): default values by header name
        """
        return self.tableheaders_to_dict_by_header_name(self.get_defaults())

    @staticmethod
    def _loader(fn):
        """load_data method decorator that handles atomic transactions, running modes, exceptions, and load stats.

        Args:
            fn (function)

        Raises:
            DryRun
            AggregatedErrors

        Returns:
            load_wrapper (function)
                Args:
                    None

                Raises:
                    TBD by derived class

                Returns:
                    What's returned by the wrapped method
        """

        def load_wrapper(self, *args, **kwargs):
            # if df is None and self.df is None:
            #     # Raise programming errors immediately
            #     raise ValueError("A dataframe [the df argument to either this method or the constructor] is required")
            # elif df is not None:
            #     if self.df is not None:
            #         self.aggregated_errors_object.buffer_warning(
            #             ValueError(
            #                 "Overwriting the dataframe from the constructor with the one supplied to load_data."
            #             )
            #         )
            #     self.df = df

            retval = None
            with transaction.atomic():
                try:
                    self.check_dataframe_headers()
                    self.check_unique_constraints()

                    retval = fn(*args, **kwargs)

                except AggregatedErrors as aes:
                    if aes != self.aggregated_errors_object:
                        self.aggregated_errors_object.merge_aggregated_errors_object(
                            aes
                        )
                except Exception as e:
                    # Add this unanticipated error to the other buffered errors
                    self.aggregated_errors_object.buffer_error(e)

                # Summarize any ConflictingValueError errors reported
                cves = self.aggregated_errors_object.remove_exception_type(
                    ConflictingValueError
                )
                if len(cves) > 0:
                    self.aggregated_errors_object.buffer_error(
                        ConflictingValueErrors(cves)
                    )

                # Summarize any RequiredValueError errors reported
                rves = self.aggregated_errors_object.remove_exception_type(
                    RequiredValueError
                )
                if len(rves) > 0:
                    self.aggregated_errors_object.buffer_error(
                        RequiredValueErrors(rves)
                    )

                # Summarize any DuplicateValues errors reported
                dvs = self.aggregated_errors_object.remove_exception_type(
                    DuplicateValues
                )
                if len(dvs) > 0:
                    self.aggregated_errors_object.buffer_error(
                        DuplicateValueErrors(dvs)
                    )

                # Summarize any RequiredColumnValue errors reported
                rcvs = self.aggregated_errors_object.remove_exception_type(
                    RequiredColumnValue
                )
                if len(rcvs) > 0:
                    self.aggregated_errors_object.buffer_error(
                        RequiredColumnValues(rcvs)
                    )

                if (
                    self.aggregated_errors_object.should_raise()
                    and not self.defer_rollback
                ):
                    # Raise here to cause a rollback
                    raise self.aggregated_errors_object

                if self.dry_run:
                    raise DryRun()

            if self.aggregated_errors_object.should_raise():
                # Raise here to NOT cause a rollback
                raise self.aggregated_errors_object

            return retval

        return load_wrapper

    def _get_model_name(self, model_name=None):
        """Returns the model name registered to the class (or as supplied).

        If model_name is supplied, it returns that model name.  If not supplied, and models is of length 1, it returns
        that one model.  The purpose of this method is so that simple 1-model loaders do not need to supply the model
        name to the created, existed, and errored methods.

        Args:
            model_name (str)

        Raises:
            ValueError

        Returns:
            model_name (str)
        """
        if model_name is not None:
            return model_name
        if self.Models is not None and len(self.Models) == 1:
            return self.Models[0].__name__
        # If we get here, it's a programming error, so raise immediately
        raise self.aggregated_errors_object.buffer_error(
            ValueError(
                "A model name is required when there is not exactly 1 model initialized in the constructor."
            )
        )

    @classmethod
    def get_models(cls):
        cls.check_class_attributes()
        return cls.Models

    def created(self, model_name: Optional[str] = None):
        """Increments a created record count for a model.

        Args:
            model_name (Optional[str])

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.record_counts[self._get_model_name(model_name)]["created"] += 1

    def existed(self, model_name: Optional[str] = None):
        """Increments an existed(/skipped) record count for a model.

        Args:
            model_name (Optional[str])

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.record_counts[self._get_model_name(model_name)]["existed"] += 1

    def errored(self, model_name: Optional[str] = None):
        """Increments an errored record count for a model.

        Note, this is not for all errors.  It only pertains to data-specific errors from the input file.

        Args:
            model_name (Optional[str])

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.record_counts[self._get_model_name(model_name)]["errored"] += 1

    def get_load_stats(self):
        """Returns the model record status counts.

        Args:
            None

        Raises:
            Nothing

        Returns:
            record_counts (dict of dicts of ints): Counts by model and status
        """
        return self.record_counts

    def check_for_inconsistencies(self, rec, rec_dict):
        """Generate ConflictingValueError exceptions based on differences between a supplied record and dict.

        This function compares the supplied database model record with the dict that was used to (get or) create a
        record that resulted (or will result) in an IntegrityError (i.e. a unique constraint violation).  Call this
        method inside an `except IntegrityError` block, e.g.:
            try:
                rec_dict = {field values for record creation}
                rec, created = Model.objects.get_or_create(**rec_dict)
            except IntegrityError as ie:
                rec = Model.objects.get(name="unique value")
                self.check_for_inconsistencies(rec, rec_dict)

        It can also be called pre-emptively by querying for only a record's unique field and supply the record and a
        dict for record creation.  E.g.:
            rec_dict = {field values for record creation}
            rec = Model.objects.get(name="unique value")
            self.check_for_inconsistencies(rec, rec_dict)

        The purpose of this function is to provide helpful information in an exception (i.e. repackage an
        IntegrityError) so that users working to resolve the error can quickly identify and resolve the issue.

        It buffers any issues it encounters as a ConflictingValueError inside self.aggregated_errors_object.

        Args:
            rec (Model object)
            rec_dict (dict of objects): A dict (e.g., as supplied to get_or_create() or create())

        Raises (or buffers):
            ConflictingValueError

        Returns:
            found_errors (boolean)
        """
        found_errors = False
        differences = {}
        for field, new_value in rec_dict.items():
            orig_value = getattr(rec, field)
            if orig_value != new_value:
                differences[field] = {
                    "orig": orig_value,
                    "new": new_value,
                }
        if len(differences.keys()) > 0:
            found_errors = True
            self.aggregated_errors_object.buffer_error(
                ConflictingValueError(
                    rec,
                    differences,
                    rec_dict=rec_dict,
                    rownum=self.rownum,
                    sheet=self.sheet,
                    file=self.file,
                )
            )
        return found_errors

    def handle_load_db_errors(
        self,
        exception,
        model,
        rec_dict,
        handle_all=True,
    ):
        """Handles IntegrityErrors and ValidationErrors raised during database loading.  Put in `except` block.

        The purpose of this function is to provide helpful information in an exception (i.e. repackage an IntegrityError
        or a ValidationError) so that users working to resolve errors can quickly identify and resolve data issues.  It
        calls check_for_inconsistencies.

        This function evaluates whether the supplied exception is the result of either a field value conflict or a
        validation error (triggered by a full_clean).  It will either buffer a ConflictingValue error in either the
        supplied conflicts_list or AggregatedErrors object, or raise an exception.

        Args:
            exception (Exception): Exception, e.g. obtained from `except` block
            model (Model): Model being loaded when the exception occurred
            rec_dict (dict): Fields and their values that were passed to either `create` or `get_or_create`
            sheet (str): Name of the Excel sheet that was being loaded when the exception occurred.
            file (str): Name (path optional) of the file that was being loaded when the exception occurred.

        Raises (or buffers):
            ValueError
            InfileDatabaseError
            ConflictingValueError
            RequiredValuesError

        Returns:
            boolean indicating whether an error was handled(/buffered).
        """
        # We may or may not use estr and exc, but we're pre-making them here to reduce code duplication
        estr = str(exception)
        exc = InfileDatabaseError(
            exception, rec_dict, rownum=self.rownum, sheet=self.sheet, file=self.file
        )

        if isinstance(exception, IntegrityError):
            if "duplicate key value violates unique constraint" in estr:
                # Create a list of lists of unique fields and unique combos of fields
                # First, get unique fields and force them into a list of lists (so that we only need to loop once)
                unique_combos = [
                    [f] for f in self.get_unique_fields(model, fields=rec_dict.keys())
                ]
                # Now add in the unique field combos from the model's unique constraints
                unique_combos.extend(self.get_unique_constraint_fields(model))

                # Create a set of the fields in the dict causing the error so that we can only check unique its fields
                field_set = set(rec_dict.keys())

                # We're going to loop over unique records until we find one that conflicts with the dict
                for combo_fields in unique_combos:
                    # Only proceed if we have all the values
                    combo_set = set(combo_fields)
                    if not combo_set.issubset(field_set):
                        continue

                    # Retrieve the record with the conflicting value(s) that caused the unique constraint error using
                    # the unique fields
                    q = Q()
                    for uf in combo_fields:
                        q &= Q(**{f"{uf}__exact": rec_dict[uf]})
                    qs = model.objects.filter(q)

                    # If there was a record found using a unique field (combo)
                    if qs.count() == 1:
                        rec = qs.first()
                        errs_found = self.check_for_inconsistencies(rec, rec_dict)
                        if errs_found:
                            return True

            elif "violates not-null constraint" in estr:
                # Parse the field name out of the exception string
                regexp = re.compile(r"^null value in column \"(?P<fldname>[^\"]+)\"")
                match = re.search(regexp, estr)
                if match:
                    fldname = match.group("fldname")
                    colname = fldname
                    # Convert the database column name to the file column header, if available
                    if (
                        self.FieldToHeader is not None
                        and colname in self.FieldToHeader[model.__name__].keys()
                    ):
                        colname = self.FieldToHeader[model.__name__][fldname]
                    self.aggregated_errors_object.buffer_error(
                        RequiredValueError(
                            column=colname,
                            rownum=self.rownum,
                            model_name=model.__name__,
                            field_name=fldname,
                            sheet=self.sheet,
                            file=self.file,
                            rec_dict=rec_dict,
                        )
                    )
                    return True

        elif isinstance(exception, ValidationError):
            if "is not a valid choice" in estr:
                choice_fields = self.get_enumerated_fields(
                    model, fields=rec_dict.keys()
                )
                for choice_field in choice_fields:
                    if choice_field in estr and rec_dict[choice_field] is not None:
                        # Only include error once
                        if not self.aggregated_errors_object.exception_type_exists(
                            InfileDatabaseError
                        ):
                            self.aggregated_errors_object.buffer_error(exc)
                        else:
                            already_buffered = False
                            ides = self.aggregated_errors_object.get_exception_type(
                                InfileDatabaseError
                            )
                            for existing_exc in ides:
                                # If the triggering exception (stored in the InfileDatabaseError exception) is the same,
                                # skip it
                                if str(existing_exc.exception) == str(exc.exception):
                                    already_buffered = True
                            if not already_buffered:
                                self.aggregated_errors_object.buffer_error(exc)
                        # Whether we buffered or not, the error was identified and handled (by either buffering or
                        # ignoring a duplicate)
                        return True

        elif isinstance(exception, RequiredColumnValue):
            # This "catch" was added to force the developer to not continue the loop if they failed to call this method
            self.aggregated_errors_object.buffer_error(exception)
            return True

        if handle_all:
            if rec_dict is not None and len(rec_dict.keys()) > 0:
                self.aggregated_errors_object.buffer_error(exc)
            else:
                self.aggregated_errors_object.buffer_error(exception)
            return True

        # If we get here, we did not identify the error as one we knew what to do with
        return False

    @classmethod
    def get_one_column_dupes(cls, data, col_key, ignore_row_idxs=None):
        """Find duplicate values in a single column from file table data.

        Args:
            data (DataFrame or list of dicts): The table data parsed from a file.
            unique_col_keys (list of column name strings): Column names whose combination must be unique.
            ignore_row_idxs (list of integers): Rows to ignore.

        Returns:
            1. A dict keyed on duplicate values and the value is a list of integers for the rows where it occurs.
            2. A list of all row indexes containing duplicate data.
        """
        all_row_idxs_with_dupes = []
        vals_dict = defaultdict(list)
        dupe_dict = defaultdict(dict)
        dict_list = data if type(data) == list else data.to_dict("records")

        for rowidx, row in enumerate(dict_list):
            # Ignore rows where the animal name is empty
            if ignore_row_idxs is not None and rowidx in ignore_row_idxs:
                continue
            try:
                vals_dict[row[col_key]].append(rowidx)
            except KeyError:
                raise UnknownHeadersError([col_key])

        for key in vals_dict.keys():
            if len(vals_dict[key]) > 1:
                dupe_dict[key] = vals_dict[key]
                all_row_idxs_with_dupes.extend(vals_dict[key])

        return dupe_dict, all_row_idxs_with_dupes

    @classmethod
    def get_unique_constraint_fields(cls, model):
        """
        Returns a list of lists of names of fields involved in UniqueConstraints in a given model.
        """
        uflds = []
        if hasattr(model._meta, "constraints"):
            for constraint in model._meta.constraints:
                if type(constraint).__name__ == "UniqueConstraint":
                    uflds.append(constraint.fields)
        return uflds

    @classmethod
    def get_unique_fields(cls, model, fields=None):
        """
        Returns a list of non-auto-field names where unique is True.

        If fields (list of field names) is provided, the returned field names are limited to the list provided.
        """
        return [
            f.name if hasattr(f, "name") else f.field_name
            for f in cls.get_non_auto_model_fields(model)
            if (fields is None or cls.field_in_fieldnames(f, fields))
            and hasattr(f, "unique")
            and f.unique
        ]

    @classmethod
    def get_enumerated_fields(cls, model, fields=None):
        """
        Returns a list of non-auto-field names where choices is populated.

        If fields (list of field names) is provided, the returned field names are limited to the list provided.
        """
        return [
            f.name if hasattr(f, "name") else f.field_name
            for f in cls.get_non_auto_model_fields(model)
            if (fields is None or cls.field_in_fieldnames(f, fields))
            and hasattr(f, "choices")
            and f.choices
        ]

    @classmethod
    def get_non_auto_model_fields(cls, model):
        """
        Retrieves all non-auto-fields from the supplied model and returns as a list of actual fields.
        """
        return [
            f for f in model._meta.get_fields() if f.get_internal_type() != "AutoField"
        ]

    @classmethod
    def field_in_fieldnames(cls, fld, fld_names):
        """
        Accessory function to get_unique_fields and get_enumerated_fields.  This only exists in order to avoid JSCPD
        errors.
        """
        # Relation fields do not have "name" attributes.  Instead, they have "field_name" attributes.  The values of
        # both are the attributes of the model object that we are after (because they can be used in queries).  It is
        # assumed that a field is guaranteed to have one or the other.
        return (hasattr(fld, "name") and fld.name in fld_names) or (
            hasattr(fld, "field_name") and fld.field_name in fld_names
        )
