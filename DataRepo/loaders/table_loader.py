import re
from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
from typing import Dict, Optional, Type

import pandas as pd
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Model, Q
from django.db.utils import ProgrammingError

from DataRepo.models.utilities import get_model_fields
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    ConflictingValueErrors,
    DryRun,
    DuplicateHeaders,
    DuplicateValueErrors,
    DuplicateValues,
    ExcelSheetsNotFound,
    InfileDatabaseError,
    InvalidHeaderCrossReferenceError,
    NoLoadData,
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
)


class TableLoader(ABC):
    """Class to be used as a superclass for defining a (derived) loader class used to load a (sheet of) an input file.

    Class Attributes:
        DataTableHeaders (namedtuple): Defines the header keys.
        DataHeaders (DataTableHeaders of strings): Default header names by header key.
        DataRequiredHeaders (list of strings and lists): Required header keys. Note, this is an N-dimensional list
            where each dimension alternates between "all required" and "any required".  See get_missing_headers for
            detailed examples.
        DataRequiredValues (list of strings and lists): Required header keys. Note, this is an N-dimensional list
            where each dimension alternates between "all required" and "any required".  See get_missing_headers for
            detailed examples (required headers are similar to required values).
        DataUniqueColumnConstraints (list of lists of strings): Sets of unique column name combinations defining what
            values must be unique in the file.
        FieldToDataHeaderKey (dict): Header keys by field name.
        DataColumnTypes (Optional[dict]): Column value types by header key.
        DataDefaultValues (Optional[DataTableHeaders of objects]): Column default values by header key.  Auto-filled.
        Models (list of Models): List of model classes.

    Instance Attributes:
        headers (DataTableHeaders of strings): Customized header names by header key.
        defaults (DataTableHeaders of objects): Customized default values by header key.
        all_headers (list of strings): Customized header names.
        reqd_headers (list of strings and lists): Required header names. Note, this is an N-dimensional list
            where each dimension alternates between "all required" and "any required".  See get_missing_headers for
            detailed examples.
        reqd_values (list of strings and lists): Required value header names. Note, this is an N-dimensional list
            where each dimension alternates between "all required" and "any required".  See get_missing_headers for
            detailed examples (required headers are similar to required values).
        FieldToHeader (dict of dicts of strings): Header names by model and field.
        unique_constraints (list of lists of strings): Header key combos whose columns must be unique.
        dry_run (boolean) [False]: Dry Run mode.
        defer_rollback (boolean) [False]: Defer rollback mode.
        sheet (str): Name of excel sheet to be loaded.
        file (str): Name of file to be loaded.
    """

    # NOTE: Abstract method and properties(/class attributes) must be initialized in the derived class.
    #       See TissuesLoader for a concrete example.

    @property
    @abstractmethod
    def Models(self):
        # list of Model classes that will be loaded
        pass

    @property
    @abstractmethod
    def DataSheetName(self):
        # str
        pass

    @property
    @abstractmethod
    def DataTableHeaders(self):
        # namedtuple spec
        pass

    @property
    @abstractmethod
    def DataHeaders(self):
        # namedtuple of strings
        pass

    @property
    @abstractmethod
    def DataRequiredHeaders(self):
        # N-dimensional list of strings.  See get_missing_headers for examples.
        pass

    @property
    @abstractmethod
    def DataRequiredValues(self):
        # N-dimensional list of strings.  See get_missing_headers for examples.
        pass

    @property
    @abstractmethod
    def DataUniqueColumnConstraints(self):
        # list of lists of header keys (e.g. the values in DataTableHeaders)
        pass

    @property
    @abstractmethod
    def FieldToDataHeaderKey(self):
        # dict of model dicts of field names and header keys
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

    # DataDefaultValues is populated automatically (with Nones)
    DataDefaultValues: Optional[tuple] = None  # namedtuple

    # DataColumnTypes is optional unless read_from_file needs a dtype argument
    # (converted to by-header-name in get_column_types)
    DataColumnTypes: Optional[
        Dict[str, Type[str]]
    ] = None  # dict of types by header key

    # For the defaults sheet...
    DefaultsSheetName = "Defaults"

    # The keys for the headers in the "Defaults" sheet.
    DefaultsTableHeaders = namedtuple(
        "DefaultsTableHeaders",
        [
            "SHEET_NAME",
            "COLUMN_NAME",
            "DEFAULT_VALUE",
        ],
    )

    # These are the headers for the "Defaults" sheet.  These are not customizable.
    DefaultsHeaders = DefaultsTableHeaders(
        SHEET_NAME="Sheet Name",
        COLUMN_NAME="Column Header",
        DEFAULT_VALUE="Default Value",
    )

    # DEFAULT_VALUE is not required (allow user to selete the value) - but note that all the headers are required
    DefaultsRequiredValues = ["SHEET_NAME", "COLUMN_NAME"]

    def __init__(
        self,
        df=None,
        dry_run=False,
        defer_rollback=False,  # DO NOT USE MANUALLY - A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
        file=None,
        data_sheet=None,
        defaults_df=None,
        defaults_sheet=None,
        defaults_file=None,
        user_headers=None,
        headers=None,
        defaults=None,
    ):
        """Constructor.

        Note, headers and defaults are intended for copying custom values from one object to another.

        Args:
            df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
            dry_run (Optional[boolean]) [False]: Dry run mode.
            defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT MUST
                HANDLE THE ROLLBACK.
            data_sheet (Optional[str]) [None]: Sheet name (for error reporting).
            defaults_sheet (Optional[str]) [None]: Sheet name (for error reporting).
            file (Optional[str]) [None]: File name (for error reporting).
            user_headers (Optional[dict]): Header names by header key.
            defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
            defaults_file (Optional[str]) [None]: Defaults file name (None if the same as infile).
            headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
            defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.

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
        self.defaults_df = defaults_df

        # For retrieving data from df
        self.user_headers = user_headers

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

        # This is for preserving derived class headers and defaults
        self.headers = headers
        self.defaults = defaults

        # Metadata
        self.initialize_metadata()

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
        decorated_derived_class_method = self._loader()(getattr(self, "load_data"))
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
        if index is None:
            self.rownum = None
        else:
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

    def get_headers(self):
        """Returns current headers.

        Args:
            None

        Raises:
            Nothing

        Returns:
            headers (namedtuple of DataTableHeaders)
        """
        if hasattr(self, "headers") and self.headers is not None:
            return self.headers
        return self.DataHeaders

    def set_headers(self, custom_headers=None):
        """Sets instance's header names.  If no custom headers are provided, it reverts to class defaults.

        This method sets the following instance attributes because they involve header names (not header keys), so
        anytime the header names are updated or changed, these need to be reset:

        - headers (DataTableHeaders namedtuple of strings): Customized header names by header key.
        - all_headers (list of strings): Customized header names.
        - reqd_headers (list of strings and lists): Required header names. Note, this is an N-dimensional list
            where each dimension alternates between "all required" and "any required".  See get_missing_headers for
            detailed examples.
        - FieldToHeader (dict of dicts of strings): Header names by model and field.
        - unique_constraints (list of lists of strings): Header name combos whose columns must be unique.
        - reqd_values (list of strings and lists): Required value header names. Note, this is an N-dimensional list
            where each dimension alternates between "all required" and "any required".  See get_missing_headers for
            detailed examples (required headers are similar to required values).
        - defaults_by_header (dict): Default values by header name.
        - reverse_headers (dict): Header keys by header name.

        Args:
            custom_headers (dict): Header names by header key

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.headers = self._merge_headers(custom_headers)

        # Create a list of all header string values from a namedtuple of header key/value pairs, in the order in which
        # they were defined
        self.all_headers = [getattr(self.headers, hk) for hk in self.headers._fields]

        # Create a dict of header names that map to header key (a reverse lookup)
        duphns = defaultdict(int)
        self.reverse_headers = {}
        for hk, hn in self.headers._asdict().items():
            if hn in self.reverse_headers.keys():
                duphns[hn] += 1
                continue
            self.reverse_headers[hn] = hk
        if len(duphns) > 0:
            for k in duphns.keys():
                duphns[k] += 1
            self.aggregated_errors_object.buffer_error(
                DuplicateHeaders(duphns, self.all_headers)
            )

        # Create a dict of database field keys to header names, from a dict of field name keys and header keys
        self.FieldToHeader = defaultdict(lambda: defaultdict(str))
        for mdl in self.FieldToDataHeaderKey.keys():
            for fld, hk in self.FieldToDataHeaderKey[mdl].items():
                self.FieldToHeader[mdl][fld] = getattr(self.headers, hk)

        # Create a list of the required columns by header name from an N-dimensional list of header keys
        self.reqd_headers = self.header_keys_to_names(self.DataRequiredHeaders)

        # Create a list of the required column values by header name from an N-dimensional list of header keys
        self.reqd_values = self.header_keys_to_names(self.DataRequiredValues)

        # Create a list lists of header string values whose combinations must be unique, from a list of lists of header
        # keys
        self.unique_constraints = []
        for header_list_combo in self.DataUniqueColumnConstraints:
            self.unique_constraints.append([])
            for header_key in header_list_combo:
                header_val = getattr(self.headers, header_key)
                self.unique_constraints[-1].append(header_val)

        # Now create a defaults by header name dict (for use by get_row_val)
        self.defaults_by_header = self.get_defaults_dict_by_header_name()

    def _merge_headers(self, custom_headers=None):
        """Merges user, developer (custom headers), and class headers hierarchically.

        Args:
            custom_headers (dict): Header names by header key

        Raises:
            AggregatedErrors
                TypeError
                ValueError

        Returns:
            headers (namedtuple of DataTableHeaders)
        """
        # The starting headers are those previously set or defined by the class
        if hasattr(self, "headers") and self.headers is not None:
            final_custom_headers = self.headers
        else:
            final_custom_headers = self.DataHeaders

        # custom headers can be trumped by user headers, so we will set the custom headers next, overwriting anything
        # set in the class
        extras = []
        if custom_headers is not None:
            if type(custom_headers) != dict:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise self.aggregated_errors_object.buffer_error(
                    TypeError(
                        f"Invalid argument: [custom_header_data] dict required, {type(custom_headers)} supplied."
                    )
                )

            new_dh_dict = final_custom_headers._asdict()
            for hk in custom_headers.keys():
                if hk in new_dh_dict.keys():
                    # If None was sent in as a value, fall back to the default so that errors about this header (e.g.
                    # default values of required headers) reference *something*.
                    if custom_headers[hk] is not None:
                        new_dh_dict[hk] = custom_headers[hk]
                else:
                    extras.append(hk)

            # Raise programming errors immediately
            if len(extras) > 0:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise self.aggregated_errors_object.buffer_error(
                    ValueError(
                        f"Unexpected header keys: {extras} in custom_headers argument."
                    )
                )

            final_custom_headers = final_custom_headers._replace(**new_dh_dict)

        # If user headers are defined, overwrite anything previously set with them
        if self.user_headers is not None:
            new_uh_dict = final_custom_headers._asdict()
            # To support incomplete headers dicts
            for hk in self.user_headers.keys():
                if hk in new_uh_dict.keys():
                    new_uh_dict[hk] = self.user_headers[hk]
                else:
                    extras.append(hk)

            # Raise programming errors immediately
            if len(extras) > 0:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise self.aggregated_errors_object.buffer_error(
                    ValueError(f"Unexpected header keys: {extras} in user headers.")
                )

            final_custom_headers = final_custom_headers._replace(**new_uh_dict)

        # The loader_class method get_headers will merge the custom headers
        return final_custom_headers

    def get_pretty_headers(
        self,
        headers=None,
        markers=True,
        legend=True,
        reqd_only=False,
        reqd_spec=None,
        all_reqd=True,
    ):
        """Generate a string of header names, with appended asterisks(*) if required, up-caret(^) if 1 of a group
        required, and a message about the required annotations.

        Args:
            headers (namedtuple of TableHeaders) [self.get_headers()]: Header names by header key
            markers (boolean) [True]: Whether required headers should have an appended asterisk.  Note, this does not
                apply to the 1 of any markers (^).  Note that setting markers to False turns off the legend as well
                (effectively).
            legend (boolean) [True]: Whether to append a legend.  Note, a legend will always be included if there are
                any groups of headers where 1 of the group is required (^).  While it may be easy to infer an asterisk
                to mean required, an ^ to mean any of a group is required is assumed to not be intuitive.
            reqd_only (boolean) [False]: Whether to include optional headers in the result
            reqd_spec (N-dimensional list of strings) [self.header_keys_to_names(self.DataRequiredHeaders, headers)]:
                Required header *names* where each dimension alternates between all required and 1 of any required.
                Note that the default uses the current self.headers.  If the names differ from the current defaults, you
                must supply headers.
            all_reqd (boolean) [True]: Whether the first dimension of reqd_spec is all required or not (1 of any
                required)

        Raises:
            Nothing

        Returns:
            pretty_headers (string)
        """
        if headers is None:
            headers = self.get_headers()

        if reqd_spec is None:
            reqd = self.header_keys_to_names(self.DataRequiredHeaders, headers)
        else:
            reqd = reqd_spec

        flat_reqd = self.flatten_ndim_strings(reqd)
        optionals = list(set(headers._asdict().values()) - set(flat_reqd))
        delim = ", "

        pretty_headers = self._get_pretty_headers_helper(
            reqd, delim, _anded=all_reqd, markers=markers
        )

        if not reqd_only:
            if pretty_headers != "" and len(optionals) > 0:
                pretty_headers += delim
            pretty_headers += delim.join(optionals)

        if legend and ("*" in pretty_headers or "^" in pretty_headers):
            if pretty_headers != "":
                pretty_headers += " "
            pretty_headers += "("
            if len(flat_reqd) == 0:
                if reqd_only:
                    pretty_headers += "None Required"
                else:
                    pretty_headers += "All Optional"
            else:
                if "*" in pretty_headers:
                    pretty_headers += "* = Required"
                    if "^" in pretty_headers:
                        pretty_headers += ", "
                if "^" in pretty_headers:
                    pretty_headers += "^ = Any Required"
            pretty_headers += ")"
        elif "^" in pretty_headers:
            # We will still include the "1 of any" legend if that case exists
            if "^" in pretty_headers:
                pretty_headers += " (^ = Any Required)"

        return pretty_headers

    def _get_pretty_headers_helper(
        self, reqd_headers, delim=", ", _first_dim=True, _anded=True, markers=True
    ):
        """Generate a string of header names, with appended asterisks(*) if required and an up-caret(^) if 1 of a group
        required.

        Args:
            reqd_headers (N-dimensional list of strings): Required header names
            delim (string) [, ]: Delimiter
            _first_dim (boolean) [True]: Private.  Whether this is the first dimension or not.
            _anded (boolean) [True]: Private.  Whether all or 1 of the header items in this dimension are required
            markers (boolean) [True]: Whether to include "all required" annotations appended to items (*) and
                parenthases around the outer group.

        Raises:
            Nothing

        Returns:
            pretty_headers (string)
        """
        pretty_headers = ""

        for hdr_item in reqd_headers:
            if pretty_headers != "":
                pretty_headers += delim

            if type(hdr_item) == list:
                pretty_headers += "("
                pretty_headers += self._get_pretty_headers_helper(
                    hdr_item,
                    delim=delim,
                    _first_dim=False,
                    _anded=not _anded,
                    markers=markers,
                )
                pretty_headers += ")"

                if markers:
                    # The sub-group is the opposite of "anded"
                    pretty_headers += "^" if _anded else "*"
            else:
                pretty_headers += hdr_item

                # Only append required labels to individual items on the first dimension
                if _first_dim and (_anded or len(reqd_headers) == 1) and markers:
                    pretty_headers += "*"

        if _first_dim and not _anded and len(reqd_headers) != 1 and markers:
            pretty_headers = f"({pretty_headers})^"

        return pretty_headers

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
        for hk in list(cls.DataHeaders._asdict().keys()):
            keys.append(hk)

        return keys

    def get_defaults(self):
        """Returns the current default values.

        Args:
            None

        Raises:
            Nothing

        Returns:
            defaults (Optional[namedtuple of DataTableHeaders])
        """
        if hasattr(self, "defaults") and self.defaults is not None:
            return self.defaults
        return self.DataDefaultValues

    def set_defaults(self, custom_defaults=None):
        """Updates an instance's default values, taking derived class defaults and user defaults into account.

        This method sets the following instance attributes because they involve header names (not header keys):

        - defaults (DataTableHeaders namedtuple of objects): Customized default values by header key.
        - defaults_by_header (dict): Default values by header name.

        Args:
            custom_defaults (dict): Default values by header key

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.defaults = self._merge_defaults(custom_defaults)

        # Now create a defaults by header name dict (for use by get_row_val)
        self.defaults_by_header = self.get_defaults_dict_by_header_name()

    def _merge_defaults(self, custom_defaults):
        """Merges base class, derived class, and user defaults hierarchically, returning the merged result.

        This method also sets the following instance attributes because they involve header names (not header keys):

        - defaults_by_header

        Args:
            custom_defaults (dict): Default values by header key

        Raises:
            AggregatedErrors
                TypeError
                ValueError

        Returns:
            defaults (Optional[namedtuple of DataTableHeaders])
        """
        if hasattr(self, "defaults") and self.defaults is not None:
            final_defaults = self.defaults
        else:
            final_defaults = self.DataDefaultValues

        extras = []
        if custom_defaults is not None:
            if type(custom_defaults) != dict:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise self.aggregated_errors_object.buffer_error(
                    TypeError(
                        f"Invalid argument: [custom_default_data] dict required, {type(custom_defaults)} supplied."
                    )
                )

            new_dv_dict = final_defaults._asdict()
            for hk in custom_defaults.keys():
                if hk in new_dv_dict.keys():
                    new_dv_dict[hk] = custom_defaults[hk]
                else:
                    extras.append(hk)

            # Raise programming errors immediately
            if len(extras) > 0:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise self.aggregated_errors_object.buffer_error(
                    ValueError(
                        f"Unexpected default keys: {extras} in custom_defaults argument."
                    )
                )

            final_defaults = final_defaults._replace(**new_dv_dict)

        # If user defaults are defined, overwrite anything previously set with them
        extras = []
        tmp_user_defaults = self.get_user_defaults()
        if tmp_user_defaults is not None:
            user_defaults = self.header_name_to_key(tmp_user_defaults)
            new_ud_dict = final_defaults._asdict()
            # To support incomplete headers dicts
            for hk in user_defaults.keys():
                if hk in new_ud_dict.keys():
                    new_ud_dict[hk] = user_defaults[hk]
                else:
                    extras.append(hk)

            # Raise programming errors immediately
            if len(extras) > 0:
                # We create an aggregated errors object in class methods because we may not have an instance with one
                raise self.aggregated_errors_object.buffer_error(
                    ValueError(
                        f"Unexpected default keys: {extras} in user defaults.  Expected: {list(new_ud_dict.keys())}"
                    )
                )

            final_defaults = final_defaults._replace(**new_ud_dict)

        return final_defaults

    @classmethod
    def check_class_attributes(cls):
        """Checks that the class and instance attributes are properly defined and initialize optional ones.

        Checks the type of:
            Models (class attribute, list of Model classes): Must contain at least 1 model class
            DataHeaders (class attribute, namedtuple of DataTableHeaders of strings)
            DataRequiredHeaders (class attribute, N-dimensional list of header keys.  See get_missing_headers.)
            DataRequiredValues (class attribute, N-dimensional list of header keys.  See get_missing_headers.)
            DataUniqueColumnConstraints (class attribute, list of lists of strings): Sets of unique column combinations
            FieldToDataHeaderKey (class attribute, dict): Header keys by field name
            DataColumnTypes (class attribute, Optional[dict]): Column value types by header key
            DataDefaultValues (Optional[namedtuple of DataTableHeaders of objects]): Column default values by header key

        Fills in default None values for header keys in DataDefaultValues.

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
            if not cls.isnamedtupletype(cls.DataTableHeaders):
                typeerrs.append(
                    f"attribute [{cls.__name__}.DataTableHeaders] (namedtuple) type required, "
                    f"{type(cls.DataTableHeaders)} set"
                )

            if not cls.isnamedtupletype(cls.DefaultsTableHeaders):
                typeerrs.append(
                    f"attribute [{cls.__name__}.DefaultsTableHeaders] (namedtuple) type required, "
                    f"{type(cls.DefaultsTableHeaders)} set"
                )

            if type(cls.DataSheetName) != str:
                typeerrs.append(
                    f"attribute [{cls.__name__}.DataSheetName] str required, {type(cls.DataSheetName)} set"
                )

            if type(cls.DefaultsSheetName) != str:
                typeerrs.append(
                    f"attribute [{cls.__name__}.DefaultsSheetName] str required, {type(cls.DefaultsSheetName)} set"
                )

            if not cls.isnamedtuple(cls.DataHeaders):
                typeerrs.append(
                    f"attribute [{cls.__name__}.DataHeaders] namedtuple required, {type(cls.DataHeaders)} set"
                )

            invalid_types = cls.get_invalid_types_from_ndim_strings(
                cls.DataRequiredHeaders
            )
            if len(invalid_types) > 0:
                typeerrs.append(
                    f"attribute [{cls.__name__}.DataRequiredHeaders] N-dimensional list of strings required, but "
                    f"contains {invalid_types}"
                )

            invalid_types = cls.get_invalid_types_from_ndim_strings(
                cls.DataRequiredValues
            )
            if len(invalid_types) > 0:
                typeerrs.append(
                    f"attribute [{cls.__name__}.DataRequiredValues] N-dimensional list of strings required, but "
                    f"contains {invalid_types}"
                )

            if type(cls.DataUniqueColumnConstraints) != list:
                typeerrs.append(
                    f"attribute [{cls.__name__}.DataUniqueColumnConstraints] list required, "
                    f"{type(cls.DataUniqueColumnConstraints)} set"
                )

            if type(cls.FieldToDataHeaderKey) != dict:
                typeerrs.append(
                    f"attribute [{cls.__name__}.FieldToDataHeaderKey] dict required, {type(cls.FieldToDataHeaderKey)} "
                    "set"
                )

            valid_types = False
            # DataColumnTypes is optional.  Allow to be left as None.
            if cls.DataColumnTypes is not None:
                valid_types = True
                if type(cls.DataColumnTypes) != dict:
                    typeerrs.append(
                        f"attribute [{cls.__name__}.DataColumnTypes] dict required, {type(cls.DataColumnTypes)} set"
                    )
                    valid_types = False
                elif cls.DataHeaders is not None:
                    # If the DataHeaders was correctly set, check further to validate the dict
                    for hk in cls.DataColumnTypes.keys():
                        if hk in cls.DataHeaders._asdict().keys():
                            if type(cls.DataColumnTypes[hk]) != type:
                                typeerrs.append(
                                    f"dict attribute [{cls.__name__}.DataColumnTypes] must have values that are types, "
                                    f"but key [{hk}] has {type(cls.DataColumnTypes[hk])}"
                                )
                                valid_types = False
                        else:
                            typeerrs.append(
                                f"dict attribute [{cls.__name__}.DataColumnTypes] has an invalid key: [{hk}].  Keys "
                                f"must be one of {list(cls.DataHeaders._asdict().keys())}"
                            )
                            valid_types = False

            if cls.DataDefaultValues is None:
                # DataDefaultValues is optional (not often used/needed). Set all to None using DataHeaders
                if cls.DataHeaders is not None:
                    # Initialize the same "keys" as the DataHeaders, then set all values to None
                    dv_dict = cls.DataHeaders._asdict()
                    for hk in dv_dict.keys():
                        dv_dict[hk] = None
                    cls.DataDefaultValues = cls.DataHeaders._replace(**dv_dict)
            elif not cls.isnamedtuple(cls.DataDefaultValues):
                typeerrs.append(
                    f"attribute [{cls.__name__}.DataDefaultValues] namedtuple required, {type(cls.DataDefaultValues)} "
                    "set"
                )
            elif valid_types:
                # Check the types of the default values
                for hk in cls.DataDefaultValues._asdict().keys():
                    if hk in cls.DataColumnTypes.keys():
                        dv = getattr(cls.DataDefaultValues, hk)
                        dv_type = type(dv)
                        if (
                            cls.DataColumnTypes[hk] is not None
                            and dv is not None
                            and dv_type != cls.DataColumnTypes[hk]
                        ):
                            typeerrs.append(
                                f"attribute [{cls.__name__}.DataDefaultValues.{hk}] {cls.DataColumnTypes[hk].__name__} "
                                f"required (according to {cls.__name__}.DataColumnTypes['{hk}']), but "
                                f"{dv_type.__name__} set"
                            )

            if not cls.isnamedtuple(cls.DefaultsHeaders):
                typeerrs.append(
                    f"attribute [{cls.__name__}.DefaultsHeaders] namedtuple required, "
                    f"{type(cls.DefaultsHeaders)} set"
                )

            if cls.Models is None or len(cls.Models) == 0:
                # Raise programming-related errors immediately
                typeerrs.append("Models is required to have at least 1 Model class")
            else:
                mdlerrs = []
                for mdl in cls.Models:
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

    def initialize_metadata(self):
        """Initializes metadata.

        Note, when this method is called, headers and defaults are composed using available user-supplied values
        (e.g. a yaml-file defining custom headers and a a defaults dataframe that came from a parsed defaults file/excel
        sheet).  The derived class defines custom headers/defaults using the class attributes, but they can also change
        these values dynamically using set_headers and set_defaults after an object is instantiated (which calls this
        method).

        Metadata initialized:
        - record_counts (dict of dicts of ints): Created, existed, and errored counts by model.
        - defaults_current_type (str): Set the self.sheet (before sheet is potentially set to None).
        - sheet (str): Name of the data sheet in an excel file (changes to None if not excel).
        - defaults_sheet (str): Name of the defaults sheet in an excel file (changes to None if not excel).
        - record_counts (dict): Record created, existed, and errored counts by model name.  All set to 0.
        - Note, other attributes are initialized in set_headers and set_defaults

        Args:
            headers (DataTableHeaders namedtuple of strings): Customized header names by header key.
            defaults (DataTableHeaders namedtuple of objects): Customized default values by header key.

        Raises:
            AggregatedErrors
                TypeError

        Returns:
            Nothing
        """
        typeerrs = []

        self.defaults_current_type = self.sheet
        if is_excel(self.file):
            if self.defaults_df is not None:
                self.defaults_file = self.file
            if self.sheet is None:
                self.sheet = self.DataSheetName
        else:
            self.sheet = None
            self.defaults_sheet = None

        try:
            self.set_headers()
        except TypeError as te:
            typeerrs.append(str(te))
            # TODO: Buffering at the caught exception would be better, because it provides a better trace.  Refactor
            # spots where I do this.
            # self.aggregated_errors_object.buffer_error(te)

        try:
            self.set_defaults()
        except TypeError as te:
            typeerrs.append(str(te))
            # self.aggregated_errors_object.buffer_error(te)

        if len(typeerrs) > 0:
            nlt = "\n\t"
            msg = f"Invalid arguments:\n\t{nlt.join(typeerrs)}"
            self.aggregated_errors_object.buffer_error(TypeError(msg))
            if self.aggregated_errors_object.should_raise():
                raise self.aggregated_errors_object

        self.record_counts = defaultdict(lambda: defaultdict(int))
        for mdl in self.Models:
            self.record_counts[mdl.__name__]["created"] = 0
            self.record_counts[mdl.__name__]["existed"] = 0
            self.record_counts[mdl.__name__]["skipped"] = 0
            self.record_counts[mdl.__name__]["errored"] = 0

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

    @staticmethod
    def isnamedtupletype(obj) -> bool:
        """Determined if obj is a namedtuple type (i.e. what namedtuples are made from).

        Based on: https://stackoverflow.com/a/62692640/2057516

        Args:
            obj (object): Any object.

        Raises:
            Nothing

        Returns:
            boolean
        """
        return type(obj) == type and hasattr(obj, "_asdict") and hasattr(obj, "_fields")

    def get_column_types(self):
        """Returns a dict of column types by header name (not header key).

        Args:
            None

        Raises:
            Nothing

        Returns:
            dtypes (dict): Types by header name (instead of by header key)
        """
        if self.DataColumnTypes is None:
            return None

        if hasattr(self, "headers") and self.headers is not None:
            headers = self.headers
        else:
            headers = self.DataHeaders

        dtypes = {}
        for key in self.DataColumnTypes.keys():
            hdr = getattr(headers, key)
            dtypes[hdr] = self.DataColumnTypes[key]

        return dtypes

    @classmethod
    def header_key_to_name(cls, indict, headers=None):
        """Returns the supplied indict, but its keys are changed from header key to header name.

        This class method is used to obtain a dtypes dict to be able to supply to read_from_file.  You can supply it
        "headers", which is a namedtuple that can be obtained from cls.get_headers.

        Args:
            indict (dict of objects): Any objects by header key
            headers (DataTableHeaders namedtuple of strings): Customized header names by header key.

        Raises:
            AggregatedErrors
                TypeError

        Returns:
            outdict (dict): objects by header name (instead of by header key)
        """
        cls.check_class_attributes()

        if indict is None:
            return None

        if headers is None:
            headers = cls.DataHeaders
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

    def header_name_to_key(self, indict):
        """Returns the supplied indict, but its keys are changed from header name to header key.

        This method is used to convert user defaults by header name to by header key (in order to set self.defaults).

        Args:
            indict (dict of objects): Any objects by header key

        Exceptions:
            Raises:
                Nothing
            Buffers:
                KeyError

        Returns:
            outdict (dict): objects by header name (instead of by header key)
        """
        if indict is None:
            return None

        outdict = {}
        for hn, dv in indict.items():
            if hn not in self.reverse_headers.keys():
                self.aggregated_errors_object.buffer_error(
                    KeyError(
                        f"Header [{hn}] not in reverse headers: {self.reverse_headers}"
                    )
                )
                outdict[hn] = dv
                continue
            outdict[self.reverse_headers[hn]] = dv

        return outdict

    def check_dataframe_headers(self, reading_defaults=False):
        """Error-checks the headers in the dataframe.

        Args:
            reading_defaults (boolean) [False]: Whether defaults data is being read or not

        Exceptions:
            Raises:
                AggregatedErrors
                    RequiredHeadersError
                ValueError
            Buffers:
                UnknownHeadersError

        Returns:
            Nothing
        """
        if reading_defaults:
            df = self.defaults_df
            all_headers = list(self.DefaultsHeaders._asdict().values())
            file = self.defaults_file
            sheet = self.defaults_sheet
            reqd_headers = all_headers
        else:
            df = self.df
            all_headers = self.all_headers
            file = self.file
            sheet = self.sheet
            reqd_headers = self.reqd_headers

        if df is None:
            if not self.aggregated_errors_object.exception_type_exists(NoLoadData):
                self.aggregated_errors_object.buffer_warning(
                    NoLoadData("No dataframe [df] provided.  Nothing to load.")
                )
            return

        missing_headers = None
        if reqd_headers is not None:
            missing_headers, all_reqd = self.get_missing_headers(
                df.columns, reqd_headers=reqd_headers
            )
            if missing_headers is not None:
                pretty_missing_headers = self.get_pretty_headers(
                    reqd_spec=missing_headers,
                    all_reqd=all_reqd,
                    reqd_only=True,
                    legend=False,
                    markers=False,
                )
                self.aggregated_errors_object.buffer_error(
                    RequiredHeadersError(pretty_missing_headers, file=file, sheet=sheet)
                )

        if all_headers is not None:
            unknown_headers = []
            for file_header in df.columns:
                if file_header not in all_headers:
                    unknown_headers.append(file_header)
            if len(unknown_headers) > 0:
                if not reading_defaults or missing_headers is not None:
                    self.aggregated_errors_object.buffer_error(
                        UnknownHeadersError(unknown_headers, file=file, sheet=sheet)
                    )

        if missing_headers is not None:
            raise self.aggregated_errors_object

    def get_missing_headers(
        self, supd_headers, reqd_headers=None, _anded=True, _first=True
    ):
        """Given a 1-dimensional list of supplied header names and an N-dimensional list of required header names, get
        the missing required headers.

        Note that the N-dimensional list of required headers alternates between all required and any required.  The
        first dimension is "all" required.

        reqd_headers input examples:

        - [a, b, c] - a, b, and c are all required
        - [a, [b, c]] - a is required and either b or c is required
        - [[a, b], [b, c]] - a or b is required, and b or c is required
        - [[[a, b], c], d] - a and b, or c is required, and d is required

        Usage example:

        supd_headers = ["a", "c"]
        reqd_headers = ["c", [["a", ["d", "c"]], ["a", "e"]]]  # c and ((a and (d or c)) or (a and e)) are required
        return = (None, True)  # All header requirements are satisfied

        supd_headers = ["a", "c"]
        reqd_headers = ["c", [["a", "d"], ["a", "e"]]]  # c and either a and d, or a and e are required
        return = (['d', 'e'], False)  # Either d or e are required, but missing

        supd_headers = ["a", "c"]
        reqd_headers = ["c", [["a", ["d", "f"]], ["a", "e"]]]  # c and either (a and (d or f)) or (a and e) are required
        return = (['d', 'f', 'e'], False)  # Either d, f, or e is required, but missing

        Args:
            supd_headers (list of strings): Supplied/present header names.
            reqd_headers (list of strings and lists): N-dimensional list of required header names.  See above.
            _anded (boolean) [True]: Whether the outer reqd_headers dimension items are all-required (and'ed) or
                any-required (or'ed).  Private argument.  Used in recursion.  Do not supply.
            _first (boolean) [True]: Whether this is the first or a recursive call or not.  Private argument.  Used in
                recursion.  Do not supply.

        Exceptions:
            None

        Returns:
            missing (list of strings and lists): an N-dimensional list of missing headers where every dimension deeper
                alternates between all required and 1 of any required
            all (boolean): Whether the first dimension is all required or not
        """
        if _first and reqd_headers is None:
            reqd_headers = self.reqd_headers
        if reqd_headers is None:
            raise ValueError("reqd_headers cannot be None.")

        missing = []
        sublist_anded = not _anded
        for rh in reqd_headers:
            sublist_anded = not _anded
            missing_header_item = None

            if type(rh) == list:
                missing_header_item, sublist_anded = self.get_missing_headers(
                    supd_headers, rh, _anded=not _anded, _first=False
                )
            elif rh not in supd_headers:
                missing_header_item = rh

            if missing_header_item is None:
                # If None, it means that nothing was missing for this item.
                # So if the outer group is an "or" group, it means we can immediately return None without continuing to
                # look at the rest of the list, as it is satisfied.
                if not _anded:
                    return None, _anded
            elif type(missing_header_item) == list and sublist_anded == _anded:
                # If the sublist and outer list are both "anded" or both "ored", merge them
                missing.extend(missing_header_item)
            else:
                missing.append(missing_header_item)

        if len(missing) == 0:
            return None, _anded

        if len(missing) == 1 and (not _first or type(missing[0]) == list):
            # Make sure we always return a list (or None) to the original (not recursive, i.e. not _first) call, but
            # skip outer lists with only 1 list member and return that member (and it's _anded state)
            return missing[0], sublist_anded

        return missing, _anded

    def header_keys_to_names(self, ndim_header_keys, headers=None):
        """Given an N-dimensional list of header keys, return the same list with the header keys replaced with names.

        Args:
            ndim_header_keys (list of strings and lists): N-dimensional list of header keys.  See get_missing_headers.
            headers (Optional[namedtuple of TableHeaders]) [self.get_headers()]: header names by key

        Exceptions:
            None

        Returns
            ndim_header_names (list of strings and lists): N-dimensional list of header keys.  See get_missing_headers.
        """
        if headers is None:
            headers = self.get_headers()
        ndim_header_names = []
        for hk_item in ndim_header_keys:
            if type(hk_item) == list:
                ndim_header_names.append(self.header_keys_to_names(hk_item, headers))
            else:
                ndim_header_names.append(getattr(headers, hk_item))
        return ndim_header_names

    @classmethod
    def get_invalid_types_from_ndim_strings(cls, ndim_strings):
        """Given an N-dimensional list of strings, return a list of any type names that are not list or str.

        Args:
            ndim_strings (list of strings and lists)

        Exceptions:
            None

        Returns
            invalid_types (list of strings): Names of all invalid types contained in any dimension od ndim_strings
        """
        invalid_types = []
        if ndim_strings is None:
            invalid_types.append(type(ndim_strings).__name__)
            return invalid_types
        for item in ndim_strings:
            if type(item) == list:
                invalid_types.extend(cls.get_invalid_types_from_ndim_strings(item))
            elif type(item) != str and type(item).__name__ not in invalid_types:
                invalid_types.append(type(item).__name__)
        return invalid_types

    @classmethod
    def flatten_ndim_strings(cls, ndim_strings):
        """Given an N-dimensional list of strings, return a unique flat list of all contained items.

        Example:
            input = [a, [b, c], [[c, d], [a, e]]]
            output = [a, b, c, d, e]

        Args:
            ndim_strings (list of strings and lists)

        Exceptions:
            None

        Returns
            flat_uniques (list of strings): a unique flat list of all items in ndim_strings
        """
        flat_uniques = []
        for item in ndim_strings:
            new_items = []
            if type(item) == list:
                new_items.extend(cls.flatten_ndim_strings(item))
            else:
                new_items.append(item)
            for ni in new_items:
                if ni not in flat_uniques:
                    flat_uniques.append(ni)
        return flat_uniques

    def check_unique_constraints(self, df=None):
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
        if self.unique_constraints is None:
            return

        if df is None and self.df is not None:
            df = self.df

        if df is None:
            if not self.aggregated_errors_object.exception_type_exists(NoLoadData):
                self.aggregated_errors_object.buffer_warning(
                    NoLoadData("No dataframe [df] provided.  Nothing to load.")
                )
            return

        for unique_combo in self.unique_constraints:
            # A single field unique requirements is much cleaner to display than unique combos, so handle differently
            if len(unique_combo) == 1:
                dupes, row_idxs = self.get_one_column_dupes(df, unique_combo[0])
            else:
                dupes, row_idxs = get_column_dupes(df, unique_combo)
            self.add_skip_row_index(index_list=row_idxs)
            if len(dupes) > 0:
                self.aggregated_errors_object.buffer_error(
                    DuplicateValues(
                        dupes, unique_combo, sheet=self.sheet, file=self.file
                    )
                )

    def check_dataframe_values(self, reading_defaults=False):
        """Preprocesses the dataframe to ensure that required values are satisfied.

        If there are missing required values, a RequiredColumnValue exception is buffered and the row is marked to be
        skipped.

        Args:
            df (pandas dataframe) [self.df]

        Exceptions:
            Raises:
                None
            Buffers:
                RequiredColumnValue

        Returns:
            None
        """
        if reading_defaults:
            df = self.defaults_df
            file = self.defaults_file
            sheet = self.defaults_sheet
            headers = self.DefaultsHeaders
            reqd_values = [getattr(headers, k) for k in self.DefaultsRequiredValues]
        else:
            df = self.df
            file = self.file
            sheet = self.sheet
            headers = self.headers
            reqd_values = self.reqd_values

        if df is None:
            if not self.aggregated_errors_object.exception_type_exists(NoLoadData):
                self.aggregated_errors_object.buffer_warning(
                    NoLoadData("No dataframe [df] provided.  Nothing to load.")
                )
            return

        # Do we need to do anything?
        if reqd_values is None or len(reqd_values) == 0:
            return

        # Is there data to check?
        if df is None:
            if not self.aggregated_errors_object.exception_type_exists(NoLoadData):
                self.aggregated_errors_object.buffer_warning(
                    NoLoadData("No dataframe [df] provided.  Nothing to load.")
                )
            return

        # Are we in the proper initialized state?
        if hasattr(self, "row_index") and self.row_index is not None:
            raise ProgrammingError(
                "check_dataframe_values must not be called during or after the dataframe has been processed.  This has "
                "been inferred by the fact that self.row_index has a non-null value.  Call self.set_row_index(None) "
                "before check_dataframe_values is called (e.g. from the load_data wrapper.)"
            )

        for _, row in df.iterrows():
            missing_reqd_vals, all_reqd = self.get_missing_values(
                row, reqd_values=reqd_values, headers=headers
            )

            if missing_reqd_vals is not None:
                pretty_missing_reqd_vals = self.get_pretty_headers(
                    reqd_spec=missing_reqd_vals,
                    all_reqd=all_reqd,
                    reqd_only=True,
                    legend=False,
                    markers=False,
                )
                self.aggregated_errors_object.buffer_error(
                    RequiredColumnValue(
                        pretty_missing_reqd_vals,
                        file=file,
                        sheet=sheet,
                        rownum=row.name + 2,
                    )
                )
                if not reading_defaults:
                    self.add_skip_row_index(row.name)

        # Reset the row index (which was altered by get_row_val inside get_missing_values)
        self.set_row_index(None)

    def get_missing_values(
        self,
        row,
        reqd_values=None,
        headers=None,
        reading_defaults=False,
        _anded=True,
        _first=True,
    ):
        """Given a row of pandas dataframe data and an N-dimensional list of required values (by header name), get
        the header names of the missing required values.

        Note that the N-dimensional list of required values alternates between all required and any required.  The
        first dimension is "all" required by default (which is controlled by the _anded private argument).

        This ends up converting the row data into a 1-dimensional list of header names that do not have a value (either
        in the row dataframe or via the defaults mechanism in get_row_val).  That list is used to call
        get_missing_headers to do the work.

        Args:
            row (pandas dataframe row)
            reqd_values (list of strings and lists): N-dimensional list of required values by header name.
            headers (namedtuple of TableHeaders): Header names by header keys.
            reading_defaults (boolean): Whether the defaults sheet is being read.
            _anded (boolean) [True]: Whether the outer reqd_values dimension items are all-required (and'ed) or
                any-required (or'ed).  Private argument.  Used in recursion.  Do not supply.
            _first (boolean) [True]: Whether this is the first or a recursive call or not.  Private argument.  Used in
                recursion.  Do not supply.

        Exceptions:
            None

        Returns:
            missing (list of strings and lists): an N-dimensional list of headers that have missing required values on
                the row where every dimension deeper alternates between all required and 1 of any required
            all (boolean): Whether the first dimension is all required or not
        """
        if _first and reqd_values is None:
            reqd_values = self.reqd_values
        if reqd_values is None:
            raise ValueError("reqd_values cannot be None.")
        if headers is None:
            headers = self.headers

        # Collect all the values from the row.  We could do this using pandas' .to_dict() method, but we want to
        # take advantage of self.defaults and the file context reporting metadata, so...
        headers_of_supplied_values = []
        for header in headers._asdict().values():
            val = self.get_row_val(row, header, reading_defaults=reading_defaults)
            if val is not None:
                headers_of_supplied_values.append(header)

        # This is exactly the same as get_missing_headers (because you refer to missing values by their column header)
        # So it requires the header names of supplied (non-null) values on a single row
        return self.get_missing_headers(
            headers_of_supplied_values,
            reqd_headers=reqd_values,
            _anded=_anded,
            _first=_first,
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
            if not hasattr(self, "row_index") and self.row_index is None:
                # Raise programming errors (data errors are buffered)
                raise ValueError("Either an index or index_list argument is required.")
            else:
                index = self.row_index

        if index is not None and index not in self.skip_row_indexes:
            self.skip_row_indexes.append(index)

        if index_list is not None:
            self.skip_row_indexes = list(
                set(self.skip_row_indexes).union(set(index_list))
            )

    def get_skip_row_indexes(self):
        """Returns skip_row_indexes.

        Args:
            None

        Raises:
            Nothing

        Returns:
            skip_row_indexes (list of integers)
        """
        return self.skip_row_indexes

    def get_row_val(self, row, header, strip=True, reading_defaults=False):
        """Returns value from the row (presumably from df) and column (identified by header).

        Converts empty strings and "nan"s to None.  Strips leading/trailing spaces.

        Args:
            row (row of a dataframe): Row of data.
            header (str): Column header name.
            strip (boolean) [True]: Whether to strip leading and trailing spaces.
            reading_defaults (boolean) [False]: Whether defaults data is currently being read.  Only 2 different files
                or sheets are supported, the ones for the data being loaded and the defaults.

        Raises:
            ValueError
            RequiredColumnValue

        Returns:
            val (object): Data from the row at the column (header)
        """
        if not reading_defaults:
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
        elif (
            not reading_defaults
            and self.all_headers is not None
            and header not in self.all_headers
        ):
            # Missing headers are addressed way before this. If we get here, it's a programming issue, so raise instead
            # of buffer
            raise ValueError(
                f"Invalid data header [{header}] supplied to get_row_val.  Must be one of: {self.all_headers}."
            )
        elif reading_defaults and header not in self.DefaultsHeaders._asdict().values():
            # Missing headers are addressed way before this. If we get here, it's a programming issue, so raise instead
            # of buffer
            raise ValueError(
                f"Invalid data header [{header}] supplied to get_row_val while processing defaults.  Must be one of: "
                f"{list(self.DefaultsHeaders._asdict().values())}."
            )

        # If val is None
        if val is None:
            # Fill in a default value
            val = self.defaults_by_header.get(header, None)

        return val

    def get_user_defaults(self):
        """Retrieves user defaults from a dataframe.

        The dataframe contains defaults for different types.  Only the defaults relating to the currently loaded data
        are returned.

        Args:
            None

        Exceptions:
            Raises:
                Nothing
            Buffers:
                ExcelSheetsNotFound
                InvalidHeaderCrossReferenceError
                TypeError

        Returns:
            user_defaults (dict): Default values by header name
        """
        if self.defaults_df is None:
            return None

        # Return value
        user_defaults = {}

        self.check_dataframe_headers(reading_defaults=True)
        self.check_dataframe_values(reading_defaults=True)

        # Save the headers from the infile data (not the defaults data)
        infile_headers = self.df.columns if self.df is not None else None

        # Get all the sheet names in the current (assumed: excel) file
        all_sheet_names = None
        apply_types = True
        if self.file is not None and is_excel(self.file):
            all_sheet_names = get_sheet_names(self.file)
            # Excel automatically detects data types in every cell
            apply_types = False

        # Get the column types by header name
        coltypes = self.get_column_types()

        # Error tracking
        unknown_sheets = defaultdict(list)
        unknown_headers = defaultdict(list)
        invalid_type_errs = []

        for _, row in self.defaults_df.iterrows():
            # Note, self.rownum is only for the infile data sheet, not this defaults sheet
            rownum = row.name + 2

            # Get the sheet from the row
            sheet_name = self.get_row_val(
                row, self.DefaultsHeaders.SHEET_NAME, reading_defaults=True
            )

            if sheet_name is None:
                # This would already have been caught in check_dataframe_values above.  Just skip
                continue

            # If the sheet name was not found in the file
            if (
                all_sheet_names is not None
                and sheet_name not in all_sheet_names
                and sheet_name not in unknown_sheets
            ):
                unknown_sheets[sheet_name].append(rownum)
                continue

            # Skip sheets that are not the target load_sheet
            # Note, self.sheet is None if self.file is not an excel file, but the sheet is always preserved in
            # self.defaults_current_type as a sort of identifier for the data type
            if (
                self.defaults_current_type is not None
                and sheet_name != self.defaults_current_type
            ):
                continue

            # Get the header from the row
            header_name = str(
                self.get_row_val(
                    row, self.DefaultsHeaders.COLUMN_NAME, reading_defaults=True
                )
            )

            if header_name is None:
                # This would already have been caught in check_dataframe_values above.  Just skip
                continue

            # If the header name from the defaults sheet is not an actual header on the load_sheet
            if infile_headers is not None and header_name not in infile_headers:
                unknown_headers[header_name].append(rownum)
                continue

            # Grab the default value
            default_val = self.get_row_val(
                row, self.DefaultsHeaders.DEFAULT_VALUE, reading_defaults=True
            )

            if default_val is None:
                # This would already have been caught in check_dataframe_values above.  Just skip
                continue

            if (
                coltypes is not None
                and header_name in coltypes.keys()
                and default_val is not None
                and coltypes[header_name] is not None
            ):
                if apply_types:
                    # This is necessary for non-excel file data.  This castes the default value to the type defined for
                    # the column this default value is a default for
                    default_val = coltypes[header_name](default_val)
                elif type(default_val) != coltypes[header_name]:
                    # Otherwise, the type can be controlled by the user in excel, so just log an error if it is wrong
                    invalid_type_errs.append(
                        f"Invalid default value on row {rownum}: [{default_val}] (of type "
                        f"{type(default_val).__name__}).  Should be [{coltypes[header_name].__name__}]."
                    )

            user_defaults[header_name] = default_val

        if len(unknown_sheets.keys()) > 0:
            prev_unk_sheets = self.aggregated_errors_object.get_exception_type(
                ExcelSheetsNotFound
            )
            if unknown_sheets not in prev_unk_sheets:
                self.aggregated_errors_object.buffer_error(
                    ExcelSheetsNotFound(
                        unknown_sheets,
                        all_sheet_names,
                        source_file=self.defaults_file,
                        source_column=self.DefaultsHeaders.SHEET_NAME,
                        source_sheet=self.defaults_sheet,
                    )
                )

        if len(unknown_headers.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                InvalidHeaderCrossReferenceError(
                    source_file=self.defaults_file,
                    source_sheet=self.defaults_sheet,
                    column=self.DefaultsHeaders.COLUMN_NAME,
                    unknown_headers=unknown_headers,
                    target_file=self.file,
                    target_sheet=self.sheet,
                    target_headers=infile_headers,
                )
            )

        if len(invalid_type_errs) > 0:
            loc = generate_file_location_string(
                column=self.DefaultsHeaders.DEFAULT_VALUE,
                sheet=self.defaults_sheet,
                file=self.defaults_file,
            )
            deets = "\n\t".join(invalid_type_errs)
            self.aggregated_errors_object.buffer_error(
                TypeError(
                    f"Invalid default values encountered in {loc} on the indicated rows:\n\t{deets}"
                )
            )

        return user_defaults

    def tableheaders_to_dict_by_header_name(self, intuple):
        """Convert the intuple (a DataTableHeaders namedtuple) into a dict by (custom) header name.

        Args:
            intuple (DataTableHeaders namedtuple): objects by header key

        Exceptions:
            Raises:
                Nothing
            Buffers:
                TypeError

        Returns:
            defdict (Optional[dict of objects]): objects by header name
        """
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

    @classmethod
    def _loader(cls):
        """Class method that returns a decorator function.

        Args:
            cls (TableLoader)

        Raises:
            Nothing

        Returns:
            load_decorator (function)
        """

        def load_decorator(fn):
            """Decorator method that applies a wrapper function to a method.

            Args:
                cls (TableLoader)

            Raises:
                Nothing

            Returns:
                apply_wrapper (function)
            """

            def load_wrapper(self, *args, **kwargs):
                """Wraps the load_data() method in the derived class.

                Checks the file data and handles atomic transactions, running modes, exceptions, and load stats.

                Args:
                    None

                Raises:
                    TBD by derived class

                Returns:
                    What's returned by the wrapped method
                """
                retval = None
                with transaction.atomic():
                    try:
                        self.check_dataframe_headers()
                        self.check_dataframe_values()
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

        return load_decorator

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
        """Returns a list of model classes.

        Args:
            None

        Raises:
            Nothing

        Returns:
            models (list of model classes)
        """
        cls.check_class_attributes()
        return cls.Models

    def created(self, model_name: Optional[str] = None, num=1):
        """Increments a created record count for a model.

        Args:
            model_name (Optional[str])

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.record_counts[self._get_model_name(model_name)]["created"] += num

    def existed(self, model_name: Optional[str] = None, num=1):
        """Increments an existed record count for a model.

        Args:
            model_name (Optional[str])

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.record_counts[self._get_model_name(model_name)]["existed"] += num

    def skipped(self, model_name: Optional[str] = None, num=1):
        """Increments a skipped (i.e. "unattempted) record count for a model.

        Args:
            model_name (Optional[str])

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.record_counts[self._get_model_name(model_name)]["skipped"] += num

    def errored(self, model_name: Optional[str] = None, num=1):
        """Increments an errored record count for a model.

        Note, this is not for all errors.  It only pertains to data-specific errors from the input file.

        Args:
            model_name (Optional[str])

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.record_counts[self._get_model_name(model_name)]["errored"] += num

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

        Exceptions:
            Raises:
                Nothing
            Buffers:
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

        Exceptions:
            Raises:
                Nothing
            Buffers:
                ValueError
                InfileDatabaseError
                ConflictingValueError
                RequiredValueError

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

        Raises:
            UnknownHeadersError

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
        """Returns a list of lists of names of fields involved in UniqueConstraints in a given model.
        Args:
            model (Model)
        Raises:
            Nothing
        Returns:
            uflds (List of model fields)
        """
        uflds = []
        if hasattr(model._meta, "constraints"):
            for constraint in model._meta.constraints:
                if type(constraint).__name__ == "UniqueConstraint":
                    uflds.append(constraint.fields)
        return uflds

    @classmethod
    def get_unique_fields(cls, model, fields=None):
        """Returns a list of non-auto-field names where unique is True.

        If fields (list of field names) is provided, the returned field names are limited to the list provided.

        Args:
            model (Model)
            fields (Optional[list of Model Fields])
        Raises:
            Nothing
        Returns:
            fields (list of strings): field names
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
        """Returns a list of non-auto-field names where choices is populated.

        If fields (list of field names) is provided, the returned field names are limited to the list provided.

        Args:
            model (Model)
            fields (Optional[list of Model Fields])
        Raises:
            Nothing
        Returns:
            fields (list of strings): field names
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
        """Retrieves all non-auto-fields from the supplied model and returns as a list of actual fields.
        Args:
            model (Model)
        Raises:
            Nothing
        Returns:
            uflds (List of model fields)
        """
        return [
            f for f in model._meta.get_fields() if f.get_internal_type() != "AutoField"
        ]

    @classmethod
    def field_in_fieldnames(cls, fld, fld_names):
        """Determines if a supplied model field is in a list of field names.

        Accessory function to get_unique_fields and get_enumerated_fields.  This only exists in order to avoid JSCPD
        errors.

        Args:
            fld (Model Field)
            fld_names (list of strings): field names
        Raises:
            Nothing
        Returns:
            boolean: Whether the fld is in fld_names
        """
        # Relation fields do not have "name" attributes.  Instead, they have "field_name" attributes.  The values of
        # both are the attributes of the model object that we are after (because they can be used in queries).  It is
        # assumed that a field is guaranteed to have one or the other.
        return (hasattr(fld, "name") and fld.name in fld_names) or (
            hasattr(fld, "field_name") and fld.field_name in fld_names
        )

    def get_dataframe_template(self, populate=False):
        """Generate a pandas dataframe either populated with all database records or not.  Note, only loader classes
        with a single model are supported.  Override this method to generate a dataframe with data from multiple models.

        Args:
            populate (boolean) [False]: Whether to add all of the database data to the dataframe.

        Exceptions:
            Exception

        Returns:
            A pandas dataframe with current headers as the column names and any model field that directly maps to a
            column pre-populated with the record field values.
        """
        display_headers = self.get_ordered_display_headers()
        out_dict = dict([(hdr, []) for hdr in display_headers])

        if populate is True:
            if len(self.Models) > 1:
                raise Exception(
                    f"get_dataframe_template does not currently support multiple models ({len(self.Models)} present: "
                    f"{self.Models}).  The derived class must override this method to add support."
                )
            for model_class in [
                mdl for mdl in self.Models if mdl.__name__ in self.FieldToHeader.keys()
            ]:
                qs = model_class.objects.all()
                for rec in qs:
                    for fld in get_model_fields(model_class):
                        if fld in self.FieldToHeader[model_class.__name__].keys():
                            header = self.FieldToHeader[model_class.__name__][fld]
                            out_dict[header].append(getattr(rec, fld))

                for hdr in display_headers:
                    if hdr not in out_dict.keys():
                        out_dict[hdr] = [None for _ in range(qs.count())]

        return pd.DataFrame.from_dict(out_dict)

    def get_ordered_display_headers(self, all=False):
        """This returns current header names in the order in which the headers were defined in DataTableHeaders, that do
        not have class-defined default values.

        The reason it only excludes class-defined defaults (e.g. header columns that do not have a non-None value
        defined in DataDefaultValues) is for consistency in the assortment of headers returned.  A user can define
        defaults for any column, but that shouldn't change the columns in the output display.

        The selection of the defaulted columns is arbitrary because it fits the current need.  If you want a different
        behavior, just override this method in the derived class and return, for example, self.all_headers, or filter
        them however you wish.  You can even define a custom order.

        Args:
            all (boolean) [False]: Whether to return all ordered current headers (or just those without class defaults)

        Exceptions:
            None

        Returns:
            header names (list of strings): Current header names (not keys) in the order in which they were defined in
                DataTableHeaders
        """
        if all is True:
            # This is to mitigate the unexpected case where all columns have default values
            return self.all_headers

        class_defaulted_headers = [
            hn
            for (hk, hn) in self.headers._asdict().items()
            if self.DataDefaultValues is None
            or getattr(self.DataDefaultValues, hk) is None
        ]

        return [hn for hn in self.all_headers if hn not in class_defaulted_headers]
