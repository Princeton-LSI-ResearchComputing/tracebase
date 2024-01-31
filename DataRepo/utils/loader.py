from abc import ABC, abstractmethod
import re
from collections import defaultdict
from typing import Optional

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Q

from DataRepo.models.utilities import (
    get_enumerated_fields,
    get_model_by_name,
    get_unique_constraint_fields,
    get_unique_fields,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    ConflictingValueErrors,
    DryRun,
    DuplicateValueErrors,
    DuplicateValues,
    InfileDatabaseError,
    RequiredColumnValue,
    RequiredColumnValues,
    RequiredHeadersError,
    RequiredValueError,
    RequiredValueErrors,
    UnknownHeadersError,
)
from DataRepo.utils.file_utils import get_column_dupes, get_one_column_dupes


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
        DefaultValues (Optional[dict]): Column default values by header key.  Auto-filled.

    Instance Attributes:
        headers (TableHeaders of strings): Customized header names by header key.
        defaults (TableHeaders of objects): Customized default values by header key.
        all_headers (list of strings): Customized header names.
        reqd_headers (TableHeaders of booleans): Required header booleans.
        FieldToHeader (dict of dicts of strings): Header names by model and field.
        unique_constraints (list of lists of strings): Header key combos whose columns must be unique.
        models (list of Models): List of model classes.
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
    def UniqueColumnConstraints(self):  # list of lists of header keys (e.g. the values in TableHeaders)
        pass

    @property
    @abstractmethod
    def FieldToHeaderKey(self):  # dict of model dicts of field names and header keys
        pass

    # DefaultValues is populated automatically (with Nones)
    DefaultValues = None  # namedtuple

    # ColumnTypes is optional unless read_from_file needs a dtype argument
    ColumnTypes = None  # dict of types by header key (converted to by-header-name in get_column_types)

    def __init__(
        self,
        df,
        headers=None,
        defaults=None,
        dry_run=False,
        defer_rollback=False,  # DO NOT USE MANUALLY - A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
        sheet=None,
        file=None,
        models=None,
    ):
        # Check class attribute validity
        self.check_class_attributes()

        # Apply the loader decorator to the load_data method in the derived class
        self.apply_loader_wrapper()

        # File data
        self.df = df

        # Running Modes
        self.dry_run = dry_run
        self.defer_rollback = defer_rollback

        # Error tracking
        self.skip_row_indexes = []
        self.aggregated_errors_object = AggregatedErrors()
        self.conflicting_value_errors = []
        self.required_value_errors = []

        # For error reporting
        self.file = file
        self.sheet = sheet

        # Metadata
        self.initialize_metadata(headers, defaults, models)

        # Load stats
        self.record_counts = defaultdict(lambda: defaultdict(int))
        if models is not None:
            for mdl in models:
                self.record_counts[mdl.__name__]["created"] = 0
                self.record_counts[mdl.__name__]["existed"] = 0
                self.record_counts[mdl.__name__]["errored"] = 0

    def apply_loader_wrapper(self):
        """This applies a decorator to the derived class's load_data method.

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
        bound = decorated_derived_class_method.__get__(self)
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

    @classmethod
    def get_headers(cls, custom_header_data=None):
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
        if type(custom_header_data) != dict:
            raise TypeError(
                f"Invalid argument: [custom_header_data] dict required, {type(custom_header_data)} supplied."
            )

        cls.check_class_attributes()

        headers = cls.DefaultHeaders
        extras = []
        if custom_header_data is not None:
            for hk in custom_header_data.keys():
                if hasattr(headers, hk):
                    setattr(headers, hk, custom_header_data[hk])
                else:
                    extras.append(hk)

            if len(extras) > 0:
                ValueError(f"Unexpected header keys: {extras}.")

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
    def get_defaults(cls, custom_default_data=None):
        """Returns file headers.

        Note, this method calls check_class_attributes to ensure the derived class is completely defined since that
        check is only otherwise called during object instantiation.

        Args:
            custom_default_data (dict): Default values by header key.  The dict may contain a subset of header keys.
                Missing header keys will fall back to the defaults defined in the class.

        Raises:
            ValueError if a header key is not in cls.DefaultValues

        Returns:
            headers (Optional[namedtuple of TableHeaders])
        """
        if type(custom_default_data) != dict:
            raise TypeError(
                f"Invalid argument: [custom_default_data] dict required, {type(custom_default_data)} supplied."
            )

        cls.check_class_attributes()

        defaults = cls.DefaultValues
        extras = []
        if custom_default_data is not None:
            for hk in custom_default_data.keys():
                if hasattr(defaults, hk):
                    setattr(defaults, hk, custom_default_data[hk])
                else:
                    extras.append(hk)

            if len(extras) > 0:
                ValueError(f"Unexpected default keys: {extras}.")

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
            DefaultValues (class attribute, Optional[dict]): Column default values by header key

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
        # Error check the derived class for required attributes
        typeerrs = []

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
                for hk in list(cls.DefaultHeaders._asdict().keys()):
                    setattr(cls.DefaultValues, hk, None)
        elif type(cls.DefaultValues) != dict:
            typeerrs.append(
                f"attribute [{cls.__name__}.DefaultValues] dict required, {type(cls.DefaultValues)} set"
            )

        # Immediately raise programming related errors
        if len(typeerrs) > 0:
            aes = AggregatedErrors()
            nlt = "\n\t"
            aes.buffer_error(TypeError(f"Invalid attributes:\n\t{nlt.join(typeerrs)}"))
            if aes.should_raise():
                raise aes

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

    def initialize_metadata(self, headers=None, defaults=None, models=None):
        """Initializes metadata.

        Metadata initialized:
            headers (TableHeaders namedtuple of strings): Customized header names by header key.
            defaults (TableHeaders namedtuple of objects): Customized default values by header key.
            all_headers (list of strings): Customized header names.
            reqd_headers (TableHeaders namedtuple of booleans): Required header booleans.
            FieldToHeader (dict of dicts of strings): Header names by model and field.
            unique_constraints (list of lists of strings): Header key combos whose columns must be unique.
            models (list of Models): List of model classes.

        Args:
            headers (TableHeaders namedtuple of strings): Customized header names by header key.
            defaults (TableHeaders namedtuple of objects): Customized default values by header key.
            models (list of Models): List of model classes.

        Raises:
            AggregatedErrors
                TypeError

        Returns:
            Nothing
        """
        typeerrs = []

        if headers is None:
            self.headers = self.DefaultHeaders
        elif not self.isnamedtuple(headers):
            # Immediately raise programming related errors
            typeerrs.append(f"Invalid headers. namedtuple required, {type(headers)} supplied")
        else:
            self.headers = headers

        if defaults is None:
            self.defaults = self.DefaultValues
        elif not self.isnamedtuple(defaults):
            # Immediately raise programming related errors
            typeerrs.append(f"Invalid defaults. namedtuple required, {type(defaults)} supplied")
        else:
            self.defaults = defaults

        self.models = models
        if models is None or len(models) == 0:
            # Raise programming-related errors immediately
            typeerrs.append("models is required to have at least 1 Model class")
        else:
            mdlerrs = []
            for mdl in models:
                try:
                    get_model_by_name(mdl.__name__)
                except Exception as e:
                    mdlerrs.append(f"{type(e).__name__}: {str(e)}")
            if len(mdlerrs) > 0:
                nltt = "\n\t\t"
                typeerrs.append(
                    "models must all be valid/accessible TraceBase models, but the following errors were encountered:\n"
                    f"\t\t{nltt.join(mdlerrs)}"
                )

        if len(typeerrs) > 0:
            nlt = "\n\t"
            msg = f"Invalid arguments:\n\t{nlt.join(typeerrs)}"
            self.aggregated_errors_object.buffer_error(TypeError(msg))
            if self.aggregated_errors_object.should_raise():
                raise self.aggregated_errors_object

        # Create a list of all header string values from a namedtuple of header key/value pairs
        self.all_headers = list(self.headers._asdict().values())

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

        # Create a dict of database field keys to header values, from a dict of field name keys and header keys
        self.FieldToHeader = {}
        for mdl in self.FieldToHeaderKey.keys():
            for fld, hk in self.FieldToHeaderKey[mdl].items():
                self.FieldToHeader[mdl][fld] = self.headers[hk]

        # Create a list lists of header string values whose combinations must be unique, from a list of lists of header
        # keys
        self.unique_constraints = []
        for header_list_combo in self.UniqueColumnConstraints:
            self.unique_constraints.append([])
            for header_key in header_list_combo:
                header_val = getattr(self.headers, header_key)
                self.unique_constraints[-1].append(header_val)

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
            isinstance(obj, tuple) and
            hasattr(obj, '_asdict') and
            hasattr(obj, '_fields')
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
            dtypes (dict): Types by header name
        """
        if cls.ColumnTypes is None:
            return None

        if headers is None:
            headers = cls.DefaultHeaders
        elif not cls.isnamedtuple(headers):
            # Immediately raise programming related errors
            raise TypeError(f"Invalid headers. namedtuple required, {type(headers)} supplied")

        dtypes = {}
        for key in cls.ColumnTypes.keys():
            hdr = getattr(headers, key)
            dtypes[hdr] = getattr(cls.ColumnTypes, key)

        return dtypes

    def check_headers(self):
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
                raise RequiredHeadersError(
                    missing_headers, file=self.file, sheet=self.sheet
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
        if self.unique_constraints is None:
            return
        for unique_combo in self.unique_constraints:
            # A single field unique requirements is much cleaner to display than unique combos, so handle differently
            if len(unique_combo) == 1:
                dupes, row_idxs = get_one_column_dupes(self.df, unique_combo[0])
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
            index (int): Row index.  Mutually exclusive with index_list.  Required is index_list is None.
            index_list (list of ints)L Row indexes.  Mutually exclusive with index.  Required is index is None.

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

    def getRowVal(self, row, header):
        """Returns value from the row (presumably from df) and column (identified by header).

        Converts empty strings and "nan"s to None.  Strips leading/trailing spaces.

        Args:
            row (row of a dataframe): Row of data.
            header (str): Column header name.

        Raises:
            ValueError
            RequiredColumnValue

        Returns:
            val (object): Data from the row at the column (header)
        """
        none_vals = ["", "nan"]
        val = None

        if header in row:
            val = row[header]
            if type(val) == str:
                val = val.strip()
            if val in none_vals:
                val = None
        elif self.all_headers is not None and header not in self.all_headers:
            # Missing headers are addressed way before this. If we get here, it's a programming issue, so raise instead
            # of buffer
            raise ValueError(
                f"Incorrect header supplied: [{header}].  Must be one of: {self.all_headers}"
            )

        if val is None:
            if header in list(self.defaults._asdict().keys()):
                val = getattr(self.defaults, header)
            elif header in self.reqd_values:
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
                    record_counts (dict): Model record counts by model and load status (created, existed, and errored)
        """
        def load_wrapper(self, *args, **kwargs):
            with transaction.atomic():
                try:
                    self.check_headers()
                    self.check_unique_constraints()

                    fn(self, *args, **kwargs)

                except Exception as e:
                    # Add this unanticipated error to the other buffered errors
                    self.aggregated_errors_object.buffer_error(e)

                if len(self.conflicting_value_errors) > 0:
                    self.aggregated_errors_object.buffer_error(
                        ConflictingValueErrors(self.conflicting_value_errors)
                    )

                if len(self.required_value_errors) > 0:
                    self.aggregated_errors_object.buffer_error(
                        RequiredValueErrors(self.required_value_errors)
                    )

                # Summarize any DuplicateValues reported
                dupe_errs = self.aggregated_errors_object.get_exception_type(
                    DuplicateValues, remove=True
                )
                if len(dupe_errs) > 0:
                    self.aggregated_errors_object.buffer_error(
                        DuplicateValueErrors(dupe_errs)
                    )

                # Summarize any DuplicateValues reported
                rcvs = self.aggregated_errors_object.get_exception_type(
                    RequiredColumnValue, remove=True
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

            return self.record_counts

        return load_wrapper

    def _get_model_name(self, model_name):
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
        if self.models is not None and len(self.models) == 1:
            return self.models[0].__name__
        # Raise a programming error
        raise ValueError(
            "A model name is required when there is not exactly 1 model initialized in the constructor."
        )

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
                conflicts.extend(check_for_inconsistencies(rec, rec_dict))
        (It can also be called pre-emptively by querying for only a record's unique field and supply the record and a
        dict for record creation.  E.g.:
            rec_dict = {field values for record creation}
            rec = Model.objects.get(name="unique value")
            new_conflicts = check_for_inconsistencies(rec, rec_dict)
            if len(new_conflicts) == 0:
                Model.objects.create(**rec_dict)
        The purpose of this function is to provide helpful information in an exception (i.e. repackage an
        IntegrityError) so that users working to resolve the error can quickly identify and resolve the issue.

        Args:
            rec (Model object)
            rec_dict (dict of objects): A dict (e.g., as supplied to get_or_create() or create())

        Raises:
            Nothing

        Returns:
            conflicting_value_errors (list of ConflictingValueErrors)
        """
        conflicting_value_errors = []
        differences = {}
        for field, new_value in rec_dict.items():
            orig_value = getattr(rec, field)
            if orig_value != new_value:
                differences[field] = {
                    "orig": orig_value,
                    "new": new_value,
                }
        if len(differences.keys()) > 0:
            conflicting_value_errors.append(
                ConflictingValueError(
                    rec,
                    differences,
                    rec_dict=rec_dict,
                    rownum=self.rownum,
                    sheet=self.sheet,
                    file=self.file,
                )
            )
        return conflicting_value_errors

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
        rownum = self.rownum

        if self.aggregated_errors_object is None and (
            self.required_value_errors is None or self.conflicting_value_errors is None
        ):
            raise ValueError(
                "Either an AggregatedErrors object or both a required_value_errors and conflicting_value_errors list "
                "is required."
            )
        elif handle_all and self.aggregated_errors_object is None:
            raise ValueError(
                "An AggregatedErrors object is required when handle_all is True."
            )

        # We may or may not use estr and exc, but we're pre-making them here to reduce code duplication
        estr = str(exception)
        exc = InfileDatabaseError(
            exception, rec_dict, rownum=rownum, sheet=self.sheet, file=self.file
        )

        if isinstance(exception, IntegrityError):
            if "duplicate key value violates unique constraint" in estr:
                # Create a list of lists of unique fields and unique combos of fields
                # First, get unique fields and force them into a list of lists (so that we only need to loop once)
                unique_combos = [
                    [f] for f in get_unique_fields(model, fields=rec_dict.keys())
                ]
                # Now add in the unique field combos from the model's unique constraints
                unique_combos.extend(get_unique_constraint_fields(model))

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
                        errs = self.check_for_inconsistencies(rec, rec_dict)
                        if len(errs) > 0:
                            if self.conflicting_value_errors is not None:
                                self.conflicting_value_errors.extend(errs)
                                return True
                            elif self.aggregated_errors_object:
                                for err in errs:
                                    self.aggregated_errors_object.buffer_error(err)
                                return True

            elif "violates not-null constraint" in estr:
                # Parse the field name out of the exception string
                regexp = re.compile(r"^null value in column \"(?P<fldname>[^\"]+)\"")
                match = re.search(regexp, estr)
                if match:
                    fldname = match.group("fldname")
                    colname = fldname
                    # Convert the database column name to the file column header, if available
                    if self.FieldToHeader is not None and colname in self.FieldToHeader[model.__name__].keys():
                        colname = self.FieldToHeader[model.__name__][fldname]
                    err = RequiredValueError(
                        column=colname,
                        rownum=rownum,
                        model_name=model.__name__,
                        field_name=fldname,
                        sheet=self.sheet,
                        file=self.file,
                        rec_dict=rec_dict,
                    )
                    if self.required_value_errors is not None:
                        self.required_value_errors.append(err)
                        return True
                    elif self.aggregated_errors_object:
                        for err in errs:
                            self.aggregated_errors_object.buffer_error(err)
                        return True

        elif isinstance(exception, ValidationError):
            if "is not a valid choice" in estr:
                choice_fields = get_enumerated_fields(model, fields=rec_dict.keys())
                for choice_field in choice_fields:
                    if choice_field in estr and rec_dict[choice_field] is not None:
                        if self.aggregated_errors_object is not None:
                            # Only include error once
                            if not self.aggregated_errors_object.exception_type_exists(
                                InfileDatabaseError
                            ):
                                self.aggregated_errors_object.buffer_error(exc)
                            else:
                                # NOTE: This is not perfect.  There can be multiple field values with issues.  Repeated
                                # calls to this function could potentially reference the same record and contain an
                                # error about a different field.  We only buffer/raise one of them because the details
                                # include the entire record and dict causing the issue(s).
                                already_buffered = False
                                for (
                                    existing_exc
                                ) in self.aggregated_errors_object.get_exception_type(
                                    InfileDatabaseError
                                ):
                                    if existing_exc.rec_dict != rec_dict:
                                        already_buffered = True
                                if not already_buffered:
                                    self.aggregated_errors_object.buffer_error(exc)
                            # Whether we buffered or not, the error was identified and handled (by either buffering or
                            # ignoring a duplicate)
                            return True

        elif isinstance(exception, RequiredColumnValue):
            # This "catch" was added to force the developer to not continue the loop if they failed to call this method
            self.aggregated_errors_object.buffer_error(exception)

        if handle_all and self.aggregated_errors_object is not None:
            self.aggregated_errors_object.buffer_error(exc)
            return True

        # If we get here, we did not identify the error as one we knew what to do with
        return False
