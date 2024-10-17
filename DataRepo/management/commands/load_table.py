import argparse
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Type

from django.core.management import BaseCommand, CommandError
from django.db import ProgrammingError

from DataRepo.loaders import TableLoader
from DataRepo.utils import (
    AggregatedErrors,
    DryRun,
    MutuallyExclusiveOptions,
    OptionsNotAvailable,
    RequiredOptions,
    get_sheet_names,
    is_excel,
    read_from_file,
)
from DataRepo.utils.exceptions import (
    ConditionallyRequiredOptions,
    NotATableLoader,
)


class LoadTableCommand(ABC, BaseCommand):
    """Command superclass to be used to load a database given a table-like file.

    This class establishes the basic common command line interface for loading a database from a table-like file
    (supported file types: excel, tsv, csv).  It also handles common errors and presents them to the user in the context
    of the input file.

    Use this class as the base class for any command that intends to load the database from a table-like file.

    Usage:
        1. Inherit from LoadTableCommand
        2. If you define a derived class add_arguments method, be sure to call super().add_arguments(parser)
        3. Call self.load_data() inside the handle() method.  See load_data() for custom headers, arguments, etc.

    Example:
        class Command(LoadTableCommand):
            help = "Loads data from a compound table into the database"
            loader_class = CompoundsLoader

            def add_arguments(self, parser):
                super().add_arguments(parser)
                parser.add_argument("--synonyms-delimiter", type=str)

            def handle(self, *args, **options):
                self.load_data(synonyms_delimiter=options["synonyms_delimiter"])

    Attributes:
        help (str): Default help string to be printed when the CLI is used with the help command.
        loader_class (TableLoader derived class): A derived class of TableLoader.  This class defines headers,
            data constraints, data types, effected database models/fields etc.  LoadTableCommand uses this class to be
            able to rerad the infile correctly.
    """

    help = "Loads data from a file into the database."

    # Abstract required class attributes
    # Must be initialized in the derived class.
    # See load_tissues.Command for a concrete example.
    @property
    @abstractmethod
    def loader_class(self) -> Type[TableLoader]:
        pass

    def __init__(
        self,
        *args,
        required_optnames: Optional[List[str]] = None,
        required_optname_groups: Optional[List[List[str]]] = None,
        opt_defaults: Optional[Dict[str, object]] = None,
        custom_loader_init: bool = False,
        **kwargs,
    ):
        """This init auto-applies a decorator to the derived class's handle method.

        Args:
            required_optnames (list of strings): The variable version of this class's options that should be required.
            required_optname_groups (list of lists of strings):  Groups of the variable version of this class's options
                that should be coniditionally required, e.g. either infile or defaults_file are required.
        Exceptions:
            None
        Returns:
            instance
        """
        default_required_optnames = set(["infile"])
        if required_optname_groups is not None and required_optnames is None:
            for optgroup in required_optname_groups:
                if not isinstance(optgroup, list):
                    # I don't know why the type hint doesn't raise this exception...
                    raise AggregatedErrors().buffer_error(
                        TypeError("required_optname_groups must be a list of lists.")
                    )
                default_required_optnames -= set(optgroup)
        self.required_optnames = (
            list(default_required_optnames)
            if required_optnames is None
            else required_optnames
        )

        self.required_optname_groups = (
            [] if required_optname_groups is None else required_optname_groups
        )

        self.check_class_attributes()

        # This gives each derived command class the opportunity to change the defaults of the stock options
        if opt_defaults is None:
            opt_defaults = {}
        self.opt_defaults = {
            "infile": opt_defaults.get("infile", None),
            "data_sheet": opt_defaults.get(
                "data_sheet", self.loader_class.DataSheetName
            ),
            "defaults_file": opt_defaults.get("defaults_file", None),
            "defaults_sheet": opt_defaults.get(
                "defaults_sheet", self.loader_class.DefaultsSheetName
            ),
            "headers": opt_defaults.get("headers", None),
            "dry_run": opt_defaults.get("dry_run", False),
            "defer_rollback": opt_defaults.get("defer_rollback", False),
        }

        # Use this for classes that you plan to initialize inside the handle() method, e.g. to determine the loader
        # class on the fly, like when using abstract classes.
        self.custom_loader_init = custom_loader_init

        # Apply the handler decorator to the handle method in the derived class
        self.apply_handle_wrapper()
        # options are set in the override of handle(), but we need to know if options are available in the get_* methods
        self.options = None
        self.dry_run_exception = None
        # We will set initial values here.  The derived class must call set if they have custom default values for any
        # of these, but note that what users supply on the command line will trump anything they supply.  The values
        # they supply are only custom defaults.  Note, these are just the defaults and are provided so that the derived
        # class can retrieve the structure of the defaults (e.g. all of the header keys) to be able to construct custom
        # defaults for things like headers and default values.  This is before the user supplies anything on the command
        # line.  When they do, those are set in the handle() method and a new loader object with values updated from the
        # user-supplied values are updated.
        self.init_loader()
        super().__init__(*args, **kwargs)

        # The handle_wrapper method uses saved_aes to manage AggregatedErrors exceptions raised by the loader(s)
        self.saved_aes = None

    def init_loader(self, *args, **kwargs):
        # These are used to copy derived class headers and defaults to newly created objects
        saved_headers = None
        saved_defaults = None

        if hasattr(self, "loader"):
            # The derived class code may have called set_headers or set_defaults to establish dynamic headers/defaults.
            # This ensures those are copied to the new loader.
            saved_headers = self.get_headers()
            saved_defaults = self.get_defaults()

        # TODO: Move the disallowed_args up above the if self.options conditional and add headers and defaults to it.
        kwargs["headers"] = saved_headers
        kwargs["defaults"] = saved_defaults

        if self.options is not None:
            superclass_args = [
                "df",
                "file",
                "data_sheet",
                "user_headers",
                "defaults_df",
                "defaults_file",
                "defaults_sheet",
                "dry_run",
                "defer_rollback",
            ]
            disallowed_args = []
            for key in kwargs.keys():
                if key in superclass_args:
                    disallowed_args.append(key)
            if len(disallowed_args) > 0:
                # Immediately raise programming errors
                raise ValueError(
                    "The following supplied arguments are under direct control of the LoadTableCommand superclass: "
                    f"{disallowed_args}.  The superclass uses the command line options to fill in user-supplied "
                    "values.  The only arguments that are allowed are arguments specific to the derived class "
                    f"[{self.loader_class.__name__}] constructor."
                )

            # We need to incorporate user user-supplied options BEFORE parsing the dataframe, because get_dataframe uses
            # DataColumnTypes via self.loader.get_column_types (which is keyed by header *name*).  It does this because
            # it supplies the datatypes in the columns using the dtype option, keyed by header name.  All of that needs
            # to be initialized before we attempt to parse that data.
            # So we will set an intermediate loader object to initialize the user-supplied values.  Note, this would all
            # be solved (perhaps more elegantly) by parsing the file from the loader (super) class (i.e. calling
            # read_from_file inside the loader class instead of in the command class), but I'm sticking with the
            # established design pattern:

            # Infile metadata/data
            kwargs["file"] = self.get_infile()
            kwargs["data_sheet"] = self.options["data_sheet"]
            kwargs["user_headers"] = self.get_user_headers()

            # Defaults metadata/data
            kwargs["defaults_file"] = self.options["defaults_file"]
            kwargs["defaults_sheet"] = self.get_defaults_sheet()
            kwargs["defaults_df"] = self.get_user_defaults()

            # Modes
            kwargs["dry_run"] = self.options["dry_run"]
            kwargs["defer_rollback"] = self.options["defer_rollback"]

            # Intermediate loader state (needed before calling get_dataframe, because it performs checks on the sheet,
            # and the sheet's headers can be changed by the constructor based on the type of input file)
            self.loader: TableLoader = self.loader_class(*args, **kwargs)

            # Now we can parse the dataframe using the user-customized dtype dict
            kwargs["df"] = self.get_dataframe()

        # Before handle() has been called (with options), just initialize the loader with all supplied arguments.
        # Note that the derived class MUST have a defined value for the class attribute `loader_class`, so if the
        # derived class is an abstract class as well, you must set a default loader_class.
        self.loader: TableLoader = self.loader_class(*args, **kwargs)

    def apply_handle_wrapper(self):
        """This applies a decorator to the derived class's handle method.

        See:

        https://stackoverflow.com/questions/72666230/wrapping-derived-class-method-from-base-class

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # Apply the handler decorator to the handle method in the derived class
        decorated_derived_class_method = self._handler(getattr(self, "handle"))

        # Get the binding for the decorated method
        # False positive from pylint
        # pylint: disable=assignment-from-no-return
        bound = decorated_derived_class_method.__get__(self, None)
        # pylint: enable=assignment-from-no-return

        # Apply the binding to the handle method in the object
        setattr(self, "handle", bound)

    def add_arguments(self, parser):
        """Adds command line options.

        Args:
            parser (argparse object)
        Exceptions:
            None
        Returns:
            None
        """
        parser.add_argument(
            "--infile",
            type=str,
            help=(
                f"Path to either a tab-delimited or excel file (with a sheet named '{self.loader_class.DataSheetName}' "
                "- See --data-sheet).  See --headers for column composition."
            ),
            default=self.opt_defaults.get("infile"),
            required="infile" in self.required_optnames,
        )

        parser.add_argument(
            "--data-sheet",
            type=str,
            help=(
                "Name of excel sheet/tab.  Only used if --infile is an excel spreadsheet.  Default: [%(default)s]."
            ),
            default=self.opt_defaults.get("data_sheet"),
            required="data_sheet" in self.required_optnames,
        )

        parser.add_argument(
            "--defaults-file",
            type=str,
            help=(
                "Path to a tab-delimited file containing default values.  If --infile is an excel file, you must use "
                "--defaults-sheet instead.  Required headers: "
                f"{self.loader_class.DefaultsHeaders._asdict().values()}."
            ),
            default=self.opt_defaults.get("defaults_file"),
            required="defaults_file" in self.required_optnames,
        )

        parser.add_argument(
            "--defaults-sheet",
            type=str,
            help=(
                "Name of excel sheet/tab containing default values for the columns in the data sheet (see "
                "--data-sheet).  Only used if --infile is an excel file.  Default: [%(default)s].  See --defaults-file "
                "for column composition."
            ),
            default=self.opt_defaults.get("defaults_sheet"),
            required="defaults_sheet" in self.required_optnames,
        )

        parser.add_argument(
            "--headers",
            type=str,
            help=f"YAML file defining headers to be used.  Default headers: {self.loader.get_pretty_headers()}.",
            default=self.opt_defaults.get("headers"),
            required="headers" in self.required_optnames,
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=self.opt_defaults.get("dry_run"),
            help="If supplied, nothing will be saved to the database.",
        )

        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY.  A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
            action="store_true",
            default=self.opt_defaults.get("defer_rollback"),
            help=argparse.SUPPRESS,
        )

    def check_class_attributes(self):
        """Checks that the class attributes are properly defined.

        Checks existence and type of:
            loader_class (class attribute, TableLoader class)

        Args:
            None
        Exceptions:
            Buffered:
                TypeError
            Raised:
                AggregatedErrors
        Returns:
            None
        """
        if not issubclass(self.loader_class, TableLoader):
            # Immediately raise programming related errors
            raise AggregatedErrors().buffer_error(NotATableLoader(self))

    @staticmethod
    def _handler(fn):
        """Decorator to be applied to a Command class's handle method.

        Adds a wrapper to handle the common tasks amongst all the classes that provide load commands.

        This method is provate because it is automatically applied to handle methods of the derived classes in __init__.

        Args:
            fn (function)
        Exceptions:
            AggregatedErrors
        Returns:
            handle_wrapper (function)
                Args:
                    **options (command line options)
                Exceptions:
                    TBD by the wrapped method
                Returns:
                    TBD by the wrapped method
        """

        def handle_wrapper(self, *args, **options):
            self.saved_aes = None
            retval = None
            self.options = options
            self.dry_run_exception = None

            missing_reqd = []
            for optname in options.keys():
                if options[optname] is None and optname in self.required_optnames:
                    missing_reqd.append(optname)
            if len(missing_reqd) > 0:
                raise RequiredOptions(missing_reqd)

            # Raise an error if any of a set of conditionally required options was not supplied
            # self.required_optname_groups is a list of lists of option names, at least one option of each set is
            # required (if any were supplied in __init__).
            failed_cond_reqd_opt_sets = []
            for cond_reqd_opt_set in self.required_optname_groups:
                reqd_err = True
                for optname in cond_reqd_opt_set:
                    if options.get(optname) is not None:
                        reqd_err = False
                        break
                if reqd_err:
                    failed_cond_reqd_opt_sets.append(cond_reqd_opt_set)
            if len(failed_cond_reqd_opt_sets) > 0:
                cond_reqd_opt_sets_str = "\n\t".join(
                    [", ".join([f"{cros}" for cros in failed_cond_reqd_opt_sets])]
                )
                raise AggregatedErrors().buffer_error(
                    ConditionallyRequiredOptions(
                        f"One of each of the following sets of options is required:\n\t{cond_reqd_opt_sets_str}"
                    )
                )

            # So that derived classes don't need to call init_loader unless their derived loader class takes custom
            # arguments
            if not self.custom_loader_init:
                self.init_loader()

            try:
                retval = fn(self, *args, **options)

            except DryRun as dr:
                self.dry_run_exception = dr
                pass
            except AggregatedErrors as aes:
                self.saved_aes = aes
            except Exception as e:
                # Add this error (which wasn't added to the aggregated errors, because it was unanticipated) to the
                # other buffered errors
                self.saved_aes = AggregatedErrors()
                self.saved_aes.buffer_error(e)

            if not self.options.get("help"):
                self.report_status()

            if self.saved_aes is not None and self.saved_aes.should_raise():
                self.saved_aes.print_summary()
                raise self.saved_aes

            return retval

        return handle_wrapper

    def load_data(self, *args, **kwargs):
        """Creates loader_class object in self.loader and calls self.loader.load_data().

        Args:
            df (pandas dataframe):
            headers (TableLoader.DataTableHeaders): Custom header names by header key.
            defaults (TableLoader.DataTableHeaders): Custom default values by header key.
            dry_run (boolean): Dry Run mode.
            defer_rollback (boolean): Defer rollback mode.   DO NOT USE MANUALLY.  A PARENT SCRIPT MUST HANDLE ROLLBACK.
            sheet (str): Name of the sheet to load (for error reporting only).
            file (str): Name of the file to load (for error reporting only).
            **kwargs (key/value pairs): Any custom args for the derived loader class, e.g. compound synonyms delimiter
        Exceptions:
            None
        Returns:
            The return of loader.load_data()
        """
        return self.loader.load_data(*args, **kwargs)

    def report_status(self):
        """Prints load status per model.

        Reports counts of created, existed, deleted, updated, skipped, errored, and warned records.  Includes a note
        about dry run mode, if active.  Respects the verbosity option.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        if self.options["verbosity"] == 0:
            return
        msg = "Done.\n"
        if self.options["dry_run"]:
            msg = "Dry-run complete.  The following would occur during a real load:\n"

        load_stats = self.loader.get_load_stats()
        for mdl_name in load_stats.keys():
            try:
                msg += (
                    "{0} records created: [{created}], existed: [{existed}], deleted: [{deleted}], updated: "
                    "[{updated}], skipped [{skipped}], errored: [{errored}], and warned: [{warned}].\n"
                ).format(mdl_name, **load_stats[mdl_name])
            except KeyError as ke:
                raise AggregatedErrors().buffer_error(
                    ProgrammingError(
                        f"Encountered uninitialized record stats for model [{mdl_name}] in loader "
                        f"{self.loader_class.__name__}.  Please make sure all model record count increment method "
                        f"calls (such as {ke}) are in {self.loader_class.__name__}.Models: "
                        f"{[m.__name__ for m in self.loader.Models]}."
                    ),
                    orig_exception=ke,
                )

        if self.saved_aes is not None and self.saved_aes.get_num_errors() > 0:
            status = self.style.ERROR(msg)
        elif self.options["dry_run"]:
            # Errors are raised before dry run, but if there were no errors and dry run was called for, print the dry-
            # run message or raise if DryRun was not raised
            if self.dry_run_exception is not None:
                # The MIGRATE_HEADING IS BOLD BLUE
                status = self.style.MIGRATE_HEADING(msg)
            else:
                raise CommandError("DryRun exception not raised in --dry-run mode!")
        elif self.saved_aes is not None and self.saved_aes.get_num_warnings() > 0:
            status = self.style.WARNING(msg)
        else:
            status = self.style.SUCCESS(msg)

        self.stdout.write(status)

    # Getters and setters

    def get_defaults_sheet(self):
        """Uses options["defaults_sheet"] to return the sheet name.

        Note that self.loader_class.DefaultsSheetName is set as the default for the --defaults-sheet option.  See
        add_arguments().

        Args:
            None
        Exceptions:
            None
        Returns:
            defaults_sheet (str)
        """
        if self.options is None:
            raise OptionsNotAvailable()
        return self.options["defaults_sheet"] if is_excel(self.get_infile()) else None

    def get_dataframe(self, typing=True):
        """Parses data from the infile (and sheet) using the headers and the column types.

        The column types are optionally defined in self.loader.

        Args:
            typing (bool): Doesn't pass dtype to read_from_file even if its available.  Useful when trying to determine
                file type.
        Exceptions:
            None
        Returns:
            df (pandas DataFrame)
        """
        if self.options is None:
            raise OptionsNotAvailable()

        file = self.get_infile()
        if file is None:
            # The derived class can decide to handle the load completely without an input file (e.g. using all defaults
            # and/or custom options
            return None

        sheet = self.options["data_sheet"]

        dtypes = None
        # This method is used in some calls to determine the loader class, in which case, there is no instantiated
        # loader and it doesn't need the dtypes - it just needs the sheet and column names.
        if typing and hasattr(self, "loader"):
            dtypes = self.loader.get_column_types()

        df = None
        if dtypes is None:
            df = read_from_file(file, sheet=sheet)
        else:
            keep_default_na = False
            if len([val for val in dtypes.values() if not isinstance(val, str)]) > 0:
                # pandas will throw an error on empty cells if it cannot convert an empty string into a specified type,
                # so setting keep_default_na to True will allow them to just be null.
                keep_default_na = True
            df = read_from_file(
                file, dtype=dtypes, sheet=sheet, keep_default_na=keep_default_na
            )

        return df

    def get_headers(self):
        """Returns the merge of the current user, developer (custom default), and class default headers.

        Args:
            None
        Exceptions:
            None
        Returns:
            headers (namedtuple of TableLoader.DataTableHeaders containing strings of header names)
        """
        return self.loader.get_headers()

    def get_user_headers(self):
        if self.options is None:
            raise OptionsNotAvailable()

        # User-level defaults are supplied via options (a yaml file via the --headers option)
        user_headers = None
        if self.options is not None:
            user_headers = (
                read_from_file(self.options["headers"])
                if self.options["headers"]
                else None
            )

        return user_headers

    def set_headers(self, custom_headers=None):
        """Sets instance's header names.  If no custom headers are provided, it reverts to user-privided and/or class
        defaults.

        There are a few places where headers can be defined:

        - User: Supplied by the user via --headers (a yaml file whose parsing returns a dict).
        - Developer: Supplied via the custom_headers (dict) argument.  (Can be trumped by user supplied headers.)
        - Loader: Defined in the loader_class.  self.loader.get_headers() is used to obtain default values by
          header key (in a namedtuple).  (Can be trumped by developer and user headers.)

        Each individual header is assigned in order of precedence:

            User > Developer > Loader

        Args:
            custom_headers (namedtupe of loader_class.DataTableHeaders): Header names by header key
        Exceptions:
            None
        Returns:
            headers (namedtupe of loader_class.DataTableHeaders): Header names by header key
        """
        return self.loader.set_headers(custom_headers)

    def get_defaults(self):
        """Returns current defaults.

        Args:
            None
        Exceptions:
            None
        Returns:
            defaults (namedtuple of TableLoader.DataTableHeaders containing strings of header names)
        """
        return self.loader.get_defaults()

    def set_defaults(self, custom_defaults=None):
        """Sets instance's default values.  If no custom defaults are provided, it reverts to user-provided and/or class
        defaults.

        There are a few places where defaults can be defined:

        - User: Supplied by the user via --defaults-file (or --infile and --defaults-sheet.)
        - Developer: Supplied via the custom_defaults (dict) argument.  (Can be trumped by user supplied defaults.)
        - Loader: Defined in the loader_class.  self.loader.get_defaults() is used to obtain default values by
          header key (in a namedtuple).  (Can be trumped by developer and user defaults.)

        Each individual header is assigned in order of precedence:

            User > Developer > Loader

        Args:
            custom_defaults (namedtupe of loader_class.DataTableHeaders): Default values by header key
        Exceptions:
            None
        Returns:
            defaults (namedtupe of loader_class.DataTableHeaders): Header names by header key
        """
        return self.loader.set_defaults(custom_defaults)

    def get_user_defaults(self):
        """Retrieves defaults dataframe from the defaults file.

        Args:
            None
        Exceptions:
            None
        Returns:
            user_defaults (pandas dataframe)
        """
        if self.options is None:
            raise OptionsNotAvailable()

        infile = self.get_infile()
        defaults_sheet = None

        if is_excel(infile):
            if self.options["defaults_file"] is not None:
                raise MutuallyExclusiveOptions(
                    "--defaults-file cannot be provided when --infile is an excel file.  Use --defaults-sheet and add "
                    "defaults to the excel file."
                )
            defaults_sheet = self.get_defaults_sheet()
            all_sheets = get_sheet_names(infile)
            if defaults_sheet in all_sheets:
                defaults_file = infile
            else:
                defaults_file = None
        elif self.options["defaults_file"] is not None:
            defaults_file = self.options["defaults_file"]
        else:
            defaults_file = None

        if defaults_file is None:
            return None

        return read_from_file(defaults_file, sheet=defaults_sheet)

    def get_infile(self):
        """Uses options["infile"] to return the input file name.

        Args:
            None
        Exceptions:
            None
        Returns:
            infile (str)
        """
        if self.options is None:
            raise OptionsNotAvailable()
        return self.options["infile"]
