import argparse
from abc import ABC, abstractmethod

from django.core.management import BaseCommand

from DataRepo.loaders.table_loader import TableLoader
from DataRepo.utils import (
    AggregatedErrors,
    DryRun,
    MutuallyExclusiveOptions,
    OptionsNotAvailable,
    get_sheet_names,
    is_excel,
    read_from_file,
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
                parser.add_argument("--synonym-separator", type=str)

            def handle(self, *args, **options):
                self.load_data(synonym_separator=options["synonym_separator"])

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
    def loader_class(self):  # type[TableLoader]
        pass

    def __init__(self, *args, **kwargs):
        """This init auto-applies a decorator to the derived class's handle method."""
        # Apply the handler decorator to the handle method in the derived class
        self.apply_handle_wrapper()
        self.check_class_attributes()
        # options are set in the override of handle(), but we need to know if options are available in the get_* methods
        self.options = None
        # We will set initial values here.  The derived class must call set if they have custom default values for any
        # of these, but note that what users supply on the command line will trump anything they supply.  The values
        # they supply are only custom defaults.  Note, these are just the defaults and are provided so that the derived
        # class can retrieve the structure of the defaults (e.g. all of the header keys) to be able to construct custom
        # defaults for things like headers and default values.  This is before the user supplies anything on the command
        # line.  When they do, those are set in the handle() method and a new loader object with values updated from the
        # user-supplied values are updated.
        self.init_loader()
        super().__init__(*args, **kwargs)

    def init_loader(self, *args, **kwargs):
        # These are used to copy derived class headers and defaults to newly created objects
        saved_headers = None
        saved_defaults = None

        if hasattr(self, "loader"):
            # The derived class code may have called set_headers or set_defaults to establish dynamic headers/defaults.
            # This ensures those are copied to the new loader.
            saved_headers = self.get_headers()
            saved_defaults = self.get_defaults()

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
                    "The following supplied agrguments are under direct control of the LoadTableCommand superclass: "
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

            # Intermediate loader state (needed before calling get_dataframe)
            self.loader = self.loader_class(*args, **kwargs)

            # Now we can parse the dataframe using the user-customized dtype dict
            kwargs["df"] = self.get_dataframe()

            # Re-initialize associated data
            self.loader = self.loader_class(*args, **kwargs)
        else:
            # Before handle() has been called (with options), just initialize the loader with all supplied arguments
            self.loader = self.loader_class(*args, **kwargs)

    def apply_handle_wrapper(self):
        """This applies a decorator to the derived class's handle method.

        See:

        https://stackoverflow.com/questions/72666230/wrapping-derived-class-method-from-base-class

        Args:
            None

        Raises:
            Nothing

        Returns:
            Nothing
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

        Raises:
            Nothing

        Returns:
            Nothing
        """
        parser.add_argument(
            "--infile",
            type=str,
            help=(
                f"Path to either a tab-delimited or excel file (with a sheet named '{self.loader_class.DataSheetName}' "
                "- See --data-sheet).  See --headers for column composition."
            ),
            required=True,
        )

        parser.add_argument(
            "--data-sheet",
            type=str,
            help=(
                "Name of excel sheet/tab.  Only used if --infile is an excel spreadsheet.  Default: [%(default)s]."
            ),
            default=self.loader_class.DataSheetName,
        )

        parser.add_argument(
            "--defaults-file",
            type=str,
            help=(
                "Path to a tab-delimited file containing default values.  If --infile is an excel file, you must use "
                "--defaults-sheet instead.  Required headers: "
                f"{self.loader_class.DefaultsHeaders._asdict().values()}."
            ),
            required=False,
        )

        parser.add_argument(
            "--defaults-sheet",
            type=str,
            help=(
                "Name of excel sheet/tab containing default values for the columns in the data sheet (see "
                "--data-sheet).  Only used if --infile is an excel file.  Default: [%(default)s].  See --defaults-file "
                "for column composition."
            ),
            default=self.loader_class.DefaultsSheetName,
        )

        parser.add_argument(
            "--headers",
            type=str,
            help=f"YAML file defining headers to be used.  Default headers: {self.loader.get_pretty_headers()}.",
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="If supplied, nothing will be saved to the database.",
        )

        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY.  A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def check_class_attributes(self):
        """Checks that the class attributes are properly defined.

        Checks existence and type of:
            loader_class (class attribute, TableLoader class)

        Args:
            None

        Raises:
            AggregatedErrors
                TypeError

        Returns:
            Nothing
        """
        here = f"{type(self).__module__}.{type(self).__name__}"
        if not issubclass(self.loader_class, TableLoader):
            # Immediately raise programming related errors
            aes = AggregatedErrors()
            aes.buffer_error(
                TypeError(
                    f"Invalid attribute [{here}.loader_class] TableLoader required, {type(self.loader_class)} set"
                )
            )
            if aes.should_raise():
                raise aes

    @staticmethod
    def _handler(fn):
        """Decorator to be applied to a Command class's handle method.

        Adds a wrapper to handle the common tasks amongst all the classes that provide load commands.

        This method is provate because it is automatically applied to handle methods of the derived classes in __init__.

        Args:
            fn (function)

        Raises:
            AggregatedErrors

        Returns:
            handle_wrapper (function)
                Args:
                    **options (command line options)
                Raises:
                    TBD by the wrapped method
                Returns:
                    TBD by the wrapped method
        """

        def handle_wrapper(self, *args, **options):
            self.saved_aes = None
            retval = None
            self.options = options

            # So that derived classes don't need to call init_loader unless their derived loader class takes custom
            # arguments
            self.init_loader()

            try:
                retval = fn(self, *args, **options)

            except DryRun:
                pass
            except AggregatedErrors as aes:
                self.saved_aes = aes
            except Exception as e:
                # Add this error (which wasn't added to the aggregated errors, because it was unanticipated) to the
                # other buffered errors
                self.saved_aes = AggregatedErrors()
                self.saved_aes.buffer_error(e)

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
            **kwargs (key/value pairs): Any custom args for the derived loader class, e.g. compound synonyms separator

        Raises:
            Nothing

        Returns:
            The return of loader.load_data()
        """
        return self.loader.load_data(*args, **kwargs)

    def report_status(self):
        """Prints load status per model.

        Reports counts of loaded, existed and errored records.  Includes a note about dry run mode, if active.  Respects
        the verbosity option.

        Args:
            None

        Raises:
            Nothing

        Returns:
            Nothing
        """
        if self.options["verbosity"] == 0:
            return
        msg = "Done.\n"
        if self.options["dry_run"]:
            msg = "Dry-run complete.  The following would occur during a real load:\n"

        load_stats = self.loader.get_load_stats()
        for mdl in self.loader_class.get_models():
            mdl_name = mdl.__name__
            if mdl_name in load_stats.keys():
                msg += (
                    "%s records created: [%i], existed: [%i], skipped [%i], and errored: [%i]."
                    % (
                        mdl_name,
                        load_stats[mdl_name]["created"],
                        load_stats[mdl_name]["existed"],
                        load_stats[mdl_name]["skipped"],
                        load_stats[mdl_name]["errored"],
                    )
                )

        if self.saved_aes is not None and self.saved_aes.get_num_errors() > 0:
            status = self.style.ERROR(msg)
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

        Raises:
            Nothing

        Returns:
            defaults_sheet (str)
        """
        if self.options is None:
            raise OptionsNotAvailable()
        return self.options["defaults_sheet"] if is_excel(self.get_infile()) else None

    def get_dataframe(self):
        """Parses data from the infile (and sheet) using the headers and the column types.

        The column types are optionally defined in self.loader.

        Args:
            None

        Raises:
            Nothing

        Returns:
            df (pandas DataFrame)
        """
        if self.options is None:
            raise OptionsNotAvailable()
        file = self.get_infile()
        sheet = self.options["data_sheet"]
        dtypes = self.loader.get_column_types()
        df = None
        if dtypes is None:
            df = read_from_file(file, sheet=sheet)
        else:
            df = read_from_file(file, dtype=dtypes, sheet=sheet)
        return df

    def get_headers(self):
        """Returns the merge of the current user, developer (custom default), and class default headers.

        Args:
            None

        Raises:
            Nothing

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

        Raises:
            Nothing

        Returns:
            headers (namedtupe of loader_class.DataTableHeaders): Header names by header key
        """
        return self.loader.set_headers(custom_headers)

    def get_defaults(self):
        """Returns current defaults.

        Args:
            None

        Raises:
            Nothing

        Returns:
            defaults (namedtuple of TableLoader.DataTableHeaders containing strings of header names)
        """
        return self.loader.get_defaults()

    def set_defaults(self, custom_defaults=None):
        """Sets instance's default values.  If no custom defaults are provided, it reverts to user-privided and/or class
        defaults.

        There are a few places where defaults can be defined:

        - User: Supplied by the user via --defaults-file (or --infile and --defaults-sheet.)
        - Developer: Supplied via the custom_defaults (dict) argument.  (Can be trumped by user supplied defaults.)
        - Loader: Defined in the loader_class.  self.loader.get_defaults() is used to obtain default values by
          header key (in a namedtuple).  (Can be trumped by developer and user defaults.)

        Each individual header is assigned in order of precedence:

            User > Developer > Loader

        Args:
            custom_defaults (namedtupe of loader_class.DataTableHeaders): Header names by header key

        Raises:
            Nothing

        Returns:
            defaults (namedtupe of loader_class.DataTableHeaders): Header names by header key
        """
        return self.loader.set_defaults(custom_defaults)

    def get_user_defaults(self):
        """Retrieves defaults dataframe from the defaults file.

        Args:
            None

        Raises:
            Nothing

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

        Raises:
            Nothing

        Returns:
            infile (str)
        """
        if self.options is None:
            raise OptionsNotAvailable()
        return self.options["infile"]
