import argparse
from abc import ABC, abstractmethod

from django.core.management import BaseCommand
from django.db.utils import ProgrammingError

from DataRepo.utils import AggregatedErrors, DryRun, is_excel, read_from_file
from DataRepo.utils.loader import TraceBaseLoader


class LoadFromTableCommand(ABC, BaseCommand):
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
        class Command(LoadFromTableCommand):
            help = "Loads data from a compound table into the database"
            loader_class = CompoundsLoader
            data_sheet_default = "Compounds"
            defaults_sheet_default = "Defaults"

            def add_arguments(self, parser):
                super().add_arguments(parser)
                parser.add_argument("--synonym-separator", type=str)

            def handle(self, *args, **options):
                self.load_data(synonym_separator=options["synonym_separator"])

    Attributes:
        help (str): Default help string to be printed when the CLI is used with the help command.
        loader_class (TraceBaseLoader derived class): A derived class of TraceBaseLoader.  This class defines headers,
            data constraints, data types, effected database models/fields etc.  LoadTableCommand uses this class to be
            able to rerad the infile correctly.
        data_sheet_default (str): Default name of the excel sheet with the data to load (though note that an option to
            define a custom name is provided).
        defaults_sheet_default (str): Default name of the excel sheet containing default values for the data sheet
            columns.
    """

    help = "Loads data from a file into the database."
    defaults_sheet_default = "Defaults"

    # Abstract required class attributes
    # Must be initialized in the derived class.
    # See load_tissues.Command for a concrete example.
    @property
    @abstractmethod
    def loader_class(self):  # type[TraceBaseLoader]
        pass

    @property
    @abstractmethod
    def data_sheet_default(self):  # str
        pass

    def __init__(self, *args, **kwargs):
        """This init auto-applies a decorator to the derived class's handle method."""
        # Apply the handler decorator to the handle method in the derived class
        self.apply_handle_wrapper()
        super().__init__(*args, **kwargs)
        # Init default headers and defaults
        self.check_class_attributes()
        self.headers = self.loader_class.get_class_headers()
        self.defaults = self.loader_class.get_class_defaults()

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
                "Path to either a tab-delimited or excel file (with a sheet named 'Study').  "
                f"Default headers: {self.loader_class.get_pretty_default_headers()}.  See --headers."
            ),
            required=True,
        )

        parser.add_argument(
            "--sheet",
            type=str,
            help=(
                "Name of excel sheet/tab.  Only used if --infile is an excel spreadsheet.  Default: "
                f"[{self.data_sheet_default}]."
            ),
            default=self.data_sheet_default,
        )

        parser.add_argument(
            "--defaults-sheet",
            type=str,
            help=(
                "Name of excel sheet/tab containing default values for the columns in the data sheet (see --sheet).  "
                f"Only used if --infile is an excel spreadsheet.  Default: [{self.defaults_sheet_default}]."
            ),
            default=self.defaults_sheet_default,
        )

        parser.add_argument(
            "--headers",
            type=str,
            help="YAML file defining headers to be used.",
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
            self.check_class_attributes()
            self.saved_aes = None
            self.options = options
            retval = None

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

    def get_sheet(self, default=False):
        """Uses options["sheet"] to return the sheet name.

        Note that self.data_sheet_default is set as the default for the --sheet option.  See add_arguments().

        Args:
            default (boolean): Whether or not to get the sheet name regardless of infile type

        Raises:
            Nothing

        Returns:
            sheet (str)
        """
        return (
            self.options["sheet"]
            if not default and is_excel(self.get_infile())
            else None
        )

    def get_defaults_sheet(self):
        """Uses options["defaults_sheet"] to return the sheet name.

        Note that self.defaults_sheet_default is set as the default for the --defaults-sheet option.  See
        add_arguments().

        Args:
            None

        Raises:
            Nothing

        Returns:
            defaults_sheet (str)
        """
        return self.options["defaults_sheet"] if is_excel(self.get_infile()) else None

    def get_dataframe(self):
        """Parses data from the infile (and sheet) using the headers and the column types.

        The column types are optionally defined in self.loader_class.

        Args:
            None

        Raises:
            Nothing

        Returns:
            df (pandas DataFrame)
        """
        file = self.get_infile()
        sheet = self.get_sheet()
        headers = self.get_headers()
        dtypes = self.loader_class.get_column_types(headers)
        df = None
        if dtypes is None:
            df = read_from_file(file, sheet=sheet)
        else:
            df = read_from_file(file, dtype=dtypes, sheet=sheet)
        return df

    def get_headers(self):
        """Returns self.headers.  Initializes them if not set.

        Args:
            None

        Raises:
            Nothing

        Returns:
            headers (namedtuple of TraceBaseLoader.TableHeaders containing strings of header names)
        """
        if hasattr(self, "headers") and self.headers is not None:
            return self.headers
        return self.set_headers()

    def set_headers(self, custom_headers=None):
        """Sets instance's header names.  If no custom headers are provided, it reverts to user-privided and/or class
        defaults.

        There are a few places where headers can be defined:

        - User: Supplied by the user via --headers (a yaml file whose parsing returns a dict).
        - Developer: Supplied via the custom_headers (dict) argument.  (Can be trumped by user supplied headers.)
        - Loader: Defined in the loader_class.  self.loader_class.get_headers() is used to obtain default values by
          header key (in a namedtuple).  (Can be trumped by developer and user headers.)

        Each individual header is assigned in order of precedence:

            User > Developer > Loader

        Args:
            custom_headers (namedtupe of loader_class.TableHeaders): Header names by header key

        Raises:
            Nothing

        Returns:
            headers (namedtupe of loader_class.TableHeaders): Header names by header key
        """
        final_custom_headers = None
        user_headers = (
            read_from_file(self.options["headers"]) if self.options["headers"] else None
        )
        dev_headers = custom_headers

        if user_headers is not None and dev_headers is not None:
            final_custom_headers = {}
            # To support incomplete headers dicts
            for hk in list(set(user_headers.keys()) + set(dev_headers.keys())):
                final_custom_headers[hk] = user_headers.get(
                    hk, dev_headers.get(hk, None)
                )
            if len(final_custom_headers.keys()) == 0:
                final_custom_headers = None
        elif user_headers is not None:
            final_custom_headers = user_headers
        elif dev_headers is not None:
            final_custom_headers = dev_headers

        self.headers = self.loader_class.get_class_headers(final_custom_headers)

        return self.headers

    def get_defaults(self):
        """Returns self.defaults.  Initializes them if not set.

        Args:
            None

        Raises:
            Nothing

        Returns:
            defaults (namedtuple of TraceBaseLoader.TableHeaders containing strings of header names)
        """
        if hasattr(self, "defaults") and self.defaults is not None:
            return self.defaults
        return self.set_defaults()

    def set_defaults(self, custom_defaults=None):
        """Sets instance's default values.  If no custom defaults are provided, it reverts to user-privided and/or class
        defaults.

        There are a few places where defaults can be defined:

        - User: Supplied by the user via the defaults sheet when --infile is an excel file (See defaults_sheet_default.)
        - Developer: Supplied via the custom_defaults (dict) argument.  (Can be trumped by user supplied defaults.)
        - Loader: Defined in the loader_class.  self.loader_class.get_defaults() is used to obtain default values by
          header key (in a namedtuple).  (Can be trumped by developer and user defaults.)

        Each individual header is assigned in order of precedence:

            User > Developer > Loader

        Args:
            custom_defaults (namedtupe of loader_class.TableHeaders): Header names by header key

        Raises:
            Nothing

        Returns:
            defaults (namedtupe of loader_class.TableHeaders): Header names by header key
        """
        final_custom_defaults = None
        user_defaults = self.get_user_defaults()
        dev_defaults = custom_defaults

        if user_defaults is not None and dev_defaults is not None:
            final_custom_defaults = {}
            # To support incomplete defaults dicts
            for hk in list(set(user_defaults.keys()) + set(dev_defaults.keys())):
                final_custom_defaults[hk] = user_defaults.get(
                    hk, dev_defaults.get(hk, None)
                )
            if len(final_custom_defaults.keys()) == 0:
                final_custom_defaults = None
        elif user_defaults is not None:
            final_custom_defaults = user_defaults
        elif dev_defaults is not None:
            final_custom_defaults = dev_defaults

        self.defaults = self.loader_class.get_class_defaults(final_custom_defaults)

        return self.defaults

    def get_user_defaults(self):
        """Retrieves defaults from the defaults excel sheet and converts them into a dict where the keys are header keys
        (matched to values in the first column of the defaults sheet) and the values are from the second column.

        Note, if the user supplies custom options on the command line, it's up to the developer to call
        self.set_defaults with a dict composed of header key keys and values from the command line.

        Args:
            None

        Raises:
            Nothing

        Returns:
            user_defaults (dict): default values by header key
        """
        infile = self.get_infile()

        if not is_excel(infile):
            return None

        defaults_sheet = self.get_defaults_sheet()
        df = read_from_file(infile, sheet=defaults_sheet)

        user_defaults = self.loader_class.get_user_defaults(df, infile, defaults_sheet)

        if len(user_defaults.keys()) == 0:
            user_defaults = None

        return user_defaults

    def get_infile(self):
        """Uses options["infile"] to return the input file name.

        Args:
            None

        Raises:
            Nothing

        Returns:
            infile (str)
        """
        return self.options["infile"]

    def get_dry_run(self):
        """Uses options["dry_run"] to return the dry run mode.

        Args:
            None

        Raises:
            Nothing

        Returns:
            dry_run (boolean)
        """
        return self.options["dry_run"]

    def get_defer_rollback(self):
        """Uses options["defer_rollback"] to return the defer rollback mode.

        Args:
            None

        Raises:
            Nothing

        Returns:
            defer_rollback (boolean)
        """
        return self.options["defer_rollback"]

    def load_data(
        self,
        df=None,
        headers=None,
        defaults=None,
        dry_run=None,
        defer_rollback=None,
        sheet=None,
        defaults_sheet=None,
        file=None,
        **kwargs,
    ):
        """Creates loader_class object in self.loader and calls self.loader.load_data().

        Args:
            df (pandas dataframe):
            headers (TraceBaseLoader.TableHeaders): Custom header names by header key.
            defaults (TraceBaseLoader.TableHeaders): Custom default values by header key.
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
        if df is None:
            df = self.get_dataframe()
        if headers is None:
            headers = self.get_headers()
        if defaults is None:
            defaults = self.get_defaults()
        if dry_run is None:
            dry_run = self.get_dry_run()
        if defer_rollback is None:
            defer_rollback = self.get_defer_rollback()
        if sheet is None:
            sheet = self.get_sheet()
        if file is None:
            file = self.get_infile()

        if self.loader_class is not None:
            # False positive from pylint
            # pylint: disable=not-callable
            self.loader = self.loader_class(
                df,
                headers=headers,
                defaults=defaults,
                dry_run=dry_run,
                defer_rollback=defer_rollback,
                sheet=sheet,
                file=file,
                **kwargs,
            )
            # pylint: enable=not-callable
        else:
            raise ProgrammingError(
                f"{self.__module__}.{type(self).__name__}.loader_class is undefined."
            )

        return self.loader.load_data()

    def check_class_attributes(self):
        """Checks that the class attributes are properly defined.

        Checks existence and type of:
            loader_class (class attribute, TraceBaseLoader class)
            data_sheet_default (class attribute, str)
            defaults_sheet_default (class attribute, str)

        Args:
            None

        Raises:
            AggregatedErrors
                TypeError

        Returns:
            Nothing
        """
        typeerrs = []
        here = f"{type(self).__module__}.{type(self).__name__}"
        if not issubclass(self.loader_class, TraceBaseLoader):
            typeerrs.append(
                f"attribute [{here}.loader_class] TraceBaseLoader required, {type(self.loader_class)} set"
            )

        if type(self.data_sheet_default) != str:
            typeerrs.append(
                f"attribute [{here}.data_sheet_default] str required, {type(self.data_sheet_default)} set"
            )

        if type(self.defaults_sheet_default) != str:
            typeerrs.append(
                f"attribute [{here}.defaults_sheet_default] str required, {type(self.defaults_sheet_default)} set"
            )

        # Immediately raise programming related errors
        if len(typeerrs) > 0:
            aes = AggregatedErrors()
            nlt = "\n\t"
            if len(typeerrs) > 0:
                aes.buffer_error(
                    TypeError(f"Invalid attributes:\n\t{nlt.join(typeerrs)}")
                )
            if aes.should_raise():
                raise aes

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
        msg = "Done.\n"
        if self.options["dry_run"]:
            msg = "Dry-run complete.  The following would occur during a real load:\n"

        load_stats = self.loader.get_load_stats()
        for mdl in self.loader_class.get_models():
            mdl_name = mdl.__name__
            if mdl_name in load_stats.keys():
                msg += "%s records loaded: [%i], skipped: [%i], and errored: [%i]." % (
                    mdl_name,
                    load_stats[mdl_name]["created"],
                    load_stats[mdl_name]["skipped"],
                    load_stats[mdl_name]["errored"],
                )

        if self.saved_aes is not None and self.saved_aes.get_num_errors() > 0:
            status = self.style.ERROR(msg)
        elif self.saved_aes is not None and self.saved_aes.get_num_warnings() > 0:
            status = self.style.WARNING(msg)
        else:
            status = self.style.SUCCESS(msg)

        if self.options["verbosity"] > 0:
            self.stdout.write(status)
