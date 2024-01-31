import argparse

from django.core.management import BaseCommand

from DataRepo.utils.loader import TraceBaseLoader
from DataRepo.utils import (
    AggregatedErrors,
    DryRun,
    is_excel,
    read_from_file,
)


class LoadFromTableCommand(BaseCommand):
    """Command superclass to be used to load a database given a table-like file.

    This class establishes the basic common command line interface for loading a database from a table-like file
    (supported file types: excel, tsv, csv).  It also handles common errors and presents them to the user in the context
    of the input file.

    Use this class as the base class for any command that intends to load the database from a table-like file.

    Usage:
        1. Inherit from LoadTableCommand
        2. Do not manually decorate a derived class handle method with @LoadTableCommand.handler.  Decorations of the
            handle method happen automatically via __init__.
        3. If you define a derived class add_arguments method, be sure to call super().add_arguments(parser)

    Attributes:
        help (str): Default help string to be printed when the CLI is used with the help command.
        loader_class (TraceBaseLoader derived class): A derived class of TraceBaseLoader.  This class defines headers,
            data constraints, data types, effected database models/fields etc.  LoadTableCommand uses this class to be
            able to rerad the infile correctly.
        sheet_default (str): Default name of the excel sheet (though note that an option to define a custom name is
            provided).
    """
    help = "Loads data from a file into the database."
    loader_class = None
    sheet_default = None

    def __init__(self):
        """This init auto-applies a decorator to the derived class's handle method."""
        # Apply the handler decorator to the handle method in the derived class
        self.apply_handle_wrapper()

    def apply_handle_wrapper(self):
        """This applies a decorator to the derived class's handle method.

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
        bound = decorated_derived_class_method.__get__(self)
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
                f"[{self.sheet_default}]."
            ),
            default=self.sheet_default,
        )

        parser.add_argument(
            "--headers",
            type=str,
            help=f"YAML file defining headers to be used.",
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
        """

        def handle_wrapper(self, *args, **options):
            self.check_class_attributes()
            self.saved_aes = None
            self.options = options

            try:

                fn(self, *args, **options)

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

        return handle_wrapper

    def get_sheet(self):
        """Uses options["sheet"] to return the sheet name.

        Note that self.sheet_default is set as the default for the --sheet option.  See add_arguments().

        Args:
            None

        Raises:
            Nothing

        Returns:
            sheet (str)
        """
        return self.options["sheet"] if is_excel(self.get_infile()) else None

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

    def get_headers(self, custom_default_header_data=None):
        """Uses options["headers"] to return header names by header key.

        Note that self.loader_class.get_headers() is used to return the headers spec, which is either
        self.loader_class.DefaultHeaders or a headers spec as customized via the custom_default_header_data argument.

        Args:
            custom_default_header_data (namedtuple of TraceBaseLoader.TableHeaders): If header data was not parsed from
                a file supplied by --headers, these custom defaults are used.  If no custom defaults are supplied, it
                falls back to the default headers defined in self.loader_class.DefaultHeaders (as implemented in
                self.loader_class.get_headers()).

        Raises:
            Nothing

        Returns:
            headers (namedtuple of TraceBaseLoader.TableHeaders containing strings of header names)
        """
        header_data = (
            read_from_file(self.options["headers"]) if self.options["headers"]
            else None
        )
        if header_data is None and custom_default_header_data is not None:
            header_data = custom_default_header_data

        return self.loader_class.get_headers(header_data)

    def get_defaults(self, defaults_dict=None):
        """Uses self.loader_class.get_defaults() to return default values by header key.

        Note that self.loader_class.get_defaults() is used to return the default values, which is either
        self.loader_class.DefaultValues or a copy that has been modified to contain the supplied defaults.  Defaults for
        unsupplied header keys will remain as they are defined in self.loader_class.DefaultValues.

        Args:
            defaults_dict (dict of default values by header key): A dict is taken as an argument so that only the
                columns with desired default values can be supplied.  The header keys are the same as those in a
                namedtuple of TraceBaseLoader.TableHeaders.

        Raises:
            Nothing

        Returns:
            defaults (Optional[namedtuple of TraceBaseLoader.TableHeaders containing default values])
        """
        return self.loader_class.get_defaults(defaults_dict)

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

    def load_data(self):
        """Calls self.loader.load_data().

        Note, self.loader must have been set inside the handle method of the derived class before calling this method.
        See self.set_loader().

        Args:
            None

        Raises:
            Nothing

        Returns:
            Nothing
        """
        return self.loader.load_data()

    def set_loader(self, loader):
        """Sets self.loader to the supplied loader.

        Args:
            loader (TraceBaseLoader): A derived class whose superclass is TraceBaseLoader and implements a load_data()
                method.

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.loader = loader

    def check_attributes(self):
        """Checks that the class and instance attributes are properly defined.

        Checks existence and type of:
            loader_class (class attribute, TraceBaseLoader class)
            sheet_default (class attribute, str)
            loader (instance attribute, TraceBaseLoader object)

        Args:
            None

        Raises:
            AggregatedErrors
                ValueError
                TypeError

        Returns:
            Nothing
        """
        undefs = []
        typeerrs = []
        if self.loader_class is None:
            undefs.append("loader_class")
        elif not issubclass(self.loader_class, TraceBaseLoader):
            typeerrs.append(
                f"attribute [{self.__name__}.loader_class] TraceBaseLoader required, {type(self.loader_class)} set"
            )

        if self.sheet_default is None:
            undefs.append("sheet_default")
        elif type(self.sheet_default) != str:
            typeerrs.append(
                f"attribute [{self.__name__}.sheet_default] str required, {type(self.sheet_default)} set"
            )

        if not hasattr(self, "loader"):
            undefs.append(f"{type(self).__name__}.set_loader() has not been called.")
        elif type(self.loader) != self.loader_class:
            typeerrs.append(
                f"member [{self.__name__}.loader] {self.loader_class.__name__} required, {type(self.loader)} set"
            )

        # Immediately raise programming related errors
        if len(undefs) > 0 or len(typeerrs) > 0:
            aes = AggregatedErrors()
            nlt = "\n\t"
            if len(undefs) > 0:
                aes.buffer_error(ValueError(f"Required attributes missing:\n{nlt.join(undefs)}"))
            elif len(typeerrs) > 0:
                aes.buffer_error(TypeError(f"Invalid attributes:\n{nlt.join(typeerrs)}"))
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
        for mdl in self.loader_class.models:
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
