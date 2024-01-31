import argparse

from django.core.management import BaseCommand

from DataRepo.utils.loader import TraceBaseLoader
from DataRepo.utils import (
    AggregatedErrors,
    DryRun,
    is_excel,
    read_from_file,
)


class LoadTableCommand(BaseCommand):
    help = "Loads data from a file into the database."
    loader_class = None
    sheet_default = None

    def add_arguments(self, parser):
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
    def handler(fn):
        """Decorator to be applied to a Command class's handle method.
        Adds a wrapper to handle the common tasks amongst all the classes that provide load commands.
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
        return self.options["sheet"] if is_excel(self.get_infile()) else None

    def get_dataframe(self):
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
        header_data = (
            read_from_file(self.options["headers"]) if self.options["headers"]
            else None
        )
        if header_data is None and custom_default_header_data is not None:
            header_data = custom_default_header_data
        return self.loader_class.get_headers(header_data)

    def get_defaults(self, defaults_dict=None):
        return self.loader_class.get_defaults(defaults_dict)

    def get_infile(self):
        return self.options["infile"]

    def get_dry_run(self):
        return self.options["dry_run"]

    def get_defer_rollback(self):
        return self.options["defer_rollback"]

    def load_data(self):
        return self.loader.load_data()

    def set_loader(self, loader):
        self.loader = loader

    def check_class_attributes(self):
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
        nlt = "\n\t"
        if len(undefs) > 0:
            raise ValueError(f"Required attributes missing:\n{nlt.join(undefs)}")
        elif len(typeerrs) > 0:
            raise TypeError(f"Invalid attributes:\n{nlt.join(typeerrs)}")


    def report_status(self):
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
