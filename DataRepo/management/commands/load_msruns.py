import os
import sys
from typing import Type

from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.management.commands.load_table import LoadTableCommand
from DataRepo.utils.exceptions import (
    ConditionallyRequiredOptions,
    MutuallyExclusiveOptions,
)


class Command(LoadTableCommand):
    """Command to load the MSRunSample model from mzXML files and/or a table-like file."""

    help = "Loads data from mzXML files and/or a MSRunSample table into the database"
    loader_class: Type[TableLoader] = MSRunsLoader

    def __init__(self, *args, **kwargs):
        # Don't require any options (i.e. don't require the --infile option)
        super().__init__(
            *args,
            required_optnames=[],
            **kwargs,
        )

    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        # Add additional options for this specific script
        parser.add_argument(
            "--mzxml-dir",
            type=str,
            help=(
                "The root directory of all mzXML files (containing instrument run data) associated with the "
                f"{MSRunsLoader.DataSheetName} sheet."
            ),
            default=None,
            required=False,
        )
        parser.add_argument(
            "--mzxml-files",
            type=str,
            help="Filepaths of mzXML files containing instrument run data.",
            default=None,
            required=False,
            nargs="*",
        )
        parser.add_argument(
            "--operator",
            type=str,
            help=(
                "Default researcher (who operated the mass spec).  Used to assign each MS Run Sample to an MS Run "
                "Sequence (only used if --infile is either not provided or column "
                f"{self.loader_class.DataHeaders.SEQNAME} has no value).  Mutually exclusive with a default "
                f"{SequencesLoader.DataHeaders.OPERATOR} defined in either --defaults-file or --defaults-sheet."
            ),
            required=False,
        )
        parser.add_argument(
            "--date",
            type=str,
            help=(
                "Default date the mass spec was run.  Used to assign each MS Run Sample to an MS Run Sequence (only "
                f"used if --infile is either not provided or column {self.loader_class.DataHeaders.SEQNAME} has no "
                f"value).  Mutually exclusive with a default {SequencesLoader.DataHeaders.DATE} defined in either "
                "--defaults-file or --defaults-sheet."
            ),
            required=False,
        )
        parser.add_argument(
            "--lc-protocol-name",
            type=str,
            help=(
                "Default liquid chromatography protocol name.  Used to assign each MS Run Sample to an MS Run Sequence "
                f"(only used if --infile is either not provided or column {self.loader_class.DataHeaders.SEQNAME} "
                f"has no value).  Mutually exclusive with a default {SequencesLoader.DataHeaders.LCNAME} defined in "
                "either --defaults-file or --defaults-sheet."
            ),
            required=False,
        )
        parser.add_argument(
            "--instrument",
            type=str,
            help=(
                "Default mass spec instrument name.  Used to assign each MS Run Sample to an MS Run Sequence (only "
                f"used if --infile is either not provided or column {self.loader_class.DataHeaders.SEQNAME} has no "
                f"value).  Mutually exclusive with a default {SequencesLoader.DataHeaders.INSTRUMENT} defined in "
                "either --defaults-file or --defaults-sheet."
            ),
            required=False,
        )
        parser.add_argument(
            "--exact-mode",
            action="store_true",
            default=False,
            help=(
                f"When the {MSRunsLoader.DataHeaders.MZXMLNAME} column is empty, consider underscores and dashes to be "
                "equivalent when matching mzXML files with peak annotation file sample headers.  When true, only allow "
                "exact matches."
            ),
        )

    def handle(self, *args, **options):
        """Code to run when the command is called from the command line.

        This code is automatically wrapped by LoadTableCommand._handler, which handles:
            - Retrieving the base-class-provided option values (and fills in the defaults provided by the loader_class)
            - Atomic transactions with optionally deferred rollback
            - Exception handling:
                - DryRun Exceptions
                - Contextualization of exceptions to the associated input in the file
            - Validation
                - Header and data type
                - Unique file constraints

        Args:
            options (dict of strings): String values provided on the command line by option name.
        Exceptions:
            None
        Returns:
            None
        """
        if (
            len(sys.argv) == 2 and sys.argv[1] == "load_msruns"
        ):  # ['manage.py', 'load_msruns']
            self.print_help(
                "manage.py", list(os.path.splitext(os.path.basename(__file__)))[1]
            )
            self.options["help"] = True
            return

        if (
            options.get("mzxml_files") is not None
            and options.get("mzxml_dir") is not None
        ):
            raise MutuallyExclusiveOptions(
                "--mzxml-files and --mzxml-dir are mutually exclusive options."
            )

        mzxml_files = MSRunsLoader.get_mzxml_files(
            files=options.get("mzxml_files"), dir=options.get("mzxml_dir")
        )

        if len(mzxml_files) == 0 and options.get("infile") is None:
            raise ConditionallyRequiredOptions(
                "Either --mzxml-dir (with a directory containing mzxml files), --mzxml-files, or --infile is "
                "required."
            )

        # Check conditionally required options
        if (
            len(mzxml_files) > 0
            and options.get("infile") is None
            and options.get("defaults_file") is None
            and (
                options.get("operator") is None
                or options.get("date") is None
                or options.get("lc_protocol_name") is None
                or options.get("instrument") is None
            )
        ):
            missing = [
                f"{flag}"
                for flag in ["operator", "date", "lc_protocol_name", "instrument"]
                if options.get(flag) is None
            ]
            missing_str = ""
            if len(missing) > 0:
                missing_str = f" or {missing}"
            raise ConditionallyRequiredOptions(
                "When mzxml files are supplied (using --mzxml-dir or --mzxml-files) without an --infile, either a "
                "--defaults-file must be provided or each of these default options must all be supplied: --operator, "
                f"--date, --lc-protocol-name, and --instrument.  Missing: infile or defaults_file{missing_str}."
            )

        # The MSRunsLoader class constructor has custom arguments, so we must call init_loader to supply them
        self.init_loader(
            mzxml_files=mzxml_files,
            mzxml_dir=options.get("mzxml_dir"),
            operator=options.get("operator"),
            date=options.get("date"),
            lc_protocol_name=options.get("lc_protocol_name"),
            instrument=options.get("instrument"),
            exact_mode=options.get("exact_mode"),
        )
        self.load_data()
