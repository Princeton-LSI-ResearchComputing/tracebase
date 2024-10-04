from typing import Type

from DataRepo.loaders import MSRunsLoader, PeakAnnotationFilesLoader
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.peak_group_conflicts import PeakGroupConflicts
from DataRepo.management.commands.load_table import LoadTableCommand
from DataRepo.utils.exceptions import ConditionallyRequiredOptions
from DataRepo.utils.file_utils import read_from_file


class Command(LoadTableCommand):
    """Command to load all of the peak annotation files in a study doc's Peak Annotation Files sheet."""

    help = "Loads data from all peak annotation files in 1 study."
    loader_class: Type[TableLoader] = PeakAnnotationFilesLoader

    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        # These are optionally used to retrieve specific MSRunSample records needed to link PeakGroup records.  See the
        # options below for an alternative way of finding MSRunSample records using MSRunSequence defaults.
        parser.add_argument(
            "--peak-annotation-details-file",
            type=str,
            help=(
                f"Path to either a tab-delimited or excel file (with a sheet named '{MSRunsLoader.DataSheetName}' "
                "- See --data-sheet).  See: [load_msruns.py --infile] for column composition."
            ),
            required=False,
        )
        parser.add_argument(
            "--peak-annotation-details-sheet",
            type=str,
            help=(
                "Name of excel sheet/tab.  Only used if --peak-annotation-details-file is an excel spreadsheet.  "
                "Default: [%(default)s]."
            ),
            default=MSRunsLoader.DataSheetName,
            required=False,
        )

        # These are optionally used to resolve PeakGroup conflicts arising from the unique constraint that there may
        # only be 1 peak group compound representation per sample.
        parser.add_argument(
            "--peak-group-conflicts-file",
            type=str,
            help=(
                "Path to either a tab-delimited or excel file (with a sheet named "
                f"'{PeakGroupConflicts.DataSheetName}' - See --data-sheet)."
            ),
            required=False,
        )
        parser.add_argument(
            "--peak-group-conflicts-sheet",
            type=str,
            help=(
                "Name of excel sheet/tab.  Only used if --peak-group-conflicts-file is an excel spreadsheet.  "
                "Default: [%(default)s]."
            ),
            default=PeakGroupConflicts.DataSheetName,
            required=False,
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
            Raises:
                ConditionallyRequiredOptions
            Buffers:
                None
        Returns:
            None
        """
        peak_group_conflicts_file = None
        peak_group_conflicts_sheet = None
        peak_group_conflicts_df = None

        peak_annotation_details_file = None
        peak_annotation_details_sheet = None
        peak_annotation_details_df = None

        if options.get("infile") is not None:
            peak_group_conflicts_file = options.pop("peak_group_conflicts_file", None)
            peak_group_conflicts_sheet = options.pop("peak_group_conflicts_sheet", None)
            if peak_group_conflicts_file is not None:
                peak_group_conflicts_df = read_from_file(
                    peak_group_conflicts_file, peak_group_conflicts_sheet
                )

            peak_annotation_details_file = options.pop(
                "peak_annotation_details_file", None
            )
            peak_annotation_details_sheet = options.pop(
                "peak_annotation_details_sheet", None
            )
            if peak_annotation_details_file is not None:
                peak_annotation_details_df = read_from_file(
                    peak_annotation_details_file, peak_annotation_details_sheet
                )
        elif (
            options.pop("peak_annotation_details_file", None) is None
            or options.pop("peak_group_conflicts_file", None) is None
        ):
            raise ConditionallyRequiredOptions(
                "--infile is required if --peak-annotation-details-file or --peak-group-conflicts-file is supplied."
            )

        # The MSRunsLoader class constructor has custom arguments, so once we get them off the command line, we must
        # call init_loader (again) to supply them.
        self.init_loader(
            peak_annotation_details_file=peak_annotation_details_file,
            peak_annotation_details_sheet=peak_annotation_details_sheet,
            peak_annotation_details_df=peak_annotation_details_df,
            peak_group_conflicts_file=peak_group_conflicts_file,
            peak_group_conflicts_sheet=peak_group_conflicts_sheet,
            peak_group_conflicts_df=peak_group_conflicts_df,
        )

        # TODO: (This needs to be done in all the loaders:) Catch AggregatedErrors exceptions and repackage as
        # CommandErrors to add command line flags to suggestions in applicable exception types.
        self.load_data()
