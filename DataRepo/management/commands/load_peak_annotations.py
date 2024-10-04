from typing import Type

from django.core.management import CommandError

from DataRepo.loaders import (
    AccucorLoader,
    IsoautocorrLoader,
    IsocorrLoader,
    MSRunsLoader,
    PeakAnnotationsLoader,
    SequencesLoader,
    UnicorrLoader,
)
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.peak_group_conflicts import PeakGroupConflicts
from DataRepo.management.commands.load_table import LoadTableCommand
from DataRepo.utils.exceptions import ConditionallyRequiredOptions
from DataRepo.utils.file_utils import read_from_file


class Command(LoadTableCommand):
    """Command to load the ArchiveFile, PeakData, PeakDataLabel, PeakGroup, PeakGroupLabel, and PeakGroupCompound models
    from a table-like file."""

    help = "Loads data from a table-like file into the database"
    loader_class: Type[TableLoader] = UnicorrLoader

    def __init__(self, *args, **kwargs):
        # Don't require any options (i.e. don't require the --infile option)
        super().__init__(
            *args,
            required_optname_groups=[["mzxml_files", "infile"]],
            opt_defaults={"data_sheet": None},
            custom_loader_init=True,
            **kwargs,
        )

    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        # TODO: Check to make sure that infile and defaults-file can be supplied together.  infile here is not a study
        # excel doc.  Might consider adding a defaults sheet to the converted df?  Give it a think.

        # Add additional options for this specific script

        # This option overrides dynamic format determination.
        parser.add_argument(
            "--format",
            type=str,
            help=(
                f"{{{PeakAnnotationsLoader.get_supported_formats()}}} Format of the peak annotations file (--infile).  "
                "Default: dynamically determined format."
            ),
            choices=PeakAnnotationsLoader.get_supported_formats(),
            required=False,
        )

        # These are optionally used to resolve peak group conflicts.
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
            default=MSRunsLoader.DataSheetName,
            required=False,
        )

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

        # These are used to imbibe defaults for finding MSRunSequence when there is no peak-annotation-details-file
        # supplied, or when that file doesn't fully account for all headers in the supplied peak annotations file.  (The
        # only ones that MUST be supplied are ones where the sample header names don't match the names of the samples in
        # the database or when the headers are not unique across all peak annotation files (i.e. there are multiple
        # mzXML files with the same name)).
        parser.add_argument(
            "--operator",
            type=str,
            help=(
                "Default researcher (name) who operated the mass spec.  Used to assign each MS Run Sample to an MS Run "
                "Sequence (only used if --peak-annotation-details-file is either not provided or column "
                f"{SequencesLoader.DataHeaders.SEQNAME} has no value).  Mutually exclusive with a default "
                f"{SequencesLoader.DataHeaders.OPERATOR} defined in either --defaults-file or --defaults-sheet."
            ),
            required=False,
        )
        parser.add_argument(
            "--date",
            type=str,
            help=(
                "Default date the mass spec was run.  Used to assign each MS Run Sample to an MS Run Sequence (only "
                "used if --peak-annotation-details-file is either not provided or column "
                f"{SequencesLoader.DataHeaders.SEQNAME} has no value).  Mutually exclusive with a default "
                f"{SequencesLoader.DataHeaders.DATE} defined in either --defaults-file or --defaults-sheet."
            ),
            required=False,
        )
        parser.add_argument(
            "--lc-protocol-name",
            type=str,
            help=(
                "Default liquid chromatography protocol name.  Used to assign each MS Run Sample to an MS Run Sequence "
                "(only used if --peak-annotation-details-file is either not provided or column "
                f"{SequencesLoader.DataHeaders.SEQNAME} has no value).  Mutually exclusive with a default "
                f"{SequencesLoader.DataHeaders.LCNAME} defined in either --defaults-file or --defaults-sheet."
            ),
            required=False,
        )
        parser.add_argument(
            "--instrument",
            type=str,
            help=(
                "Default mass spec instrument name.  Used to assign each MS Run Sample to an MS Run Sequence (only "
                "used if --peak-annotation-details-file is either not provided or column "
                f"{SequencesLoader.DataHeaders.SEQNAME} has no value).  Mutually exclusive with a default "
                f"{SequencesLoader.DataHeaders.INSTRUMENT} defined in either --defaults-file or --defaults-sheet."
            ),
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
                CommandError
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

        if options.get("format") is not None:
            matching_formats = [options.get("format")]
        else:
            matching_formats = PeakAnnotationsLoader.determine_matching_formats(
                # Do not enforce column types when we don't know what columns exist yet
                self.get_dataframe(typing=False)
            )

        if len(matching_formats) == 1:
            if matching_formats[0] == AccucorLoader.format_code:
                self.loader_class = AccucorLoader
            elif matching_formats[0] == IsocorrLoader.format_code:
                self.loader_class = IsocorrLoader
            elif matching_formats[0] == IsoautocorrLoader.format_code:
                self.loader_class = IsoautocorrLoader
            elif matching_formats[0] == UnicorrLoader.format_code:
                self.loader_class = UnicorrLoader
            else:
                raise CommandError(f"Unrecognized format code: {matching_formats}.")
        elif len(matching_formats) == 0:
            raise CommandError(
                "No matching formats.  Please supply one of the supported formats "
                f"{PeakAnnotationsLoader.get_supported_formats()} using --format."
            )
        else:
            raise CommandError(
                "Unable to determine the file format.  Please supply one of these multiple matching formats to the "
                f"--format option: {matching_formats}."
            )

        # The MSRunsLoader class constructor has custom arguments, so we must call init_loader to supply them
        self.init_loader(
            peak_group_conflicts_file=peak_group_conflicts_file,
            peak_group_conflicts_sheet=peak_group_conflicts_sheet,
            peak_group_conflicts_df=peak_group_conflicts_df,
            peak_annotation_details_file=peak_annotation_details_file,
            peak_annotation_details_sheet=peak_annotation_details_sheet,
            peak_annotation_details_df=peak_annotation_details_df,
            operator=options.get("operator"),
            date=options.get("date"),
            lc_protocol_name=options.get("lc_protocol_name"),
            instrument=options.get("instrument"),
        )

        # TODO: (This needs to be done in all the loaders:) Catch AggregatedErrors exceptions and add command line flags
        # to suggestions in applicable exception types.
        self.load_data()
