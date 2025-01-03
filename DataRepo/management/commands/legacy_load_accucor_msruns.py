import argparse
import os
from typing import Optional

from django.core.management import BaseCommand, CommandError

from DataRepo.loaders.legacy.accucor_data_loader import AccuCorDataLoader
from DataRepo.models import DataFormat
from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.utils import get_sheet_names, is_excel, read_from_file
from DataRepo.utils.exceptions import WrongExcelSheet
from DataRepo.utils.legacy.lcms_metadata_parser import (
    read_lcms_metadata_from_file,
)


class Command(BaseCommand):
    help = (
        "Loads data from an Accucor or Isocorr excel file (e.g. a workbook containing corrected (and optional "
        "original) data worksheets), or a csv file exported from only the corrected worksheet into tracebase"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--accucor-file",
            type=str,
            help=(
                "Filepath of a peak annotation file. The format of Excel (xlsx) files will be automatically detected"
                "Text (csv) files must use the --data-format argument to specify a format."
            ),
            required=True,
        )
        # Retain for backward compatibility
        parser.add_argument(
            "--isocorr-format",
            required=False,
            action="store_true",
            default=False,
            help="Supply this flag if the file supplied to --accucor-file is an Isocorr csv format file.",
        )
        parser.add_argument(
            "--data-format",
            required=False,
            type=str,
            choices=AccuCorDataLoader.DATA_SHEETS.keys(),
            help="Specify data format (required for csv format files)",
        )
        parser.add_argument(
            "--lcms-file",
            type=str,
            help=(
                "Filepath of either an xlsx or csv file containing metadata associated with the liquid chromatography "
                "and mass spec instrument run."
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
            "--lc-protocol-name",
            type=str,
            help=(
                "Default LCMethod.name of the liquid chromatography protocol used.  Used if --lcms-file is not "
                "supplied, or specifies no LC info for a sample."
            ),
            default=None,
            required=False,
        )
        parser.add_argument(
            "--instrument",
            type=str,
            help=(
                "Default name of the LCMS instrument that analyzed the samples.  Used if --lcms-file is not supplied, "
                "or specifies no instrument for a sample."
            ),
            default=None,
            required=False,
        )
        parser.add_argument(
            "--date",
            type=str,
            help=(
                "Default date MSRun was performed, formatted as YYYY-MM-DD.  Used if --lcms-file is not supplied, or "
                "specifies no date for a sample."
            ),
            default=None,
            required=False,
        )
        parser.add_argument(
            "--researcher",
            type=str,
            help=(
                "Default name or ID of the researcher.  Used if --lcms-file is not supplied, or specifies no "
                "researcher for a sample."
            ),
            default=None,
            required=False,
        )
        parser.add_argument(
            "--skip-samples",
            type=str,
            nargs="+",
            help="List of sample names to skip loading (useful for blank samples)",
            required=False,
        )
        parser.add_argument(
            "--sample-name-prefix",
            type=str,
            help="Sample name prefix",
            default=None,
            required=False,
        )
        # optional dry run argument
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Dry run mode. Will not change the database.",
        )
        # optional new researcher argument (circumvents existing researcher check)
        parser.add_argument(
            "--new-researcher",
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )
        # Used internally by the DataValidationView
        parser.add_argument(
            "--validate",
            required=False,
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )
        # Used internally by the DataValidationView to avoid referencing randomized temporary accucor file names
        parser.add_argument(
            "--accucor-file-name",
            type=str,
            help=argparse.SUPPRESS,
            default=None,
        )
        # Used internally by the validation view, as temporary data should not trigger cache deletions
        parser.add_argument(
            "--skip-cache-updates",
            required=False,
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )

    @MaintainedModel.defer_autoupdates(
        disable_opt_names=["validate", "dry_run"],
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def handle(self, *args, **options):
        lcms_metadata_df = None
        if options["lcms_file"] is not None:
            lcms_metadata_df = read_lcms_metadata_from_file(options["lcms_file"])

        fmt = self.determine_data_format(
            peak_annot_file=options["accucor_file"],
            options=options,
        )
        if fmt is None:
            if is_excel(options["accucor_file"]):
                msg = (
                    "Sheets in Excel file did not match a known format:\n"
                    f"{AccuCorDataLoader.DATA_SHEETS}"
                )
            else:
                msg = (
                    "Text (csv) files require the use of the --data-format flag to "
                    f"specifiy a format: {AccuCorDataLoader.DATA_SHEETS.keys()}"
                )
            raise CommandError(
                f"Unknown peak annotation file format for file: {options['accucor_file']}\n"
                f"{msg}"
            )
        print(f"Reading {fmt} file: {options['accucor_file']}")
        print(f"LOADING WITH PREFIX: {options['sample_name_prefix']}")

        self.extract_dataframes_from_peakannotation_file(
            peak_annot_file=options["accucor_file"],
            fmt=fmt,
        )

        peak_annotation_file = options["accucor_file"]

        # This name is used internally by the validation interface because the form renames the file with a random hash
        # value that is unrecognizable to users in an error
        peak_annotation_filename = os.path.basename(options["accucor_file"]).strip()
        if options["accucor_file_name"] is not None:
            peak_annotation_filename = options["accucor_file_name"]

        mzxml_files = None
        if options["mzxml_files"] is not None and len(options["mzxml_files"]) > 0:
            mzxml_files = options["mzxml_files"]

        loader = AccuCorDataLoader(
            # Peak annotation file data
            data_format=fmt,
            accucor_original_df=self.original,
            accucor_corrected_df=self.corrected,
            peak_annotation_file=peak_annotation_file,
            # LCMS metadata
            lcms_metadata_df=lcms_metadata_df,
            # LCMS batch defaults
            date=options["date"],
            lc_protocol_name=options["lc_protocol_name"],
            researcher=options["researcher"],
            instrument=options["instrument"],
            mzxml_files=mzxml_files,
            peak_annotation_filename=peak_annotation_filename,
            # Sample options
            skip_samples=options["skip_samples"],
            sample_name_prefix=options["sample_name_prefix"],
            # Modes
            allow_new_researchers=options["new_researcher"],
            validate=options["validate"],
            verbosity=options["verbosity"],
            dry_run=options["dry_run"],
            update_caches=not options["skip_cache_updates"],
        )

        loader.load_accucor_data()

        print(f"Done loading {fmt} data into MsRun, PeakGroups, and PeakData")

    def extract_dataframes_from_peakannotation_file(
        self, peak_annot_file: str, fmt: DataFormat
    ) -> None:
        # Validate the format (Accucor vs Isocorr) using the sheet names (returns None if not an excel file)
        sheet_names = None
        if is_excel(peak_annot_file):
            sheet_names = get_sheet_names(peak_annot_file)
        if fmt.code == AccuCorDataLoader.ISOCORR_FORMAT_CODE:
            if sheet_names is not None and "absolte" not in sheet_names:
                raise WrongExcelSheet("Isocorr", sheet_names[1], "absolte", 2)

            self.original = (
                None  # We don't need the "original" sheet for isocorr format
            )
            corrected_sheet_name = "absolte"
        elif fmt.code == AccuCorDataLoader.ACCUCOR_FORMAT_CODE:
            if sheet_names is not None:
                if "Original" not in sheet_names:
                    raise WrongExcelSheet("Accucor", sheet_names[0], "Original", 1)
                if "Corrected" not in sheet_names:
                    raise WrongExcelSheet("Accucor", sheet_names[1], "Corrected", 2)
                # get the "original" sheet when in accucor format
                self.original = read_from_file(peak_annot_file, sheet="Original")
            else:
                self.original = (
                    None  # There is no original sheet if the file is not an excel file
                )
            corrected_sheet_name = "Corrected"
        elif fmt.code == AccuCorDataLoader.ISOAUTOCORR_FORMAT_CODE:
            if sheet_names is not None:
                if "original" not in sheet_names:
                    raise WrongExcelSheet("IsoAutoCorr", sheet_names[0], "original", 1)
                if "cor_abs" not in sheet_names:
                    raise WrongExcelSheet("IsoAutoCorr", sheet_names[2], "cor_abs", 3)
                # get the "original" sheet when in isoautocorr format
                self.original = read_from_file(peak_annot_file, sheet="original")
            else:
                self.original = (
                    None  # There is no original sheet if the file is not an excel file
                )
            corrected_sheet_name = "cor_abs"

        self.corrected = read_from_file(peak_annot_file, sheet=corrected_sheet_name)

    def determine_data_format(
        self, peak_annot_file: str, options: dict
    ) -> Optional[DataFormat]:
        """Detect format of Excel files, otherwise use arguments to determine format

        Args:
            is_isocorr: boolean flag indicating if file is isocorrr (ignored for Excel files)
            peak_annot_file: peak annotation file name

        Returns:
            fmt: DataFormat of peak annotation file, None if it cannot be determined
        """

        fmt = None
        if is_excel(peak_annot_file):
            # Detect format of Excel files
            fmt = AccuCorDataLoader.detect_data_format(peak_annot_file)
        elif options["data_format"]:
            # csv format with data-format specified
            fmt = DataFormat.objects.get(code=options["data_format"])
        elif options["isocorr_format"]:
            # csv and isocorr_format specified
            fmt = DataFormat.objects.get(code="isocorr")
        return fmt
