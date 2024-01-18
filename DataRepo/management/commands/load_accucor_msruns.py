import argparse
import os

from django.core.management import BaseCommand

from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.utils import (
    AccuCorDataLoader,
    get_sheet_names,
    is_excel,
    read_from_file,
)
from DataRepo.utils.exceptions import WrongExcelSheet
from DataRepo.utils.lcms_metadata_parser import read_lcms_metadata_from_file


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
                "Filepath of either an Accucor xlsx output, an Accucor csv export of the corrected data worksheet, "
                "or (with --isocorr-format) an Isocorr corrected data csv output.  The excel sheet names must be: "
                "Accucor: 'Original' and 'Corrected'; Isocorr: 'absolte'."
            ),
            required=True,
        )
        parser.add_argument(
            "--isocorr-format",
            required=False,
            action="store_true",
            default=False,
            help="Supply this flag if the file supplied to --accucor-file is an Isocorr csv format file.",
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
            "--polarity",
            type=str,
            help=(
                "Default ion mode of the LCMS instrument that analyzed the samples.  Used if --lcms-file is not "
                "supplied, or specifies no polarity for a sample."
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

        fmt = "Isocorr" if options["isocorr_format"] else "Accucor"
        print(f"Reading {fmt} file: {options['accucor_file']}")
        print(f"LOADING WITH PREFIX: {options['sample_name_prefix']}")

        self.extract_dataframes_from_peakannotation_file(
            options["isocorr_format"], options["accucor_file"]
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
            isocorr_format=options["isocorr_format"],
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
            polarity=options["polarity"],
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

    def extract_dataframes_from_peakannotation_file(self, is_isocorr, peak_annot_file):
        # Validate the format (Accucor vs Isocorr) using the sheet names (returns None if not an excel file)
        sheet_names = None
        if is_excel(peak_annot_file):
            sheet_names = get_sheet_names(peak_annot_file)
        if is_isocorr:
            if sheet_names is not None and "absolte" not in sheet_names:
                raise WrongExcelSheet("Isocorr", sheet_names[1], "absolte", 2)

            self.original = (
                None  # We don't need the "original" sheet for isocorr format
            )
            corrected_sheet_name = "absolte"
        else:
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

        self.corrected = read_from_file(peak_annot_file, sheet=corrected_sheet_name)
