import base64
import os.path
import re
import shutil
import tempfile
from collections import defaultdict
from io import BytesIO
from sqlite3 import ProgrammingError
from typing import Dict, List, Optional, cast

import pandas as pd
import yaml  # type: ignore
from django.conf import settings
from django.core.management import call_command
from django.shortcuts import redirect, render
from django.views.generic.edit import FormView
from jsonschema import ValidationError

from DataRepo.forms import DataSubmissionValidationForm
from DataRepo.loaders.accucor_data_loader import AccuCorDataLoader
from DataRepo.loaders.protocols_loader import ProtocolsLoader
from DataRepo.loaders.sample_table_loader import SampleTableLoader
from DataRepo.loaders.tissues_loader import TissuesLoader
from DataRepo.models import LCMethod, MSRunSample, MSRunSequence, Researcher
from DataRepo.models.protocol import Protocol
from DataRepo.utils.exceptions import (
    AllMissingSamplesError,
    AllMissingTissues,
    AllMissingTreatments,
    MissingDataAdded,
    MultiLoadStatus,
    NonUniqueSampleDataHeader,
    NonUniqueSampleDataHeaders,
)
from DataRepo.utils.file_utils import read_from_file
from DataRepo.utils.lcms_metadata_parser import (
    LCMS_DB_SAMPLE_HDR,
    LCMS_FL_SAMPLE_HDR,
    LCMS_PEAK_ANNOT_HDR,
)

# TODO: If these are still unused once the study submission refactor is done, delete them.
# from sqlite3 import ProgrammingError
# from DataRepo.loaders.accucor_data_loader import get_sample_headers
# from DataRepo.utils.file_utils import read_headers_from_file


class DataValidationView(FormView):
    form_class = DataSubmissionValidationForm
    template_name = "DataRepo/validate_submission.html"
    success_url = ""
    accucor_filenames: List[str] = []
    accucor_files: List[str] = []
    isocorr_filenames: List[str] = []
    isocorr_files: List[str] = []
    animal_sample_filename = None
    animal_sample_file = None
    submission_url = settings.DATA_SUBMISSION_URL
    # These are common suffixes repeatedly appended to accucor/isocorr sample names to make them unique across different
    # polarities and scan ranges.  This is not perfect.  See the get_approx_sample_header_replacement_regex method for
    # the full pattern.
    DEFAULT_SAMPLE_HEADER_SUFFIXES = [
        r"_pos",
        r"_neg",
        r"_scan[0-9]+",
    ]
    # Study doc version (default and supported list)
    default_version = "2"
    supported_versions = [default_version]
    ANIMALS_SHEET = "Animals"
    SAMPLES_SHEET = "Samples"

    def __init__(self):
        super().__init__()
        self.autofill_dict = {
            self.SAMPLES_SHEET: defaultdict(dict),
            TissuesLoader.DataSheetName: defaultdict(dict),
            ProtocolsLoader.DataSheetName: defaultdict(dict),
        }
        self.extracted_exceptions = {
            AllMissingSamplesError.__name__: {"errors": [], "warnings": []},
            AllMissingTissues.__name__: {"errors": [], "warnings": []},
            AllMissingTreatments.__name__: {"errors": [], "warnings": []},
        }
        self.valid = None
        self.results = {}
        self.exceptions = {}
        self.ordered_keys = []
        self.load_status_data: Optional[MultiLoadStatus] = None
        self.tissues_loader = TissuesLoader()
        # Providing a dummy excel file will change the headers in the returned column types to the custom excel headers
        # TODO: Make it possible to explicitly set the type of headers we want so that a dummy file name is not required
        self.treatments_loader = ProtocolsLoader(file="dummy.xlsx")

    def set_files(
        self,
        sample_file=None,
        sample_filename: Optional[str] = None,
        peak_annotation_files=None,
        peak_annotation_filenames: Optional[List[str]] = None,
    ):
        """
        This method allows the files to be set.  It takes 2 different optional params for file names (that are used in
        reporting) to accommodate random temporary file names.  If file names are not supplied, the basename of the
        actual files is used for reporting.
        """
        self.animal_sample_file = sample_file
        self.animal_sample_filename = sample_filename
        if sample_filename is None and sample_file is not None:
            self.animal_sample_filename = str(os.path.basename(sample_file))

        if self.animal_sample_file is not None:
            # Refresh the loader objects using the actual supplied file
            self.tissues_loader = TissuesLoader(file=self.animal_sample_file)
            # Providing an excel file will change the headers in the returned column types to the custom excel headers
            self.treatments_loader = ProtocolsLoader(file=self.animal_sample_file)

        if (
            peak_annotation_filenames is None
            and peak_annotation_files is not None
            and len(peak_annotation_files) > 0
        ):
            peak_annotation_filenames = [
                str(os.path.basename(f)) for f in peak_annotation_files
            ]

        if (
            peak_annotation_filenames is not None
            and len(peak_annotation_filenames) > 0
            and (
                peak_annotation_files is None
                or len(peak_annotation_filenames) != len(peak_annotation_files)
            )
        ):
            raise ProgrammingError(
                f"The number of peak annotation file names [{len(peak_annotation_filenames)}] must be equal to the "
                f"number of peak annotation files [{peak_annotation_files}]."
            )

        not_peak_annot_files = []
        self.accucor_files = []
        self.isocorr_files = []
        self.accucor_filenames = []
        self.isocorr_filenames = []
        if peak_annotation_files is not None and len(peak_annotation_files) > 0:
            # Convince mypy that peak_annotation_files is defined
            peak_annot_filenames: List[str] = cast(List[str], peak_annotation_filenames)
            for index, peak_annot_file in enumerate(peak_annotation_files):
                peak_annotation_filename = peak_annot_filenames[index]
                if AccuCorDataLoader.is_accucor(peak_annot_file):
                    self.accucor_files.append(peak_annot_file)
                    self.accucor_filenames.append(peak_annotation_filename)
                elif AccuCorDataLoader.is_isocorr(peak_annot_file):
                    self.isocorr_files.append(peak_annot_file)
                    self.isocorr_filenames.append(peak_annotation_filename)
                else:
                    not_peak_annot_files.append(peak_annot_filenames[index])

        if len(not_peak_annot_files) > 0:
            raise ValidationError(
                "Peak annotation files must be either Accucor or Isocorr excel files.  Could not identify the type of "
                f"the following supplied files: {not_peak_annot_files}."
            )

    def dispatch(self, request, *args, **kwargs):
        # check if there is some video onsite
        if not settings.VALIDATION_ENABLED:
            return redirect("validatedown")
        else:
            return super(DataValidationView, self).dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        form_class = self.get_form_class()
        form = self.get_form(form_class)

        if not form.is_valid():
            return self.form_invalid(form)

        if "animal_sample_table" in request.FILES:
            sample_file = request.FILES["animal_sample_table"]
            tmp_sample_file = sample_file.temporary_file_path()
        else:
            # Ignore missing study file (allow user to validate just the accucor/isocorr file(s))
            sample_file = None
            tmp_sample_file = None

        if "peak_annotation_files" in request.FILES:
            peak_annotation_files = request.FILES.getlist("peak_annotation_files")
        else:
            # Ignore missing accucor files (allow user to validate just the sample file)
            peak_annotation_files = []

        self.set_files(
            tmp_sample_file,
            sample_filename=str(sample_file) if sample_file is not None else None,
            peak_annotation_files=[
                fp.temporary_file_path() for fp in peak_annotation_files
            ],
            peak_annotation_filenames=[str(fp) for fp in peak_annotation_files],
        )

        return self.form_valid(form)

    def form_valid(self, form):
        """
        Upon valid file submission, adds validation messages to the context of the validation page.
        """

        debug = f"asf: {self.animal_sample_file} num afs: {len(self.accucor_files)} num ifs: {len(self.isocorr_files)}"

        self.validate_study()

        self.extract_autofill_exceptions()

        self.format_validation_results_for_template()

        self.dfs_dict = self.get_or_create_dfs_dict()

        self.add_extracted_autofill_data()

        study_stream = BytesIO()

        xlsxwriter = self.create_study_file_writer(study_stream)

        self.annotate_study_excel(xlsxwriter)

        xlsxwriter.close()
        # Rewind the buffer so that when it is read(), you won't get an error about opening a zero-length file in Excel
        study_stream.seek(0)

        study_data = base64.b64encode(study_stream.read()).decode("utf-8")
        study_filename = self.animal_sample_filename
        if study_filename is None:
            study_filename = "study.xlsx"

        return self.render_to_response(
            self.get_context_data(
                results=self.results,
                debug=debug,
                valid=self.valid,
                form=form,
                exceptions=self.exceptions,
                submission_url=self.submission_url,
                ordered_keys=self.ordered_keys,
                study_data=study_data,
                study_filename=study_filename,
            ),
        )

    def annotate_study_excel(self, xlsxwriter):
        """Add annotations, formulas, colors (indicating errors/warning/required-values/read-only-values/etc).

        Also performs some formatting, such as setting the column width.

        Args:
            xlsxwriter (xlsxwriter): A study doc in an xlsx writer object.
        Exceptions:
            None
        Returns:
            None
        """
        # TODO: Use the xlsxwriter to decorate the excel sheets with errors/warnings as cell comments, colors to
        # indicate errors/warning/required-values/read-only-values, and formulas for inter-sheet population of
        # dropdowns.
        for sheet in xlsxwriter.sheets.keys():
            xlsxwriter.sheets[sheet].autofit()

    def extract_autofill_exceptions(self):
        """Remove exceptions related to references to missing underlying data and extract their data.

        This produces a dict named autofill_dict keyed on sheet name.

        This method detects:
        - AllMissingSamplesError
          - Error exceptions
            - Removes the exception
            - Puts {unique_record_key: {header: sample_name}} in the Samples sheet key of autofill_dict
            - Puts {unique_record_key: {sample_hdr: sample_name, header_hdr: header_name, peak_annot_hdr: peak_annot}}
                in Peak Annotation Details sheet
            - Puts {unique_record_key: {peak_annot_hdr: peak_annot, filetype_hdr: filetype}} in Peak Annotation Files
                sheet
          - Warning exceptions
            - Removes the exception
        - AllMissingTissues
          - Error exceptions
            - Removes the exception
            - Puts {unique_record_key: {header: tissue_name}} in the Tissues sheet key of autofill_dict
          - Warning exceptions
            - Removes the exception
        - AllMissingTreatments
          - Error exceptions
            - Removes the exception
            - Puts {unique_record_key: {header: treatment_name}} in the Treatments sheet key of autofill_dict
          - Warning exceptions
            - Removes the exception
        - In any of the above cases:
          - If the load key's value ends up empty, the load key is removed

        TODO: It does not yet handle AllMissingCompounds, as there's not a sheet yet for compounds in the animal/sample
        doc

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        self.extracted_exceptions = {
            AllMissingSamplesError.__name__: {"errors": [], "warnings": []},
            AllMissingTissues.__name__: {"errors": [], "warnings": []},
            AllMissingTreatments.__name__: {"errors": [], "warnings": []},
        }
        # Init the autofill dict for the subsequent calls
        self.autofill_dict = {
            self.SAMPLES_SHEET: defaultdict(dict),
            TissuesLoader.DataSheetName: defaultdict(dict),
            ProtocolsLoader.DataSheetName: defaultdict(dict),
        }
        warning_load_key = "Autofill Note"
        data_added = []
        # For every AggregatedErrors objects associated with a file or category
        for aes in [
            v["aggregated_errors"]
            for v in self.load_status_data.statuses.values()
            if v["aggregated_errors"] is not None
        ]:
            # For each exception class we want to extract from the AggregatedErrors object (in order to "fix" the data)
            for exc_class in [
                AllMissingSamplesError,
                AllMissingTissues,
                AllMissingTreatments,
            ]:
                # Remove exceptions of exc_class from the AggregatedErrors object (without modifying them)
                for exc in aes.remove_exception_type(exc_class, modify=False):
                    # If this is an error (as opposed to a warning)
                    if not hasattr(exc, "is_error") or exc.is_error:
                        self.extracted_exceptions[exc_class.__name__]["errors"].append(
                            exc
                        )

                        if exc_class == AllMissingSamplesError:
                            data_added.append(
                                f"{len(exc.missing_samples_dict['all_missing_samples'].keys())} sample names"
                            )
                            self.extract_all_missing_samples(exc)
                        elif exc_class == AllMissingTissues:
                            data_added.append(
                                f"{len(exc.missing_tissue_errors)} tissue names"
                            )
                            self.extract_all_missing_tissues(exc)
                        elif exc_class == AllMissingTreatments:
                            data_added.append(
                                f"{len(exc.missing_treatment_errors)} treatment names"
                            )
                            self.extract_all_missing_treatments(exc)
                    else:
                        self.extracted_exceptions[exc_class.__name__][
                            "warnings"
                        ].append(exc)
        if len(data_added) > 0:
            # Refresh the load statuses
            new_load_status_data = MultiLoadStatus()
            new_load_status_data.copy_constructor(self.load_status_data)
            self.load_status_data = new_load_status_data

            # Add a warning about added data
            added_warning = MissingDataAdded(
                addition_notes=data_added, file="Output Study Doc"
            )
            added_warning.is_error = False
            added_warning.is_fatal = False
            self.load_status_data.set_load_exception(added_warning, warning_load_key)

    def extract_all_missing_samples(self, exc):
        """Extracts autofill data from the supplied AllMissingSamplesError exception and puts it in self.autofill_dict.
        The contained missing sample data is in the form of accucor/isocorr column headers, so an attempt is made to
        remove common appended suffixes, e.g. sample1_pos_scan1.

        self.autofill_dict = {
            "Samples": {unique_record_key: {header: sample_name}},  # Fills in entries here
            "Tissues": defaultdict(dict),
            "Treatments": defaultdict(dict),
        }

        Args:
            exc (AllMissingSamplesError): And exception object containing data about missing Samples.

        Exceptions:
            None

        Returns:
            None
        """
        for sample_header in exc.missing_samples_dict["all_missing_samples"].keys():
            pattern = self.get_approx_sample_header_replacement_regex()
            sample_name = re.sub(pattern, "", sample_header)
            # TODO: Check if the modified name exists in the dfs_dict or in the database already.  If it does, then
            # nothing needs to be added to the samples sheet.  We just need to add to the LCMS data/sheet (once that's
            # done).  Note, we could also opt to supply the lcms file produced by the code developed earlier (below).
            # See build_lcms_dict().
            self.autofill_dict[self.SAMPLES_SHEET][sample_name] = {
                SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_NAME: sample_name
            }

    def extract_all_missing_tissues(self, exc):
        """Extracts autofill data from the supplied AllMissingTissues exception and puts it in self.autofill_dict.

        self.autofill_dict = {
            "Samples": defaultdict(dict),
            "Tissues": {unique_record_key: {header: tissue_name}},  # Fills in entries here
            "Treatments": defaultdict(dict),
        }

        Args:
            exc (AllMissingTissues): And exception object containing data about missing Tissues.

        Exceptions:
            None

        Returns:
            None
        """
        for mte in exc.missing_tissue_errors:
            self.autofill_dict[TissuesLoader.DataSheetName][mte.tissue_name] = {
                TissuesLoader.DataHeaders.NAME: mte.tissue_name
            }

    def extract_all_missing_treatments(self, exc):
        """Extracts autofill data from the supplied AllMissingTreatments exception and puts it in self.autofill_dict.

        self.autofill_dict = {
            "Samples": defaultdict(dict),
            "Tissues": defaultdict(dict),
            "Treatments": {unique_record_key: {header: treatment_name}},  # Fills in entries here
        }

        Args:
            exc (AllMissingTreatments): And exception object containing data about missing Treatments.

        Exceptions:
            None

        Returns:
            None
        """
        for mte in exc.missing_treatment_errors:
            self.autofill_dict[ProtocolsLoader.DataSheetName][mte.treatment_name] = {
                ProtocolsLoader.DataHeadersExcel.NAME: mte.treatment_name
            }

    def add_extracted_autofill_data(self):
        """Appends new rows from self.autofill_dict to self.dfs_dict.

        Populate the dfs_dict sheets (keys) with some basal data (e.g. tissues in the tissues sheet), then add data that
        was missing, as indicated via the exceptions generated from having tried to load the submitted accucor/isocorr
        (and/or existing study doc), to the template by appending rows.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        self.add_autofill_data(self.SAMPLES_SHEET)
        self.add_autofill_data(TissuesLoader.DataSheetName)
        self.add_autofill_data(ProtocolsLoader.DataSheetName)

    def add_autofill_data(self, sheet):
        """This method, given a sheet name, adds the data from self.autofill_dict[sheet] to self.dfs_dict[sheet],
        starting at the first empty row.

        Note the structures of the source dicts involved:
        - self.autofill_dict[sheet] is structured like {unique_key_str: {header1: value1, header2: value2}}
        - self.dfs_dict[sheet] is structured like {header1: {0: rowindex0_value, 1: rowindex1_value}}

        Note: It assumes a few things:
        - sheet is a key in both self.dfs_dict and self.autofill_dict.
        - self.dfs_dict[sheet] and self.autofill_dict[sheet] each contain a dict.
        - None of the data in self.autofill_dict[sheet] is already present on an existing row.
        - The keys in the self.dfs_dict[sheet] dict are contiguous integers starting from 0 (which must be true at the
            time of implementation).

        Args:
            sheet (string): The name of the sheet, which is the first key in both the self.autofill_dict and
                self.dfs_dict.

        Exceptions:
            None

        Returns:
            None
        """
        # Get the first row index where we will be adding data (we will be incrementing this)
        index = self.get_next_row_index(sheet)
        # We don't need the record keys, so we're going to iterate over the records of new data
        for sheet_dict in self.autofill_dict[sheet].values():
            # We're going to iterate over the headers present in the dfs_dict, but we need to keep track is and headers
            # in the sheet_dict are absent in the dfs_dict, so we can catch it up after the loop
            headers_present = dict((k, False) for k in sheet_dict.keys())
            # For the columns in the sheet (dfs_dict)
            for header in self.dfs_dict[sheet].keys():
                # If the header is one we're adding data to
                if header in sheet_dict.keys():
                    # Record that the header was found
                    headers_present[header] = True
                    # Add the new data  in the next row
                    self.dfs_dict[sheet][header][index] = sheet_dict[header]
                else:
                    # Fill in the columns we're not adding data to with None
                    self.dfs_dict[sheet][header][index] = None
            # Now catch up any columns that were not present
            for missing_header in [
                h for h, present in headers_present.items() if not present
            ]:
                # Create the column
                self.dfs_dict[sheet][missing_header] = {}
                # Iterate over the missing rows and set them to None
                for missing_index in range(index):
                    self.dfs_dict[sheet][missing_header][missing_index] = None
                # Now set the new row value for the missing column
                self.dfs_dict[sheet][header][index] = sheet_dict[header]
            # Increment the new row number
            index += 1

    def get_study_sheet_column_display_order(self):
        """Returns a list of lists to specify the sheet and column order of a created study excel file.

        The structure of the returned list is:

            [
                [sheet_name, [column_names]],
                ...
            ]

        Args:
            None

        Exceptions:
            None

        Returns:
            list of lists of a string (sheet name) and list of strings (column names)
        """
        return [
            [self.ANIMALS_SHEET, self.animals_ordered_display_headers],
            [self.SAMPLES_SHEET, self.samples_ordered_display_headers],
            [
                ProtocolsLoader.DataSheetName,
                self.treatments_loader.get_ordered_display_headers(),
            ],
            [
                TissuesLoader.DataSheetName,
                self.tissues_loader.get_ordered_display_headers(),
            ],
        ]

    def get_next_row_index(self, sheet):
        """Retrieves the next row index from self.dfs_dict[sheet] (from the first arbitrary column).

        Note: This assumes each column has the same number of rows

        Args:
            sheet (string): Name of the sheet to get the last row from

        Exceptions:
            None

        Returns:
            last_index (Optional[int]): None if there are no rows, otherwise the max row index.
        """
        for hdr in self.dfs_dict[sheet].keys():
            # This assumes that indexes are contiguous starting from 0 and that the values are never None
            return len(self.dfs_dict[sheet][hdr].keys())

    def get_or_create_dfs_dict(self, version=default_version):
        """Get or create dataframes dict templates for each sheet in self.animal_sample_file as a dict keyed on sheet.

        Generate a dict for the returned study doc (based on either the study doc supplied or create a fresh one).

        Note that creation populates the dfs_dict sheets (keys) with some basal data (e.g. tissues in the tissues
        sheet), but the "get" method does not.  Any missing data will need to be added via exceptions.  The intent
        being, that we initially give users all the data.  If they remove any that's not relevant to their study, that's
        fine.  We only want to add data if another sheet references it and it's not there.  That will be handled outside
        of this method.

        Args:
            version (string) [2]: tracebase study doc version number

        Exceptions:
            Exception

        Returns:
            dict of dicts: dataframes-style dicts dict keyed on sheet name
        """
        if self.animal_sample_file is None:
            return self.create_study_dfs_dict(version=version)
        return self.get_study_dfs_dict(version=version)

    def create_study_dfs_dict(
        self, dfs_dict: Optional[Dict[str, dict]] = None, version=default_version
    ):
        """Create dataframe template dicts for each sheet in self.animal_sample_file as a dict keyed on sheet.

        Treatments and tissues dataframes are populated using all of the data in the database for their models.
        Animals and Samples dataframes are not populated.

        In neither case, is missing data attempted to be auto-filled by this method.  Nor are unrecognized sheets or
        columns removed.

        Args:
            dfs_dict (dict of dicts): Supply this if you want to "fill in" missing sheets only.
            version (string) [2]: Tracebase study doc version number.

        Exceptions:
            NotImplementedError

        Returns:
            dfs_dict (dict of dicts): pandas' style list-dicts keyed on sheet name
        """
        if version == self.default_version or version.startswith(
            f"{self.default_version}."
        ):
            if dfs_dict is None:
                # Setting sheet to None reads all sheets and returns a dict keyed on sheet name
                return {
                    # TODO: Update the animal and sample entries below once the loader has been refactored
                    # The sample table loader has not yet been refactored/split
                    self.ANIMALS_SHEET: self.animals_dict,
                    self.SAMPLES_SHEET: self.samples_dict,
                    ProtocolsLoader.DataSheetName: self.treatments_loader.get_dataframe_template(
                        populate=True,
                        filter={"category": Protocol.ANIMAL_TREATMENT},
                    ),
                    TissuesLoader.DataSheetName: self.tissues_loader.get_dataframe_template(
                        populate=True
                    ),
                }

            if (
                self.ANIMALS_SHEET in dfs_dict.keys()
                and len(dfs_dict[self.ANIMALS_SHEET].keys()) > 0
            ):
                self.fill_in_missing_columns(
                    dfs_dict, self.ANIMALS_SHEET, self.animals_dict
                )
            else:
                dfs_dict[self.ANIMALS_SHEET] = self.animals_dict

            if (
                self.SAMPLES_SHEET in dfs_dict.keys()
                and len(dfs_dict[self.SAMPLES_SHEET].keys()) > 0
            ):
                self.fill_in_missing_columns(
                    dfs_dict, self.SAMPLES_SHEET, self.samples_dict
                )
            else:
                dfs_dict[self.SAMPLES_SHEET] = self.samples_dict

            if ProtocolsLoader.DataSheetName in dfs_dict.keys():
                self.fill_in_missing_columns(
                    dfs_dict,
                    ProtocolsLoader.DataSheetName,
                    self.treatments_loader.get_dataframe_template(),
                )
            else:
                dfs_dict[ProtocolsLoader.DataSheetName] = (
                    self.treatments_loader.get_dataframe_template(populate=True)
                )

            if TissuesLoader.DataSheetName in dfs_dict.keys():
                self.fill_in_missing_columns(
                    dfs_dict,
                    TissuesLoader.DataSheetName,
                    self.tissues_loader.get_dataframe_template(),
                )
            else:
                dfs_dict[TissuesLoader.DataSheetName] = (
                    self.tissues_loader.get_dataframe_template(populate=True)
                )

            return dfs_dict

        raise NotImplementedError(
            f"Version {version} is not yet supported.  Supported versions: {self.supported_versions}"
        )

    @classmethod
    def fill_in_missing_columns(cls, dfs_dict, sheet, template):
        """Takes a dict of pandas-style dicts (dfs_dict), the sheet name (which is the key to the dict), and a template
        dict by which to check the completeness of the data in the dfs_dict, and modifies the dfs_dict to add
        missing columns (with the same number of rows as the existing data).

        Assumes that the dfs_dict has at least 1 defined column.

        Args:
            dfs_dict (dict of dicts): This is a dict presumed to have been parsed from a file
            sheet (string): Name of the sheet key in the dfs_dict that the template corresponds to.  The sheet is
                assumed to be present as a key in dfs_dict.
            tamplate (dict of dicts): A dict containing all of the expected columns (the values are not used).

        Exceptions:
            None

        Returns:
            None
        """
        sheet_dict = dfs_dict[sheet]
        headers = list(sheet_dict.keys())
        first_present_header_key = headers[0]
        # Assumes all columns have the same number of rows
        num_rows = len(sheet_dict[first_present_header_key])
        modified = False
        for header in template.keys():
            if header not in sheet_dict.keys():
                modified = True
                sheet_dict[header] = dict((i, None) for i in range(num_rows))
        if modified:
            # No need to change anything if nothing was missing
            dfs_dict[sheet] = sheet_dict

    @property
    def animals_dict(self):
        """Property to return an empty dict template for the Animals sheet.
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # TODO: Eliminate this property once the sample table loader is split and inherits from TableLoader
        return {
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_NAME: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_AGE: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_SEX: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_GENOTYPE: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_WEIGHT: {},
            SampleTableLoader.DefaultSampleTableHeaders.INFUSATE: {},
            SampleTableLoader.DefaultSampleTableHeaders.TRACER_CONCENTRATIONS: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_INFUSION_RATE: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_DIET: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_FEEDING_STATUS: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_TREATMENT: {},
            SampleTableLoader.DefaultSampleTableHeaders.STUDY_NAME: {},
            SampleTableLoader.DefaultSampleTableHeaders.STUDY_DESCRIPTION: {},
        }

    @property
    def animals_ordered_display_headers(self):
        """Property to return the ordered Animals sheet headers intended to be included in the template.

        Headers may be a subset of those in self.animals_dict.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # TODO: Eliminate this property once the sample table loader is split and inherits from TableLoader
        return [
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_NAME,
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_AGE,
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_SEX,
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_GENOTYPE,
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_TREATMENT,
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_WEIGHT,
            SampleTableLoader.DefaultSampleTableHeaders.INFUSATE,
            SampleTableLoader.DefaultSampleTableHeaders.TRACER_CONCENTRATIONS,
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_INFUSION_RATE,
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_DIET,
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_FEEDING_STATUS,
            SampleTableLoader.DefaultSampleTableHeaders.STUDY_NAME,
            SampleTableLoader.DefaultSampleTableHeaders.STUDY_DESCRIPTION,
        ]

    @property
    def samples_dict(self):
        """Property to return an empty dict template for the Samples sheet.
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # TODO: Eliminate this property once the sample table loader is split and inherits from TableLoader
        return {
            SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_NAME: {},
            SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_DATE: {},
            SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_RESEARCHER: {},
            SampleTableLoader.DefaultSampleTableHeaders.TISSUE_NAME: {},
            SampleTableLoader.DefaultSampleTableHeaders.TIME_COLLECTED: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_NAME: {},
        }

    @property
    def samples_ordered_display_headers(self):
        """Property to return the ordered Samples sheet headers intended to be included in the template.

        Headers may be a subset of those in self.samples_dict.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # TODO: Eliminate this property once the sample table loader is split and inherits from TableLoader
        return [
            SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_NAME,
            SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_DATE,
            SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_RESEARCHER,
            SampleTableLoader.DefaultSampleTableHeaders.TISSUE_NAME,
            SampleTableLoader.DefaultSampleTableHeaders.TIME_COLLECTED,
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_NAME,
        ]

    def get_study_dfs_dict(self, version=default_version):
        """Read in each sheet in self.animal_sample_file as a dict of dicts keyed on sheet (filling in any missing
        sheets and columns).

        Args:
            version (string) [2]: tracebase study doc version number

        Exceptions:
            NotImplementedError

        Returns:
            dfs_dict (dict of dicts): pandas-style dicts dict keyed on sheet name
        """
        if version == self.default_version or version.startswith(
            f"{self.default_version}."
        ):
            dict_of_dataframes = read_from_file(
                self.animal_sample_file, sheet=None, dtype=self.get_study_dtypes_dict()
            )

            # We're not ready yet for actual dataframes.  It will be easier to move forward with dicts to be able to add
            # data
            dfs_dict = {}
            for k, v in dict_of_dataframes.items():
                dfs_dict[k] = v.to_dict()

            # create_study_dfs_dict, if given a dict, will fill in any missing sheets and columns with empty row values
            self.create_study_dfs_dict(dfs_dict=dfs_dict)

            return dfs_dict

        raise NotImplementedError(
            f"Version {version} is not yet supported.  Supported versions: {self.supported_versions}"
        )

    def get_study_dtypes_dict(self, version=default_version):
        """Retrieve the dtype data for each sheet.

        Args:
            version (string) [2]: tracebase study doc version number

        Exceptions:
            NotImplementedError

        Returns:
            dtypes (Dict[str, Dict[str, type]]): dtype dicts keyed by sheet name
        """
        if version == self.default_version or version.startswith(
            f"{self.default_version}."
        ):
            animal_sample_headers = SampleTableLoader.DefaultSampleTableHeaders
            return {
                # TODO: Update the animal and sample entries below once the loader has been refactored
                # The sample table loader has not yet been refactored/split
                self.ANIMALS_SHEET: {
                    animal_sample_headers.ANIMAL_NAME: str,
                    animal_sample_headers.ANIMAL_TREATMENT: str,
                },
                self.SAMPLES_SHEET: {animal_sample_headers.ANIMAL_NAME: str},
                ProtocolsLoader.DataSheetName: self.treatments_loader.get_column_types(),
                TissuesLoader.DataSheetName: self.tissues_loader.get_column_types(),
            }
        raise NotImplementedError(
            f"Version {version} is not yet supported.  Supported versions: {self.supported_versions}"
        )

    def format_validation_results_for_template(self):
        valid = self.load_status_data.is_valid
        results = {}
        exceptions = {}
        ordered_keys = []

        for load_key in self.load_status_data.get_ordered_status_keys():
            # The load_key is the absolute path, but we only want to report errors in the context of the file's name
            short_load_key = os.path.basename(load_key)

            ordered_keys.append(short_load_key)
            results[short_load_key] = self.load_status_data.statuses[load_key]["state"]

            exceptions[short_load_key] = []
            # Get the AggregatedErrors object
            aes = self.load_status_data.statuses[load_key]["aggregated_errors"]
            # aes is None if there were no exceptions
            if aes is not None:
                for exc in aes.exceptions:
                    exc_str = aes.get_buffered_exception_summary_string(
                        exc, numbered=False, typed=False
                    )
                    exceptions[short_load_key].append(
                        {
                            "message": exc_str,
                            "is_error": exc.is_error,
                            "type": type(exc).__name__,
                        }
                    )

        self.valid = valid
        self.results = results
        self.exceptions = exceptions
        self.ordered_keys = ordered_keys

        return valid

    # No need to disable autoupdates adding the @no_autoupdates decorator to this function because supplying
    # `validate=True` automatically disables them
    def validate_study(self):
        tmpdir_obj = tempfile.TemporaryDirectory()
        tmpdir = tmpdir_obj.name

        # TODO: create_yaml() *could* raise ValueError or KeyError if the required sample file is not provided or if 2
        # accucor/isocorr files have the same name, but there should be checks on the submitted form data in the Django
        # form which should call form_invalid in those cases, so instead of adding a graceful exception to the user
        # *here* to catch those cases, add those checks to the form validation.  Check that the required sample file is
        # provided and that none of the accucor/isocorr files have the same name)
        yaml_file = self.create_yaml(tmpdir)

        load_status_data = MultiLoadStatus()

        try:
            call_command(
                "load_study",
                yaml_file,
                validate=True,
                verbosity=3,
                skip_cache_updates=True,
            )
        except MultiLoadStatus as mls:
            load_status_data = mls

        tmpdir_obj.cleanup()

        self.load_status_data = load_status_data

        return self.load_status_data.is_valid

    def create_yaml(self, tmpdir):
        basic_loading_data = {
            # TODO: Add the ability for the validation interface to take tissues, compounds, & a separate protocols file
            # The following are placeholders - Not yet supported by the validation view
            #   "tissues": "tissues.tsv",
            #   "compounds": "compounds.tsv",
            # The following placeholders are added by add_sample_data() if an animal sample table is provided
            #   "protocols": None,
            #   "animals_samples_treatments": {
            #       "table": None,
            #       "skip_researcher_check": False,
            #   },
            "accucor_data": {
                "accucor_files": [
                    # {
                    #     "name": None,  # Added by self.add_ms_data()
                    #     "isocorr_format": False,  # Set by self.add_ms_data()
                    # },
                ],
                "lc_protocol": LCMethod.create_name(),
                "instrument": MSRunSequence.INSTRUMENT_DEFAULT,
                "polarity": MSRunSample.POLARITY_DEFAULT,
                "date": "1972-11-24",
                "researcher": Researcher.RESEARCHER_DEFAULT,
                "new_researcher": False,
            },
        }

        # Going to use a temp directory so we can report the user's given file names (not the randomized name supplied
        # by django forms)
        self.add_sample_data(basic_loading_data, tmpdir)
        self.add_ms_data(
            basic_loading_data,
            tmpdir,
            self.accucor_files,
            self.accucor_filenames,
            False,
        )
        self.add_ms_data(
            basic_loading_data, tmpdir, self.isocorr_files, self.isocorr_filenames, True
        )

        loading_yaml = os.path.join(tmpdir, "loading.yaml")

        with open(loading_yaml, "w") as file:
            yaml.dump(basic_loading_data, file)

        return loading_yaml

    def add_sample_data(self, basic_loading_data, tmpdir):
        if self.animal_sample_file is None:
            # The animal sample file is optional
            return

        form_sample_file_path = self.animal_sample_file

        # The django form gives a random file name, but the user's name is available.  Here, we're supporting with or
        # without the user's file name to support tests that don't use a randomized temp file.
        if self.animal_sample_filename:
            sf = self.animal_sample_filename
        else:
            # This is for non-random file names (e.g. for the test code)
            sf = os.path.basename(form_sample_file_path)

        # To associate the file with the yaml file created in the temp directory, we must copy it
        sfp = os.path.join(tmpdir, str(sf))
        shutil.copyfile(form_sample_file_path, sfp)

        basic_loading_data["protocols"] = sfp
        basic_loading_data["animals_samples_treatments"] = {
            "table": sfp,
            "skip_researcher_check": False,
        }

    def add_ms_data(self, basic_loading_data, tmpdir, files, filenames, is_isocorr):
        for i, form_file_path in enumerate(files):
            if len(files) == len(filenames):
                fn = filenames[i]
            else:
                # This is for non-random file names (e.g. for the test code)
                fn = os.path.basename(form_file_path)

            fp = os.path.join(tmpdir, str(fn))
            shutil.copyfile(form_file_path, fp)

            if fn in [
                dct["name"]
                for dct in basic_loading_data["accucor_data"]["accucor_files"]
            ]:
                ft = "Isocorr" if is_isocorr else "Accucor"
                raise KeyError(
                    f"{ft} filename conflict: {fn}.  All Accucor/Isocorr file names must be unique."
                )

            basic_loading_data["accucor_data"]["accucor_files"].append(
                {
                    "name": fp,
                    "isocorr_format": is_isocorr,
                }
            )

    # TODO: If this still exists and is unused once the study submission refactor is done, delete it.
    def build_lcms_dict(
        self,
        peak_annot_sample_headers,
        peak_annot_file_name,
        sample_suffixes=None,
    ):
        """Build a partial LCMS metadata dict keyed on the headers and containing dicts keyed on the headers of an LCMS
        file.

        Args:
            peak_annot_sample_headers (list of strings): Sample headers (including blanks) parsed from the accucor or
                isocorr file
            peak_annot_file_name (string): And accucor or isocorr file name without the path
            sample_suffixes (list of raw strings) [self.DEFAULT_SAMPLE_HEADER_SUFFIXES]: Regular expressions of suffix
                strings found at the end of a peak annotation file sample header (e.g. "_pos")

        Exceptions:
            ProgrammingError

        Returns:
            lcms_dict
        """

        # Set the suffixes to be stripped from the sample header names to convert them to database sample names
        if sample_suffixes is None:
            suffixes = self.DEFAULT_SAMPLE_HEADER_SUFFIXES
        pattern = self.get_approx_sample_header_replacement_regex(suffixes)

        # Initialize the dict we'll be returning
        lcms_dict = defaultdict(dict)
        # Keep track of duplicate sample headers
        dupe_headers = defaultdict(lambda: defaultdict(list))

        # Traverse the headers and built the dict
        for peak_annot_sample_header in peak_annot_sample_headers:
            # The tracebase sample name is the header with manually (and repeatedly) added suffixes removed
            # This is a heuristic.  It is not perfect.  If may be possible to allow the user to enter them in the form
            # in the future, but for now, this uses the common ones.
            db_sample_name = re.sub(pattern, "", peak_annot_sample_header)

            # If we've already seen this header, it is a duplicate
            if peak_annot_sample_header in dupe_headers.keys():
                dupe_headers[peak_annot_sample_header][peak_annot_file_name] += 1
            elif peak_annot_sample_header in lcms_dict.keys():
                dupe_headers[peak_annot_sample_header][peak_annot_file_name] = 2
            else:
                lcms_dict[peak_annot_sample_header] = {
                    "sort level": 0,
                    LCMS_DB_SAMPLE_HDR: db_sample_name,
                    LCMS_FL_SAMPLE_HDR: peak_annot_sample_header,
                    LCMS_PEAK_ANNOT_HDR: peak_annot_file_name,
                }

        # Add individual errors to be added as cell comments to the excel file.
        # Represent duplicate headers as errors in the column values, for the user to manually address
        all_dup_errs = []
        for duph in dupe_headers.keys():
            lcms_dict[duph]["sort level"] = 1
            lcms_dict[duph]["error"] = NonUniqueSampleDataHeader(
                duph, dupe_headers[duph]
            )
            all_dup_errs.append(lcms_dict[duph]["error"])

        # Errors for reporting on the web page
        if len(all_dup_errs) > 0:
            # TODO: If this is added to the results, this error should be passed to the template
            self.lcms_build_errors = NonUniqueSampleDataHeaders(all_dup_errs)

        return lcms_dict

    # TODO: If this still exists and is unused once the study submission refactor is done, delete it.
    @classmethod
    def lcms_dict_to_tsv_string(cls, lcms_dict):
        """Takes the lcms_dict and creates a string of (destined to be) file content.

        It includes a header line and the following lines are sorted by the state of the row (full, missing, and various
        types of errors), then by accucor and sample.

        Args:
            lcms_dict (defaultdict(dict(str))): The keys of the outer dict are not included in the output, but the keys
                of the inner dict are the column headers.

        Exceptions:
            None

        Returns:
            lcms_data (string): The eventual content of an lcms file, including headers
        """
        headers = [
            LCMS_DB_SAMPLE_HDR,
            LCMS_FL_SAMPLE_HDR,
            LCMS_PEAK_ANNOT_HDR,
        ]
        lcms_data = "\t".join(headers) + "\n"
        for key in dict(
            sorted(
                lcms_dict.items(),
                key=lambda x: (
                    x[1][
                        "sort level"
                    ],  # This sorts erroneous and missing data to the bottom
                    x[1][LCMS_PEAK_ANNOT_HDR],  # Then sort by accucor file name
                    x[1][LCMS_FL_SAMPLE_HDR],  # Then by sample (header)
                ),
            )
        ).keys():
            lcms_data += (
                "\t".join(map(lambda head: lcms_dict[key][head], headers)) + "\n"
            )

        return lcms_data

    @classmethod
    def get_approx_sample_header_replacement_regex(cls, suffixes=None, add=True):
        """Returns a regular expression combining sample header suffixes, to be used to generate tracebase sample names.

        Args:
            suffixes (list of raw strings) [cls.DEFAULT_SAMPLE_HEADER_SUFFIXES]: Uncompiled regular expressions for
                individual suffixed, e.g. r"_scan[0-9]+"
            add (boolean): Whether or not to add the supplied suffixes or replace them

        Exceptions:
            None

        Returns:
            Python re compiled regular expression
        """
        if suffixes is None:
            suffixes = cls.DEFAULT_SAMPLE_HEADER_SUFFIXES
        elif add:
            suffixes.extend(cls.DEFAULT_SAMPLE_HEADER_SUFFIXES)

        return re.compile(r"(" + "|".join(suffixes) + r")+$")

    def create_study_file_writer(self, stream_obj: BytesIO):
        """Returns an xlsxwriter for an excel file created from the self.dfs_dict.

        The created writer will output to the supplied stream_obj.

        Args:
            stream_obj (BytesIO)
        Exceptions:
            None
        Returns:
            xlsxwriter (xlsxwriter)
        """
        xlsxwriter = pd.ExcelWriter(stream_obj, engine="xlsxwriter")

        for order_spec in self.get_study_sheet_column_display_order():
            sheet = order_spec[0]
            columns = order_spec[1]

            # Error-check the ordered sheets/columns
            if sheet not in self.dfs_dict.keys():
                raise KeyError(
                    f"Sheet [{sheet}] from get_study_sheet_column_display_order not in self.dfs_dict: "
                    f"{self.dfs_dict.keys()}"
                )
            else:
                missing_headers = []
                for header in columns:
                    if header not in self.dfs_dict[sheet].keys():
                        missing_headers.append(header)
                if len(missing_headers) > 0:
                    KeyError(
                        f"The following headers for sheet [{sheet}] obtained from get_study_sheet_column_display_order "
                        f"are not in self.dfs_dict[{sheet}]: {missing_headers}"
                    )

            # Create a dataframe and add it as an excel object to an xlsxwriter sheet
            pd.DataFrame.from_dict(self.dfs_dict[sheet]).to_excel(
                excel_writer=xlsxwriter, sheet_name=sheet, columns=columns, index=False
            )

        return xlsxwriter


def validation_disabled(request):
    return render(request, "validation_disabled.html")
