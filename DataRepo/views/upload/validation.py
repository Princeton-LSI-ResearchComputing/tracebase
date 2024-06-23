import base64
import os.path
from collections import defaultdict
from io import BytesIO
from typing import Dict, List, Optional, Type

import pandas as pd
import xlsxwriter
from django.conf import settings
from django.db.utils import ProgrammingError
from django.forms import ValidationError
from django.shortcuts import redirect, render
from django.views.generic.edit import FormView

from DataRepo.forms import DataSubmissionValidationForm
from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.compounds_loader import CompoundsLoader
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.loaders.peak_annotation_files_loader import (
    PeakAnnotationFilesLoader,
)
from DataRepo.loaders.peak_annotations_loader import (
    AccucorLoader,
    IsoautocorrLoader,
    IsocorrLoader,
    PeakAnnotationsLoader,
    UnicorrLoader,
)
from DataRepo.loaders.protocols_loader import ProtocolsLoader
from DataRepo.loaders.samples_loader import SamplesLoader
from DataRepo.loaders.study_loader import StudyLoader
from DataRepo.loaders.tissues_loader import TissuesLoader
from DataRepo.models.protocol import Protocol
from DataRepo.utils.exceptions import (
    AllMissingCompounds,
    AllMissingSamples,
    AllMissingTissues,
    AllMissingTreatments,
    MissingDataAdded,
    MultiLoadStatus,
    NoSamples,
)
from DataRepo.utils.file_utils import get_sheet_names, read_from_file
from DataRepo.utils.text_utils import autowrap


class DataValidationView(FormView):
    form_class = DataSubmissionValidationForm
    template_name = "DataRepo/validate_submission.html"
    success_url = ""
    peak_annot_filenames_replacement: List[str] = []
    peak_annot_files_replacement: List[str] = []
    study_filename = None
    study_file = None
    submission_url = settings.SUBMISSION_FORM_URL
    # Study doc version (default and supported list)
    default_version = "3"
    supported_versions = [default_version]
    row_key_delim = "__DELIM__"

    def __init__(self):
        super().__init__()

        self.autofill_dict = defaultdict(lambda: defaultdict(dict))
        self.autofill_dict[SamplesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[TissuesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[ProtocolsLoader.DataSheetName] = defaultdict(dict)
        # TODO: Add Peak Annotation Details
        # self.autofill_dict[MSRunsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[CompoundsLoader.DataSheetName] = defaultdict(dict)

        self.extracted_exceptions = defaultdict(lambda: {"errors": [], "warnings": []})
        self.valid = None
        self.results = {}
        self.exceptions = {}
        self.ordered_keys = []
        self.load_status_data: Optional[MultiLoadStatus] = None
        self.animals_loader = AnimalsLoader()
        self.samples_loader = SamplesLoader()
        self.tissues_loader = TissuesLoader()
        self.treatments_loader = ProtocolsLoader(
            headers=ProtocolsLoader.DataHeadersExcel
        )
        self.compounds_loader = CompoundsLoader()
        self.peak_annotations_loaders = []
        self.output_study_filename = "study.xlsx"
        self.autofill_only_mode = True
        self.dfs_dict = self.create_study_dfs_dict()
        self.study_file = None
        self.peak_annot_files = None
        self.peak_annot_filenames = []

    def set_files(
        self,
        study_file=None,
        study_filename: Optional[str] = None,
        peak_annot_files=None,
        peak_annot_filenames=None,
    ):
        """
        This method allows the files to be set.  It takes 2 different optional params for file names (that are used in
        reporting) to accommodate random temporary file names.  If file names are not supplied, the basename of the
        actual files is used for reporting.
        """
        if peak_annot_files is None:
            peak_annot_files = []
        if peak_annot_filenames is None:
            peak_annot_filenames = []
        self.all_infile_names = []

        self.study_file = study_file
        self.study_filename = study_filename
        if study_filename is None and study_file is not None:
            self.study_filename = str(os.path.basename(study_file))

        if self.study_filename is not None:
            self.output_study_filename = self.study_filename
            self.all_infile_names.append(self.study_filename)

        self.peak_annot_files = peak_annot_files
        if len(peak_annot_filenames) == 0 and len(peak_annot_files) > 0:
            peak_annot_filenames = [str(os.path.basename(f)) for f in peak_annot_files]

        if (
            len(peak_annot_filenames) == 0
            and len(peak_annot_filenames) > 0
            and (
                len(peak_annot_files) == 0
                or len(peak_annot_filenames) != len(peak_annot_files)
            )
        ):
            raise ProgrammingError(
                f"The number of peak annotation file names [{len(peak_annot_filenames)}] must be equal to the "
                f"number of peak annotation files [{peak_annot_files}]."
            )

        # Get an initial dfs_dict (a dict representation of the output study doc, either created or as obtained from the
        # user)
        self.dfs_dict = self.get_or_create_dfs_dict()
        # Now that self.animal_sample_file, self.peak_annotation_files, and self.dfs_dict have been set, determine
        # validation readiness
        self.determine_study_file_validation_readiness()

        user_file_formats: Dict[str, str] = {}
        error_messages = []

        if self.study_file is not None:
            # The purpose of this block is to allow the user to specify the peak annotation file format (incase it's
            # ambiguous).  We just want a loader object to be able to read the file to extract missing samples and
            # compounds in the event we're not ready to validate.

            if PeakAnnotationFilesLoader.DataSheetName in get_sheet_names(
                self.study_file
            ):
                pafl = PeakAnnotationFilesLoader(
                    df=read_from_file(
                        self.study_file,
                        sheet=PeakAnnotationFilesLoader.DataSheetName,
                        dtype=PeakAnnotationFilesLoader._get_column_types(),
                    ),
                    file=self.study_file,
                )
                for _, row in pafl.df.iterrows():
                    # "actual" means not the original file from the form submission, but the nonsense filename created
                    # by the browser
                    user_filename, _, user_format_code = pafl.get_file_and_format(row)
                    supported = PeakAnnotationsLoader.get_supported_formats()

                    if user_format_code is not None and user_format_code in supported:
                        user_file_formats[user_filename] = user_format_code
                    elif user_format_code is not None:
                        error_messages.append(
                            f"Unrecognized format code: {user_format_code} from {self.study_file}, sheet "
                            f"'{PeakAnnotationFilesLoader.DataSheetName}', row {row.name + 2}."
                        )

        # Now we will sort through the peak annotation files that were supplied and create loader objects for them to
        # extract autofill data
        self.annot_files_dict: Dict[str, str] = {}
        self.peak_annot_filenames = []
        self.peak_annotations_loaders = []
        peak_annot_loader_class: Type[PeakAnnotationsLoader]

        if peak_annot_files is not None and len(peak_annot_files) > 0:
            for index, peak_annot_file in enumerate(peak_annot_files):
                # Save the user's file name (different from the actual nonsense filename from the web form)
                peak_annot_filename = peak_annot_filenames[index]
                self.all_infile_names.append(peak_annot_filename)
                self.peak_annot_filenames.append(peak_annot_filename)
                if (
                    peak_annot_filename in self.annot_files_dict.keys()
                    and self.annot_files_dict[peak_annot_filename] != peak_annot_file
                ):
                    error_messages.append(
                        f"Peak annotation filenames must be unique.  Filename {peak_annot_filename} was "
                        "encountered multiple times."
                    )
                    continue

                # Map the user's filename to the web form file path
                self.annot_files_dict[peak_annot_filename] = peak_annot_file

                # Now we will determine the format to decide which loader to create.
                # We will defer to the user's supplied format (if any)
                matching_formats = []
                if peak_annot_filename in user_file_formats.keys():
                    user_format_code = user_file_formats[peak_annot_filename]
                    matching_formats = [user_format_code]
                else:
                    matching_formats = PeakAnnotationsLoader.determine_matching_formats(
                        # Do not enforce column types when we don't know what columns exist yet
                        read_from_file(peak_annot_file, sheet=None)
                    )

                if len(matching_formats) == 1:
                    if matching_formats[0] == AccucorLoader.format_code:
                        peak_annot_loader_class = AccucorLoader
                    elif matching_formats[0] == IsocorrLoader.format_code:
                        peak_annot_loader_class = IsocorrLoader
                    elif matching_formats[0] == IsoautocorrLoader.format_code:
                        peak_annot_loader_class = IsoautocorrLoader
                    elif matching_formats[0] == UnicorrLoader.format_code:
                        peak_annot_loader_class = UnicorrLoader
                    else:
                        raise ProgrammingError(
                            f"Unrecognized format code: {matching_formats}."
                        )

                    # Create an instance of the loader, appended onto the loaders
                    self.peak_annotations_loaders.append(
                        peak_annot_loader_class(
                            # These are the essential arguments
                            df=read_from_file(peak_annot_file, sheet=None),
                            file=peak_annot_file,
                            # We don't need the default sequence info - we only want to read the file
                        )
                    )

                elif len(matching_formats) == 0:
                    error_messages.append(
                        f"No matching formats for peak annotation file: {peak_annot_filenames[index]}.  Must be one of "
                        f"{PeakAnnotationsLoader.get_supported_formats()}.  Please enter the correct "
                        f"'{PeakAnnotationFilesLoader.DataHeaders.FORMAT}' for "
                        f"'{PeakAnnotationFilesLoader.DataHeaders.FILE}' in the "
                        f"'{PeakAnnotationFilesLoader.DataSheetName}' sheet of '{self.study_filename}'."
                    )
                else:
                    error_messages.append(
                        f"Multiple matching formats: {matching_formats} for peak annotation file: "
                        f"{peak_annot_filenames[index]}.  Please enter the correct "
                        f"'{PeakAnnotationFilesLoader.DataHeaders.FORMAT}' for "
                        f"'{PeakAnnotationFilesLoader.DataHeaders.FILE}' in the "
                        f"'{PeakAnnotationFilesLoader.DataSheetName}' sheet of '{self.study_filename}'."
                    )

        if len(error_messages) > 0:
            msg = "\n\t".join(error_messages)
            raise ValidationError(
                f"Could not identify the type of the peak annotation files for the reasons shown below:\n\t{msg}\n"
                f"Supported formats: {PeakAnnotationsLoader.get_supported_formats()}.  Note that format determination "
                "is made using sheet names and column headers.  If a csv or tsv was supplied, the determination can be "
                "ambiguous due to common header names."
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
            tmp_study_file = sample_file.temporary_file_path()
        else:
            # Ignore missing study file (allow user to validate just the accucor/isocorr file(s))
            sample_file = None
            tmp_study_file = None

        if "peak_annotation_files" in request.FILES:
            peak_annotation_files = request.FILES.getlist("peak_annotation_files")
        else:
            # Ignore missing accucor files (allow user to validate just the sample file)
            peak_annotation_files = []

        self.set_files(
            tmp_study_file,
            study_filename=str(sample_file) if sample_file is not None else None,
            peak_annot_files=[fp.temporary_file_path() for fp in peak_annotation_files],
            peak_annot_filenames=[str(fp) for fp in peak_annotation_files],
        )

        return self.form_valid(form)

    def form_valid(self, form):
        """
        Upon valid file submission, adds validation messages to the context of the validation page.
        """

        debug = f"sf: {self.study_file} num pafs: {len(self.peak_annot_files)}"

        # Initialize a status object for the results for each input file
        # TODO: Make the MultiLoadStatus class more of a "status" class for multuple "categories" of things that
        # must succeed (as opposed to just load-related things)
        self.load_status_data = MultiLoadStatus(load_keys=self.all_infile_names)

        if self.autofill_only_mode:
            # autofill_only_mode means that there was no study file submitted.  (The form validation guarantees that
            # we have at least 2 peak annotation file.)

            # Extract autofill data directly from the peak annotation files
            self.extract_autofill_from_peak_annotation_files()
        else:
            self.validate_study()

            # Extract errors from the validation that can be used to autofill missing values in the study doc
            self.extract_autofill_from_exceptions(
                retain_as_warnings=not self.autofill_only_mode,
            )

        self.format_results_for_template()

        self.add_extracted_autofill_data()

        study_stream = BytesIO()

        xlsxwriter = self.create_study_file_writer(study_stream)

        self.annotate_study_excel(xlsxwriter)

        xlsxwriter.close()
        # Rewind the buffer so that when it is read(), you won't get an error about opening a zero-length file in Excel
        study_stream.seek(0)

        study_data = base64.b64encode(study_stream.read()).decode("utf-8")

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
                study_filename=self.output_study_filename,
                quiet_mode=self.autofill_only_mode,
            ),
        )

    def determine_study_file_validation_readiness(self):
        """Determines if the data is ready for validation by seeing if the study doc has any values in it other than
        sample names and if there are peak annotation files to populate it.  It does this by inspecting
        self.study_file, self.peak_annot_files, and the data parsed into self.dfs_dict from self.study_file.

        The purpose of this method is to avoid time consuming processing of a file that is only destined to produce
        missing sample name errors by identifying this futile case and setting self.autofill_only_mode to True (or
        False).

        Args:
            None
        Exceptions:
            None
        Returns:
            ready_for_validation (boolean): The opposite of the autofill_only_mode
        """
        if self.peak_annot_files is None or len(self.peak_annot_files) == 0:
            # Cannot do autofill-only if there is no source of autofill data (currently)
            # This can happen if there were exceptions (like missing required headers)
            self.autofill_only_mode = False
            return not self.autofill_only_mode

        if self.study_file is None or not self.dfs_dict_is_valid():
            # If there's no study data, we will want to autofill
            self.autofill_only_mode = True
            return not self.autofill_only_mode

        # TODO: Instead of doing this none_vals strategy, dynamically create a dataframe out of the dfs_dict and then
        # use get_row_val and check if the result is None
        # TODO: I should probably alter this strategy and check if all required columns have values on every row?
        none_vals = ["", "nan"]
        for header in self.samples_loader.get_ordered_display_headers():
            if (
                header == SamplesLoader.DataHeaders.SAMPLE
                or header not in self.dfs_dict[SamplesLoader.DataSheetName].keys()
            ):
                # Skipping the Sample header and any unknown header
                continue

            for val in self.dfs_dict[SamplesLoader.DataSheetName][header].values():
                if val is not None and isinstance(val, str) and val not in none_vals:
                    # If *any* data has been manually added, we will validate
                    self.autofill_only_mode = False
                    return not self.autofill_only_mode

        # There is nothing that needs to be validated
        self.autofill_only_mode = True
        return not self.autofill_only_mode

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
        column_metadata = {
            AnimalsLoader.DataSheetName: self.animals_loader.get_header_metadata(),
            SamplesLoader.DataSheetName: self.samples_loader.get_header_metadata(),
            TissuesLoader.DataSheetName: self.tissues_loader.get_header_metadata(),
            ProtocolsLoader.DataSheetName: self.treatments_loader.get_header_metadata(),
            CompoundsLoader.DataSheetName: self.compounds_loader.get_header_metadata(),
        }
        for order_spec in self.get_study_sheet_column_display_order():
            sheet = order_spec[0]
            worksheet = xlsxwriter.sheets[sheet]
            headers = order_spec[1]

            # Add comments to header cells
            for header in headers:
                comment = column_metadata[sheet][header].comment
                if comment is not None:
                    cell = self.header_to_cell(sheet=sheet, header=header)
                    wrapped, nlines, nchars = autowrap(comment)
                    font_width = 8
                    font_height = 10
                    line_height = 18
                    worksheet.write_comment(
                        cell,
                        wrapped,
                        {
                            "author": "TraceBase Dev Team",
                            "font_name": "Courier",  # Fixed width for box height/width calcs
                            "font_size": font_height,
                            "height": nlines * line_height,
                            "width": nchars * font_width,
                        },
                    )
            xlsxwriter.sheets[sheet].autofit()

    def extract_autofill_from_peak_annotation_files(self):
        """Extracts data from multiple peak annotation files that can be used to populate a made-from-scratch study doc.

        Populates self.autofill_dict.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        for i in range(len(self.peak_annot_files)):
            # TODO: Add commented code when Peak Annotation Details sheet is added
            # peak_annot_filename = self.peak_annot_filenames[i]
            peak_annot_loader: PeakAnnotationsLoader = self.peak_annotations_loaders[i]

            # Extracting sample, sample header, compound, mzxml,
            for _, row in peak_annot_loader.df.iterrows():
                sample_header = peak_annot_loader.get_row_val(
                    row, peak_annot_loader.headers.SAMPLEHEADER
                )
                sample_name = MSRunsLoader.guess_sample_name(sample_header)

                if not PeakAnnotationsLoader.is_a_blank(sample_name):
                    self.autofill_dict[SamplesLoader.DataSheetName][sample_name] = {
                        SamplesLoader.DataHeaders.SAMPLE: sample_name
                    }

                # TODO: Add Peak Annotation Details sheet
                # # This unique key is based on MSRunsLoader.DataUniqueColumnConstraints
                # unique_annot_deets_key = self.row_key_delim.join(
                #     [sample_header, peak_annot_filename]
                # )
                # self.autofill_dict[MSRunsLoader.DataSheetName][
                #     unique_annot_deets_key
                # ] = {
                #     MSRunsLoader.DataHeaders.SAMPLEHEADER: sample_header,
                #     MSRunsLoader.DataHeaders.SAMPLENAME: sample_name,
                #     # No point in entering the mzXML. It will default to this in its loader:
                #     # MSRunsLoader.DataHeaders.MZXMLNAME: f"{sample_header}.mzXML",
                #     MSRunsLoader.DataHeaders.ANNOTNAME: peak_annot_filename,
                #     MSRunsLoader.DataHeaders.SKIP: PeakAnnotationsLoader.is_a_blank(
                #         sample_header
                #     ),
                #     # Not going to fill in a SEQNAME - will rely on the default in the Peak Annotation Files sheet
                # }

                # Get compounds - Note that the result of the second arg (list of compounds), will buffer errors in the
                # peak_annot_loader if any compounds are not in the database
                formula = peak_annot_loader.get_row_val(
                    row, peak_annot_loader.headers.FORMULA
                )
                delimited_compound_names, compound_recs = (
                    peak_annot_loader.get_peak_group_name_and_compounds(row)
                )
                for index, compound_string in enumerate(
                    delimited_compound_names.split(
                        PeakAnnotationsLoader.CompoundNamesDelimiter
                    )
                ):
                    compound_rec = compound_recs[index]
                    if compound_rec is not None:
                        compound_name = compound_rec.name
                        # Override whatever formula we found in the file with the one linked to this compound
                        formula = compound_rec.formula
                    else:
                        compound_name = compound_string.strip()

                    # TODO: Add a check for discrepancies, e.g. if the formula is different on different rows
                    self.autofill_dict[CompoundsLoader.DataSheetName][compound_name] = {
                        CompoundsLoader.DataHeaders.NAME: compound_name,
                        CompoundsLoader.DataHeaders.FORMULA: formula,
                    }

    def extract_autofill_from_exceptions(self, retain_as_warnings=True):
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

        Args:
            retain_as_warnings (boolean): Track extracted error and warning exceptions as warnings.  If False, no pre-
                existing errors or warnings will be reported.
        Exceptions:
            Buffers:
                MissingDataAdded
            Raises:
                None
        Returns:
            None
        """
        self.extracted_exceptions = defaultdict(lambda: {"errors": [], "warnings": []})
        # Init the autofill dict for the subsequent calls
        self.autofill_dict = defaultdict(lambda: defaultdict(dict))
        self.autofill_dict[SamplesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[TissuesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[ProtocolsLoader.DataSheetName] = defaultdict(dict)
        # TODO: Add Peak Annotation Details
        # self.autofill_dict[MSRunsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[CompoundsLoader.DataSheetName] = defaultdict(dict)
        data_added = []

        # For every AggregatedErrors objects associated with a file or category
        for load_key in [
            k
            for k, v in self.load_status_data.statuses.items()
            if v["aggregated_errors"] is not None
        ]:
            # For each exception class we want to extract from the AggregatedErrors object (in order to both "fix" the
            # data and to remove related errors that those fixes address)
            for exc_class in [
                AllMissingCompounds,  # Only removing - These are extracted already from the peak annotations files
                AllMissingSamples,  # Note, these are extracted from the peak annotations files, so this is extra
                AllMissingTissues,
                AllMissingTreatments,
                NoSamples,  # Note, these are extracted from the peak annotations files, so this is extra
            ]:

                # Remove exceptions of exc_class from the AggregatedErrors object (without modifying them)
                for exc in self.load_status_data.remove_exception_type(
                    load_key, exc_class, modify=False
                ):
                    # If this is an error (as opposed to a warning)
                    if not hasattr(exc, "is_error") or exc.is_error:
                        if retain_as_warnings:
                            self.extracted_exceptions[exc_class.__name__][
                                "errors"
                            ].append(exc)

                        if exc_class == NoSamples:
                            data_added.append(f"{len(exc.search_terms)} sample names")
                            self.extract_all_missing_samples(exc)
                        elif exc_class == AllMissingSamples:
                            data_added.append(f"{len(exc.search_terms)} sample names")
                            self.extract_all_missing_samples(exc)
                        elif exc_class == AllMissingTissues:
                            data_added.append(f"{len(exc.search_terms)} tissue names")
                            self.extract_all_missing_tissues(exc)
                        elif exc_class == AllMissingTreatments:
                            data_added.append(
                                f"{len(exc.search_terms)} treatment names"
                            )
                            self.extract_all_missing_treatments(exc)
                        # We're only removing NoSamples. All their samples are added to the AllMissingSamplesError

                    elif retain_as_warnings:
                        self.extracted_exceptions[exc_class.__name__][
                            "warnings"
                        ].append(exc)

    def extract_all_missing_samples(self, exc):
        """Extracts autofill data from the supplied AllMissingSamples exception and puts it in self.autofill_dict.

        self.autofill_dict = {
            "Samples": {unique_record_key: {header: sample_name}},  # Fills in entries here
            "Tissues": defaultdict(dict),
            "Treatments": defaultdict(dict),
        }

        Args:
            exc (AllMissingSamples): And exception object containing data about missing Samples.
        Exceptions:
            None
        Returns:
            None
        """
        # NOTE: `exc.search_terms` is only a list of sample names because the database query that hit an error was
        # searching using only the sample name.  In other words, if some other Sample model query were performed (e.g.
        # using the sample description, or some search for a substring of a sample name), then this will be wrong.  If
        # there are code changes that cause that to happen, the construction of the exceptions will have to be
        # refactored.
        for sample_name in exc.search_terms:
            self.autofill_dict[SamplesLoader.DataSheetName][sample_name] = {
                SamplesLoader.DataHeaders.SAMPLE: sample_name
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
        # NOTE: `exc.search_terms` is only a list of tissue names because the database query that hit an error was
        # searching using only the tissue name.  In other words, if some other Tissue model query were performed (e.g.
        # using the tissue description, or some search for a substring of a tissue name), then this will be wrong.  If
        # there are code changes that cause that to happen, the construction of the exceptions will have to be
        # refactored.
        for tissue_name in exc.search_terms:
            self.autofill_dict[TissuesLoader.DataSheetName][tissue_name] = {
                TissuesLoader.DataHeaders.NAME: tissue_name
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
        # NOTE: `exc.search_terms` is only a list of treatment names because the database query that hit an error was
        # searching using only the treatment name.  In other words, if some other Protocol model query were performed
        # (e.g. using the treatment description, or some search for a substring of a treatment name), then this will be
        # wrong.  If there are code changes that cause that to happen, the construction of the exceptions will have to
        # be refactored.
        for treatment_name in exc.search_terms:
            self.autofill_dict[ProtocolsLoader.DataSheetName][treatment_name] = {
                ProtocolsLoader.DataHeadersExcel.NAME: treatment_name
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
        # NOTE: If add_autofill_data added data without looking at the existing data, so it could inadvertently add data
        # that already exists in the file.  So, in order to see if the data already exists, we need to tell
        # add_autofill_data how to construct the unique key (via the 2nd arg in each of the calls below) to be able to
        # see if rows already exist or not.  We use a potentially composite key based on the unique column constraints
        # because there can exist the same individual value multiple times in a column and we do not want to prevent an
        # add if it adds a unique row).
        samples_added = self.add_autofill_data(
            SamplesLoader.DataSheetName,
            self.samples_loader.header_keys_to_names(
                # Specifically selected unique column constraint group - used to find existing rows
                SamplesLoader.DataUniqueColumnConstraints[0]
            ),
        )
        tissues_added = self.add_autofill_data(
            TissuesLoader.DataSheetName,
            self.tissues_loader.header_keys_to_names(
                # Specifically selected unique column constraint group - used to find existing rows
                TissuesLoader.DataUniqueColumnConstraints[0]
            ),
        )
        treatments_added = self.add_autofill_data(
            ProtocolsLoader.DataSheetName,
            self.treatments_loader.header_keys_to_names(
                # Specifically selected unique column constraint group - used to find existing rows
                ProtocolsLoader.DataUniqueColumnConstraints[0]
            ),
        )
        compounds_added = self.add_autofill_data(
            CompoundsLoader.DataSheetName,
            self.compounds_loader.header_keys_to_names(
                # Specifically selected unique column constraint group - used to find existing rows
                CompoundsLoader.DataUniqueColumnConstraints[0]
            ),
        )

        data_added = (
            samples_added or tissues_added or treatments_added or compounds_added
        )

        if data_added and not self.autofill_only_mode:
            # Add a warning about added data
            added_warning = MissingDataAdded(
                addition_notes=data_added, file=self.output_study_filename
            )
            self.load_status_data.set_load_exception(
                added_warning,
                "Autofill Note",
                top=True,
                default_is_error=False,
                default_is_fatal=False,
            )

    def add_autofill_data(self, sheet, row_key_headers):
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
            row_key_headers (List[str]): These are the headers whose values are used to construct the unique row keys.
                E.g. The unique row key for the Compounds sheet is "Name".  The value of that key from the autofill_dict
                is used to see if a row already exists in the dfs_dict where the autofill should occur.  If one doesn't
                exist, a new row is created.
        Exceptions:
            None
        Returns:
            None
        """
        # Get the first row index where we will be adding data (we will be incrementing this)
        next_empty_index = self.get_next_row_index(sheet)
        current_last_index = next_empty_index - 1 if next_empty_index > 0 else 0
        data_added = False

        # Iterate over the records of new data
        for unique_row_key, sheet_dict in self.autofill_dict[sheet].items():

            # We're going to iterate over the headers present in the dfs_dict, but we need to keep track if any headers
            # in the sheet_dict are absent in the dfs_dict, so we can catch it up after the loop
            headers_present = dict((k, False) for k in sheet_dict.keys())

            # Check to see if this row already exists
            index = next_empty_index
            existing_index = self.get_existing_dfs_index(
                sheet, row_key_headers, unique_row_key
            )
            if existing_index is not None:
                index = existing_index

            # For the columns in the sheet (dfs_dict)
            for header in self.dfs_dict[sheet].keys():
                # If the header is one we're adding data to
                if header in sheet_dict.keys():
                    # Record that the header was found
                    headers_present[header] = True

                    # Only fill in the value if it doesn't already have a value.
                    # Custom-entered values from the user trump autofill.
                    if self.dfs_dict[sheet][header].get(index) is None:
                        # Add the new data
                        self.dfs_dict[sheet][header][index] = sheet_dict[header]
                        data_added = True

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
                for missing_index in range(current_last_index):
                    self.dfs_dict[sheet][missing_header][missing_index] = None

                # Now set the new row value for the missing column
                if self.dfs_dict[sheet][header].get(index) is None:
                    self.dfs_dict[sheet][header][index] = sheet_dict[header]
                    data_added = True

            # Increment the new row number
            if existing_index is None:
                next_empty_index += 1

        return data_added

    def get_existing_dfs_index(self, sheet, row_key_headers, unique_row_key):
        """This determines if a row already exists in dfs_dict that matches the provided unique_row_key.

        Note, the row_key_headers are used on each row in the dfs_dict to construct its unique key and compare it to the
        provided unique_row_key.  If no matching row exists, None is returned.

        Args:
            sheet (str)
            row_key_headers (List[str])
            unique_row_key (str)
        Exceptions:
            None
        Returns:
            index (Optional[int])
        """
        first_empty_index = self.get_next_row_index(sheet)
        if first_empty_index is None:
            return None
        for index in range(first_empty_index):
            # This assumes that the row_key_headers all have values (since they are required keys)
            cur_row_key = self.row_key_delim.join(
                [self.dfs_dict[sheet][hdr].get(index, "") for hdr in row_key_headers]
            )
            if cur_row_key == unique_row_key:
                # This assumes that the value combo is unique
                return index
        return None

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
            [
                AnimalsLoader.DataSheetName,
                self.animals_loader.get_ordered_display_headers(),
            ],
            [
                SamplesLoader.DataSheetName,
                self.samples_loader.get_ordered_display_headers(),
            ],
            [
                ProtocolsLoader.DataSheetName,
                self.treatments_loader.get_ordered_display_headers(),
            ],
            [
                TissuesLoader.DataSheetName,
                self.tissues_loader.get_ordered_display_headers(),
            ],
            [
                CompoundsLoader.DataSheetName,
                self.compounds_loader.get_ordered_display_headers(),
            ],
        ]

    def header_to_cell(self, sheet, header, letter_only=False):
        """Convert a sheet name and header string into the corresponding excel cell location, e.g. "A1".
        Args:
            sheet (string): Name of the target sheet
            header (string): Name of the column header
            letter_only (boolean): Whether to include the row number
        Exceptions:
            Raises:
                ValueError
        Returns:
            (string): Cell location or column letter
        """
        headers = []
        if sheet == AnimalsLoader.DataSheetName:
            headers = self.animals_loader.get_ordered_display_headers()
        elif sheet == SamplesLoader.DataSheetName:
            headers = self.samples_loader.get_ordered_display_headers()
        elif sheet == ProtocolsLoader.DataSheetName:
            headers = self.treatments_loader.get_ordered_display_headers()
        elif sheet == TissuesLoader.DataSheetName:
            headers = self.tissues_loader.get_ordered_display_headers()
        elif sheet == CompoundsLoader.DataSheetName:
            headers = self.compounds_loader.get_ordered_display_headers()
        else:
            raise ValueError(f"Invalid sheet: [{sheet}].")
        column_letter = xlsxwriter.utility.xl_col_to_name(headers.index(header))
        if letter_only:
            return column_letter
        return f"{column_letter}1"

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
            version (string) [3]: tracebase study doc version number
        Exceptions:
            None
        Returns:
            dict of dicts: dataframes-style dicts dict keyed on sheet name
        """
        if self.study_file is None:
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
            version (string) [3]: Tracebase study doc version number.
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
                    AnimalsLoader.DataSheetName: self.animals_loader.get_dataframe_template(),
                    SamplesLoader.DataSheetName: self.samples_loader.get_dataframe_template(),
                    ProtocolsLoader.DataSheetName: self.treatments_loader.get_dataframe_template(
                        populate=True,
                        filter={"category": Protocol.ANIMAL_TREATMENT},
                    ),
                    TissuesLoader.DataSheetName: self.tissues_loader.get_dataframe_template(
                        populate=True
                    ),
                    CompoundsLoader.DataSheetName: self.compounds_loader.get_dataframe_template(),
                }

            if (
                AnimalsLoader.DataSheetName in dfs_dict.keys()
                and len(dfs_dict[AnimalsLoader.DataSheetName].keys()) > 0
            ):
                self.fill_in_missing_columns(
                    dfs_dict,
                    AnimalsLoader.DataSheetName,
                    self.animals_loader.get_dataframe_template(),
                )
            else:
                dfs_dict[AnimalsLoader.DataSheetName] = (
                    self.animals_loader.get_dataframe_template()
                )

            if (
                SamplesLoader.DataSheetName in dfs_dict.keys()
                and len(dfs_dict[SamplesLoader.DataSheetName].keys()) > 0
            ):
                self.fill_in_missing_columns(
                    dfs_dict,
                    SamplesLoader.DataSheetName,
                    self.samples_loader.get_dataframe_template(),
                )
            else:
                dfs_dict[SamplesLoader.DataSheetName] = (
                    self.samples_loader.get_dataframe_template()
                )

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

            if CompoundsLoader.DataSheetName in dfs_dict.keys():
                self.fill_in_missing_columns(
                    dfs_dict,
                    CompoundsLoader.DataSheetName,
                    self.compounds_loader.get_dataframe_template(),
                )
            else:
                dfs_dict[CompoundsLoader.DataSheetName] = (
                    self.compounds_loader.get_dataframe_template()
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

    def get_study_dfs_dict(self, version=default_version):
        """Read in each sheet in self.study_file as a dict of dicts keyed on sheet (filling in any missing sheets and
        columns).

        Args:
            version (string) [3]: tracebase study doc version number
        Exceptions:
            NotImplementedError
        Returns:
            dfs_dict (dict of dicts): pandas-style dicts dict keyed on sheet name
        """
        if version == self.default_version or version.startswith(
            f"{self.default_version}."
        ):
            dict_of_dataframes = read_from_file(
                self.study_file, sheet=None, dtype=self.get_study_dtypes_dict()
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
            version (string) [3]: tracebase study doc version number

        Exceptions:
            Raises:
                NotImplementedError
            Buffers:
                None
        Returns:
            dtypes (Dict[str, Dict[str, type]]): dtype dicts keyed by sheet name
        """
        if version == self.default_version or version.startswith(
            f"{self.default_version}."
        ):
            return {
                AnimalsLoader.DataSheetName: self.animals_loader.get_column_types(),
                SamplesLoader.DataSheetName: self.samples_loader.get_column_types(),
                ProtocolsLoader.DataSheetName: self.treatments_loader.get_column_types(),
                TissuesLoader.DataSheetName: self.tissues_loader.get_column_types(),
                CompoundsLoader.DataSheetName: self.compounds_loader.get_column_types(),
            }
        raise NotImplementedError(
            f"Version {version} is not yet supported.  Supported versions: {self.supported_versions}"
        )

    def format_results_for_template(self):
        """This populates:
        - self.valid
        - self.results
        - self.exceptions
        - self.ordered_keys
        based on the contents of self.load_status_data

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
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

    # No need to disable autoupdates adding the @no_autoupdates decorator to this function because supplying
    # `validate=True` automatically disables them
    def validate_study(self):
        load_status_data = MultiLoadStatus(load_keys=self.all_infile_names)

        try:
            StudyLoader(
                file=self.study_file,
                filename=self.study_filename,
                _validate=True,
                annot_files_dict=self.annot_files_dict,
            ).load_data()
        except MultiLoadStatus as mls:
            load_status_data = mls

        self.load_status_data = load_status_data

        return self.load_status_data.is_valid

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
        if not self.dfs_dict_is_valid():
            raise ValueError(
                "Cannot call create_study_file_writer when dfs_dict is not valid/created."
            )

        xlsxwriter = pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
            stream_obj, engine="xlsxwriter"
        )

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

    def dfs_dict_is_valid(self):
        """This determines whether self.dfs_dict is correctly structured (not populated), e.g. all required headers are
        present.

        Args:
            None
        Exceptions:
            None
        Returns:
            valid (boolean)
        """
        if self.dfs_dict is None or len(
            [
                s
                for s in self.get_study_sheet_column_display_order()
                if s[0] in self.dfs_dict.keys()
            ]
        ) < len(self.get_study_sheet_column_display_order()):
            return False

        return (
            # Required headers present in each sheet
            SamplesLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[SamplesLoader.DataSheetName])
            ).check_dataframe_headers()
            and AnimalsLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[AnimalsLoader.DataSheetName])
            ).check_dataframe_headers()
            and TissuesLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[TissuesLoader.DataSheetName])
            ).check_dataframe_headers()
            and ProtocolsLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[ProtocolsLoader.DataSheetName]),
                headers=ProtocolsLoader.DataHeadersExcel,
            ).check_dataframe_headers()
            and CompoundsLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[CompoundsLoader.DataSheetName])
            ).check_dataframe_headers()
        )


def validation_disabled(request):
    return render(request, "validation_disabled.html")
