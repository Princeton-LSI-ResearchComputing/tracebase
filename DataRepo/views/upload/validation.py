import os.path
import re
import shutil
import tempfile
from collections import defaultdict
from sqlite3 import ProgrammingError
from typing import Dict, List, Optional, cast

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
            sample_filename=str(sample_file),
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
        # TODO: Turn the following into a means to create a sample sheet
        # peak_annot_file_name = None
        # peak_annot_files = list(set(self.accucor_files).union(set(self.isocorr_files)))

        # if len(peak_annot_files) == 1:
        #     # This assumes that, due to the form class's clean method, there is exactly 1 peak annotation file
        #     peak_annot_file = list(
        #         set(self.accucor_files).union(set(self.isocorr_files))
        #     )[0]

        #     # Assumes the second sheet (index 1) is the "corrected" (accucor) or "absolte" (isocorr)
        #     corrected_sheet = 1
        #     peak_annot_sample_headers = get_sample_headers(
        #         read_headers_from_file(
        #             peak_annot_file, sheet=corrected_sheet, filetype="excel"
        #         )
        #     )

        #     peak_annot_file_name = list(
        #         set(self.accucor_filenames).union(set(self.isocorr_filenames))
        #     )[0]

        #     lcms_dict = self.build_lcms_dict(
        #         peak_annot_sample_headers,
        #         peak_annot_file_name,
        #     )

        debug = f"asf: {self.animal_sample_file} num afs: {len(self.accucor_files)} num ifs: {len(self.isocorr_files)}"

        valid, results, exceptions, ordered_keys = self.get_validation_results()

        study_file = self.get_output_study_file()

        return self.render_to_response(
            self.get_context_data(
                results=results,
                debug=debug,
                valid=valid,
                form=form,
                exceptions=exceptions,
                submission_url=self.submission_url,
                ordered_keys=ordered_keys,
                study_file=study_file,
            )
        )

    def get_output_study_file(self):
        # TODO: In subsequent PRs, uncomment, generate an excel file with a custom column order, then populate from
        # data contained in exceptions, comment with warning and error messages, and add formulas for inter-sheet
        # population of drop-downs

        # study_dfs_dict = self.get_or_create_study_dataframes()

        return "TODO: Return study_dfs_dict once get_output_study_file is implemented"

    def get_or_create_study_dataframes(self, version=default_version):
        """Get or create dataframes dict templates for each sheet in self.animal_sample_file as a dict keyed on sheet.

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
        tl = TissuesLoader()
        # Providing a dummy excel file will change the headers in the returned column types to the custom excel headers
        # TODO: Make it possible to explicitly set the type of headers we want so that a dummy file name is not required
        pl = ProtocolsLoader(file="dummy.xlsx")

        if version == self.default_version or version.startswith(
            f"{self.default_version}."
        ):
            if dfs_dict is None:
                # Setting sheet to None reads all sheets and returns a dict keyed on sheet name
                return {
                    # TODO: Update the animal and sample entries below once the loader has been refactored
                    # The sample table loader has not yet been refactored/split
                    "Animals": self.animals_dict,
                    "Samples": self.samples_dict,
                    ProtocolsLoader.DataSheetName: pl.get_dataframe_template(
                        populate=True,
                        filter={"category": Protocol.ANIMAL_TREATMENT},
                    ),
                    TissuesLoader.DataSheetName: tl.get_dataframe_template(
                        populate=True
                    ),
                }

            if "Animals" in dfs_dict.keys() and len(dfs_dict["Animals"].keys()) > 0:
                self.fill_in_missing_columns(dfs_dict, "Animals", self.animals_dict)
            else:
                dfs_dict["Animals"] = self.animals_dict

            if "Samples" in dfs_dict.keys() and len(dfs_dict["Samples"].keys()) > 0:
                self.fill_in_missing_columns(dfs_dict, "Samples", self.samples_dict)
            else:
                dfs_dict["Samples"] = self.samples_dict

            if ProtocolsLoader.DataSheetName in dfs_dict.keys():
                self.fill_in_missing_columns(
                    dfs_dict,
                    ProtocolsLoader.DataSheetName,
                    pl.get_dataframe_template(),
                )
            else:
                dfs_dict[ProtocolsLoader.DataSheetName] = pl.get_dataframe_template(
                    populate=True
                )

            if TissuesLoader.DataSheetName in dfs_dict.keys():
                self.fill_in_missing_columns(
                    dfs_dict,
                    TissuesLoader.DataSheetName,
                    tl.get_dataframe_template(),
                )
            else:
                dfs_dict[TissuesLoader.DataSheetName] = tl.get_dataframe_template(
                    populate=True
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
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_TREATMENT: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_WEIGHT: {},
            SampleTableLoader.DefaultSampleTableHeaders.INFUSATE: {},
            SampleTableLoader.DefaultSampleTableHeaders.TRACER_CONCENTRATIONS: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_INFUSION_RATE: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_DIET: {},
            SampleTableLoader.DefaultSampleTableHeaders.ANIMAL_FEEDING_STATUS: {},
            SampleTableLoader.DefaultSampleTableHeaders.STUDY_NAME: {},
            SampleTableLoader.DefaultSampleTableHeaders.STUDY_DESCRIPTION: {},
        }

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
        tl = TissuesLoader()
        # Providing an excel file will change the headers in the returned column types to the custom excel headers
        pl = ProtocolsLoader(file=self.animal_sample_file)

        if version == self.default_version or version.startswith(
            f"{self.default_version}."
        ):
            animal_sample_headers = SampleTableLoader.DefaultSampleTableHeaders
            return {
                # TODO: Update the animal and sample entries below once the loader has been refactored
                # The sample table loader has not yet been refactored/split
                "Animals": {
                    animal_sample_headers.ANIMAL_NAME: str,
                    animal_sample_headers.ANIMAL_TREATMENT: str,
                },
                "Samples": {animal_sample_headers.ANIMAL_NAME: str},
                ProtocolsLoader.DataSheetName: pl.get_column_types(),
                TissuesLoader.DataSheetName: tl.get_column_types(),
            }
        raise NotImplementedError(
            f"Version {version} is not yet supported.  Supported versions: {self.supported_versions}"
        )

    def get_validation_results(self):
        load_status_data = self.validate_study()

        valid = load_status_data.is_valid
        results = {}
        exceptions = {}
        ordered_keys = []

        for load_key in load_status_data.get_ordered_status_keys():
            # The load_key is the absolute path, but we only want to report errors in the context of the file's name
            short_load_key = os.path.basename(load_key)

            ordered_keys.append(short_load_key)
            results[short_load_key] = load_status_data.statuses[load_key]["state"]

            exceptions[short_load_key] = []
            # Get the AggregatedErrors object
            aes = load_status_data.statuses[load_key]["aggregated_errors"]
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

        return valid, results, exceptions, ordered_keys

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

        return load_status_data

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


def validation_disabled(request):
    return render(request, "validation_disabled.html")
