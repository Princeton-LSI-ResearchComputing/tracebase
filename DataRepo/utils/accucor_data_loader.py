import hashlib
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, TypedDict

import regex
import xmltodict
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.core.files import File
from django.db import IntegrityError, transaction

from DataRepo.models import (
    ArchiveFile,
    Compound,
    DataFormat,
    DataType,
    ElementLabel,
    LCMethod,
    MaintainedModel,
    MSRunSample,
    MSRunSequence,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupLabel,
    Researcher,
    Sample,
)
from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.researcher import (
    UnknownResearcherError,
    get_researchers,
    validate_researchers,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    ConflictingValueErrors,
    CorrectedCompoundHeaderMissing,
    DryRun,
    DupeCompoundIsotopeCombos,
    DuplicatePeakGroup,
    DuplicatePeakGroups,
    EmptyColumnsError,
    InvalidLCMSHeaders,
    IsotopeObservationParsingError,
    IsotopeStringDupe,
    LCMethodFixturesMissing,
    LCMSDefaultsRequired,
    MassNumberNotFound,
    MismatchedSampleHeaderMZXML,
    MissingCompounds,
    MissingLCMSSampleDataHeaders,
    MissingMZXMLFiles,
    MissingSamplesError,
    MixedPolarityErrors,
    MultipleAccucorTracerLabelColumnsError,
    MultipleMassNumbers,
    NoSamplesError,
    NoTracerLabeledElements,
    PeakAnnotFileMismatches,
    PolarityConflictErrors,
    ResearcherNotNew,
    SampleColumnInconsistency,
    SampleIndexNotFound,
    TracerLabeledElementNotFound,
    UnexpectedIsotopes,
    UnexpectedLCMSSampleDataHeaders,
    UnskippedBlanksError,
)
from DataRepo.utils.lcms_metadata_parser import (
    lcms_df_to_dict,
    lcms_headers_are_valid,
)

# Global variables for Accucor/Isocorr corrected column names/patterns
# List of column names from data files that we know are not samples.  Note that the column names are the same in
# Accucor versus Isocorr - they're just ordered differently.
NONSAMPLE_COLUMN_NAMES = [
    "label",  # Accucor format
    "metaGroupId",
    "groupId",
    "goodPeakCount",
    "medMz",
    "medRt",
    "maxQuality",
    "isotopeLabel",
    "compound",
    "compoundId",
    "formula",
    "expectedRtDiff",
    "ppmDiff",
    "parent",
    "Compound",
    "adductName",
]
# append the *_Label columns of the corrected dataframe for accucor format
for element in ElementLabel.labeled_elements_list():
    NONSAMPLE_COLUMN_NAMES.append(f"{element}_Label")
ACCUCOR_LABEL_PATTERN = re.compile(
    f"^({'|'.join(ElementLabel.labeled_elements_list())})_Label$"
)
# regex has the ability to store repeated capture groups' values and put them in a list
ISOTOPE_LABEL_PATTERN = regex.compile(
    # Match repeated elements and mass numbers (e.g. "C13N15")
    r"^(?:(?P<elements>["
    + "".join(ElementLabel.labeled_elements_list())
    + r"]{1,2})(?P<mass_numbers>\d+))+"
    # Match either " PARENT" or repeated counts (e.g. "-labels-2-1")
    + r"(?: (?P<parent>PARENT)|-label(?:-(?P<counts>\d+))+)$"
)


class IsotopeObservationData(TypedDict):
    element: str
    mass_number: int
    count: int
    parent: bool


class PeakGroupAttrs(TypedDict):
    name: str
    formula: str
    compounds: List[Compound]


class AccuCorDataLoader:
    """
    Load the LCMethod, MsRunSequence, MSRunSample, PeakGroup, and PeakData tables
    """

    def __init__(
        self,
        accucor_original_df,
        accucor_corrected_df,
        peak_annotation_file,
        peak_annotation_filename=None,
        lcms_metadata_df=None,
        instrument=None,
        polarity=None,
        date=None,
        lc_protocol_name=None,
        mzxml_files=None,
        researcher=None,
        skip_samples=None,
        sample_name_prefix=None,
        allow_new_researchers=False,
        validate=False,
        isocorr_format=False,
        verbosity=1,
        dry_run=False,
        update_caches=True,
    ):
        # File data
        self.accucor_original_df = accucor_original_df
        self.accucor_corrected_df = accucor_corrected_df
        self.peak_annotation_filepath = peak_annotation_file
        self.isocorr_format = isocorr_format
        self.lcms_metadata_df = lcms_metadata_df
        self.mzxml_files_list = mzxml_files
        self.peak_annotation_filename = None
        if peak_annotation_filename:
            self.peak_annotation_filename = peak_annotation_filename.strip()

        # LCMS Defaults
        self.lc_protocol_name = lc_protocol_name
        self.date = date
        self.researcher = researcher
        self.instrument = instrument
        self.polarity = polarity

        # Modes
        self.allow_new_researchers = allow_new_researchers
        self.verbosity = verbosity
        self.dry_run = dry_run
        self.validate = validate
        self.update_caches = update_caches
        # Making update_caches True causes existing caches associated with loaded records to be deleted

        # Tracking Data
        self.skip_samples = skip_samples if skip_samples else []
        self.sample_name_prefix = sample_name_prefix if sample_name_prefix else ""
        self.peak_group_dict: Dict[str, PeakGroupAttrs] = {}
        self.corrected_sample_headers = []
        self.original_sample_headers = []
        self.db_samples_dict = None
        self.labeled_element_header = None
        self.labeled_element = None  # For accucor only
        self.tracer_labeled_elements = []  # For isocorr only
        self.mzxml_data = {}

        # Error tracking
        self.aggregated_errors_object = AggregatedErrors()
        self.missing_samples = []
        self.missing_sample_headers = []
        self.unexpected_sample_headers = []
        self.missing_compounds = {}
        self.dupe_isotope_compounds = {
            "original": defaultdict(dict),
            "corrected": defaultdict(dict),
        }
        self.duplicate_peak_groups = []
        self.conflicting_peak_groups = []
        self.missing_mzxmls = []
        self.mismatching_mzxmls = []
        self.mixed_polarities = {}
        self.conflicting_polarities = {}

    def process_default_lcms_opts(self):
        """
        This validates and initializes all of the default LCMS (command line) options.

        It also checks the LCMS metadata file for any missing headers, to determine whether any particular default
        option is necessary.
        """
        # Required LCMS Metadata Defaults
        reqd_lcms_defaults = {
            "lc_protocol_name": self.lc_protocol_name,
            "date": self.date,
            "researcher": self.researcher,
            "instrument": self.instrument,
        }
        # Initialize LCMS Defaults
        lcms_defaults = {
            "lc_protocol_name": None,
            "date": None,
            "researcher": None,
            "instrument": None,
            "polarity": MSRunSample.POLARITY_DEFAULT,
            "peak_annot_file": self.peak_annotation_filename,
        }

        if self.lc_protocol_name is not None and self.lc_protocol_name.strip() != "":
            lcms_defaults["lc_protocol_name"] = self.lc_protocol_name.strip()
        if self.date is not None and self.date.strip() != "":
            lcms_defaults["date"] = datetime.strptime(self.date.strip(), "%Y-%m-%d")
        if self.researcher is not None and self.researcher.strip() != "":
            lcms_defaults["researcher"] = self.researcher.strip()
        if self.instrument is not None and self.instrument.strip() != "":
            lcms_defaults["instrument"] = self.instrument.strip()
        if self.polarity is not None and self.polarity.strip() != "":
            lcms_defaults["polarity"] = self.polarity.strip()

        # Check LCMS metadata (after having initialized the lcms defaults)
        if self.lcms_metadata_df is None and None in reqd_lcms_defaults.values():
            missing = [
                key
                for key in reqd_lcms_defaults.keys()
                if reqd_lcms_defaults[key] is None
            ]
            self.aggregated_errors_object.buffer_error(
                LCMSDefaultsRequired(missing_defaults_list=missing)
            )
            self.set_lcms_placeholder_defaults()
        elif self.lcms_metadata_df is not None and not lcms_headers_are_valid(
            list(self.lcms_metadata_df.columns)
        ):
            self.aggregated_errors_object.buffer_error(
                InvalidLCMSHeaders(list(self.lcms_metadata_df.columns))
            )
            self.set_lcms_placeholder_defaults()

        return lcms_defaults

    def associate_mzxml_files_with_sample_headers(self):
        """
        This creates an mzxml_files_dict, keyed by sample header and contains both the mzxml file "path" and "base" name
        """
        mzxml_files_dict = None
        if self.mzxml_files_list is not None and len(self.mzxml_files_list) > 0:
            # self.mzxml_files_list is assumed to be populated with basenames
            mzxml_files_dict = defaultdict(dict)
            for fn in self.mzxml_files_list:
                mz_basename = os.path.basename(fn)
                hdr = self.get_sample_header_by_mzxml_basename(mz_basename)
                if self.lcms_metadata_df is not None and hdr is not None:
                    mzxml_files_dict[hdr]["path"] = fn
                    mzxml_files_dict[hdr]["base"] = mz_basename
                else:
                    nm, _ = os.path.splitext(mz_basename)
                    # pylint: disable=unsupported-assignment-operation
                    mzxml_files_dict[nm]["path"] = fn
                    mzxml_files_dict[nm]["base"] = mz_basename
                    # pylint: enable=unsupported-assignment-operation
        return mzxml_files_dict

    def get_sample_header_by_mzxml_basename(self, mzxml_basename):
        """
        This method searches the lcms_metadata dict for an mzxml file that matches the a supplied basename from the
        actual files supplied.

        Note, previously, an assumption was made that the header and mzxml file will always match.  While this may or
        may not be true in a real world use-case, it caused problems in the tests, and instead of create new input
        files, I just decided to eliminate the assumption.
        """
        results = []
        for header in self.lcms_metadata.keys():
            if mzxml_basename == self.lcms_metadata[header]["mzxml"]:
                results.append(header)
        if len(results) == 0:
            return None
        elif len(results) > 1:
            self.aggregated_errors_object.buffer_error(
                ValueError(
                    f"{len(results)} instances of mzxml file [{mzxml_basename}] in the LCMS metadata file."
                )
            )
            return None
        return results[0]

    def initialize_preloaded_animal_sample_data(self):
        """
        Without caching updates enables, retrieve loaded matching samples and labeled elements.
        """
        # The samples/animal (and the infusate) are required to already have been loaded
        self.initialize_db_samples_dict()
        self.initialize_tracer_labeled_elements()

    def preprocess_data(self):
        # Prepare all of the LCMS Metadata
        self.prepare_metadata()
        # Clean up the peak annotation dataframes
        self.clean_peak_annotation_dataframes()
        # Obtain the sample names from the headers
        self.initialize_sample_names()
        # Use the obtained sample names to fill in default values in the lcms_metadata dict
        self.fill_in_lcms_defaults()

    def prepare_metadata(self):
        # Process the LCMS default arguments
        self.lcms_defaults = self.process_default_lcms_opts()
        # Process the LCMS metadata "file"
        self.lcms_metadata = lcms_df_to_dict(
            self.lcms_metadata_df, self.aggregated_errors_object
        )
        # Associate the actual mzxml files with the sample headers in the peak annot file
        self.mzxml_files_dict = self.associate_mzxml_files_with_sample_headers()

    def clean_peak_annotation_dataframes(self):
        if self.accucor_original_df is not None:
            # Strip white space from all original sheet headers
            self.accucor_original_df.rename(columns=lambda x: x.strip())

            # Strip whitespace from a few columns
            self.accucor_original_df["compound"] = self.accucor_original_df[
                "compound"
            ].str.strip()
            self.accucor_original_df["formula"] = self.accucor_original_df[
                "formula"
            ].str.strip()

        # Strip white space from all corrected sheet headers
        self.accucor_corrected_df.rename(columns=lambda x: x.strip())

        if self.isocorr_format:  # Isocorr
            self.compound_header = "compound"
            self.labeled_element_header = "isotopeLabel"
            self.labeled_element = None  # Determined on each row
        else:  # AccuCor
            self.compound_header = "Compound"
            if self.compound_header not in list(self.accucor_corrected_df.columns):
                # Cannot proceed to try and catch more errors.  The compound column is required.
                raise CorrectedCompoundHeaderMissing()

            # Accucor has the labeled element in a header in the corrected data
            self.labeled_element_header = self.accucor_corrected_df.filter(
                regex=(ACCUCOR_LABEL_PATTERN)
            ).columns[0]
            match = ACCUCOR_LABEL_PATTERN.match(self.labeled_element_header)
            self.labeled_element = match.group(1)

        # Strip whitespace from the compound column
        self.accucor_corrected_df[self.compound_header] = self.accucor_corrected_df[
            self.compound_header
        ].str.strip()

    def validate_data(self):
        """
        Basic sanity/integrity checks for the data inputs
        """
        self.validate_sample_headers()
        self.validate_researcher()
        self.validate_compounds()
        self.validate_peak_groups()

    def validate_researcher(self):
        # Compile a list of reasearchers potentially being added
        adding_researchers = []
        for sample_header in self.corrected_sample_headers:
            if (
                self.lcms_metadata[sample_header]["researcher"]
                not in adding_researchers
            ):
                adding_researchers.append(
                    self.lcms_metadata[sample_header]["researcher"]
                )

        if self.allow_new_researchers is True:
            researchers = get_researchers()
            all_existing = True
            for res_add in adding_researchers:
                if res_add not in researchers:
                    all_existing = False
                    break
            if all_existing and len(adding_researchers) > 0:
                self.aggregated_errors_object.buffer_error(
                    ResearcherNotNew(
                        adding_researchers, "--new-researcher", researchers
                    )
                )
        else:
            try:
                validate_researchers(adding_researchers, skip_flag="--new-researcher")
            except UnknownResearcherError as ure:
                # This is a raised warning when in validate mode.  The user should know about it (so it's raised), but
                # they can't address it if the researcher is valid.
                # This is a raised error when not in validate mode.  The curator must know about and address the error.
                self.aggregated_errors_object.buffer_exception(
                    ure, is_error=not self.validate, is_fatal=True
                )

    def initialize_sample_names(self):
        self.corrected_sample_headers = get_sample_headers(
            self.accucor_corrected_df, self.skip_samples
        )

        if self.accucor_original_df is not None:
            minimum_sample_index = get_first_sample_column_index(
                self.accucor_original_df
            )
            if minimum_sample_index is None:
                # Sample columns are required to proceed
                raise SampleIndexNotFound(
                    "original",
                    list(self.accucor_original_df.columns),
                    NONSAMPLE_COLUMN_NAMES,
                )
            self.original_sample_headers = [
                sample
                for sample in list(self.accucor_original_df)[minimum_sample_index:]
                if sample not in self.skip_samples
            ]

    def fill_in_lcms_defaults(self):
        # Validate the sample data headers WRT to lcms metadata and fill in placeholder defaults for missing defaults
        # Note, we won't know if any defaults are actually required until this check is done.  I.e. defaults are not
        # required if the LCMS metadata file is fully fleshed out.
        if self.lcms_metadata_df is not None:
            # We loop on self.lcms_metadata.keys() instead of self.corrected_sample_headers in order to catch issues
            # where incorrect sample data headers are associated with the wrong accucor file.  This assumes that sample
            # data headers are unique across all accucor files in a study.
            for sample_header in self.lcms_metadata.keys():
                # Excess sample data headers are allowed to be supplied to make it easy to supply data across multiple
                # accucor files, but if a sample data header associated with the current accucor file in the LCMS
                # metadata is not found among the headers in the file, buffer it as an unexpected sample data header (to
                # be raised as an error exception)
                if (
                    (
                        self.lcms_metadata[sample_header]["peak_annot_file"] is None
                        or self.lcms_metadata[sample_header]["peak_annot_file"]
                        == self.peak_annotation_filename
                    )
                    # sample data header from the LCMS metadata is not in the accucor file
                    and sample_header not in self.corrected_sample_headers
                ):
                    self.unexpected_sample_headers.append(sample_header)

            for sample_header in self.corrected_sample_headers:
                if sample_header not in self.lcms_metadata.keys():
                    self.missing_sample_headers.append(sample_header)

            if len(self.missing_sample_headers) > 0:
                # Defaults are required if any sample is missing in the lcms_metadata file
                self.aggregated_errors_object.buffer_exception(
                    MissingLCMSSampleDataHeaders(
                        self.missing_sample_headers,
                        self.peak_annotation_filename,
                        self.get_missing_required_lcms_defaults(),
                    ),
                    is_error=not self.lcms_defaults_supplied(),
                    is_fatal=not self.lcms_defaults_supplied(),
                )
                if not self.lcms_defaults_supplied():
                    self.set_lcms_placeholder_defaults()
                    # The above will raise an exception.  In order to catch more errros and skip errors cause by no
                    # defaults, fill in temporary placeholders.

            if len(self.unexpected_sample_headers) > 0:
                self.aggregated_errors_object.buffer_error(
                    UnexpectedLCMSSampleDataHeaders(
                        self.unexpected_sample_headers, self.peak_annotation_filename
                    )
                )

        # Fill in any LCMS metadata that caused errors above with available default values
        incorrect_pgs_files = {}
        missing_header_defaults = defaultdict(dict)
        placeholders_needed = False
        for sample_header in self.corrected_sample_headers:
            # Determine the polarity using the LCMS metadata file's value, the parsed mzXML file value, the command line
            # default value, and the global default value.
            # Precedence: mzXML > LCMS file > Command line default > global default.
            polarity = self.lcms_defaults["polarity"]
            if (
                self.mzxml_files_dict is not None
                and sample_header in self.mzxml_files_dict.keys()
            ):
                parsed_polarity = None
                path_obj = Path(str(self.mzxml_files_dict[sample_header]["path"]))
                if path_obj.is_file():
                    self.mzxml_data[sample_header] = self.parse_mzxml(path_obj)
                    parsed_polarity = self.mzxml_data[sample_header]["polarity"]
                    if (
                        sample_header in self.lcms_metadata.keys()
                        and parsed_polarity is not None
                        # When lcms metadata has None or default, quietly overwrite with the value from the mzxml
                        and self.lcms_metadata[sample_header]["polarity"] is not None
                        and self.lcms_metadata[sample_header]["polarity"]
                        != MSRunSample.POLARITY_DEFAULT
                        and parsed_polarity
                        != self.lcms_metadata[sample_header]["polarity"]
                    ):
                        # Add a polarity conflict
                        self.conflicting_polarities[str(path_obj)] = {
                            "sample_header": sample_header,
                            "lcms_value": self.lcms_metadata[sample_header]["polarity"],
                            "mzxml_value": parsed_polarity,
                        }
                else:
                    self.aggregated_errors_object.buffer_error(
                        ValueError(f"mzxml file does not exist: {str(path_obj)}")
                    )
                if parsed_polarity is not None:
                    polarity = parsed_polarity

            # A default file name is constructed (if missing in the LCMS metadata file, associated with a sample data
            # header).  It is used to match against the supplied bolus of mzXML files that are separately supplied.
            default_mzxml_file = self.sample_header_to_default_mzxml(sample_header)
            mzxml_file = default_mzxml_file

            # If the sample header obtained from the peak annotation file is not in the LCMS metadata file, we will need
            # to completely use the default values (and the imputed mzXML file name
            if sample_header not in self.lcms_metadata.keys():
                # Fill in default mzXML file name by checking the mzxml_files_dict
                if (
                    self.mzxml_files_dict is not None
                    and len(self.mzxml_files_dict.keys()) > 0
                    and sample_header in self.mzxml_files_dict.keys()
                ):
                    # pylint: disable=unsubscriptable-object
                    mzxml_file = self.mzxml_files_dict[sample_header]["path"]
                    # pylint: enable=unsubscriptable-object

                # Fill in all default values for the missing sample header
                self.lcms_metadata[sample_header] = {
                    "sample_header": sample_header,
                    "sample_name": sample_header,
                    "peak_annot_file": self.lcms_defaults["peak_annot_file"],
                    "mzxml": mzxml_file,
                    "researcher": self.lcms_defaults["researcher"],
                    "instrument": self.lcms_defaults["instrument"],
                    "polarity": polarity,
                    "date": self.lcms_defaults["date"],
                    "lc_protocol_name": self.lcms_defaults["lc_protocol_name"],
                    "lc_type": None,
                    "lc_run_length": None,
                    "lc_description": None,
                }
            else:
                # Note any mismatched peak annot file names
                if (
                    self.lcms_metadata[sample_header]["peak_annot_file"] is not None
                    and self.lcms_metadata[sample_header]["peak_annot_file"]
                    != self.peak_annotation_filename
                ):
                    # We can assume sample_header is unique due to previous code
                    incorrect_pgs_files[sample_header] = self.lcms_metadata[
                        sample_header
                    ]["peak_annot_file"]

                # Fill in default mzxml file if missing
                if (
                    self.lcms_metadata[sample_header]["mzxml"] is None
                    and self.mzxml_files_dict is not None
                    and len(self.mzxml_files_dict.keys()) > 0
                    and sample_header not in self.mzxml_files_dict.keys()
                ):
                    self.lcms_metadata[sample_header]["mzxml"] = default_mzxml_file

                mzxml_file = self.lcms_metadata[sample_header]["mzxml"]

                # Fill in default values for any key whose value is missing
                for key in self.lcms_defaults.keys():
                    if self.lcms_metadata[sample_header][key] is None:
                        # Special case for polarity (because it could have been parsed from the mzxml file)
                        if polarity is not None and key == "polarity":
                            self.lcms_metadata[sample_header][key] = polarity
                            continue
                        if self.lcms_defaults[key] is None:
                            placeholders_needed = True
                            missing_header_defaults["default"][key] = True
                            missing_header_defaults["header"][sample_header] = True
                        else:
                            self.lcms_metadata[sample_header][key] = self.lcms_defaults[
                                key
                            ]

            # Make sure the mzXML file name matches the sample header (warn if it's not an exact match)
            self.find_mismatching_missing_mzxml_files(sample_header, mzxml_file)

        # If there are missing LCMS headers, buffer an error
        if len(missing_header_defaults.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                LCMSDefaultsRequired(
                    missing_defaults_list=list(
                        missing_header_defaults["default"].keys()
                    ),
                    affected_sample_headers_list=list(
                        missing_header_defaults["header"].keys()
                    ),
                )
            )
            self.set_lcms_placeholder_defaults()

        # We cannot fill in placeholders for the defaults until we have made a complete accounting of all data in the
        # LCMS metadata file that is the reason placeholders are needed (i.e. a complete traversal of all of the sample
        # data headers).  Now that we've done that, we can make a second pass and fill in the placeholders.
        if placeholders_needed is True:
            # Fill in the placeholders
            for sample_header in self.corrected_sample_headers:
                for key in self.lcms_defaults.keys():
                    if (
                        self.lcms_metadata[sample_header][key] is None
                        and self.lcms_defaults[key] is not None
                    ):
                        self.lcms_metadata[sample_header][key] = self.lcms_defaults[key]

        # If there were mismatching peak annot files, buffer an error about them
        if len(incorrect_pgs_files.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                PeakAnnotFileMismatches(
                    incorrect_pgs_files, self.peak_annotation_filename
                )
            )

        self.buffer_mismatching_missing_mzxml_file_errors()

    def set_lcms_placeholder_defaults(self):
        """
        If an exception is going to be raised, use this method to fill in defaults with placeholders to prevent
        cascading errors unrelated to the root problem
        """
        # No need to fill in "lc_protocol_name".  None will cause a retrieval of the unknown protocol.
        if self.lcms_defaults["date"] is None:
            self.lcms_defaults["date"] = datetime.strptime("1972-11-24", "%Y-%m-%d")
        if self.lcms_defaults["researcher"] is None:
            self.lcms_defaults["researcher"] = Researcher.RESEARCHER_DEFAULT
        if self.lcms_defaults["instrument"] is None:
            self.lcms_defaults["instrument"] = MSRunSequence.INSTRUMENT_DEFAULT
        if self.lcms_defaults["polarity"] is None:
            self.lcms_defaults["polarity"] = MSRunSample.POLARITY_DEFAULT
        # No need to fill in "peak_annot_file".  Without this file, nothing will load

    def sample_header_to_default_mzxml(self, sample_header):
        """
        This retrieves the mzXML file name from self.mzxml_files that matches the supplied sample header.  If mzxml
        files were not provided, it will be recorded as missing, but will be automatically filled in with
        "{sample_header}.mzxml".
        """
        if self.mzxml_files_dict is not None and len(self.mzxml_files_dict.keys()) > 0:
            # pylint: disable=unsubscriptable-object
            if sample_header in self.mzxml_files_dict.keys():
                return self.mzxml_files_dict[sample_header]["path"]
            else:
                # PR REVIEW NOTE: Is this the standard extension of mzxml files???
                return f"{sample_header}.mzxml"
            # pylint: enable=unsubscriptable-object

        return None

    def find_mismatching_missing_mzxml_files(self, sample_header, mzxml_file):
        """
        This method is intended to check that a single mzxml file, listed in the LCMS metadata file, was actually
        supplied.

        It also checks that the sample data header associated with the mzXML file from the LCMS metadata file is
        contained in the mzXML file name.  If not, it notes the mismatch, which will later result in a mild warning.
        """
        # For historical reasons, we don't require mzXML files
        mzxml_basename = None
        if mzxml_file is not None:
            mzxml_basename = os.path.basename(mzxml_file)
        if (
            self.mzxml_files_dict is not None
            and len(self.mzxml_files_dict.keys()) > 0
            and mzxml_file is not None
            and mzxml_basename
            not in [d["base"] for d in self.mzxml_files_dict.values()]
        ):
            self.missing_mzxmls.append(mzxml_file)

        # Issue a warning if the sample header doesn't match the file name
        if mzxml_file is not None and mzxml_file != "":
            mzxml_noext, _ = os.path.splitext(mzxml_basename)
            if sample_header != mzxml_noext:
                self.mismatching_mzxmls.append([sample_header, mzxml_file])

    def buffer_mismatching_missing_mzxml_file_errors(self):
        """
        This method buffers exceptions if there happened to be logged issues with any mzxml files
        """
        # TODO: Implement issue #814 and then uncomment this warning
        # if self.validate and (
        #     self.mzxml_files is None or len(self.mzxml_files.keys()) == 0
        # ):
        #     # New studies should be encouraged to include mzxml files, thus validate mode should generate a warning
        #     # without them.  (Old studies should have mzXML files as optional, thus the curator only gets a printed
        #     # warning.)
        #     self.aggregated_errors_object.buffer_warning(NoMZXMLFiles())

        # elif (
        if (
            self.mzxml_files_dict is not None
            and len(self.mzxml_files_dict.keys()) > 0
            and len(self.missing_mzxmls) > 0
        ):
            # New studies should require mzxml files, thus the user validate mode is a fatal error
            # Old studies should have mzXML files as optional, thus the curator only gets a printed warning
            self.aggregated_errors_object.buffer_error(
                MissingMZXMLFiles(self.missing_mzxmls)
            )

        if len(self.mismatching_mzxmls) > 0:
            self.aggregated_errors_object.buffer_exception(
                MismatchedSampleHeaderMZXML(self.mismatching_mzxmls),
                is_error=False,
                is_fatal=self.validate,  # Fatal/raised in validate mode, will only be in load mode
            )

        # Since the mzXML files specified may contain sample headers from multiple accucor files, there is no check for
        # unused mzXML files.  See the sample_table_loader for a check of the samples, as an indirect check on unused
        # mzXML files.

    def get_missing_required_lcms_defaults(self):
        optionals = ["polarity"]
        # polarity will default to the value in the parsed mzXML file.  If the mzXML file is not supplied, it the
        # supplied --polarity default will be used, and if that default is not supplied, it will default to
        # MSRunSample.POLARITY_DEFAULT
        return [
            key
            for key in self.lcms_defaults.keys()
            if self.lcms_defaults[key] is None and key not in optionals
        ]

    def lcms_defaults_supplied(self):
        """Returns False if any default value is None"""
        missing_defaults = self.get_missing_required_lcms_defaults()
        return len(missing_defaults) == 0

    def validate_sample_headers(self):
        """
        Validate sample headers to ensure they all have sample names, sheets are consistent, and if it's an Accucor
        file, that it only has 1 label column.
        """

        if self.verbosity >= 1:
            print("Validating data...")

        # Make sure all sample columns have names
        corr_iter = Counter(self.corrected_sample_headers)
        for k, _ in corr_iter.items():
            if k.startswith("Unnamed: "):
                self.aggregated_errors_object.buffer_error(
                    EmptyColumnsError(
                        "Corrected", list(self.accucor_corrected_df.columns)
                    )
                )

        if self.original_sample_headers:
            # Make sure all sample columns have names
            orig_iter = Counter(self.original_sample_headers)
            for k, _ in orig_iter.items():
                if k.startswith("Unnamed: "):
                    self.aggregated_errors_object.buffer_error(
                        EmptyColumnsError(
                            "Original", list(self.accucor_original_df.columns)
                        )
                    )

            # Make sure that the sheets have the same number of sample columns
            if orig_iter != corr_iter:
                original_only = sorted(
                    set(self.original_sample_headers)
                    - set(self.corrected_sample_headers)
                )
                corrected_only = sorted(
                    set(self.corrected_sample_headers)
                    - set(self.original_sample_headers)
                )

                self.aggregated_errors_object.buffer_error(
                    SampleColumnInconsistency(
                        len(orig_iter),  # Num original sample columns
                        len(corr_iter),  # Num corrected sample columns
                        original_only,
                        corrected_only,
                    )
                )

                # For the sake of catching more actionale errors and not avoiding meaningless errors caused by these
                # samples missing in the original sheet, remove the samples that are missing from the corrected_samples
                # and process what we can.
                self.corrected_sample_headers = [
                    sample
                    for sample in self.corrected_sample_headers
                    if sample not in corrected_only
                ]

        if not self.isocorr_format:
            # Filter for all columns that match the labeled element header pattern
            labeled_df = self.accucor_corrected_df.filter(regex=(ACCUCOR_LABEL_PATTERN))
            if len(labeled_df.columns) != 1:
                # We can buffer this error and continue.  Only the first label column will be used.
                self.aggregated_errors_object.buffer_error(
                    MultipleAccucorTracerLabelColumnsError(labeled_df.columns)
                )

    def validate_compounds(self):
        # In case validate_compounds is ever called more than once...
        # These are used to skip duplicated data in the load
        self.dupe_isotope_compounds["original"] = defaultdict(dict)
        self.dupe_isotope_compounds["corrected"] = defaultdict(dict)
        master_dupe_dict = defaultdict(dict)

        if self.accucor_original_df is not None:
            orig_dupe_dict = defaultdict(list)
            dupe_orig_compound_isotope_labels = defaultdict(list)
            for index, row in self.accucor_original_df[
                self.accucor_original_df.duplicated(
                    subset=["compound", "isotopeLabel"], keep=False
                )
            ].iterrows():
                cmpd = row["compound"]
                lbl = row["isotopeLabel"]
                dupe_orig_compound_isotope_labels[cmpd].append(index + 2)
                dupe_key = f"Compound: [{cmpd}], Label: [{lbl}]"
                orig_dupe_dict[dupe_key].append(index + 2)

            if len(orig_dupe_dict.keys()) != 0:
                # Record the rows where this exception occurred so that subsequent downstream errors caused by this
                # exception can be ignored.
                self.dupe_isotope_compounds[
                    "original"
                ] = dupe_orig_compound_isotope_labels
                master_dupe_dict["original"] = orig_dupe_dict

        if self.isocorr_format:
            labeled_element_header = "isotopeLabel"
        else:
            labeled_element_header = self.labeled_element_header

        # do it again for the corrected
        corr_dupe_dict = defaultdict(list)
        dupe_corr_compound_isotope_counts = defaultdict(list)
        for index, row in self.accucor_corrected_df[
            self.accucor_corrected_df.duplicated(
                subset=[self.compound_header, labeled_element_header], keep=False
            )
        ].iterrows():
            cmpd = row[self.compound_header]
            lbl = row[labeled_element_header]
            dupe_corr_compound_isotope_counts[cmpd].append(index + 2)
            dupe_key = (
                f"{self.compound_header}: [{cmpd}], {labeled_element_header}: [{lbl}]"
            )
            corr_dupe_dict[dupe_key].append(index + 2)

        if len(corr_dupe_dict.keys()) != 0:
            # Record the rows where this exception occurred so that subsequent downstream errors caused by this
            # exception can be ignored.
            self.dupe_isotope_compounds["corrected"] = dupe_corr_compound_isotope_counts
            master_dupe_dict["corrected"] = corr_dupe_dict

        if len(master_dupe_dict.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                DupeCompoundIsotopeCombos(master_dupe_dict)
            )

    def initialize_db_samples_dict(self):
        self.missing_samples = []

        if self.verbosity >= 1:
            print("Checking samples...")

        # cross validate in database
        sample_dict = {}
        # Because the original dataframe might be None, here, we rely on the corrected sample list as being
        # authoritative
        for sample_data_header in self.corrected_sample_headers:
            sample_name = self.lcms_metadata[sample_data_header]["sample_name"]
            prefixed_sample_name = f"{self.sample_name_prefix}{sample_name}"
            try:
                # This relies on sample name to find the correct sample since we do not have any other information
                # about the sample in the peak annotation file Accucor/Isocor files should be accomplanied by a sample
                # and animal sheet file so we can identify potential duplicates and flag them
                sample_dict[sample_data_header] = Sample.objects.get(
                    name=prefixed_sample_name
                )
            except Sample.DoesNotExist:
                self.missing_samples.append(sample_name)

        if len(self.missing_samples) != 0:
            possible_blanks = []
            likely_missing = []
            for ms in self.missing_samples:
                if "blank" in ms.lower():
                    possible_blanks.append(ms)
                else:
                    likely_missing.append(ms)

            # Do not report an exception in validate mode.  Users using the validation view cannot specify blank
            # samples, so there's no need to alrt them to the problem.
            if (
                len(possible_blanks) > 0 and not self.validate
            ):  # Only buffer this exception in load mode
                self.aggregated_errors_object.buffer_exception(
                    UnskippedBlanksError(possible_blanks),
                    is_error=not self.validate,  # Error in load mode, warning in validate mode
                    is_fatal=not self.validate,  # Fatal in load mode, will not be raised/reported in validate mode
                    #                              unless there is an accompanying fatal exception.
                    #                              Moot unless the conditional is changed.
                )
            if len(likely_missing) > 0 and len(sample_dict.keys()) != 0:
                self.aggregated_errors_object.buffer_error(
                    MissingSamplesError(likely_missing)
                )

            if len(sample_dict.keys()) == 0:
                self.aggregated_errors_object.buffer_error(
                    NoSamplesError(len(likely_missing))
                )
                if self.aggregated_errors_object.should_raise():
                    raise self.aggregated_errors_object

        elif len(sample_dict.keys()) == 0:
            # If there are no "missing samples", but still no samples...
            raise NoSamplesError(0)

        self.db_samples_dict = sample_dict

    def initialize_tracer_labeled_elements(self):
        """
        This method queries the database and sets self.tracer_labeled_elements with a unique list of the labeled
        elements that exist among the tracers as if they were parent observations (i.e. count=0 and parent=True).  This
        is so that Isocorr data can record 0 observations for parent records.  Accucor data does present data for
        counts of 0 already.
        """
        # Assuming only 1 animal is the source of all samples and arbitrarily using the first sample to get that animal
        animal = list(self.db_samples_dict.values())[0].animal
        tracer_recs = animal.infusate.tracers.all()

        # Assuming all samples come from 1 animal, so we're only looking at 1 (any) sample
        tracer_labeled_elements = []
        for tracer in tracer_recs:
            for label in tracer.labels.all():
                this_label = IsotopeObservationData(
                    element=label.element,
                    mass_number=label.mass_number,
                    count=0,
                    parent=True,
                )
                if this_label not in tracer_labeled_elements:
                    tracer_labeled_elements.append(this_label)

        if not self.isocorr_format:
            # To allow animal and sample sheets to contain tracers with multiple labels, we will restrict the tracer
            # labeled elements to just those in the Accucor file
            accucor_labeled_elems = [
                tle
                for tle in tracer_labeled_elements
                if tle["element"] == self.labeled_element
            ]
            if len(accucor_labeled_elems) != 1:
                elems = ", ".join(
                    [tle["element"] for tle in self.tracer_labeled_elements]
                )
                raise TracerLabeledElementNotFound(
                    f"Unable to find the Accucor labeled element [{self.labeled_element}] in the tracer data for this "
                    f"animal [{animal}].  The tracers cumulatively contain {len(self.tracer_labeled_elements)} "
                    f"distinct elements: [{elems}]."
                )
            self.tracer_labeled_elements = accucor_labeled_elems
        else:
            self.tracer_labeled_elements = tracer_labeled_elements

    def validate_peak_groups(self):
        """
        Step through the original file, and note all the unique peak group names/formulas and map to database compounds
        """

        self.peak_group_dict = {}
        reference_dataframe = self.accucor_corrected_df
        peak_group_name_key = self.compound_header
        # corrected data does not have a formula column
        peak_group_formula_key = None
        if self.accucor_original_df is not None:
            reference_dataframe = self.accucor_original_df
            peak_group_name_key = "compound"
            # original data has a formula column
            peak_group_formula_key = "formula"
        elif self.isocorr_format:
            # absolut data has a formula column
            peak_group_formula_key = "formula"

        for index, row in reference_dataframe.iterrows():
            # uniquely record the group, by name
            peak_group_name = row[peak_group_name_key]
            peak_group_formula = None
            if peak_group_formula_key:
                peak_group_formula = row[peak_group_formula_key]
            if peak_group_name not in self.peak_group_dict:
                # cache it for later; note, if the first row encountered
                # is missing a formula, there will be issues later
                self.peak_group_dict[peak_group_name] = {
                    "name": peak_group_name,
                    "formula": peak_group_formula,
                }

                # cross validate in database;  this is a mapping of peak group
                # name to one or more compounds. peak groups sometimes detect
                # multiple compounds delimited by slash

                self.peak_group_dict[peak_group_name]["compounds"] = []
                compounds_input = [
                    compound_name.strip()
                    for compound_name in peak_group_name.split("/")
                ]
                compound_missing = False
                for compound_input in compounds_input:
                    try:
                        mapped_compound = Compound.compound_matching_name_or_synonym(
                            compound_input
                        )
                        if mapped_compound is not None:
                            self.peak_group_dict[peak_group_name]["compounds"].append(
                                mapped_compound
                            )
                            # If the formula was previously None because we were
                            # working with corrected data (missing column), supplement
                            # it with the mapped database compound's formula
                            if not self.peak_group_dict[peak_group_name]["formula"]:
                                self.peak_group_dict[peak_group_name][
                                    "formula"
                                ] = mapped_compound.formula
                        else:
                            compound_missing = True
                            self.record_missing_compound(
                                compound_input, peak_group_formula, index
                            )
                    except (ValidationError, Compound.DoesNotExist):
                        compound_missing = True
                        self.record_missing_compound(
                            compound_input, peak_group_formula, index
                        )

                if compound_missing:
                    # We want to try and load what we can, so we are going to remove this entry from the
                    # peak_group_dict so it can be skipped in the load.
                    self.peak_group_dict.pop(peak_group_name)

        if len(self.missing_compounds.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                MissingCompounds(self.missing_compounds)
            )

    def record_missing_compound(self, compound_input, formula, index):
        """
        Builds the dict accepted by the MissingCompounds exception.  Row numbers are assumed to be the index plus 2 (1
        for starting from 1 and 1 for a header row).
        """
        if not formula:
            # formula will be none when the accucor file is a csv
            formula = "no formula"
        if compound_input in self.missing_compounds:
            self.missing_compounds[compound_input]["rownums"].append(index + 2)
            if formula not in self.missing_compounds[compound_input]["formula"]:
                self.missing_compounds[compound_input]["formula"].append(formula)
        else:
            self.missing_compounds[compound_input] = {
                "formula": [formula],
                "rownums": [index + 2],
            }

    def get_or_create_lc_protocol(self, sample_data_header):
        # lcms_metadata should be populated either from the lcms_metadata file or via the headers and the default
        # options/args.
        if sample_data_header in self.corrected_sample_headers:
            type = self.lcms_metadata[sample_data_header]["lc_type"]
            run_length = self.lcms_metadata[sample_data_header]["lc_run_length"]
            desc = self.lcms_metadata[sample_data_header]["lc_description"]
            name = self.lcms_metadata[sample_data_header]["lc_protocol_name"]
        else:
            # This should have been encountered before, but adding this here to be robust to code changes.
            self.aggregated_errors_object.buffer_warning(
                MissingLCMSSampleDataHeaders(
                    [sample_data_header],
                    self.peak_annotation_filename,
                    self.get_missing_required_lcms_defaults(),
                )
            )
            # Create an unknown record in order to proceed and catch more errors
            name = LCMethod.create_name()

        # Create a dict for the get_or_create call, using all available values (so that the get can work)
        rec_dict = {}
        if name is not None:
            rec_dict["name"] = name
        if type is not None:
            rec_dict["type"] = type
        if run_length is not None:
            rec_dict["run_length"] = run_length
        if desc is not None:
            rec_dict["description"] = desc

        if len(rec_dict.keys()) == 0:
            # An error will have already been buffered, so return the "unknown" protocol as a placeholder and keep going
            return LCMethod.objects.get(
                name__exact=LCMethod.DEFAULT_TYPE, type__exact=LCMethod.DEFAULT_TYPE
            )

        try:
            try:
                rec = LCMethod.objects.get(**rec_dict)
            except Exception:
                rec, _ = LCMethod.objects.get_or_create(**rec_dict)
        except Exception as e:
            try:
                # Fall back to the unknown record in order to proceed and catch more errors
                # If this fails, die.  If the fixtures don't exist, that's a low level problem.
                rec = LCMethod.objects.get(
                    name__exact=LCMethod.DEFAULT_TYPE, type__exact=LCMethod.DEFAULT_TYPE
                )
                self.aggregated_errors_object.buffer_error(e)
            except LCMethod.DoesNotExist as dne:
                self.aggregated_errors_object.buffer_error(LCMethodFixturesMissing(dne))
                rec = None
            except Exception:
                # We don't know what the new exception is, so revert to the enclosing exception
                self.aggregated_errors_object.buffer_error(e)
                rec = None

        return rec

    def get_or_create_archive_file(
        self,
        path_obj,
        code,
        format,
        is_binary=False,
        allow_missing=False,
        checksum=None,
    ):
        if not allow_missing and checksum is not None:
            raise ValueError(
                "A custom checksum value can only be supplied when allow_missing is False."
            )

        checksum_val = None
        if not path_obj.is_file() and allow_missing and checksum is not None:
            checksum_val = checksum
        else:
            checksum_val = hash_file(path_obj, allow_missing)

        rec_dict = {
            "filename": path_obj.name,
            "checksum": checksum_val,
            "data_type": DataType.objects.get(code=code),
            "data_format": DataFormat.objects.get(code=format),
            # file_location is conditionally set below
        }

        mode = "rb" if is_binary else "r"

        if self.dry_run or self.validate:
            # Don't store the file during dry-run or validation
            try:
                archivefile_rec = ArchiveFile.objects.get(**rec_dict)
            except ObjectDoesNotExist:
                rec_dict["file_location"] = None
                archivefile_rec = ArchiveFile.objects.create(**rec_dict)
        elif path_obj.is_file():
            with path_obj.open(mode=mode) as f:
                with_loc = {**rec_dict}
                with_loc["file_location"] = File(f, name=path_obj.name)
                archivefile_rec, _ = ArchiveFile.objects.get_or_create(**with_loc)
        elif allow_missing:
            archivefile_rec, _ = ArchiveFile.objects.get_or_create(**rec_dict)
        else:
            self.aggregated_errors_object.buffer_error(
                FileNotFoundError(f"No such file: {str(path_obj)}")
            )
            # Placeholder record, so we can proceed to find more errors:
            archivefile_rec, _ = ArchiveFile.objects.get_or_create(**rec_dict)

        return archivefile_rec

    def get_or_create_raw_file(self, mz_dict):
        """
        This takes an mzXML file's path object and creates an ArchiveFile record based on the contents of the mzXML
        file.  It parses the file and extracts the RAW file's name (which includes its path) and its sha1.
        """
        raw_file_name = mz_dict["raw_file_name"]

        if raw_file_name is None:
            return None

        return self.get_or_create_archive_file(
            path_obj=Path(raw_file_name),
            checksum=mz_dict["raw_file_sha1"],
            code="ms_data",
            format="ms_raw",
            allow_missing=True,
        )

    def parse_mzxml(self, mzxml_path_obj, full_dict=False):
        """Creates a dict of select data parsed from an mzXML file

        This extracts the raw file name, raw file's sha1, and the polarity from an mzxml file and returns a condensed
        dictionary of only those values (for simplicity).  The construction of the condensed dict will perform
        validation and conversion of the desired values, which will not occur when the full_dict is requested.  If
        full_dict is True, it will return the uncondensed version.

        If not all polarities of all the scans are the same, an error will be buffered.

        Args:
            mzxml_path_obj (Path): mzXML file Path object
            full_dict (boolean): Whether to return the raw/full dict of the mzXML file

        Returns:
            If mzxml_path_obj is not a real existing file:
                None
            If full_dict=False:
                {
                    "raw_file_name": <raw file base name parsed from mzXML file>,
                    "raw_file_sha1": <sha1 string parsed from mzXML file>,
                    "polarity": "positive" or "negative" (based on first polarity parsed from mzXML file),
                }
            If full_dict=True:
                xmltodict.parse(xml_content)

        Raises:
            Nothing, but buffers a ValueError
        """
        if not mzxml_path_obj.is_file():
            # mzXML files are optional, but the file names are supplied in a file, in which case, we may have a name,
            # but not the file, so just return None if what we have isn't a real file.
            return None

        # Parse the xml content
        with mzxml_path_obj.open(mode="r") as f:
            xml_content = f.read()
        mzxml_dict = xmltodict.parse(xml_content)

        if full_dict:
            return mzxml_dict

        raw_file_type = mzxml_dict["mzXML"]["msRun"]["parentFile"]["@fileType"]
        raw_file_name = Path(
            mzxml_dict["mzXML"]["msRun"]["parentFile"]["@fileName"]
        ).name
        raw_file_sha1 = mzxml_dict["mzXML"]["msRun"]["parentFile"]["@fileSha1"]
        if raw_file_type != "RAWData":
            self.aggregated_errors_object.buffer_error(
                ValueError(
                    f"Unsupported file type [{raw_file_type}] encountered in mzXML file [{str(mzxml_path_obj)}].  "
                    "Expected: [RAWData]."
                )
            )
            raw_file_name = None
            raw_file_sha1 = None

        polarity = MSRunSample.POLARITY_DEFAULT
        symbol_polarity = ""
        for entry_dict in mzxml_dict["mzXML"]["msRun"]["scan"]:
            if symbol_polarity == "":
                symbol_polarity = entry_dict["@polarity"]
            elif symbol_polarity != entry_dict["@polarity"]:
                self.mixed_polarities[str(mzxml_path_obj)] = {
                    "first": symbol_polarity,
                    "different": entry_dict["@polarity"],
                    "scan": entry_dict["@num"],
                }
                break
        if symbol_polarity == "+":
            polarity = MSRunSample.POSITIVE_POLARITY
        elif symbol_polarity == "-":
            polarity = MSRunSample.NEGATIVE_POLARITY
        elif symbol_polarity != "":
            self.aggregated_errors_object.buffer_error(
                ValueError(
                    f"Unsupported polarity value [{symbol_polarity}] encountered in mzXML file "
                    f"[{str(mzxml_path_obj)}]."
                )
            )

        return {
            "raw_file_name": raw_file_name,
            "raw_file_sha1": raw_file_sha1,
            "polarity": polarity,
        }

    def insert_peak_group(
        self,
        peak_group_attrs: PeakGroupAttrs,
        msrun_sample: MSRunSample,
        peak_annotation_file: ArchiveFile,
    ):
        """Insert a PeakGroup record

        NOTE: If the C12 PARENT/0-Labeled row encountered has any issues (for example, a null formula),
        then this block will fail

        Args:
            peak_group_attrs: dictionary of peak group atrributes
            msrun_sample: MSRunSample object the PeakGroup belongs to
            peak_annotation_file: ArchiveFile object of the peak annotation file the peak group was loaded from

        Returns:
            A newly created PeakGroup object created using the supplied values

        Raises:
            DuplicatePeakGroup: A PeakGroup record with the same values already exists
            ConflictingValueError: A PeakGroup with the same unique key (MSRunSample and PeakGroup.name) exists, but
              with a different formula or different peak_annotation_file
        """

        if self.verbosity >= 1:
            print(
                f"\tInserting {peak_group_attrs['name']} peak group for sample {msrun_sample.sample}"
            )
        try:
            peak_group, created = PeakGroup.objects.get_or_create(
                msrun_sample=msrun_sample,
                name=peak_group_attrs["name"],
                formula=peak_group_attrs["formula"],
                peak_annotation_file=peak_annotation_file,
            )
            if not created:
                raise DuplicatePeakGroup(
                    adding_file=peak_annotation_file.filename,
                    msrun_sample=msrun_sample,
                    sample=msrun_sample.sample,
                    peak_group_name=peak_group_attrs["name"],
                    existing_peak_annotation_file=peak_group.peak_annotation_file,
                )
            peak_group.full_clean()
            peak_group.save()
        except IntegrityError as ie:
            iestr = str(ie)
            if (
                'duplicate key value violates unique constraint "unique_peakgroup"'
                in iestr
            ):
                existing_peak_group = PeakGroup.objects.get(
                    msrun_sample=msrun_sample, name=peak_group_attrs["name"]
                )
                conflicting_fields = []
                existing_values = []
                new_values = []
                if existing_peak_group.formula != peak_group_attrs["formula"]:
                    conflicting_fields.append("formula")
                    existing_values.append(existing_peak_group.formula)
                    new_values.append(peak_group_attrs["formula"])
                if existing_peak_group.peak_annotation_file != peak_annotation_file:
                    conflicting_fields.append("peak_annotation_file")
                    existing_values.append(
                        existing_peak_group.peak_annotation_file.filename
                    )
                    new_values.append(peak_annotation_file.filename)
                raise ConflictingValueError(
                    rec=existing_peak_group,
                    consistent_field=conflicting_fields,
                    existing_value=existing_values,
                    differing_value=new_values,
                )

            else:
                self.aggregated_errors_object.buffer_error(ie)

        """
        associate the pre-vetted compounds with the newly inserted
        PeakGroup
        """
        for compound in peak_group_attrs["compounds"]:
            # Must save the compound before it can be linked
            compound.save()
            peak_group.compounds.add(compound)

        # Insert PeakGroup Labels
        peak_labeled_elements = self.get_peak_labeled_elements(
            peak_group.compounds.all()
        )
        for peak_labeled_element in peak_labeled_elements:
            if self.verbosity >= 1:
                print(
                    f"\t\tInserting {peak_labeled_element} peak group label for peak group "
                    f"{peak_group.name}"
                )
            peak_group_label = PeakGroupLabel(
                peak_group=peak_group,
                element=peak_labeled_element["element"],
            )
            peak_group_label.full_clean()
            peak_group_label.save()

        return peak_group

    def load_data(self):
        """
        Extract and store the data for MsRunSample, PeakGroup, and PeakData
        """
        animals_to_uncache = []

        if self.verbosity >= 1:
            print("Loading data...")

        # No need to try/catch - these need to succeed to start loading samples
        path_obj = Path(self.peak_annotation_filepath)
        peak_annotation_file = self.get_or_create_archive_file(
            path_obj=path_obj,
            code="ms_peak_annotation",
            format="isocorr" if self.isocorr_format else "accucor",
            is_binary="xls" in path_obj.suffix,
        )

        sequences = {}
        sample_msrun_dict = {}

        # Each sample gets its own msrun_sample
        for sample_data_header in self.db_samples_dict.keys():
            lc_protocol = self.get_or_create_lc_protocol(sample_data_header)

            if lc_protocol is None:
                # Cannot create the msrun_sample record without protocols
                # An exception will have already been buffered by get_or_create_lc_protocol, so just move on
                continue

            sequence_key = (
                self.lcms_metadata[sample_data_header]["researcher"]
                + "."
                + str(self.lcms_metadata[sample_data_header]["date"])
                + "."
                + lc_protocol.name
                + "."
                + self.lcms_metadata[sample_data_header]["instrument"]
            )
            if sequence_key not in sequences.keys():
                try:
                    (
                        sequences[sequence_key],
                        created,
                    ) = MSRunSequence.objects.get_or_create(
                        researcher=self.lcms_metadata[sample_data_header]["researcher"],
                        date=self.lcms_metadata[sample_data_header]["date"],
                        lc_method=lc_protocol,
                        instrument=self.lcms_metadata[sample_data_header]["instrument"],
                        # TODO: implement the ability to load the notes field (which is not a part of issue #712 and was
                        # not partially implemented in #774 like it did with instrument and the mzxml files)
                    )
                except Exception as e:
                    # Note, there should be no reason to catch any IntegrityErrors UNTIL the notes field is added, since
                    # all the fields being added are a part of the unique constraint.
                    self.aggregated_errors_object.buffer_error(e)

            ms_data_file = None
            ms_raw_file = None
            if (
                self.mzxml_files_dict is not None
                and sample_data_header in self.mzxml_files_dict.keys()
            ):
                path_obj = Path(str(self.mzxml_files_dict[sample_data_header]["path"]))
                if path_obj.is_file():
                    ms_data_file = self.get_or_create_archive_file(
                        path_obj=path_obj,
                        code="ms_data",
                        format="mzxml",
                        allow_missing=True,
                    )
                    ms_raw_file = self.get_or_create_raw_file(
                        self.mzxml_data[sample_data_header]
                    )

            msrunsample_dict = {
                "msrun_sequence": sequences[sequence_key],
                "sample": self.db_samples_dict[sample_data_header],
                "polarity": self.lcms_metadata[sample_data_header]["polarity"],
                "ms_data_file": ms_data_file,
                "ms_raw_file": ms_raw_file,
            }
            try:
                # This relies on sample name lookup and accurate msrun_sample information (researcher, date, instrument,
                # etc).  Including mzXML files with accucor files will help ensure accurate msrun_sample lookup since we
                # will have checksums for the mzXML files and those are always associated with one MSRunSample record
                msrun_sample, created = MSRunSample.objects.get_or_create(
                    **msrunsample_dict
                )
                if created:
                    msrun_sample.full_clean()
                    # Already saved via create

                # This will be used to iterate the sample loop below so that we don't attempt to load samples whose
                # msrun_sample creations failed.
                sample_msrun_dict[sample_data_header] = msrun_sample

                if self.update_caches is True:
                    if (
                        msrun_sample.sample.animal not in animals_to_uncache
                        and msrun_sample.sample.animal.caches_exist()
                    ):
                        animals_to_uncache.append(msrun_sample.sample.animal)
                    elif (
                        not msrun_sample.sample.animal.caches_exist()
                        and self.verbosity >= 1
                    ):
                        print(
                            f"No cache exists for animal {msrun_sample.sample.animal.id} linked to Sample "
                            f"{msrun_sample.sample.id}"
                        )
            except Exception as e:
                self.aggregated_errors_object.buffer_error(e)
                continue

        # each msrun/sample has its own set of peak groups
        inserted_peak_group_dict = {}

        # Create all PeakGroups
        for sample_data_header in sample_msrun_dict.keys():
            msrun_sample = sample_msrun_dict[sample_data_header]

            # Pass through the rows once to identify the PeakGroups
            for _, corr_row in self.accucor_corrected_df.iterrows():
                try:
                    obs_isotopes = self.get_observed_isotopes(corr_row)
                    peak_group_name = corr_row[self.compound_header]

                    # If this is a compound that has isotope duplicates, skip it
                    # It will already have been identified as a DupeCompoundIsotopeCombos error, so this load will
                    # ultimately fail, but we con tinue so that we can find more errors
                    if (
                        peak_group_name
                        in self.dupe_isotope_compounds["corrected"].keys()
                    ):
                        continue

                    # Assuming that if the first one is the parent, they all are.  Note that subsequent isotopes in the
                    # list may be parent=True if 0 isotopes of that element were observed.
                    # We will also skip peakgroups whose names are not keys in self.peak_group_dict.  Peak group names
                    # are removed from the dict if there was a validation issue, in which case the load will ultimately
                    # fail and this script only continues so it can gather more useful errors unrelated to errors
                    # previously encountered.
                    if (
                        len(obs_isotopes) > 0
                        and obs_isotopes[0]["parent"]
                        and peak_group_name in self.peak_group_dict
                    ):
                        # Insert PeakGroup, by name (only once per file).
                        try:
                            peak_group = self.insert_peak_group(
                                peak_group_attrs=self.peak_group_dict[peak_group_name],
                                msrun_sample=msrun_sample,
                                peak_annotation_file=peak_annotation_file,
                            )
                            inserted_peak_group_dict[peak_group_name] = peak_group
                        except DuplicatePeakGroup as dup_pg:
                            self.duplicate_peak_groups.append(dup_pg)
                        except ConflictingValueError as cve:
                            self.conflicting_peak_groups.append(cve)

                except Exception as e:
                    # If we get here, a specific exception should be written to handle and explain the cause of an
                    # error.  For example, the ValidationError handled above was due to a previous error about
                    # duplicate compound/isotope pairs that would go away when the duplicate was fixed.  The duplicate
                    # was causing the data to contain a pandas structure where corrected_abundance should have been -
                    # containing 2 values instead of 1.
                    self.aggregated_errors_object.buffer_error(e)
                    continue

            # For each PeakGroup, create PeakData rows
            for peak_group_name in inserted_peak_group_dict:
                # we should have a cached PeakGroup and its labeled element now
                peak_group = inserted_peak_group_dict[peak_group_name]

                if self.accucor_original_df is not None:
                    peak_group_original_data = self.accucor_original_df.loc[
                        self.accucor_original_df["compound"] == peak_group_name
                    ]
                    # If we have an accucor_original_df, it's assumed the type is accucor and there's only 1 labeled
                    # element, hence the use of `peak_group.labels.first()`
                    peak_group_label_rec = peak_group.labels.first()

                    # Original data skips undetected counts, but corrected data does not, so as we march through the
                    # corrected data, we need to keep track of the corresponding row in the original data
                    orig_row_idx = 0
                    for labeled_count in range(
                        0, peak_group_label_rec.atom_count() + 1
                    ):
                        try:
                            raw_abundance = 0
                            med_mz = 0
                            med_rt = 0
                            # We can safely assume a single tracer labeled element (because otherwise, there would have
                            # been a TracerLabeledElementNotFound error), so...
                            mass_number = self.tracer_labeled_elements[0]["mass_number"]

                            # Try to get original data. If it's not there, set empty values
                            try:
                                orig_row = peak_group_original_data.iloc[orig_row_idx]
                                orig_isotopes = self.parse_isotope_string(
                                    orig_row["isotopeLabel"],
                                    self.tracer_labeled_elements,
                                )
                                for isotope in orig_isotopes:
                                    # If it's a matching row
                                    if (
                                        isotope["element"]
                                        == peak_group_label_rec.element
                                        and isotope["count"] == labeled_count
                                    ):
                                        # We have a matching row, use it and increment row_idx
                                        raw_abundance = orig_row[sample_data_header]
                                        med_mz = orig_row["medMz"]
                                        med_rt = orig_row["medRt"]
                                        orig_row_idx = orig_row_idx + 1
                                        mass_number = isotope["mass_number"]
                            except IndexError:
                                # We can ignore missing entries in the original sheet and use the defaults set above the
                                # try block
                                pass

                            if self.verbosity >= 1:
                                print(
                                    f"\t\tInserting peak data for {peak_group_name}:label-{labeled_count} for sample "
                                    f"{self.lcms_metadata[sample_data_header]['sample_name']}"
                                )

                            # Lookup corrected abundance by compound and label
                            corrected_abundance = self.accucor_corrected_df.loc[
                                (
                                    self.accucor_corrected_df[self.compound_header]
                                    == peak_group_name
                                )
                                & (
                                    self.accucor_corrected_df[
                                        self.labeled_element_header
                                    ]
                                    == labeled_count
                                )
                            ][sample_data_header]

                            peak_data = PeakData(
                                peak_group=peak_group,
                                raw_abundance=raw_abundance,
                                corrected_abundance=corrected_abundance,
                                med_mz=med_mz,
                                med_rt=med_rt,
                            )

                            peak_data.full_clean()
                            peak_data.save()

                            """
                            Create the PeakDataLabel records
                            """

                            if self.verbosity >= 1:
                                print(
                                    f"\t\t\tInserting peak data label [{isotope['mass_number']}{isotope['element']}"
                                    f"{isotope['count']}] parsed from cell value: [{orig_row['isotopeLabel']}] for "
                                    f"peak data ID [{peak_data.id}], peak group [{peak_group_name}], and sample "
                                    f"[{self.lcms_metadata[sample_data_header]['sample_name']}]."
                                )

                            peak_data_label = PeakDataLabel(
                                peak_data=peak_data,
                                element=peak_group_label_rec.element,
                                count=labeled_count,
                                mass_number=mass_number,
                            )

                            peak_data_label.full_clean()
                            peak_data_label.save()

                        except Exception as e:
                            self.aggregated_errors_object.buffer_error(e)
                            continue

                else:
                    peak_group_corrected_df = self.accucor_corrected_df[
                        self.accucor_corrected_df[self.compound_header]
                        == peak_group_name
                    ]

                    for _, corr_row in peak_group_corrected_df.iterrows():
                        try:
                            corrected_abundance_for_sample = corr_row[
                                sample_data_header
                            ]
                            # No original dataframe, no raw_abundance, med_mz, or med_rt
                            raw_abundance = None
                            med_mz = None
                            med_rt = None

                            if self.verbosity >= 1:
                                print(
                                    f"\t\tInserting peak data for peak group [{peak_group_name}] "
                                    f"and sample [{self.lcms_metadata[sample_data_header]['sample_name']}]."
                                )

                            peak_data = PeakData(
                                peak_group=peak_group,
                                raw_abundance=raw_abundance,
                                corrected_abundance=corrected_abundance_for_sample,
                                med_mz=med_mz,
                                med_rt=med_rt,
                            )

                            peak_data.full_clean()
                            peak_data.save()

                            """
                            Create the PeakDataLabel records
                            """

                            corr_isotopes = self.get_observed_isotopes(
                                corr_row, peak_group.compounds.all()
                            )

                            for isotope in corr_isotopes:
                                if self.verbosity >= 1:
                                    print(
                                        f"\t\t\tInserting peak data label [{isotope['mass_number']}{isotope['element']}"
                                        f"{isotope['count']}] parsed from cell value: "
                                        f"[{corr_row[self.labeled_element_header]}] for peak data ID "
                                        f"[{peak_data.id}], peak group [{peak_group_name}], and sample "
                                        f"[{self.lcms_metadata[sample_data_header]['sample_name']}]."
                                    )

                                # Note that this inserts the parent record (count 0) as always 12C, since the parent is
                                # always carbon with a mass_number of 12.
                                peak_data_label = PeakDataLabel(
                                    peak_data=peak_data,
                                    element=isotope["element"],
                                    count=isotope["count"],
                                    mass_number=isotope["mass_number"],
                                )

                                peak_data_label.full_clean()
                                peak_data_label.save()

                        except Exception as e:
                            self.aggregated_errors_object.buffer_error(e)
                            continue

        if len(self.duplicate_peak_groups) > 0:
            self.aggregated_errors_object.buffer_exception(
                DuplicatePeakGroups(
                    adding_file=peak_annotation_file.filename,
                    duplicate_peak_groups=self.duplicate_peak_groups,
                ),
                is_fatal=self.validate,
                is_error=False,
            )
        if len(self.conflicting_peak_groups) > 0:
            self.aggregated_errors_object.buffer_exception(
                ConflictingValueErrors(
                    model_name="PeakGroup",
                    conflicting_value_errors=self.conflicting_peak_groups,
                ),
            )

        if len(self.conflicting_polarities.keys()) > 0:
            self.aggregated_errors_object.buffer_exception(
                PolarityConflictErrors(self.conflicting_polarities),
            )

        if len(self.mixed_polarities.keys()) > 0:
            self.aggregated_errors_object.buffer_exception(
                MixedPolarityErrors(self.mixed_polarities),
            )

        if self.aggregated_errors_object.should_raise():
            raise self.aggregated_errors_object

        if self.dry_run:
            raise DryRun()

        if self.update_caches is True:
            if settings.DEBUG or self.verbosity >= 1:
                print("Expiring affected caches...")
            for animal in animals_to_uncache:
                if settings.DEBUG or self.verbosity >= 1:
                    print(f"Expiring animal {animal.id}'s cache")
                animal.delete_related_caches()
            if settings.DEBUG or self.verbosity >= 1:
                print("Expiring done.")

    def get_peak_labeled_elements(self, compound_recs) -> List[IsotopeObservationData]:
        """
        Gets labels present among any of the tracers in the infusate IF the elements are present in the supplied
        (measured) compounds.  Basically, if the supplied compound contains an element that is a labeled element in any
        of the tracers, it is included in the returned list.
        """
        peak_labeled_elements = []
        for compound_rec in compound_recs:
            for tracer_label in self.tracer_labeled_elements:
                if (
                    compound_rec.atom_count(tracer_label["element"]) > 0
                    and tracer_label not in peak_labeled_elements
                ):
                    peak_labeled_elements.append(tracer_label)
        return peak_labeled_elements

    def get_observed_isotopes(self, corrected_row, observed_compound_recs=None):
        """
        Given a row of corrected data, it retrieves the labeled element, count, and mass_number using a method
        corresponding to the file format.
        """
        if self.isocorr_format:
            # Establish the set of labeled elements we're working from, either all labeled elements among the tracers
            # in the infusate (when there are no observed compounds) or those in common with the measured compound
            if observed_compound_recs is None:
                parent_labels = self.tracer_labeled_elements
            else:
                parent_labels = self.get_peak_labeled_elements(observed_compound_recs)

            # E.g. Parsing C13N15-label-2-3 in isotopeLabel column
            isotopes = self.parse_isotope_string(
                corrected_row[self.labeled_element_header], parent_labels
            )

            # If there are any labeled elements unaccounted for, add them as zero-counts
            # The labeled elements exclude listing elements when the count of the isotopes is zero, but the row exists
            # if it has at least 1 labeled element.  This does not add 0 counts for missing rows - only for labeled
            # elements on existing rows with at least 1 labeled element already.
            if len(isotopes) < len(parent_labels):
                for parent_label in parent_labels:
                    match = [
                        x
                        for x in isotopes
                        if x["element"] == parent_label["element"]
                        and x["mass_number"] == parent_label["mass_number"]
                    ]
                    if len(match) == 0:
                        isotopes.append(parent_label)
                    elif len(match) > 1:
                        # If there are multiple isotope measurements that match the same parent tracer labeled element
                        # E.g. C13N15C13-label-2-1-1 would match C13 twice
                        raise IsotopeStringDupe(
                            corrected_row[self.labeled_element_header],
                            f'{parent_label["element"]}{parent_label["mass_number"]}',
                        )
            elif observed_compound_recs is not None and len(isotopes) > len(
                parent_labels
            ):
                # Apparently, there is evidence of detected heavy nitrogen in an animal whose infusate contained
                # tracers containing no heavy nitrogen.  This could suggest contamination, but we shouldn't raise an
                # exception...
                # See warnings when loading 6eaafasted2_cor.xlsx. Note the tracers for animals 971, 972, 981, 982 in
                # the sample file and note the isotopeLabels including N15
                # raise Exception(f"More measured isotopes ({isotopes}) than tracer labeled elements "
                # f"({parent_labels}) for compounds ({observed_compound_recs}).")
                self.aggregated_errors_object.buffer_warning(
                    UnexpectedIsotopes(isotopes, parent_labels, observed_compound_recs),
                    is_fatal=self.validate,  # Raise AggErrs in validate mode to alert the user.  Print in load mode.
                )

        else:
            # Get the mass number(s) from the associated tracers
            mns = [
                x["mass_number"]
                for x in self.tracer_labeled_elements
                if x["element"] == self.labeled_element
            ]
            if len(mns) > 1:
                raise MultipleMassNumbers(self.labeled_element, mns)
            elif len(mns) == 0:
                raise MassNumberNotFound(
                    self.labeled_element, self.tracer_labeled_elements
                )
            mn = mns[0]
            parent = corrected_row[self.labeled_element_header] == 0
            # E.g. Getting count value from e.g. C_Label column
            isotopes = [
                IsotopeObservationData(
                    element=self.labeled_element,
                    mass_number=mn,
                    count=corrected_row[self.labeled_element_header],
                    parent=parent,
                ),
            ]

        return isotopes

    @classmethod
    def parse_isotope_string(
        cls, label, tracer_labeled_elements=None
    ) -> List[IsotopeObservationData]:
        """
        Parse El-Maven style isotope label string, e.g. C12 PARENT, C13-label-1, C13N15-label-2-1
        Returns a list of IsotopeObservationData objects (which is a TypedDict)
        Note, on parent rows, a single (carbon) parent observation is parsed regardless of the number of labeled
        elements among the tracers or common with the measured compound, but the isotopes returned are only those among
        the tracers.  I.e. If there is no carbon labeled among the tracers, the parsed carbon is ignored.
        """

        isotope_observations = []

        match = regex.match(ISOTOPE_LABEL_PATTERN, label)

        if match:
            elements = match.captures("elements")
            mass_numbers = match.captures("mass_numbers")
            counts = match.captures("counts")
            parent_str = match.group("parent")
            parent = False

            if parent_str is not None and parent_str == "PARENT":
                parent = True
                if tracer_labeled_elements is None:
                    raise NoTracerLabeledElements()
                else:
                    isotope_observations = tracer_labeled_elements
            else:
                if len(elements) != len(mass_numbers) or len(elements) != len(counts):
                    raise IsotopeObservationParsingError(
                        f"Unable to parse the same number of elements ({len(elements)}), mass numbers "
                        f"({len(mass_numbers)}), and counts ({len(counts)}) from isotope label: [{label}]"
                    )
                else:
                    for index in range(len(elements)):
                        isotope_observations.append(
                            IsotopeObservationData(
                                element=elements[index],
                                mass_number=int(mass_numbers[index]),
                                count=int(counts[index]),
                                parent=parent,
                            )
                        )
        else:
            raise IsotopeObservationParsingError(
                f"Unable to parse isotope label: [{label}]"
            )

        return isotope_observations

    @MaintainedModel.defer_autoupdates(
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def load_accucor_data(self):
        disable_caching_updates()

        # Data validation and loading
        try:
            # Pre-processing
            # Clean up the dataframes
            self.preprocess_data()
            # Obtain required data from the database
            self.initialize_preloaded_animal_sample_data()

            with transaction.atomic():
                self.validate_data()
                self.load_data()

        except DryRun:
            raise
        except AggregatedErrors:
            # If it was an aggregated errors exception, raise it directly
            raise
        except Exception as e:
            # If it was some other (unanticipated or a single fatal) error, we want to report it, but also include
            # everything else that was stored in self.aggregated_errors_object.  An AggregatedErrors exception is
            # raised (in the called code) when errors are allowed to accumulate, but moving on to the next step/loop is
            # not possible.  And for simplicity, fatal errors that do not allow further accumulation of errors are
            # raised directly.
            self.aggregated_errors_object.buffer_error(e)
            self.aggregated_errors_object.should_raise()
            raise self.aggregated_errors_object

        enable_caching_updates()


def hash_file(path_obj, allow_missing=False):
    """
    This function returns the SHA-1 hash of the file passed into it.

    If allow_missing is True, the filename is not None, and an exception occurs during hash creation, a hash will be
    constructed using the filename.
    """

    # make a hash object
    h = hashlib.sha1()

    try:
        # open file for reading in binary mode
        with path_obj.open("rb") as file:
            # loop till the end of the file
            chunk = 0
            while chunk != b"":
                # read only 1024 bytes at a time
                chunk = file.read(1024)
                h.update(chunk)
    except Exception as e:
        if allow_missing and path_obj is not None:
            encoded_filename = str(path_obj).encode()
            h = hashlib.sha1(encoded_filename)
        else:
            raise e

    # return the hex representation of digest
    return h.hexdigest()


def get_first_sample_column_index(df):
    """
    Given a dataframe, return the column index of the likely "first" sample column
    """

    final_index = None
    max_nonsample_index = 0
    found = False
    for col_name in NONSAMPLE_COLUMN_NAMES:
        try:
            if df.columns.get_loc(col_name) > max_nonsample_index:
                max_nonsample_index = df.columns.get_loc(col_name)
                found = True
        except KeyError:
            # column is not found, so move on
            pass

    if found:
        final_index = max_nonsample_index + 1

    # the sample index should be the next column
    return final_index


def get_sample_headers(df, skip_samples=None):
    if skip_samples is None:
        skip_samples = []
    minimum_sample_index = get_first_sample_column_index(df)
    if minimum_sample_index is None:
        # Sample columns are required to proceed
        raise SampleIndexNotFound("corrected", list(df.columns), NONSAMPLE_COLUMN_NAMES)
    return [
        sample
        for sample in list(df)[minimum_sample_index:]
        if sample not in skip_samples
    ]
