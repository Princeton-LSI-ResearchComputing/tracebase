import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List, TypedDict

import regex
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction

from DataRepo.models import (
    Compound,
    ElementLabel,
    MaintainedModel,
    MSRun,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupLabel,
    PeakGroupSet,
    Protocol,
    Sample,
)
from DataRepo.models.hier_cached_model import (
    enable_caching_updates,
    disable_caching_updates,
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
    DryRun,
    DupeCompoundIsotopeCombos,
    DuplicatePeakGroup,
    DuplicatePeakGroups,
    EmptyColumnsError,
    IsotopeStringDupe,
    MissingCompounds,
    MissingSamplesError,
    MultipleAccucorTracerLabelColumnsError,
    NoSamplesError,
    NoTracerLabeledElements,
    ResearcherNotNew,
    SampleColumnInconsistency,
    UnexpectedIsotopes,
    UnskippedBlanksError,
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
    Load the Protocol, MsRun, PeakGroup, and PeakData tables
    """

    def __init__(
        self,
        accucor_original_df,
        accucor_corrected_df,
        date,
        protocol_input,
        researcher,
        peak_group_set_filename,
        skip_samples=None,
        sample_name_prefix=None,
        new_researcher=False,
        validate=False,
        isocorr_format=False,
        verbosity=1,
        # defer_autoupdates=False,
        dry_run=False,
    ):
        self.aggregated_errors_object = AggregatedErrors()

        try:
            # Data
            self.accucor_original_df = accucor_original_df
            self.accucor_corrected_df = accucor_corrected_df

            # Data format
            self.isocorr_format = isocorr_format

            # Optional data (for batch updates)
            self.protocol_input = protocol_input.strip()

            # Metadata
            self.date = datetime.strptime(date.strip(), "%Y-%m-%d")
            self.researcher = researcher.strip()
            self.new_researcher = new_researcher
            self.peak_group_set_filename = peak_group_set_filename.strip()

            # Sample Metadata
            if skip_samples is None:
                self.skip_samples = []
            else:
                self.skip_samples = skip_samples
            if sample_name_prefix is None:
                sample_name_prefix = ""
            self.sample_name_prefix = sample_name_prefix

            # Verbosity affects log prints and error verbosity (for debugging)
            self.verbosity = verbosity

            # Dry Run - don't change the database
            self.dry_run = dry_run

            # Validate mode
            self.validate = validate

            # # How to handle mass autoupdates
            # self.defer_autoupdates = defer_autoupdates

            # Tracking Data
            self.peak_group_dict: Dict[str, PeakGroupAttrs] = {}
            self.corrected_samples = []
            self.original_samples = []
            self.db_samples_dict = None
            self.labeled_element_header = None
            self.missing_samples = []
            self.missing_compounds = {}
            self.dupe_isotope_compounds = {
                "original": defaultdict(dict),
                "corrected": defaultdict(dict),
            }
            self.duplicate_peak_groups = []
            self.conflicting_peak_groups = []

            # Used for accucor
            self.labeled_element = None
            # Used for isocorr
            self.tracer_labeled_elements = []
        except Exception as e:
            self.aggregated_errors_object.buffer_error(e)
            raise self.aggregated_errors_object

    def initialize_preloaded_animal_sample_data(self):
        """
        Without caching updates enables, retrieve loaded matching samples and labeled elements.
        """
        # The samples/animal (and the infusate) are required to already have been loaded
        self.initialize_db_samples_dict()
        self.initialize_tracer_labeled_elements()

    def preprocess_data(self):
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

        # Obtain the sample names from the headers
        self.initialize_sample_names()

    def validate_data(self):
        """
        Basic sanity/integrity checks for the data inputs
        """
        self.validate_sample_headers()
        self.validate_researcher()
        self.validate_compounds()
        self.validate_peak_groups()

    def validate_researcher(self):
        if self.new_researcher is True:
            researchers = get_researchers()
            if self.researcher in researchers:
                self.aggregated_errors_object.buffer_error(
                    ResearcherNotNew(self.researcher, "--new-researcher", researchers)
                )
        else:
            try:
                validate_researchers([self.researcher], skip_flag="--new-researcher")
            except UnknownResearcherError as ure:
                # This is a raised warning when in validate mode.  The user should know about it (so it's raised), but
                # they can't address it if the researcher is valid.
                # This is a raised error when not in validate mode.  The curator must know about and address the error.
                self.aggregated_errors_object.buffer_exception(
                    ure, is_error=not self.validate, is_fatal=True
                )

    def initialize_sample_names(self):
        minimum_sample_index = self.get_first_sample_column_index(
            self.accucor_corrected_df
        )
        if minimum_sample_index is None:
            # Sample columns are required to proceed
            raise SampleIndexNotFound(
                "corrected", list(self.accucor_corrected_df.columns)
            )
        self.corrected_samples = [
            sample
            for sample in list(self.accucor_corrected_df)[minimum_sample_index:]
            if sample not in self.skip_samples
        ]
        if self.accucor_original_df is not None:
            minimum_sample_index = self.get_first_sample_column_index(
                self.accucor_original_df
            )
            if minimum_sample_index is None:
                # Sample columns are required to proceed
                raise SampleIndexNotFound(
                    "original", list(self.accucor_original_df.columns)
                )
            self.original_samples = [
                sample
                for sample in list(self.accucor_original_df)[minimum_sample_index:]
                if sample not in self.skip_samples
            ]

    def validate_sample_headers(self):
        """
        Validate sample headers to ensure they all have sample names, sheets are consistent, and if it's an Accucor
        file, that it only has 1 label column.
        """

        if self.verbosity >= 1:
            print("Validating data...")

        # Make sure all sample columns have names
        corr_iter = Counter(self.corrected_samples)
        for k, v in corr_iter.items():
            if k.startswith("Unnamed: "):
                self.aggregated_errors_object.buffer_error(
                    EmptyColumnsError(
                        "Corrected", list(self.accucor_corrected_df.columns)
                    )
                )

        if self.original_samples:
            # Make sure all sample columns have names
            orig_iter = Counter(self.original_samples)
            for k, v in orig_iter.items():
                if k.startswith("Unnamed: "):
                    self.aggregated_errors_object.buffer_error(
                        EmptyColumnsError(
                            "Original", list(self.accucor_original_df.columns)
                        )
                    )

            # Make sure that the sheets have the same number of sample columns
            if orig_iter != corr_iter:
                original_only = sorted(
                    set(self.original_samples) - set(self.corrected_samples)
                )
                corrected_only = sorted(
                    set(self.corrected_samples) - set(self.original_samples)
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
                self.corrected_samples = [
                    sample
                    for sample in self.corrected_samples
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
        """
        Because the original dataframe might be None, here, we rely on the
        corrected sample list as being authoritative
        """
        for sample_name in self.corrected_samples:
            prefixed_sample_name = f"{self.sample_name_prefix}{sample_name}"
            try:
                """
                This relies on sample name to find the correct sample since
                we do not have any other information about the sample in the
                peak annotation file Accucor/Isocor files should be
                accomplanied by a sample and animal sheet file so we can
                identify potential duplicates and flag them
                """
                # TODO Ensure the sample names in an AccuCor file match sample names in a supplied sample sheet
                sample_dict[sample_name] = Sample.objects.get(name=prefixed_sample_name)
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

    @classmethod
    def get_first_sample_column_index(cls, df):
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
                    except ValidationError:
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

    def retrieve_or_create_protocol(self):
        """
        retrieve or insert a protocol, based on input
        """
        if self.verbosity >= 1:
            print(
                f"Finding or inserting {Protocol.MSRUN_PROTOCOL} protocol for '{self.protocol_input}'..."
            )

        protocol, created = Protocol.retrieve_or_create_protocol(
            self.protocol_input,
            Protocol.MSRUN_PROTOCOL,
            f"For protocol's full text, please consult {self.researcher}.",
        )
        action = "Found"
        feedback = f"{protocol.category} protocol {protocol.id} '{protocol.name}'"
        if created:
            action = "Created"
            feedback += f" '{protocol.description}'"

        if self.verbosity >= 1:
            print(f"{action} {feedback}")

        return protocol

    def insert_peak_group_set(self):
        peak_group_set = PeakGroupSet(filename=self.peak_group_set_filename)
        peak_group_set.full_clean()
        peak_group_set.save()
        return peak_group_set

    def insert_peak_group(
        self,
        peak_group_attrs: PeakGroupAttrs,
        msrun: MSRun,
        peak_group_set: PeakGroupSet,
    ):
        """Insert a PeakGroup record

        NOTE: If the C12 PARENT/0-Labeled row encountered has any issues (for example, a null formula),
        then this block will fail

        Args:
            peak_group_attrs: dictionary of peak group atrributes
            msrun: MSRun object the PeakGroup belongs to
            peak_group_set: PeakGroupSet object the PeakGroup belongs to

        Returns:
            A newly created PeakGroup object created using the supplied values

        Raises:
            DuplicatePeakGroup: A PeakGroup record with the same values already exists
            ConflictingValueError: A PeakGroup with the same unique key (MSRun and PeakGroup.name) exists, but with a
              different formula or PeakGroupSet
        """

        if self.verbosity >= 1:
            print(
                f"\tInserting {peak_group_attrs['name']} peak group for sample {msrun.sample}"
            )
        try:
            peak_group, created = PeakGroup.objects.get_or_create(
                msrun=msrun,
                name=peak_group_attrs["name"],
                formula=peak_group_attrs["formula"],
                peak_group_set=peak_group_set,
            )
            if not created:
                raise DuplicatePeakGroup(
                    adding_file=peak_group_set.filename,
                    ms_run=msrun,
                    sample=msrun.sample,
                    peak_group_name=peak_group_attrs["name"],
                    existing_peak_group_set=peak_group.peak_group_set,
                )
            peak_group.full_clean()
            peak_group.save()
        except IntegrityError as ie:
            iestr = str(ie)
            if (
                'duplicate key value violates unique constraint "unique_peakgroup"'
                in iestr
            ):
                # "Peak group with this Name and Msrun already exists"
                existing_peak_group = PeakGroup.objects.get(
                    msrun=msrun, name=peak_group_attrs["name"]
                )
                conflicting_fields = []
                existing_values = []
                new_values = []
                if existing_peak_group.formula != peak_group_attrs["formula"]:
                    conflicting_fields.append("formula")
                    existing_values.append(existing_peak_group.formula)
                    new_values.append(peak_group_attrs["formula"])
                if existing_peak_group.peak_group_set != peak_group_set:
                    conflicting_fields.append("peak_group_set")
                    existing_values.append(existing_peak_group.peak_group_set.filename)
                    new_values.append(peak_group_set.filename)
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
        Extract and store the data for MsRun PeakGroup and PeakData
        """
        animals_to_uncache = []

        if self.verbosity >= 1:
            print("Loading data...")

        # No need to try/catch - these need to succeed to start loading samples
        protocol = self.retrieve_or_create_protocol()
        peak_group_set = self.insert_peak_group_set()

        sample_msrun_dict = {}

        # Each sample gets its own msrun
        for sample_name in self.db_samples_dict.keys():
            # each msrun/sample has its own set of peak groups
            inserted_peak_group_dict = {}

            msrun_dict = {
                "date": self.date,
                "researcher": self.researcher,
                "protocol": protocol,
                "sample": self.db_samples_dict[sample_name],
            }
            try:
                # TODO This relies on sample name lookup (see issue #684)
                # https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/684
                """This also relies on accurate msrun information (researcher and date)
                Including mzXML files with accucor files will help ensure accurate msrun
                lookup since we will have checksums for the mzXML files and those are
                always associated with one MSRun record
                """
                msrun, created = MSRun.objects.get_or_create(**msrun_dict)
                if created:
                    msrun.full_clean()
                    msrun.save()
                    if self.verbosity >= 1:
                        print(
                            f"Inserting msrun for sample {sample_name}, date {self.date}, "
                            f"researcher {self.researcher}, protocol {protocol}"
                        )

                # This will be used to iterate the sample loop below so that we don't attempt to load samples whose
                # msrun creations failed.
                sample_msrun_dict[sample_name] = msrun

                if (
                    msrun.sample.animal not in animals_to_uncache
                    and msrun.sample.animal.caches_exist()
                ):
                    animals_to_uncache.append(msrun.sample.animal)
                elif not msrun.sample.animal.caches_exist() and self.verbosity >= 1:
                    print(
                        f"No cache exists for animal {msrun.sample.animal.id} linked to Sample {msrun.sample.id}"
                    )
            except Exception as e:
                self.aggregated_errors_object.buffer_error(e)
                continue

        # Create all PeakGroups
        for sample_name in sample_msrun_dict.keys():
            msrun = sample_msrun_dict[sample_name]

            # Pass through the rows once to identify the PeakGroups
            for index, corr_row in self.accucor_corrected_df.iterrows():
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
                                msrun=msrun,
                                peak_group_set=peak_group_set,
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
                                        raw_abundance = orig_row[sample_name]
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
                                    f"{sample_name}"
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
                            ][sample_name]

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
                                    f"[{sample_name}]."
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

                    for _index, corr_row in peak_group_corrected_df.iterrows():
                        try:
                            corrected_abundance_for_sample = corr_row[sample_name]
                            # No original dataframe, no raw_abundance, med_mz, or med_rt
                            raw_abundance = None
                            med_mz = None
                            med_rt = None

                            if self.verbosity >= 1:
                                print(
                                    f"\t\tInserting peak data for peak group [{peak_group_name}] "
                                    f"and sample [{sample_name}]."
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
                                        f"[{sample_name}]."
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
                    adding_file=peak_group_set.filename,
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

        if self.aggregated_errors_object.should_raise():
            raise self.aggregated_errors_object

        if self.dry_run:
            raise DryRun()

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
        # self.pre_load_setup()
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
            # self.post_load_teardown()
            raise
        except AggregatedErrors:
            # self.post_load_teardown()
            # If it was an aggregated errors exception, raise it directly
            raise
        except Exception as e:
            # self.post_load_teardown()
            # If it was some other (unanticipated or a single fatal) error, we want to report it, but also include
            # everything else that was stored in self.aggregated_errors_object.  An AggregatedErrors exception is
            # raised (in the called code) when errors are allowed to accumulate, but moving on to the next step/loop is
            # not possible.  And for simplicity, fatal errors that do not allow further accumulation of errors are
            # raised directly.
            self.aggregated_errors_object.buffer_error(e)
            self.aggregated_errors_object.should_raise()
            raise self.aggregated_errors_object

        # # Buffered auto-updates cannot be done inside the atomic transaction block because the auto-updates associated
        # # with the models involved in an accucor load transit many-related models to perform updates of already-loaded
        # # data based on new data, which requires queries of the linked pre-loaded data, and database queries are not
        # # allowed during atomic transactions.  Some auto-updates can happen inside an atomic transaction block because
        # # it operates on the objects without hitting the database, but that's not the case here.  Specifically, if
        # # this was called inside the transaction block, it would generate the error:
        # #
        # #  An error occurred in the current transaction. You can't execute queries until the end of the 'atomic'
        # #  block.  It uses `.count()` (to see if there exist records to propagate changes), `.first()` to see if the
        # #  related model is a MaintainedModel (inside an isinstance call), and `.all()` to cycle through the related
        # #  records.

        # autoupdate_mode = not self.defer_autoupdates
        # if not self.dry_run and autoupdate_mode:
        #     MaintainedModel.perform_buffered_updates()

        # self.post_load_teardown(autoupdate_mode)
        enable_caching_updates()

    # def pre_load_setup(self):
    #     MaintainedModel.disable_autoupdates()
    #     if self.dry_run:
    #         # Don't let any auto-updates buffer because we're not going to perform the mass auto-update
    #         MaintainedModel.disable_buffering()
    #     disable_caching_updates()

    # def post_load_teardown(self, clear_autoupdate_buffer=True):
    #     if clear_autoupdate_buffer:
    #         # We need to clear the update buffer so that the next call doesn't make auto-updates on non-existent (or
    #         # incorrect) records
    #         MaintainedModel.clear_update_buffer()
    #     # And before we leave, we must re-enable things
    #     MaintainedModel.enable_autoupdates()
    #     if self.dry_run:
    #         MaintainedModel.enable_buffering()
    #     enable_caching_updates()


class IsotopeObservationParsingError(Exception):
    pass


class MultipleMassNumbers(Exception):
    def __init__(self, labeled_element, mass_numbers):
        message = (
            f"Labeled element [{labeled_element}] exists among the tracer(s) with multiple mass numbers: "
            f"[{','.join(mass_numbers)}]."
        )
        super().__init__(message)
        self.labeled_element = labeled_element
        self.mass_numbers = mass_numbers


class MassNumberNotFound(Exception):
    def __init__(self, labeled_element, tracer_labeled_elements):
        message = (
            f"Labeled element [{labeled_element}] could not be found among the tracer(s) to retrieve its mass "
            "number.  Tracer labeled elements: "
            f"[{', '.join([x['element'] + x['mass_number'] for x in tracer_labeled_elements])}]."
        )
        super().__init__(message)
        self.labeled_element = labeled_element
        self.tracer_labeled_elements = tracer_labeled_elements


class TracerLabeledElementNotFound(Exception):
    pass


class SampleIndexNotFound(Exception):
    def __init__(self, sheet_name, num_cols):
        message = (
            f"Sample columns could not be identified in the [{sheet_name}] sheet.  There were {num_cols} columns.  At "
            "least one column with one of the following names must immediately preceed the sample columns: "
            f"[{','.join(NONSAMPLE_COLUMN_NAMES)}]."
        )
        super().__init__(message)
        self.sheet_name = sheet_name
        self.num_cols = num_cols


class CorrectedCompoundHeaderMissing(Exception):
    def __init__(self):
        message = (
            "Compound header [Compound] not found in the accucor corrected data.  This may be an isocorr file.  Try "
            "again and submit this file using the isocorr file upload form input (or add the --isocorr-format option "
            "on the command line)."
        )
        super().__init__(message)
