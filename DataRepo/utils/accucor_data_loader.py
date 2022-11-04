import collections
import re
from datetime import datetime
from typing import List, TypedDict

import regex
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from pandas.errors import EmptyDataError

from DataRepo.models import (
    Compound,
    ElementLabel,
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
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import (
    clear_update_buffer,
    disable_autoupdates,
    enable_autoupdates,
    perform_buffered_updates,
)
from DataRepo.models.researcher import get_researchers
from DataRepo.utils.exceptions import (
    MissingSamplesError,
    ValidationDatabaseSetupError,
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
        debug=False,
        new_researcher=False,
        database=None,
        validate=False,
        isocorr_format=False,
    ):
        self.accucor_original_df = accucor_original_df
        self.accucor_corrected_df = accucor_corrected_df
        self.date = datetime.strptime(date.strip(), "%Y-%m-%d")
        self.protocol_input = protocol_input.strip()
        self.researcher = researcher.strip()
        self.peak_group_set_filename_input = peak_group_set_filename.strip()
        if skip_samples is None:
            self.skip_samples = []
        else:
            self.skip_samples = skip_samples
        if sample_name_prefix is None:
            sample_name_prefix = ""
        self.sample_name_prefix = sample_name_prefix
        self.debug = debug
        self.new_researcher = new_researcher
        self.db = settings.TRACEBASE_DB
        self.isocorr_format = isocorr_format
        self.compound_header = "Compound"
        if isocorr_format:
            self.compound_header = "compound"
        # If a database was explicitly supplied
        if database is not None:
            self.validate = False
            self.db = database
        else:
            self.validate = validate
            if validate:
                if settings.VALIDATION_ENABLED:
                    self.db = settings.VALIDATION_DB
                else:
                    raise ValidationDatabaseSetupError()

        # These are set elsewhere
        self.peak_group_dict = {}

        disable_caching_updates()
        self.clean_dataframes()
        self.corrected_samples = self.get_df_sample_names(
            self.accucor_corrected_df, self.skip_samples
        )
        # Determine the labeled element from the corrected data
        (
            self.labeled_element,
            self.labeled_element_header,
        ) = self.get_labeled_element_and_header()
        self.sample_dict = self.get_db_samples()
        # Assuming only 1 animal is included as a source for all samples
        tracers = list(self.sample_dict.values())[0].animal.infusate.tracers.all()
        self.tracer_labeled_elements = self.get_tracer_labels(tracers)
        enable_caching_updates()

    def validate_data(self):
        """
        basic sanity/integrity checks for the data inputs
        """
        self.validate_dataframes()

        # cross validate peak_groups/compounds in database
        self.validate_peak_groups()

        self.validate_researcher()

        self.validate_compounds()

    def validate_researcher(self):
        # For file validation, use researcher "anonymous"
        if self.researcher != "anonymous":
            researchers = get_researchers(database=self.db)
            nl = "\n"
            if self.new_researcher is True:
                err_msg = (
                    f"Researcher [{self.researcher}] exists.  --new-researcher cannot be used for existing "
                    f"researchers.  Current researchers are:{nl}{nl.join(sorted(researchers))}"
                )
                assert self.researcher not in researchers, err_msg
            elif len(researchers) != 0:
                err_msg = (
                    f"Researcher [{self.researcher}] does not exist.  Please either choose from the following "
                    f"researchers, or if this is a new researcher, add --new-researcher to your command (leaving "
                    f"`--researcher {self.researcher}` as-is).  Current researchers are:"
                    f"{nl}{nl.join(sorted(researchers))}"
                )
                assert self.researcher in researchers, err_msg

    def clean_dataframes(self):
        """
        strip any leading and trailing spaces from the headers and some
        columns, just to normalize
        """
        if self.accucor_original_df is not None:
            self.accucor_original_df.rename(columns=lambda x: x.strip())
            self.accucor_original_df["compound"] = self.accucor_original_df[
                "compound"
            ].str.strip()
            self.accucor_original_df["formula"] = self.accucor_original_df[
                "formula"
            ].str.strip()

        self.accucor_corrected_df.rename(columns=lambda x: x.strip())
        try:
            self.accucor_corrected_df[self.compound_header] = self.accucor_corrected_df[
                self.compound_header
            ].str.strip()
        except KeyError as ke:
            if not self.isocorr_format and f"'{self.compound_header}'" in str(ke):
                raise KeyError(
                    "Compound header [Compound] not found in the accucor corrected data.  Did you forget to provide "
                    "--isocorr-format?"
                )
            else:
                raise ke

    def get_df_sample_names(self, df, skip_samples):
        sample_names = []

        if df is not None:
            minimum_sample_index = self.get_first_sample_column_index(df)
            sample_names = [
                sample
                for sample in list(df)[minimum_sample_index:]
                if sample not in skip_samples
            ]

        return sample_names

    def validate_dataframes(self):

        print("Validating data...")

        """
        Validate sample headers. Get the sample names from the original header
        [all columns]
        """
        original_samples = self.get_df_sample_names(
            self.accucor_original_df, self.skip_samples
        )

        # Make sure all sample columns have names
        corr_iter = collections.Counter(self.corrected_samples)
        for k, v in corr_iter.items():
            if k.startswith("Unnamed: "):
                raise Exception(
                    "Sample columns missing headers found in the Corrected data sheet. You have "
                    + str(len(self.accucor_corrected_df.columns))
                    + " columns."
                )

        if original_samples:
            # Make sure all sample columns have names
            orig_iter = collections.Counter(original_samples)
            for k, v in orig_iter.items():
                if k.startswith("Unnamed: "):
                    raise EmptyDataError(
                        "Sample columns missing headers found in the Original data sheet. You have "
                        + str(len(self.accucor_original_df.columns))
                        + " columns. Be sure to delete any unused columns."
                    )

            # Make sure that the sheets have the same number of sample columns
            original_only = sorted(set(original_samples) - set(self.corrected_samples))
            corrected_only = sorted(set(self.corrected_samples) - set(original_samples))
            err_msg = (
                "Samples in the original and corrected sheets differ."
                f"\nOriginal contains {len(orig_iter)} samples | Corrected contains {len(corr_iter)} samples"
                f"\nSamples in original sheet missing from corrected:\n{original_only}"
                f"\nSamples in corrected sheet missing from original:\n{corrected_only}"
            )
            assert orig_iter == corr_iter, err_msg

        if not self.isocorr_format:
            labeled_df = self.accucor_corrected_df.filter(regex=(ACCUCOR_LABEL_PATTERN))

            err_msg = (
                f"{self.__class__.__name__} multiple tracer labels ({','.join(labeled_df.columns)}), in Accucor "
                "corrected data not currently supported."
            )

            assert len(labeled_df.columns) == 1, err_msg

    def validate_compounds(self):

        if self.accucor_original_df is not None:
            dupe_dict = {}
            for index, row in self.accucor_original_df[
                self.accucor_original_df.duplicated(
                    subset=["compound", "isotopeLabel"], keep=False
                )
            ].iterrows():
                dupe_key = row["compound"] + " & " + row["isotopeLabel"]
                if dupe_key not in dupe_dict:
                    dupe_dict[dupe_key] = str(index + 1)
                else:
                    dupe_dict[dupe_key] += "," + str(index + 1)

            err_msg = (
                f"The following duplicate compound/isotope pairs were found in the original data: ["
                f"{'; '.join(list(map(lambda c: c + ' on rows: ' + dupe_dict[c], dupe_dict.keys())))}]"
            )
            assert len(dupe_dict.keys()) == 0, err_msg

        if self.isocorr_format:
            labeled_element_header = "isotopeLabel"
        else:
            labeled_element_header = self.labeled_element_header

        # do it again for the corrected
        dupe_dict = {}
        for index, row in self.accucor_corrected_df[
            self.accucor_corrected_df.duplicated(
                subset=[self.compound_header, labeled_element_header], keep=False
            )
        ].iterrows():
            dupe_key = row[self.compound_header] + " & " + row[labeled_element_header]
            if dupe_key not in dupe_dict:
                dupe_dict[dupe_key] = str(index + 1)
            else:
                dupe_dict[dupe_key] += "," + str(index + 1)

        err_msg = (
            f"The following duplicate Compound/Label pairs were found in the corrected data: ["
            f"{'; '.join(list(map(lambda c: c + ' on rows: ' + dupe_dict[c], dupe_dict.keys())))}]"
        )
        assert len(dupe_dict.keys()) == 0, err_msg

    # determine the labeled element from the corrected data
    def get_labeled_element_and_header(self):

        if self.isocorr_format:
            labeled_element_header = "isotopeLabel"
            labeled_element = None  # Determined on each row
        else:
            labeled_df = self.accucor_corrected_df.filter(regex=(ACCUCOR_LABEL_PATTERN))

            # This was validated in validate_dataframes to be 1 label column
            labeled_column = labeled_df.columns[0]
            labeled_element_header = labeled_column

            match = ACCUCOR_LABEL_PATTERN.match(labeled_column)
            labeled_element = match.group(1)
            if labeled_element:
                print(f"Labeled element is {labeled_element}")

        return labeled_element, labeled_element_header

    @classmethod
    def is_integer(cls, data):
        try:
            int(data)
            return True
        except ValueError:
            return False

    def get_db_samples(self):

        missing_samples = []

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
                # If we're validating, we'll be retrieving samples from the validation database, because we're assuming
                # that the samples in the accucor files are being validated against the samples that were just loaded
                # into the validation database.  If we're not validating, then we're retrieving samples from the one
                # database we're working with anyway
                sample_dict[sample_name] = Sample.objects.using(self.db).get(
                    name=prefixed_sample_name
                )
            except Sample.DoesNotExist:
                missing_samples.append(sample_name)

        if len(missing_samples) != 0:
            raise (
                MissingSamplesError(
                    f"{len(missing_samples)} samples are missing: {', '.join(missing_samples)}",
                    missing_samples,
                )
            )

        return sample_dict

    @classmethod
    def get_tracer_labels(cls, tracer_recs) -> List[IsotopeObservationData]:
        """
        This method returns a unique list of the labeled elements that exist among the tracers as if they were parent
        observations (i.e. count=0 and parent=True).  This is so that Isocorr data can record 0 observations for parent
        records.  Accucor data does present data for counts of 0 already.
        """
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
        return tracer_labeled_elements

    @classmethod
    def get_first_sample_column_index(cls, df):
        """
        given a dataframe return the column index of the likely "first" sample
        column
        """

        max_nonsample_index = 0
        for col_name in NONSAMPLE_COLUMN_NAMES:
            try:
                if df.columns.get_loc(col_name) > max_nonsample_index:
                    max_nonsample_index = df.columns.get_loc(col_name)
            except KeyError:
                # column is not found, so move on
                pass

        # the sample index should be the next column
        return max_nonsample_index + 1

    def validate_peak_groups(self):
        """
        step through the original file, and note all the unique peak group
        names/formulas and map to database compounds
        """

        self.peak_group_dict = {}
        missing_compounds = 0
        reference_dataframe = self.accucor_corrected_df
        peak_group_name_key = self.compound_header
        # corrected data does not have a formula column
        peak_group_formula_key = None
        if self.accucor_original_df is not None:
            reference_dataframe = self.accucor_original_df
            peak_group_name_key = "compound"
            # original data has a formula column
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

                """
                cross validate in database;  this is a mapping of peak group
                name to one or more compounds. peak groups sometimes detect
                multiple compounds delimited by slash
                """

                self.peak_group_dict[peak_group_name]["compounds"] = []
                compounds_input = [
                    compound_name.strip()
                    for compound_name in peak_group_name.split("/")
                ]
                for compound_input in compounds_input:
                    try:
                        mapped_compound = Compound.compound_matching_name_or_synonym(
                            compound_input
                        )
                        if mapped_compound is not None:
                            self.peak_group_dict[peak_group_name]["compounds"].append(
                                mapped_compound
                            )
                            """
                            If the formula was previously None because we were
                            working with corrected data (missing column), supplement
                            it with the mapped database compound's formula
                            """
                            if not self.peak_group_dict[peak_group_name]["formula"]:
                                self.peak_group_dict[peak_group_name][
                                    "formula"
                                ] = mapped_compound.formula
                        else:
                            missing_compounds += 1
                            print(f"Could not find compound '{compound_input}'")
                    except ValidationError:
                        missing_compounds += 1
                        print(
                            f"Could not uniquely identify compound using {compound_input}."
                        )

        assert missing_compounds == 0, f"{missing_compounds} compounds are missing."

    def retrieve_or_create_protocol(self):
        """
        retrieve or insert a protocol, based on input
        """
        print(
            f"Finding or inserting {Protocol.MSRUN_PROTOCOL} protocol for '{self.protocol_input}'..."
        )
        protocol, created = Protocol.retrieve_or_create_protocol(
            self.protocol_input,
            Protocol.MSRUN_PROTOCOL,
            f"For protocol's full text, please consult {self.researcher}.",
            database=self.db,
        )
        action = "Found"
        feedback = f"{protocol.category} protocol {protocol.id} '{protocol.name}'"
        if created:
            action = "Created"
            feedback += f" '{protocol.description}'"

        print(f"{action} {feedback}")

        return protocol

    def insert_peak_group_set(self):
        peak_group_set = PeakGroupSet(filename=self.peak_group_set_filename_input)
        # full_clean cannot validate (e.g. uniqueness) using a non-default database
        if self.db == settings.DEFAULT_DB:
            peak_group_set.full_clean()
        peak_group_set.save(using=self.db)
        return peak_group_set

    def load_data(self):
        """
        extract and store the data for MsRun PeakGroup and PeakData
        """
        animals_to_uncache = []

        protocol = self.retrieve_or_create_protocol()

        print("Loading data...")

        peak_group_set = self.insert_peak_group_set()

        # each sample gets its own msrun
        for sample_name in self.sample_dict.keys():

            # each msrun/sample has its own set of peak groups
            inserted_peak_group_dict = {}

            print(
                f"Inserting msrun for sample {sample_name}, date {self.date}, researcher {self.researcher}, protocol "
                f"{protocol}"
            )
            msrun = MSRun(
                date=self.date,
                researcher=self.researcher,
                protocol=protocol,
                sample=self.sample_dict[sample_name],
            )
            # full_clean cannot validate (e.g. uniqueness) using a non-default database
            if self.db == settings.DEFAULT_DB:
                msrun.full_clean()
            msrun.save(using=self.db)
            if (
                msrun.sample.animal not in animals_to_uncache
                and msrun.sample.animal.caches_exist()
            ):
                animals_to_uncache.append(msrun.sample.animal)
            elif not msrun.sample.animal.caches_exist():
                print(
                    f"No cache exists for animal {msrun.sample.animal.id} linked to Sample {msrun.sample.id}"
                )

            """
            Create all PeakGroups

            Pass through the rows once to identify the PeakGroups
            """

            for index, corr_row in self.accucor_corrected_df.iterrows():

                obs_isotopes = self.get_observed_isotopes(corr_row)

                # Assuming that if the first one is the parent, they all are.  Note that subsequent isotopes in the
                # list may be parent=True if 0 isotopes of that element were observed.
                if len(obs_isotopes) > 0 and obs_isotopes[0]["parent"]:

                    """
                    Here we insert PeakGroup, by name (only once per file).
                    NOTE: If the C12 PARENT/0-Labeled row encountered has any issues (for example, a null formula),
                    then this block will fail
                    """

                    peak_group_name = corr_row[self.compound_header]
                    print(
                        f"\tInserting {peak_group_name} peak group for sample "
                        f"{sample_name}"
                    )
                    peak_group_attrs = self.peak_group_dict[peak_group_name]
                    peak_group = PeakGroup(
                        msrun=msrun,
                        name=peak_group_attrs["name"],
                        formula=peak_group_attrs["formula"],
                        peak_group_set=peak_group_set,
                    )
                    # full_clean cannot validate (e.g. uniqueness) using a non-default database
                    if self.db == settings.DEFAULT_DB:
                        peak_group.full_clean()
                    peak_group.save(using=self.db)

                    """
                    associate the pre-vetted compounds with the newly inserted
                    PeakGroup
                    """
                    for compound in peak_group_attrs["compounds"]:
                        # Must save the compound to the correct database before it can be linked
                        compound.save(using=self.db)
                        peak_group.compounds.add(compound)
                    peak_labeled_elements = self.get_peak_labeled_elements(
                        peak_group.compounds.all()
                    )

                    # Insert PeakGroup Labels
                    for peak_labeled_element in peak_labeled_elements:
                        print(
                            f"\t\tInserting {peak_labeled_element} peak group label for peak group {peak_group.name}"
                        )
                        peak_group_label = PeakGroupLabel(
                            peak_group=peak_group,
                            element=peak_labeled_element["element"],
                        )
                        # full_clean cannot validate (e.g. uniqueness) using a non-default database
                        if self.db == settings.DEFAULT_DB:
                            peak_group_label.full_clean()
                        peak_group_label.save(using=self.db)

                    # cache
                    inserted_peak_group_dict[peak_group_name] = peak_group

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
                        raw_abundance = 0
                        med_mz = 0
                        med_rt = 0
                        # Ensuing code assumes a single labeled element in the tracer(s), so raise an exception if
                        # that's not true
                        if len(self.tracer_labeled_elements) != 1:
                            raise InvalidNumberOfLabeledElements(
                                "This code only supports a single labeled elements in the original data sheet, not "
                                f"{len(self.tracer_labeled_elements)}."
                            )
                        mass_number = self.tracer_labeled_elements[0]["mass_number"]

                        # Try to get original data. If it's not there, set empty values
                        try:
                            orig_row = peak_group_original_data.iloc[orig_row_idx]
                            orig_isotopes = self.parse_isotope_string(
                                orig_row["isotopeLabel"], self.tracer_labeled_elements
                            )
                            for isotope in orig_isotopes:
                                # If it's a matching row
                                if (
                                    isotope["element"] == peak_group_label_rec.element
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

                        # Lookup corrected abundance by compound and label
                        corrected_abundance = self.accucor_corrected_df.loc[
                            (
                                self.accucor_corrected_df[self.compound_header]
                                == peak_group_name
                            )
                            & (
                                self.accucor_corrected_df[self.labeled_element_header]
                                == labeled_count
                            )
                        ][sample_name]

                        print(
                            f"\t\tInserting peak data for {peak_group_name}:label-{labeled_count} "
                            f"for sample {sample_name}"
                        )

                        peak_data = PeakData(
                            peak_group=peak_group,
                            raw_abundance=raw_abundance,
                            corrected_abundance=corrected_abundance,
                            med_mz=med_mz,
                            med_rt=med_rt,
                        )

                        # full_clean cannot validate (e.g. uniqueness) using a non-default database
                        if self.db == settings.DEFAULT_DB:
                            peak_data.full_clean()

                        peak_data.save(using=self.db)

                        """
                        Create the PeakDataLabel records
                        """

                        print(
                            f"\t\t\tInserting peak data label [{isotope['mass_number']}{isotope['element']}"
                            f"{isotope['count']}] parsed from cell value: [{orig_row['isotopeLabel']}] for peak data "
                            f"ID [{peak_data.id}], peak group [{peak_group_name}], and sample [{sample_name}]."
                        )

                        peak_data_label = PeakDataLabel(
                            peak_data=peak_data,
                            element=peak_group_label_rec.element,
                            count=labeled_count,
                            mass_number=mass_number,
                        )

                        if self.db == settings.DEFAULT_DB:
                            peak_data_label.full_clean()

                        peak_data_label.save(using=self.db)

                else:
                    peak_group_corrected_df = self.accucor_corrected_df[
                        self.accucor_corrected_df[self.compound_header]
                        == peak_group_name
                    ]

                    for _index, corr_row in peak_group_corrected_df.iterrows():

                        corrected_abundance_for_sample = corr_row[sample_name]
                        # No original dataframe, no raw_abundance, med_mz, or med_rt
                        raw_abundance = None
                        med_mz = None
                        med_rt = None

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

                        if self.db == settings.DEFAULT_DB:
                            peak_data.full_clean()

                        peak_data.save(using=self.db)

                        """
                        Create the PeakDataLabel records
                        """

                        corr_isotopes = self.get_observed_isotopes(
                            corr_row, peak_group.compounds.all()
                        )

                        for isotope in corr_isotopes:

                            print(
                                f"\t\t\tInserting peak data label [{isotope['mass_number']}{isotope['element']}"
                                f"{isotope['count']}] parsed from cell value: [{corr_row[self.labeled_element_header]}"
                                f"] for peak data ID [{peak_data.id}], peak group [{peak_group_name}], and sample "
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

                            if self.db == settings.DEFAULT_DB:
                                peak_data_label.full_clean()

                            peak_data_label.save(using=self.db)

        assert not self.debug, "Debugging..."

        if settings.DEBUG:
            print("Expiring affected caches...")

        for animal in animals_to_uncache:
            if settings.DEBUG:
                print(f"Expiring animal {animal.id}'s cache")
            animal.delete_related_caches()

        if settings.DEBUG:
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
                        raise Exception(
                            "Cannot uniquely match measured labeled elements with tracer labeled elements."
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
                print(
                    f"WARNING: More measured isotopes ({isotopes}) than tracer labeled elements ({parent_labels}) "
                    f"for compounds ({observed_compound_recs})."
                )

        else:

            # Get the mass number(s) from the associated tracers
            mns = [
                x["mass_number"]
                for x in self.tracer_labeled_elements
                if x["element"] == self.labeled_element
            ]
            if len(mns) > 1:
                raise MultipleMassNumbers(
                    f"Labeled element [{self.labeled_element}] exists among the tracer(s) with "
                    f"multiple mass numbers: [{','.join(mns)}]."
                )
            elif len(mns) == 0:
                raise MassNumberNotFound(
                    f"Labeled element [{self.labeled_element}] could not be found among the tracer(s) to retrieve its "
                    "mass number."
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
                    raise Exception(
                        "tracer_labeled_elements required to process PARENT entries."
                    )
                isotope_observations = tracer_labeled_elements
            else:
                if len(elements) != len(mass_numbers) or len(elements) != len(counts):
                    IsotopeObservationParsingError(
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

    def load_accucor_data(self):

        disable_autoupdates()
        disable_caching_updates()

        try:
            with transaction.atomic():
                self.validate_data()
                self.load_data()
        except AssertionError as e:
            if "Debugging..." in str(e):
                # If we're in debug mode, we need to clear the update buffer so that the next call doesn't make
                # auto-updates on non-existent (or incorrect) records
                clear_update_buffer()

                # And before we leave, we must re-enable things
                enable_autoupdates()
                enable_caching_updates()

            raise e

        # This cannot be in the atomic block because it needs to execute queries that generate the error:
        # An error occurred in the current transaction. You can't execute queries until the end of the 'atomic' block.
        # It comes from trying to trigger updates in many-related records, which uses `.count()` (to see if there exist
        # records to propagate changes), `.first()` to see if the related model is a MaintainedModel (inside an
        # isinstance call), and `.all()` to cycle through the related records

        # TODO: This may no longer be necessary since various bugs have been fixed.  Put this back under
        #       transaction.atomic and re-test

        if not self.debug:
            perform_buffered_updates(using=self.db)

        enable_autoupdates()
        enable_caching_updates()


class IsotopeObservationParsingError(Exception):
    pass


class MultipleMassNumbers(Exception):
    pass


class MassNumberNotFound(Exception):
    pass


class InvalidNumberOfLabeledElements(Exception):
    pass
