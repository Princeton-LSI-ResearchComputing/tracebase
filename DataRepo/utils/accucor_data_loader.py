import collections
import re
import regex
from datetime import datetime

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from pandas.errors import EmptyDataError
from typing import List, Optional, TypedDict

from DataRepo.models import (
    Compound,
    ElementLabel,
    MSRun,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupSet,
    Protocol,
    Sample,
)
from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.utilities import get_researchers
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
        self.date_input = date.strip()
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

    def validate_data(self):
        """
        basic sanity/integrity checks for the data inputs
        """
        disable_caching_updates()

        self.validate_dataframes()

        # determine the labeled element from the corrected data
        self.set_labeled_element()

        self.date = datetime.strptime(self.date_input, "%Y-%m-%d")

        self.retrieve_samples()

        self.retrieve_or_create_protocol()

        # cross validate peak_groups/compounds in database
        self.validate_peak_groups()

        self.validate_researcher()

        self.validate_compounds()

        enable_caching_updates()

    def validate_researcher(self):
        # For file validation, use researcher "anonymous"
        if self.researcher != "anonymous":
            researchers = get_researchers()
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

    def validate_dataframes(self):

        print("Validating data...")

        if self.accucor_original_df is not None:
            # column index of the first predicted sample for the original data
            original_minimum_sample_index = self.get_first_sample_column_index(
                self.accucor_original_df
            )
        # column index of the first predicted sample for the corrected data
        corrected_minimum_sample_index = self.get_first_sample_column_index(
            self.accucor_corrected_df
        )

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

        """
        Validate sample headers. Get the sample names from the original header
        [all columns]
        """
        original_samples = None
        if self.accucor_original_df is not None:
            original_samples = [
                sample
                for sample in list(self.accucor_original_df)[
                    original_minimum_sample_index:
                ]
                if sample not in self.skip_samples
            ]
        corrected_samples = [
            sample
            for sample in list(self.accucor_corrected_df)[
                corrected_minimum_sample_index:
            ]
            if sample not in self.skip_samples
        ]

        # these could be None, if there was no original dataframe
        self.original_samples = original_samples
        # but the corrected list should have something we can rely on
        self.corrected_samples = corrected_samples

        # Make sure all sample columns have names
        corr_iter = collections.Counter(corrected_samples)
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
            original_only = sorted(set(original_samples) - set(corrected_samples))
            corrected_only = sorted(set(corrected_samples) - set(original_samples))
            err_msg = (
                "Samples in the original and corrected sheets differ."
                f"\nOriginal contains {len(orig_iter)} samples | Corrected contains {len(corr_iter)} samples"
                f"\nSamples in original sheet missing from corrected:\n{original_only}"
                f"\nSamples in corrected sheet missing from original:\n{corrected_only}"
            )
            assert orig_iter == corr_iter, err_msg

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
    def set_labeled_element(self):

        if self.isocorr_format:
            self.labeled_element_header = "isotopeLabel"
            self.labeled_element = None  # Determined on each row
        else:
            labeled_df = self.accucor_corrected_df.filter(regex=(ACCUCOR_LABEL_PATTERN))

            err_msg = (
                f"{self.__class__.__name__} multiple tracer labels ({','.join(labeled_df.columns)}), in Accucor "
                "corrected data not currently supported."
            )
            assert len(labeled_df.columns) == 1, err_msg

            labeled_column = labeled_df.columns[0]
            self.labeled_element_header = labeled_column
            match = ACCUCOR_LABEL_PATTERN.match(labeled_column)
            labeled_element = match.group(1)
            if labeled_element:
                print(f"Setting labeled element to {labeled_element}")
                self.labeled_element = labeled_element

    def is_integer(self, data):
        try:
            int(data)
            return True
        except ValueError:
            return False

    def retrieve_samples(self):

        missing_samples = []

        print("Checking samples...")

        # cross validate in database
        self.sample_dict = {}
        """
        Because the original dataframe might be None, here, we rely on the
        corrected sample list as being authoritative
        """
        for sample_name in self.corrected_samples:
            prefix_sample_name = f"{self.sample_name_prefix}{sample_name}"
            try:
                # If we're validating, we'll be retrieving samples from the validation database, because we're assuming
                # that the samples in the accucor files are being validated against the samples that were just loaded
                # into the validation database.  If we're not validating, then we're retrieving samples from the one
                # database we're working with anyway
                self.sample_dict[sample_name] = Sample.objects.using(self.db).get(
                    name=prefix_sample_name
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

    def get_first_sample_column_index(self, df):
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
                compounds_input = peak_group_name.split("/")
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
                            print(f"Could not find compound {compound_input}")
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
        self.protocol, created = Protocol.retrieve_or_create_protocol(
            self.protocol_input,
            Protocol.MSRUN_PROTOCOL,
            f"For protocol's full text, please consult {self.researcher}.",
            database=self.db,
        )
        action = "Found"
        feedback = f"{self.protocol.category} protocol {self.protocol.id} '{self.protocol.name}'"
        if created:
            action = "Created"
            feedback += f" '{self.protocol.description}'"
        print(f"{action} {feedback}")

    def insert_peak_group_set(self):
        self.peak_group_set = PeakGroupSet(filename=self.peak_group_set_filename_input)
        # full_clean cannot validate (e.g. uniqueness) using a non-default database
        if self.db == settings.DEFAULT_DB:
            self.peak_group_set.full_clean()
        self.peak_group_set.save(using=self.db)

    def load_data(self):
        """
        extract and store the data for MsRun PeakGroup and PeakData
        """
        disable_caching_updates()
        animals_to_uncache = []

        print("Loading data...")

        self.insert_peak_group_set()

        self.sample_run_dict = {}

        # each sample gets its own msrun
        for sample_name in self.sample_dict.keys():

            # each msrun/sample has its own set of peak groups
            inserted_peak_group_dict = {}

            print(
                f"Inserting msrun for sample {sample_name}, date {self.date}, researcher {self.researcher}, protocol "
                f"{self.protocol}"
            )
            msrun = MSRun(
                date=self.date,
                researcher=self.researcher,
                protocol=self.protocol,
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
            self.sample_run_dict[sample_name] = msrun

            # Pass once through the corrected data to create all PeakGroups
            for index, corr_row in self.accucor_corrected_df.iterrows():
                # (
                #     labeled_element,
                #     labeled_count,
                #     mass_number,
                # )
                isotopes = self.get_isotopes(corr_row)
                # Assuming a single parent element/count/mass_number, based on available data
                if len(isotopes) == 1 and isotopes[0]["count"] == 0:
                    parent = isotopes[0]

                    """
                    Here we insert PeakGroup, by name (only once per file).
                    NOTE: if the C12 PARENT/0-Labeled row encountered has any
                    issues (for example, a null formula), then this block will
                    fail
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
                        peak_group_set=self.peak_group_set,
                    )
                    # full_clean cannot validate (e.g. uniqueness) using a non-default database
                    if self.db == settings.DEFAULT_DB:
                        peak_group.full_clean()
                    peak_group.save(using=self.db)
                    # cache
                    inserted_peak_group_dict[peak_group_name] = {
                        "group": peak_group,
                        "parent": parent,
                    }

                    """
                    associate the pre-vetted compounds with the newly inserted
                    PeakGroup
                    """
                    for compound in peak_group_attrs["compounds"]:
                        # Must save the compound to the correct database before it can be linked
                        compound.save(using=self.db)
                        peak_group.compounds.add(compound)

            # For each PeakGroup, create PeakData rows
            for peak_group_name in inserted_peak_group_dict:

                # we should have a cached PeakGroup and its labeled element now
                peak_group = inserted_peak_group_dict[peak_group_name]["group"]
                parent = inserted_peak_group_dict[peak_group_name]["parent"]

                if self.accucor_original_df is not None:

                    peak_group_original_data = self.accucor_original_df.loc[
                        self.accucor_original_df["compound"] == peak_group_name
                    ]

                    # Original data skips undetected counts, but corrected data does not, so as we march through the
                    # corrected data, we need to keep track of the corresponding row in the original data
                    orig_row_idx = 0
                    for labeled_count in range(
                        0, peak_group.atom_count(parent["element"]) + 1
                    ):
                        # Try to get original data. If it's not there, set empty values
                        try:
                            orig_row = peak_group_original_data.iloc[orig_row_idx]
                            orig_isotopes = self.parse_isotope_string(orig_row["isotopeLabel"])
                            for isotope in orig_isotopes:
                                # If it's a matching row
                                if isotope["element"] == parent["element"] and isotope["count"] == labeled_count:
                                    # We have a matching row, use it and increment row_idx
                                    raw_abundance = orig_row[sample_name]
                                    med_mz = orig_row["medMz"]
                                    med_rt = orig_row["medRt"]
                                    orig_row_idx = orig_row_idx + 1
                        except IndexError:
                            raw_abundance = 0
                            med_mz = 0
                            med_rt = 0
                            orig_isotopes = []

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

                        for isotope in orig_isotopes:

                            peak_data_label = PeakDataLabel(
                                peak_data=peak_data,
                                element=isotope["element"],
                                count=isotope["count"],
                                mass_number=isotope["mass_number"],
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

                        corr_isotopes = self.get_isotopes(corr_row)

                        for isotope in corr_isotopes:

                            print(
                                f"\t\t\tInserting peak data label [{isotope['mass_number']}{isotope['element']}"
                                f"{isotope['count']}] for peak data ID [{peak_data.id}], peak group [{peak_group_name}"
                                f"], and sample [{sample_name}]."
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

        enable_caching_updates()

        if settings.DEBUG:
            print("Expiring affected caches...")

        for animal in animals_to_uncache:
            if settings.DEBUG:
                print(f"Expiring animal {animal.id}'s cache")
            animal.delete_related_caches()

        if settings.DEBUG:
            print("Expiring done.")

    def get_isotopes(self, row):
        """
        Given a row of data, it retrieves the labeled element, count, and mass_number using a method corresponding to
        the file format
        """
        if self.isocorr_format:
            # E.g. Parsing C13N15-label-2-3 in isotopeLabel column
            isotopes = self.parse_isotope_string(
                row[self.labeled_element_header]
            )
        else:
            # E.g. Getting count value from e.g. C_Label column
            isotopes = [{
                "element": self.labeled_element,
                "count": row[self.labeled_element_header],
                "mass_number": None,  # Not defined in accucor corrected file
            }]

        return isotopes

    @classmethod
    def parse_isotope_string(cls, label) -> List[IsotopeObservationData]:
        """
        Parse El-Maven style isotope label string, e.g. C12 PARENT, C13-label-1, C13N15-label-2-1
        Returns a list of IsotopeObservationData objects (which is a TypedDict)
        """

        isotope_observations = []

        match = regex.match(ISOTOPE_LABEL_PATTERN, label)

        if match:
            elements = match.captures("elements")
            mass_numbers = match.captures("mass_numbers")
            counts = match.captures("counts")
            parent = match.group("parent")

            if parent is not None and parent == "PARENT":
                counts = [0]

            if len(elements) != len(mass_numbers) or len(elements) != len(counts):
                IsotopeParsingError(
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
                        )
                    )
        else:
            raise IsotopeParsingError(f"Unable to parse isotope label: [{label}]")

        return isotope_observations

    def load_accucor_data(self):

        with transaction.atomic():
            self.validate_data()
            self.load_data()


class IsotopeParsingError(Exception):
    pass
