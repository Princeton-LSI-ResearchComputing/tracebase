import collections
import re
from collections import namedtuple
from datetime import datetime, timedelta

import dateutil.parser
from django.db import transaction

from DataRepo.models import (
    Animal,
    Compound,
    MSRun,
    PeakData,
    PeakGroup,
    PeakGroupSet,
    Protocol,
    Sample,
    Study,
    Tissue,
    TracerLabeledClass,
    value_from_choices_label,
)


class SampleTableLoader:
    """
    Load a sample table
    """

    SampleTableHeaders = namedtuple(
        "SampleTableHeaders",
        [
            "SAMPLE_NAME",
            "SAMPLE_DATE",
            "SAMPLE_RESEARCHER",
            "TISSUE_NAME",
            "TIME_COLLECTED",
            "STUDY_NAME",
            "STUDY_DESCRIPTION",
            "ANIMAL_NAME",
            "ANIMAL_WEIGHT",
            "ANIMAL_AGE",
            "ANIMAL_SEX",
            "ANIMAL_GENOTYPE",
            "ANIMAL_FEEDING_STATUS",
            "ANIMAL_DIET",
            "ANIMAL_STATE",
            "TRACER_COMPOUND_NAME",
            "TRACER_LABELED_ELEMENT",
            "TRACER_LABELED_COUNT",
            "TRACER_INFUSION_RATE",
            "TRACER_INFUSION_CONCENTRATION",
        ],
    )

    DefaultSampleTableHeaders = SampleTableHeaders(
        SAMPLE_NAME="SAMPLE_NAME",
        SAMPLE_DATE="SAMPLE_DATE",
        SAMPLE_RESEARCHER="SAMPLE_RESEARCHER",
        TISSUE_NAME="TISSUE_NAME",
        STUDY_NAME="STUDY_NAME",
        TIME_COLLECTED="TIME_COLLECTED",
        STUDY_DESCRIPTION="STUDY_DESCRIPTION",
        ANIMAL_NAME="ANIMAL_NAME",
        ANIMAL_WEIGHT="ANIMAL_WEIGHT",
        ANIMAL_AGE="ANIMAL_AGE",
        ANIMAL_SEX="ANIMAL_SEX",
        ANIMAL_GENOTYPE="ANIMAL_GENOTYPE",
        ANIMAL_FEEDING_STATUS="ANIMAL_FEEDING_STATUS",
        ANIMAL_DIET="ANIMAL_DIET",
        ANIMAL_STATE="ANIMAL_STATE",
        TRACER_COMPOUND_NAME="TRACER_COMPOUND_NAME",
        TRACER_LABELED_ELEMENT="TRACER_LABELED_ELEMENT",
        TRACER_LABELED_COUNT="TRACER_LABELED_COUNT",
        TRACER_INFUSION_RATE="TRACER_INFUSION_RATE",
        TRACER_INFUSION_CONCENTRATION="TRACER_INFUSION_CONCENTRATION",
    )

    def __init__(self, sample_table_headers=DefaultSampleTableHeaders):
        self.headers = sample_table_headers
        self.blank = ""

    def load_sample_table(self, data):
        for row in data:

            # Skip BLANK rows
            if row[self.headers.TISSUE_NAME] == self.blank:
                print("Skipping row: Tissue field is empty, assuming blank sample")
                continue

            # Tissue
            tissue, created = Tissue.objects.get_or_create(
                name=row[self.headers.TISSUE_NAME]
            )
            if created:
                print(f"Created new record: Tissue:{tissue}")
            try:
                tissue.full_clean()
                tissue.save()
            except Exception as e:
                print(f"Error saving record: Tissue:{tissue}")
                raise (e)

            # Study
            study, created = Study.objects.get_or_create(
                name=row[self.headers.STUDY_NAME]
            )
            if created:
                if self.headers.STUDY_DESCRIPTION:
                    study.description = row[self.headers.STUDY_DESCRIPTION]
                print(f"Created new record: Study:{study}")
            try:
                study.full_clean()
                study.save()
            except Exception as e:
                print(f"Error saving record: Study:{study}")
                raise (e)

            # Animal
            animal, created = Animal.objects.get_or_create(
                name=row[self.headers.ANIMAL_NAME]
            )

            """
            We do this here, and not in the "created" block below, in case the
            researcher is creating a new study from previously-loaded animals
            """
            if animal not in study.animals.all():
                print("Adding animal to the study...")
                study.animals.add(animal)

            if created:
                print(f"Created new record: Animal:{animal}")
            if self.headers.ANIMAL_GENOTYPE:
                animal.genotype = row[self.headers.ANIMAL_GENOTYPE]
            if self.headers.ANIMAL_WEIGHT:
                animal.body_weight = row[self.headers.ANIMAL_WEIGHT]
            if self.headers.ANIMAL_FEEDING_STATUS:
                animal.feeding_status = row[self.headers.ANIMAL_FEEDING_STATUS]
            if self.headers.ANIMAL_AGE:
                animal.age = row[self.headers.ANIMAL_AGE]
            if self.headers.ANIMAL_DIET:
                animal.diet = row[self.headers.ANIMAL_DIET]
            if self.headers.ANIMAL_STATE:
                animal.state = row[self.headers.ANIMAL_STATE]
            if self.headers.ANIMAL_SEX:
                animal_sex_string = row[self.headers.ANIMAL_SEX]
                if animal_sex_string in animal.SEX_CHOICES:
                    animal_sex = animal_sex_string
                else:
                    animal_sex = value_from_choices_label(
                        animal_sex_string, animal.SEX_CHOICES
                    )
                animal.sex = animal_sex
            if self.headers.TRACER_COMPOUND_NAME:
                try:
                    tracer_compound_name = row[self.headers.TRACER_COMPOUND_NAME]
                    tracer_compound = Compound.objects.get(name=tracer_compound_name)
                    animal.tracer_compound = tracer_compound
                except Compound.DoesNotExist as e:
                    print(
                        f"ERROR: {self.headers.TRACER_COMPOUND_NAME} not found: Compound:{tracer_compound_name}"
                    )
                    raise (e)
            if self.headers.TRACER_LABELED_ELEMENT:
                tracer_labeled_atom = value_from_choices_label(
                    row[self.headers.TRACER_LABELED_ELEMENT],
                    animal.TRACER_LABELED_ELEMENT_CHOICES,
                )
                animal.tracer_labeled_atom = tracer_labeled_atom
            if self.headers.TRACER_LABELED_COUNT:
                animal.tracer_labeled_count = int(row[self.headers.TRACER_LABELED_COUNT])
            if self.headers.TRACER_INFUSION_RATE:
                animal.tracer_infusion_rate = row[self.headers.TRACER_INFUSION_RATE]
            if self.headers.TRACER_INFUSION_CONCENTRATION:
                animal.tracer_infusion_concentration = row[
                    self.headers.TRACER_INFUSION_CONCENTRATION
                ]
            try:
                animal.full_clean()
                animal.save()
            except Exception as e:
                print(f"Error saving record: Animal:{animal}")
                raise (e)

            # Sample
            sample_name = row[self.headers.SAMPLE_NAME]
            try:
                sample = Sample.objects.get(name=sample_name)
            except Sample.DoesNotExist:
                print(f"Creating new record: Sample:{sample_name}")
                sample = Sample(
                    name=row[self.headers.SAMPLE_NAME],
                    researcher=row[self.headers.SAMPLE_RESEARCHER],
                    time_collected=timedelta(
                        minutes=float(row[self.headers.TIME_COLLECTED])
                    ),
                    animal=animal,
                    tissue=tissue,
                )
                if self.headers.SAMPLE_DATE:
                    sample_date_str = row[self.headers.SAMPLE_DATE]
                    sample_date = dateutil.parser.parse(sample_date_str)
                    sample.date = sample_date
            try:
                sample.full_clean()
                sample.save()
            except Exception as e:
                print(f"Error saving record: Sample:{sample}")
                raise (e)


class AccuCorDataLoader:

    """
    Load the Protocol, MsRun, PeakGroup, and PeakData tables
    """

    def __init__(self, **kwargs):
        self.accucor_original_df = kwargs.get("accucor_original_df")
        self.accucor_corrected_df = kwargs.get("accucor_corrected_df")
        self.date_input = kwargs.get("date").strip()
        self.protocol_input = kwargs.get("protocol_input").strip()
        self.researcher = kwargs.get("researcher").strip()
        self.peak_group_set_filename_input = kwargs.get(
            "peak_group_set_filename"
        ).strip()
        self.debug = False
        if kwargs.get("debug"):
            self.debug = kwargs.get("debug")

    def validate_data(self):
        """
        basic sanity/integrity checks for the data inputs
        """

        self.validate_dataframes()

        # determine the labeled element from the corrected data
        self.set_labeled_element()

        self.date = datetime.strptime(self.date_input, "%Y-%m-%d")

        self.retrieve_samples()

        self.retrieve_protocol()

        # cross validate peak_groups/compounds in database
        self.validate_peak_groups()

    def validate_dataframes(self):

        print("Validating data...")

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
        self.accucor_original_df.rename(columns=lambda x: x.strip())
        self.accucor_corrected_df.rename(columns=lambda x: x.strip())
        self.accucor_original_df["compound"] = self.accucor_original_df[
            "compound"
        ].str.strip()
        self.accucor_original_df["formula"] = self.accucor_original_df[
            "formula"
        ].str.strip()
        self.accucor_corrected_df["Compound"] = self.accucor_corrected_df[
            "Compound"
        ].str.strip()

        """
        Validate sample headers. Get the sample names from the original header
        [all columns]
        """

        original_samples = list(self.accucor_original_df)[
            original_minimum_sample_index:
        ]
        corrected_samples = list(self.accucor_corrected_df)[
            corrected_minimum_sample_index:
        ]
        err_msg = "Samples are not equivalent in the original and corrected data"
        assert collections.Counter(original_samples) == collections.Counter(
            corrected_samples
        ), err_msg
        self.original_samples = original_samples

    def corrected_file_tracer_labeled_column_regex(self):
        regex_pattern = ""
        tracer_element_list = TracerLabeledClass.tracer_labeled_elements_list()
        regex_pattern = f"^({'|'.join(tracer_element_list)})_Label$"
        return regex_pattern

    # determine the labeled element from the corrected data
    def set_labeled_element(self):

        label_pattern = self.corrected_file_tracer_labeled_column_regex()
        labeled_df = self.accucor_corrected_df.filter(regex=(label_pattern))

        err_msg = f"{self.__class__.__name__} cannot deal with multiple tracer labels"
        err_msg += f"({','.join(labeled_df.columns)}), currently..."
        assert len(labeled_df.columns) == 1, err_msg

        labeled_column = labeled_df.columns[0]
        self.labeled_element_header = labeled_column
        re_pattern = re.compile(label_pattern)
        match = re_pattern.match(labeled_column)
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

        missing_samples = 0

        print("Checking samples...")
        # cross validate in database
        self.sample_dict = {}
        for original_sample_name in self.original_samples:
            try:
                # cached it for later
                self.sample_dict[original_sample_name] = Sample.objects.get(
                    name=original_sample_name
                )
            except Sample.DoesNotExist:
                missing_samples += 1
                print(f"Could not find sample {original_sample_name} in the database.")
        assert missing_samples == 0, f"{missing_samples} samples are missing."

    def get_first_sample_column_index(self, df):

        """
        given a dataframe return the column index of the likely "first" sample
        column
        """

        # list of column names from data files that we know are not samples
        NONSAMPLE_COLUMN_NAMES = [
            "label",
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
        ]

        # append the *_Label columns of the corrected dataframe
        tracer_element_list = TracerLabeledClass.tracer_labeled_elements_list()
        for element in tracer_element_list:
            NONSAMPLE_COLUMN_NAMES.append(f"{element}_Label")

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

        for index, row in self.accucor_original_df.iterrows():
            # uniquely record the group, by name
            peak_group_name = row["compound"]
            peak_group_formula = row["formula"]
            if peak_group_name not in self.peak_group_dict:

                """
                cross validate in database;  this is a mapping of peak group
                name to one or more compounds. peak groups sometimes detect
                multiple compounds delimited by slash
                """

                compounds_input = peak_group_name.split("/")

                for compound_input in compounds_input:
                    try:
                        # cache it for later; note, if the first row encountered
                        # is missing a formula, there will be issues later
                        self.peak_group_dict[peak_group_name] = {
                            "name": peak_group_name,
                            "formula": peak_group_formula,
                        }

                        # peaks can contain more than 1 compound
                        mapped_compound = Compound.objects.get(name=compound_input)
                        if "compounds" in self.peak_group_dict[peak_group_name]:
                            self.peak_group_dict[peak_group_name]["compounds"].append(
                                mapped_compound
                            )
                        else:
                            self.peak_group_dict[peak_group_name]["compounds"] = [
                                mapped_compound
                            ]
                    except Compound.DoesNotExist:
                        missing_compounds += 1
                        print(f"Could not find compound {compound_input}")

        assert missing_compounds == 0, f"{missing_compounds} compounds are missing."

    def retrieve_protocol(self):
        """
        retrieve or insert a protocol, based on input
        """
        print(f"Finding or inserting protocol for '{self.protocol_input}'...")

        action = "Found"

        if self.is_integer(self.protocol_input):
            try:
                self.protocol = Protocol.objects.get(id=self.protocol_input)
            except Protocol.DoesNotExist as e:
                print("Protocol does not exist.")
                raise e
        else:
            try:
                self.protocol, created = Protocol.objects.get_or_create(
                    name=self.protocol_input
                )

                if created:
                    action = "Created"
            except Exception as e:
                print(f"Failed to get or create protocol {self.protocol_input}")
                raise e

        print(f"{action} protocol {self.protocol.id} '{self.protocol.name}'")

    def insert_peak_group_set(self):
        self.peak_group_set = PeakGroupSet(filename=self.peak_group_set_filename_input)
        self.peak_group_set.full_clean()
        self.peak_group_set.save()

    def load_data(self):

        """
        extract and store the data for MsRun PeakGroup and PeakData
        """
        print("Loading data...")

        self.insert_peak_group_set()

        self.sample_run_dict = {}

        # each sample gets its own msrun
        for original_sample_name in self.sample_dict.keys():

            # each msrun/sample has its own set of peak groups
            inserted_peak_group_dict = {}

            print(f"Inserting msrun for {original_sample_name}")
            msrun = MSRun(
                date=self.date,
                researcher=self.researcher,
                protocol=self.protocol,
                sample=self.sample_dict[original_sample_name],
            )
            msrun.full_clean()
            msrun.save()
            self.sample_run_dict[original_sample_name] = msrun

            # Create all PeakGroups
            for index, row in self.accucor_original_df.iterrows():
                isotope, labeled_count = self.parse_isotope_label(row["isotopeLabel"])
                if labeled_count == 0:

                    """
                    Here we insert PeakGroup, by name (only once per file).
                    NOTE: if the C12 PARENT row encountered has any issues (for
                    example, a null formula), then this block will fail
                    """

                    peak_group_name = row["compound"]
                    print(
                        f"\tInserting {peak_group_name} peak group for sample "
                        f"{original_sample_name}"
                    )
                    peak_group_attrs = self.peak_group_dict[peak_group_name]
                    peak_group = PeakGroup(
                        ms_run=msrun,
                        name=peak_group_attrs["name"],
                        formula=peak_group_attrs["formula"],
                        peak_group_set=self.peak_group_set,
                    )
                    peak_group.full_clean()
                    peak_group.save()
                    # cache
                    inserted_peak_group_dict[peak_group_name] = peak_group

                    """
                    associate the pre-vetted compounds with the newly inserted
                    PeakGroup
                    """
                    for compound in peak_group_attrs["compounds"]:
                        peak_group.compounds.add(compound)

            # For each PeakGroup, create PeakData rows
            for peak_group_name in inserted_peak_group_dict:

                # we should have a cached PeakGroup now
                peak_group = inserted_peak_group_dict[peak_group_name]

                peak_group_original_data = self.accucor_original_df.loc[
                    self.accucor_original_df["compound"] == peak_group_name
                ]
                # get next row from original data
                row_idx = 0
                for labeled_count in range(
                    0, peak_group.atom_count(self.labeled_element) + 1
                ):
                    try:
                        row = peak_group_original_data.iloc[row_idx]
                        _atom, row_labeled_count = self.parse_isotope_label(
                            row["isotopeLabel"]
                        )
                        # Not a matching row
                        if row_labeled_count != labeled_count:
                            row = None
                    except IndexError:
                        # later rows are missing
                        row = None
                    # Lookup corrected abundance by compound and label
                    corrected_abundance = self.accucor_corrected_df.loc[
                        (self.accucor_corrected_df["Compound"] == peak_group_name)
                        & (
                            self.accucor_corrected_df[self.labeled_element_header]
                            == labeled_count
                        )
                    ][original_sample_name]

                    if row is None:
                        # No row for this labeled_count
                        raw_abundance = 0
                        med_mz = 0
                        med_rt = 0
                    else:
                        # We have a matching row, use it and increment row_idx
                        raw_abundance = row[original_sample_name]
                        med_mz = row["medMz"]
                        med_rt = row["medRt"]
                        row_idx = row_idx + 1
                    print(
                        f"\t\tInserting peak data for {peak_group_name}:label-{labeled_count} "
                        f"for sample {original_sample_name}"
                    )
                    peak_data = PeakData(
                        peak_group=peak_group,
                        labeled_element=self.labeled_element,
                        labeled_count=labeled_count,
                        raw_abundance=raw_abundance,
                        corrected_abundance=corrected_abundance,
                        med_mz=med_mz,
                        med_rt=med_rt,
                    )

                    peak_data.full_clean()
                    peak_data.save()

        assert not self.debug, "Debugging..."

    @classmethod
    def parse_isotope_label(cls, label):
        """
        Parse El-Maven style isotope label string
        e.g. C12 PARENT, C13-label-1, C13-label-2
        Returns tuple with "Atom" and "Label count"
        e.g. ("C12", 0), ("C13", 1), ("C13", 2)
        """
        atom, count = (None, None)
        if "PARENT" in label:
            atom = label.split(" ")[0]
            count = 0
        else:
            atom, count = label.split("-label-")
        return (atom, int(count))

    def load_accucor_data(self):

        with transaction.atomic():
            self.validate_data()
            self.load_data()
