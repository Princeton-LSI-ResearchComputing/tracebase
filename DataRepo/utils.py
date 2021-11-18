import collections
import json
import re
from collections import namedtuple
from datetime import datetime, timedelta

import dateutil.parser
import pandas as pd
from django.core.exceptions import ValidationError  # type: ignore
from django.db import transaction
from pandas.errors import EmptyDataError

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
    get_researchers,
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
            "ANIMAL_TREATMENT",
            "TRACER_COMPOUND_NAME",
            "TRACER_LABELED_ELEMENT",
            "TRACER_LABELED_COUNT",
            "TRACER_INFUSION_RATE",
            "TRACER_INFUSION_CONCENTRATION",
        ],
    )

    DefaultSampleTableHeaders = SampleTableHeaders(
        SAMPLE_NAME="Sample Name",
        SAMPLE_DATE="Date Collected",
        SAMPLE_RESEARCHER="Researcher Name",
        TISSUE_NAME="Tissue",
        TIME_COLLECTED="Collection Time",
        STUDY_NAME="Study Name",
        STUDY_DESCRIPTION="Study Description",
        ANIMAL_NAME="Animal ID",
        ANIMAL_WEIGHT="Animal Body Weight",
        ANIMAL_AGE="Age",
        ANIMAL_SEX="Sex",
        ANIMAL_GENOTYPE="Animal Genotype",
        ANIMAL_FEEDING_STATUS="Feeding Status",
        ANIMAL_DIET="Diet",
        ANIMAL_TREATMENT="Animal Treatment",
        TRACER_COMPOUND_NAME="Tracer Compound",
        TRACER_LABELED_ELEMENT="Tracer Labeled Element",
        TRACER_LABELED_COUNT="Tracer Label Atom Count",
        TRACER_INFUSION_RATE="Infusion Rate",
        TRACER_INFUSION_CONCENTRATION="Tracer Concentration",
    )

    def __init__(self, sample_table_headers=DefaultSampleTableHeaders):
        self.headers = sample_table_headers
        self.blank = ""
        self.researcher_errors = []
        self.header_errors = []
        self.missing_headers = []
        self.debug = False

    def validate_sample_table(self, data, skip_researcher_check=False):
        """
        Validates the data in the input file, unless the check is indicated to be skipped.
        """
        if skip_researcher_check is False:
            self.validate_researcher(data)

    def validate_researcher(self, data):
        """
        Gets a unique list of researchers from the file being loaded and ensures the researchers already exist in the
        database
        """
        db_researchers = get_researchers()
        if len(db_researchers) != 0:
            print("Checking researchers...")
            input_researchers = []
            new_researchers = []
            for row in data:
                researcher = self.getRowVal(row, self.headers.SAMPLE_RESEARCHER)
                if researcher is not None and researcher not in input_researchers:
                    input_researchers.append(researcher)
                    if researcher not in db_researchers:
                        new_researchers.append(researcher)
            if len(new_researchers) > 0:
                error = {
                    "input_researchers": input_researchers,
                    "new_researchers": new_researchers,
                    "db_researchers": db_researchers,
                }
                self.researcher_errors.append(error)

    def load_sample_table(self, data, skip_researcher_check=False, debug=False):
        self.debug = debug

        # Create a list to hold the csv reader data so that iterations from validating doesn't leave the csv reader
        # empty/at-the-end upon the import loop
        sample_table_data = list(data)

        self.validate_sample_table(
            sample_table_data, skip_researcher_check=skip_researcher_check
        )

        for row in sample_table_data:

            tissue_name = self.getRowVal(
                row,
                self.headers.TISSUE_NAME,
                hdr_required=True,
                val_required=False,  # Empties handled below due to blanks
            )

            # Skip BLANK rows
            if tissue_name == self.blank:
                print("Skipping row: Tissue field is empty, assuming blank sample")
                continue

            # Tissue
            try:
                tissue = Tissue.objects.get(name=tissue_name)
            except Tissue.DoesNotExist as e:
                raise Tissue.DoesNotExist(
                    f"Invalid tissue type specified: '{tissue_name}'"
                ) from e

            # Study
            study_exists = False
            created = False
            name = self.getRowVal(row, self.headers.STUDY_NAME)
            if name is not None:
                study, created = Study.objects.get_or_create(name=name)
                study_exists = True
            if created:
                description = self.getRowVal(
                    row,
                    self.headers.STUDY_DESCRIPTION,
                    hdr_required=False,
                    val_required=False,
                )
                if description is not None:
                    study.description = description
                print(f"Created new record: Study:{study}")
                try:
                    study.full_clean()
                    study.save()
                except Exception as e:
                    print(f"Error saving record: Study:{study}")
                    raise (e)

            # Animal
            created = False
            name = self.getRowVal(row, self.headers.ANIMAL_NAME)
            if name is not None:
                animal, created = Animal.objects.get_or_create(name=name)
            """
            We do this here, and not in the "created" block below, in case the
            researcher is creating a new study from previously-loaded animals
            """
            if study_exists and animal not in study.animals.all():
                print("Adding animal to the study...")
                study.animals.add(animal)

            """
            created block contains all the animal attribute updates if the
            animal was newly created
            """
            if created:
                print(f"Created new record: Animal:{animal}")
                genotype = self.getRowVal(
                    row, self.headers.ANIMAL_GENOTYPE, hdr_required=False
                )
                if genotype is not None:
                    animal.genotype = genotype
                weight = self.getRowVal(
                    row, self.headers.ANIMAL_WEIGHT, hdr_required=False
                )
                if weight is not None:
                    animal.body_weight = weight
                feedstatus = self.getRowVal(
                    row, self.headers.ANIMAL_FEEDING_STATUS, hdr_required=False
                )
                if feedstatus is not None:
                    animal.feeding_status = feedstatus
                age = self.getRowVal(row, self.headers.ANIMAL_AGE, hdr_required=False)
                if age is not None:
                    animal.age = age
                diet = self.getRowVal(row, self.headers.ANIMAL_DIET, hdr_required=False)
                if diet is not None:
                    animal.diet = diet
                animal_sex_string = self.getRowVal(
                    row, self.headers.ANIMAL_SEX, hdr_required=False
                )
                if animal_sex_string is not None:
                    if animal_sex_string in animal.SEX_CHOICES:
                        animal_sex = animal_sex_string
                    else:
                        animal_sex = value_from_choices_label(
                            animal_sex_string, animal.SEX_CHOICES
                        )
                    animal.sex = animal_sex
                treatment = self.getRowVal(
                    row,
                    self.headers.ANIMAL_TREATMENT,
                    hdr_required=False,
                    val_required=False,
                )
                if treatment is None:
                    print("No animal treatment found.")
                else:
                    # Animal Treatments are optional protocols
                    protocol_input = treatment
                    try:
                        assert protocol_input != ""
                        assert protocol_input != pd.isnull(protocol_input)
                    except AssertionError:
                        print("No animal treatments with empty/null values.")
                    else:
                        category = Protocol.ANIMAL_TREATMENT
                        researcher = self.getRowVal(row, self.headers.SAMPLE_RESEARCHER)
                        if researcher is not None:
                            print(
                                f"Finding or inserting {category} protocol for '{protocol_input}'..."
                            )
                            (
                                animal.treatment,
                                created,
                            ) = Protocol.retrieve_or_create_protocol(
                                protocol_input,
                                category,
                                f"For protocol's full text, please consult {researcher}.",
                            )
                            action = "Found"
                            feedback = f"{animal.treatment.category} protocol "
                            f"{animal.treatment.id} '{animal.treatment.name}'"
                            if created:
                                action = "Created"
                                feedback += f" '{animal.treatment.description}'"
                            print(f"{action} {feedback}")

                tracer_compound_name = self.getRowVal(
                    row, self.headers.TRACER_COMPOUND_NAME, hdr_required=False
                )
                if tracer_compound_name is not None:
                    try:
                        tracer_compound = Compound.objects.get(
                            name=tracer_compound_name
                        )
                        animal.tracer_compound = tracer_compound
                    except Compound.DoesNotExist as e:
                        print(
                            f"ERROR: {self.headers.TRACER_COMPOUND_NAME} not found: Compound:{tracer_compound_name}"
                        )
                        raise (e)
                tracer_labeled_elem = self.getRowVal(
                    row, self.headers.TRACER_LABELED_ELEMENT, hdr_required=False
                )
                if tracer_labeled_elem is not None:
                    tracer_labeled_atom = value_from_choices_label(
                        tracer_labeled_elem,
                        animal.TRACER_LABELED_ELEMENT_CHOICES,
                    )
                    animal.tracer_labeled_atom = tracer_labeled_atom
                tlc = self.getRowVal(
                    row, self.headers.TRACER_LABELED_COUNT, hdr_required=False
                )
                if tlc is not None:
                    animal.tracer_labeled_count = int(tlc)
                tir = self.getRowVal(
                    row, self.headers.TRACER_INFUSION_RATE, hdr_required=False
                )
                if tir is not None:
                    animal.tracer_infusion_rate = tir
                tic = self.getRowVal(
                    row, self.headers.TRACER_INFUSION_CONCENTRATION, hdr_required=False
                )
                if tic is not None:
                    animal.tracer_infusion_concentration = tic
                try:
                    animal.full_clean()
                    animal.save()
                except Exception as e:
                    print(f"Error saving record: Animal:{animal}")
                    raise (e)

            # Sample
            sample_name = self.getRowVal(row, self.headers.SAMPLE_NAME)
            if sample_name is not None:
                try:
                    sample = Sample.objects.get(name=sample_name)
                    print(f"SKIPPING existing record: Sample:{sample_name}")
                except Sample.DoesNotExist:
                    print(f"Creating new record: Sample:{sample_name}")
                    researcher = self.getRowVal(row, self.headers.SAMPLE_RESEARCHER)
                    tc = self.getRowVal(row, self.headers.TIME_COLLECTED)
                    if researcher is not None and tc is not None:
                        sample = Sample(
                            name=sample_name,
                            researcher=researcher,
                            time_collected=timedelta(minutes=float(tc)),
                            animal=animal,
                            tissue=tissue,
                        )
                    sd = self.getRowVal(
                        row, self.headers.SAMPLE_DATE, hdr_required=False
                    )
                    if sd is not None:
                        sample_date_value = sd
                        # Pandas may have already parsed the date
                        try:
                            sample_date = dateutil.parser.parse(sample_date_value)
                        except TypeError:
                            sample_date = sample_date_value
                        sample.date = sample_date
                    try:
                        sample.full_clean()
                        sample.save()
                    except Exception as e:
                        print(f"Error saving record: Sample:{sample}")
                        raise (e)

        if len(self.missing_headers) > 0:
            raise (
                HeaderError(
                    f"The following column headers were missing: {', '.join(self.missing_headers)}",
                    self.missing_headers,
                )
            )

        # Check researchers last so that other errors can be dealt with by users during validation
        # Users cannot resolve new researcher errors if they really are new
        if len(self.researcher_errors) > 0:
            nl = "\n"
            all_researcher_error_strs = []
            for ere in self.researcher_errors:
                err_msg = (
                    f"{len(ere['new_researchers'])} researchers from the sample file: ["
                    f"{','.join(sorted(ere['new_researchers']))}] out of {len(ere['input_researchers'])} researchers "
                    f"do not exist in the database.  Please ensure they are not variants of existing researchers in "
                    f"the database:{nl}{nl.join(sorted(ere['db_researchers']))}{nl}If all researchers are valid new "
                    f"researchers, add --skip-researcher-check to your command."
                )
                all_researcher_error_strs.append(err_msg)
            raise ResearcherError("\n".join(all_researcher_error_strs))

        # Throw an exception in debug mode to abort the load
        assert not debug, "Debugging..."

    def getRowVal(self, row, header, hdr_required=True, val_required=True):
        """
        Gets a value from the row, indexed by the column header.  If the header is not required but the header key is
        defined, a lookup will happen, but a missing header will only be recorded if the header is required.
        """
        val = None
        try:
            # If required, always do the lookup.  If not required, only look up the value if the header is defined
            if hdr_required or header:
                val = row[header]
            elif hdr_required:
                raise HeaderConfigError(
                    "Header required, but no header string supplied."
                )
            if header and val_required and (val == "" or val is None):
                raise RequiredValueError(
                    f"Values in column {header} are required, but some found missing"
                )
        except KeyError:
            if hdr_required and header not in self.missing_headers:
                self.missing_headers.append(header)
        return val


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

    def validate_data(self):
        """
        basic sanity/integrity checks for the data inputs
        """

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

    def validate_researcher(self):
        researchers = get_researchers()
        nl = "\n"
        if self.new_researcher is True:
            err_msg = (
                f"Researcher [{self.researcher}] exists.  --new-researcher cannot be used for existing researchers.  "
                f"Current researchers are:{nl}{nl.join(sorted(researchers))}"
            )
            assert self.researcher not in researchers, err_msg
        elif len(researchers) != 0:
            err_msg = (
                f"Researcher [{self.researcher}] does not exist.  Please either choose from the following "
                f"researchers, or if this is a new researcher, add --new-researcher to your command (leaving "
                f"`--researcher {self.researcher}` as-is).  Current researchers are:{nl}{nl.join(sorted(researchers))}"
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
        self.accucor_corrected_df["Compound"] = self.accucor_corrected_df[
            "Compound"
        ].str.strip()

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
        corr_iter_err = ""
        for k, v in corr_iter.items():
            if k.startswith("Unnamed: "):
                raise Exception(
                    "Sample columns missing headers found in the Corrected data sheet. You have "
                    + str(len(self.accucor_corrected_df.columns))
                    + " columns."
                )
            corr_iter_err += '"' + str(k) + '":"' + str(v) + '",'

        if original_samples:
            # Make sure all sample columns have names
            orig_iter = collections.Counter(original_samples)
            orig_iter_err = ""
            for k, v in orig_iter.items():
                if k.startswith("Unnamed: "):
                    raise EmptyDataError(
                        "Sample columns missing headers found in the Original data sheet. You have "
                        + str(len(self.accucor_original_df.columns))
                        + " columns. Be sure to delete any unused columns."
                    )
                orig_iter_err += '"' + str(k) + '":' + str(v) + '",'

            # Make sure that the sheets have the same number of sample columns
            err_msg = (
                "Number of samples in the original and corrected sheets differ."
                f"Original: [{orig_iter_err}] Corrected: [{corr_iter_err}]."
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

        # do it again for the corrected
        dupe_dict = {}
        for index, row in self.accucor_corrected_df[
            self.accucor_corrected_df.duplicated(
                subset=["Compound", self.labeled_element_header], keep=False
            )
        ].iterrows():
            dupe_key = row["Compound"] + " & " + row[self.labeled_element_header]
            if dupe_key not in dupe_dict:
                dupe_dict[dupe_key] = str(index + 1)
            else:
                dupe_dict[dupe_key] += "," + str(index + 1)

        err_msg = (
            f"The following duplicate Compound/Label pairs were found in the corrected data: ["
            f"{'; '.join(list(map(lambda c: c + ' on rows: ' + dupe_dict[c], dupe_dict.keys())))}]"
        )
        assert len(dupe_dict.keys()) == 0, err_msg

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
                # cached it for later
                self.sample_dict[sample_name] = Sample.objects.get(
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
            "adductName",
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
        reference_dataframe = self.accucor_corrected_df
        peak_group_name_key = "Compound"
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
                    except ValidationError as e:
                        missing_compounds += 1
                        print(
                            f"Could not uniquely identify compound using {compound_input}. {e}"
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
        )
        action = "Found"
        feedback = f"{self.protocol.category} protocol {self.protocol.id} '{self.protocol.name}'"
        if created:
            action = "Created"
            feedback += f" '{self.protocol.description}'"
        print(f"{action} {feedback}")

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
        for sample_name in self.sample_dict.keys():

            # each msrun/sample has its own set of peak groups
            inserted_peak_group_dict = {}

            print(f"Inserting msrun for {sample_name}")
            msrun = MSRun(
                date=self.date,
                researcher=self.researcher,
                protocol=self.protocol,
                sample=self.sample_dict[sample_name],
            )
            msrun.full_clean()
            msrun.save()
            self.sample_run_dict[sample_name] = msrun

            # Create all PeakGroups
            for index, row in self.accucor_corrected_df.iterrows():
                labeled_count = row[self.labeled_element_header]
                if labeled_count == 0:

                    """
                    Here we insert PeakGroup, by name (only once per file).
                    NOTE: if the C12 PARENT/0-Labeled row encountered has any
                    issues (for example, a null formula), then this block will
                    fail
                    """

                    peak_group_name = row["Compound"]
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

                if self.accucor_original_df is not None:
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
                        ][sample_name]

                        if row is None:
                            # No row for this labeled_count
                            raw_abundance = 0
                            med_mz = 0
                            med_rt = 0
                        else:
                            # We have a matching row, use it and increment row_idx
                            raw_abundance = row[sample_name]
                            med_mz = row["medMz"]
                            med_rt = row["medRt"]
                            row_idx = row_idx + 1

                        print(
                            f"\t\tInserting peak data for {peak_group_name}:label-{labeled_count} "
                            f"for sample {sample_name}"
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

                else:
                    peak_group_corrected_df = self.accucor_corrected_df[
                        self.accucor_corrected_df["Compound"] == peak_group_name
                    ]

                    for _index, row in peak_group_corrected_df.iterrows():

                        corrected_abundance_for_sample = row[sample_name]
                        labeled_count = row[self.labeled_element_header]
                        # No original dataframe, no raw_abundance, med_mz, or med_rt
                        raw_abundance = None
                        med_mz = None
                        med_rt = None

                        print(
                            f"\t\tInserting peak data for {peak_group_name}:label-{labeled_count} "
                            f"for sample {sample_name}"
                        )
                        peak_data = PeakData(
                            peak_group=peak_group,
                            labeled_element=self.labeled_element,
                            labeled_count=labeled_count,
                            raw_abundance=raw_abundance,
                            corrected_abundance=corrected_abundance_for_sample,
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


class CompoundsLoader:
    """
    Load the Compound and CompoundSynonym tables
    """

    # Define the dataframe key names and requirements
    KEY_COMPOUND_NAME = "Compound"
    KEY_HMDB = "HMDB ID"
    KEY_FORMULA = "Formula"
    KEY_SYNONYMS = "Synonyms"
    REQUIRED_KEYS = [KEY_COMPOUND_NAME, KEY_FORMULA, KEY_HMDB, KEY_SYNONYMS]

    def __init__(
        self, compounds_df, synonym_separator=";", validate_only=False, verbosity=0
    ):
        self.compounds_df = compounds_df
        self.synonym_separator = synonym_separator
        self.validate_only = validate_only
        self.validation_debug_messages = []
        self.validation_warning_messages = []
        self.validation_error_messages = []
        self.summary_messages = []
        self.validated_new_compounds_for_insertion = []
        self.verbosity = verbosity
        self.missing_headers = []

        """
        strip any leading and trailing spaces from the headers and some
        columns, just to normalize
        """
        if self.compounds_df is not None:
            self.compounds_df.rename(columns=lambda x: x.strip())
            self.check_required_headers()
            for col in (
                self.KEY_COMPOUND_NAME,
                self.KEY_FORMULA,
                self.KEY_HMDB,
                self.KEY_SYNONYMS,
            ):
                self.compounds_df[col] = self.compounds_df[col].str.strip()

    def validate_data(self):
        # validate the compounds dataframe
        self.check_for_duplicates(self.KEY_COMPOUND_NAME)
        self.check_for_duplicates(self.KEY_HMDB)

        if self.compounds_df is not None:
            for index, row in self.compounds_df.iterrows():
                # capture compound attributes and synonyms
                compound = self.find_compound_for_row(row)
                if compound is None:
                    # data does not exist in database; record for future insertion
                    new_compound = Compound(
                        name=row[self.KEY_COMPOUND_NAME],
                        formula=row[self.KEY_FORMULA],
                        hmdb_id=row[self.KEY_HMDB],
                    )
                    new_compound.full_clean()
                    self.validated_new_compounds_for_insertion.append(new_compound)

    def check_required_headers(self):
        for header in self.REQUIRED_KEYS:
            if header not in self.compounds_df.columns:
                self.missing_headers.append(header)
                err_msg = f"Could not find the required header '{header}."
                self.validation_error_messages.append(err_msg)
        if len(self.missing_headers) > 0:
            raise (
                HeaderError(
                    f"The following column headers were missing: {', '.join(self.missing_headers)}",
                    self.missing_headers,
                )
            )

    def check_for_duplicates(self, column_header):

        dupe_dict = {}
        for index, row in self.compounds_df[
            self.compounds_df.duplicated(subset=[column_header], keep=False)
        ].iterrows():
            dupe_key = row[column_header]
            if dupe_key not in dupe_dict:
                dupe_dict[dupe_key] = str(index + 1)
            else:
                dupe_dict[dupe_key] += "," + str(index + 1)

        if len(dupe_dict.keys()) != 0:
            err_msg = (
                f"The following duplicate {column_header} were found in the original data: ["
                f"{'; '.join(list(map(lambda c: c + ' on rows: ' + dupe_dict[c], dupe_dict.keys())))}]"
            )

            self.validation_error_messages.append(err_msg)

    def find_compound_for_row(self, row):
        """
        Find single Compound record matching data from the input row.

        Searches compound records using HMDB ID and name. Appends a warning to
        `validation_warning_messages` if HMDB ID is not found.  Searches
        compound records using all synonyms.  If the queries return multiple
        distinct scompounds, an `AmbiguousCompoundDefinitionError` is raised.

        Args:
            row (Series): Pandas Series representing a potential Compound
                record

        Returns:
            compound: A single compound record matching the HMDB, name, and
                synonym records in the input row

        Raises:
            AmbiguousCompoundDefinitionError: Multiple compounds were found
        """
        found_compound = None
        hmdb_compound = None
        named_compound = None
        all_found_compounds = []
        # start with the HMDB_ID
        hmdb_id = row[self.KEY_HMDB]
        name = row[self.KEY_COMPOUND_NAME]
        synonyms_string = row[self.KEY_SYNONYMS]
        try:
            hmdb_compound = Compound.objects.get(hmdb_id=hmdb_id)
            # preferred method of "finding because it is not a potential synonym
            found_compound = hmdb_compound
            self.validation_debug_messages.append(
                f"Found {found_compound.name} using HMDB ID {hmdb_id}"
            )
            all_found_compounds.append(hmdb_compound)
        except Compound.DoesNotExist:
            # must be a new compound, or a data inconsistency?
            msg = f"Database lacks HMBD ID {hmdb_id}"
            self.validation_warning_messages.append(msg)

        try:
            named_compound = Compound.objects.get(name=name)
            if hmdb_compound is None:
                found_compound = named_compound
                self.validation_debug_messages.append(
                    f"Found {found_compound.name} using Compound name {name}"
                )
                all_found_compounds.append(named_compound)
        except Compound.DoesNotExist:
            # must be a new compound, or a data inconsistency?
            msg = f"Database lacks named compound {name}"
            self.validation_warning_messages.append(msg)

        # if we have any inconsistency between these two queries above, either the
        # file or the database is "wrong"
        if hmdb_compound != named_compound:
            err_msg = f"ERROR: Data inconsistency. File input Compound={name} HMDB ID={hmdb_id} "
            if hmdb_compound is None:
                err_msg += "did not match a database record using the file's HMDB ID, "
            else:
                err_msg += f"matches a database compound (by file's HMDB ID) of Compound={hmdb_compound.name} "
                err_msg += f"HMDB ID={hmdb_compound.hmdb_id}, "

            if named_compound is None:
                err_msg += "but did not match a named database record using the file's Compound "
            else:
                err_msg += f"but matches a database compound (by file's Compound) of Compound={named_compound.name} "
                err_msg += f"HMDB ID={named_compound.hmdb_id}"

            self.validation_error_messages.append(err_msg)

        if hmdb_compound is None and named_compound is None:
            self.validation_debug_messages.append(f"Could not find {hmdb_id}")
            # attempt a query by either name or synonym(s)
            names = [name]
            if synonyms_string is not None and synonyms_string != "":
                synonyms = self.parse_synonyms(synonyms_string)
                names.extend(synonyms)
            for name in names:
                alt_name_compound = Compound.compound_matching_name_or_synonym(name)
                if alt_name_compound is not None:
                    self.validation_debug_messages.append(
                        f"Found {alt_name_compound.name} using {name}"
                    )
                    found_compound = alt_name_compound
                    if found_compound not in all_found_compounds:
                        all_found_compounds.append(alt_name_compound)
                else:
                    self.validation_debug_messages.append(
                        f"Could not find {name} in names or synonyms"
                    )

        if len(all_found_compounds) > 1:
            err_msg = f"Retrieved multiple ({len(all_found_compounds)}) "
            err_msg += f"distinct compounds using names {names}"
            raise AmbiguousCompoundDefinitionError(err_msg)

        return found_compound

    def parse_synonyms(self, synonyms_string: str) -> list:
        synonyms = [
            synonym.strip() for synonym in synonyms_string.split(self.synonym_separator)
        ]
        return synonyms

    def validation_report(self):

        if self.verbosity > 1:
            for msg in self.validation_debug_messages:
                print(msg)

        if self.verbosity > 0:
            for msg in self.validation_warning_messages:
                print(msg)

        # report on what errors were discovered by the loader
        for err_msg in self.validation_error_messages:
            print(err_msg)

        # report on what what work would be done by the loader
        print(
            f"Work to be done: {len(self.validated_new_compounds_for_insertion)} "
            "new compounds will be inserted and all names/synonyms from the file "
            "will either be found or inserted."
        )

        if len(self.validated_new_compounds_for_insertion) > 0:
            print("New compounds to be inserted:")
            for compound in self.validated_new_compounds_for_insertion:
                print(compound)

        if len(self.validation_error_messages) > 0:
            print("No work will be performed; ERRORS must be fixed, first.")

    def load_validated_compounds(self):
        count = 0
        for compound in self.validated_new_compounds_for_insertion:
            compound.save()
            count += 1
        self.summary_messages.append(
            f"{count} compound(s) inserted, with default synonyms."
        )

    def load_synonyms(self):
        # if we are here, every line should either have pre-existed, or have
        # been newly inserted.
        count = 0
        for index, row in self.compounds_df.iterrows():
            # we will use the HMDB ID to retrieve
            hmdb_id = row[self.KEY_HMDB]
            # this name might always be a synonym
            compound_name_from_file = row[self.KEY_COMPOUND_NAME]
            hmdb_compound = Compound.objects.get(hmdb_id=hmdb_id)
            synonyms_string = row[self.KEY_SYNONYMS]
            synonyms = self.parse_synonyms(synonyms_string)
            if hmdb_compound.name != compound_name_from_file:
                synonyms.append(compound_name_from_file)
            for synonym in synonyms:
                (compound_synonym, created) = hmdb_compound.get_or_create_synonym(
                    synonym
                )
                if created:
                    count += 1
        self.summary_messages.append(f"{count} additional synonym(s) inserted.")

    def load_data(self):
        # will not load data without validating first
        self.validate_data()
        self.validation_report()

        if self.validate_only:
            # don't load if only validation was requested
            return

        if len(self.validation_error_messages) == 0:
            self.load_validated_compounds()
            self.load_synonyms()
            for msg in self.summary_messages:
                print(msg)
        else:
            print("These ERRORS must be fixed before doing any work with the database:")
            # report on what errors were discovered by the loader
            for err_msg in self.validation_error_messages:
                print(err_msg)


class HeaderError(Exception):
    def __init__(self, message, headers):
        super().__init__(message)
        self.header_list = headers


class HeaderConfigError(Exception):
    pass


class RequiredValueError(Exception):
    pass


class ResearcherError(Exception):
    pass


class MissingSamplesError(Exception):
    def __init__(self, message, samples):
        super().__init__(message)
        self.sample_list = samples


class AmbiguousCompoundDefinitionError(Exception):
    pass


class QuerysetToPandasDataFrame:
    """
    convert several querysets to Pandas DataFrames, then create additional
    DataFrames for study or animal based summary data
    """

    @staticmethod
    def qs_to_df(qs, qry_to_df_fields):
        """
        convert a queryset to a Pandas DataFrame using defined field names.
        qry_to_df_fields is a dictionary mapping query fields to column names
        of the DataFrame
        """
        qry_fields = qry_to_df_fields.keys()
        qs1 = qs.values_list(*qry_fields)
        df_with_qry_fields = pd.DataFrame.from_records(qs1, columns=qry_fields)
        # rename columns for df
        out_df = df_with_qry_fields.rename(columns=qry_to_df_fields)
        return out_df

    @staticmethod
    def df_to_list_of_dict(df):
        """
        convert Pandas DataFrame into a list of dictionary, each item of the list
        is a dictionary converted from a row of the DataFrame (column_name:column_value)
        The output can be used directly for template rendering
        """
        # parsing the DataFrame to JSON records.
        json_records = df.to_json(orient="records", date_format="iso", date_unit="s")
        # output to a list of dictionary
        data = []
        data = json.loads(json_records)
        return data

    @classmethod
    def get_study_list_df(cls):
        """
        convert all study records to a DataFrame with defined column names
        """
        qs = Study.objects.all()
        qry_to_df_fields = {
            "id": "study_id",
            "name": "study",
            "description": "study_description",
        }
        stud_list_df = cls.qs_to_df(qs, qry_to_df_fields)
        return stud_list_df

    @classmethod
    def get_study_animal_all_df(cls):
        """
        generate a DataFrame for joining all studies and animals based on
        many-to-many relationships
        """
        qs = Study.objects.all().prefetch_related("animals")
        qry_to_df_fields = {
            "id": "study_id",
            "name": "study",
            "description": "study_description",
            "animals__id": "animal_id",
            "animals__name": "animal",
        }
        all_stud_anim_df = cls.qs_to_df(qs, qry_to_df_fields)
        return all_stud_anim_df

    @classmethod
    def get_animal_list_df(cls):
        """
        get all animal records with related fields for tracer and treatments,
        convert to a DataFrame with defined column names
        """
        qs = Animal.objects.select_related("compound", "protocol").all()
        qry_to_df_fields = {
            "id": "animal_id",
            "name": "animal",
            "tracer_compound_id": "tracer_compound_id",
            "tracer_compound__name": "tracer",
            "tracer_labeled_atom": "tracer_labeled_atom",
            "tracer_labeled_count": "tracer_labeled_count",
            "tracer_infusion_rate": "tracer_infusion_rate",
            "tracer_infusion_concentration": "tracer_infusion_concentration",
            "genotype": "genotype",
            "body_weight": "body_weight",
            "age": "age",
            "sex": "sex",
            "diet": "diet",
            "feeding_status": "feeding_status",
            "treatment_id": "treatment_id",
            "treatment__name": "treatment",
            "treatment__category": "treatment_category",
        }
        anim_list_df = cls.qs_to_df(qs, qry_to_df_fields)

        # some animal records have no treatment data, DataFrame converts the dtype from int to float
        # workaround: set treatment_id to 0 for NaN, and convert dtype back to int
        anim_list_df["treatment_id"] = (
            anim_list_df["treatment_id"].fillna(0).astype(int)
        )
        return anim_list_df

    @classmethod
    def get_study_gb_animal_df(cls):
        """
        generate a DataFrame for studies grouped by animal_id
        adding a column named study_id_name_list
        example for data format: ['1||obob_fasted']
        """
        stud_anim_df = cls.get_study_animal_all_df()

        # add a column by joining id and name for each study
        stud_anim_df["study_id_name"] = (
            stud_anim_df["study_id"]
            .astype(str)
            .str.cat(stud_anim_df["study"], sep="||")
        )

        # generate DataFrame grouped by animal_id and animal
        # columns=['animal_id', 'animal', 'studies', 'study_id_name_list']
        stud_gb_anim_df = (
            stud_anim_df.groupby(["animal_id", "animal"])
            .agg(
                studies=("study", "unique"),
                study_id_name_list=("study_id_name", "unique"),
            )
            .reset_index()
        )
        return stud_gb_anim_df

    @classmethod
    def get_animal_msrun_all_df(cls):
        """
        generate a DataFrame for all sample and MSRun records
        including animal data fields
        """
        qs = MSRun.objects.select_related().all()
        qry_to_df_fields = {
            "id": "msrun_id",
            "researcher": "msrun_owner",
            "date": "msrun_date",
            "protocol_id": "msrun_protocol_id",
            "protocol__name": "msrun_protocol",
            "sample_id": "sample_id",
            "sample__name": "sample",
            "sample__researcher": "sample_owner",
            "sample__date": "sample_date",
            "sample__time_collected": "sample_time_collected",
            "sample__tissue__id": "tissue_id",
            "sample__tissue__name": "tissue",
            "sample__animal__id": "animal_id",
            "sample__animal__name": "animal",
        }
        all_sam_msrun_df = cls.qs_to_df(qs, qry_to_df_fields)

        # format date columns
        all_sam_msrun_df["sample_date"] = pd.to_datetime(
            all_sam_msrun_df["sample_date"], format="%Y-%m-%d"
        )
        all_sam_msrun_df["sample_date"] = all_sam_msrun_df["sample_date"].dt.strftime(
            "%Y-%m-%d"
        )
        all_sam_msrun_df["msrun_date"] = pd.to_datetime(
            all_sam_msrun_df["msrun_date"], format="%Y-%m-%d"
        )
        all_sam_msrun_df["msrun_date"] = all_sam_msrun_df["msrun_date"].dt.strftime(
            "%Y-%m-%d"
        )

        # add a column for formatting sample_time_collected to minutes
        # copy first to avoid warning (SettingWithCopyWarning)
        all_sam_msrun_df = all_sam_msrun_df.copy()
        all_sam_msrun_df["collect_time_in_minutes"] = (
            all_sam_msrun_df["sample_time_collected"].dt.total_seconds() // 60
        )

        anim_list_df = cls.get_animal_list_df()
        stud_gb_anim_df = cls.get_study_gb_animal_df()

        # merge DataFrames to get animal based summary data
        all_anim_msrun_df = anim_list_df.merge(
            all_sam_msrun_df, on=["animal_id", "animal"]
        ).merge(stud_gb_anim_df, on=["animal_id", "animal"])
        # reindex with defined column names
        # re-order columns (animal, tissue, sample, MSrun, studies)
        column_names = [
            "animal_id",
            "animal",
            "tracer_compound_id",
            "tracer",
            "tracer_labeled_atom",
            "tracer_labeled_count",
            "tracer_infusion_rate",
            "tracer_infusion_concentration",
            "genotype",
            "body_weight",
            "age",
            "sex",
            "diet",
            "feeding_status",
            "treatment_id",
            "treatment",
            "treatment_category",
            "tissue_id",
            "tissue",
            "sample_id",
            "sample",
            "sample_owner",
            "sample_date",
            "sample_time_collected",
            "collect_time_in_minutes",
            "msrun_id",
            "msrun_owner",
            "msrun_date",
            "msrun_protocol_id",
            "msrun_protocol",
            "studies",
            "study_id_name_list",
        ]
        all_anim_msrun_df = all_anim_msrun_df.reindex(columns=column_names)
        return all_anim_msrun_df

    @classmethod
    def get_animal_list_stats_df(cls):
        """
        generate a DataFrame by adding columns to animal list, including counts
            or unique values for selected data fields grouped by an animal
        """
        anim_list_df = cls.get_animal_list_df()
        all_anim_msrun_df = cls.get_animal_msrun_all_df()
        stud_gb_anim_df = cls.get_study_gb_animal_df()

        # get unique count or values for selected fields grouped by animal_id
        anim_gb_df = (
            all_anim_msrun_df.groupby("animal_id")
            .agg(
                total_tissue=("tissue", "nunique"),
                total_sample=("sample_id", "nunique"),
                total_msrun=("msrun_id", "nunique"),
                sample_owners=("sample_owner", "unique"),
            )
            .reset_index()
        )
        # merge DataFrames to add stats and studies to each row of animal list
        anim_list_stats_df = anim_list_df.merge(anim_gb_df, on="animal_id").merge(
            stud_gb_anim_df, on=["animal_id", "animal"]
        )

        # reindex with defined column names
        column_names = [
            "animal_id",
            "animal",
            "tracer_compound_id",
            "tracer",
            "tracer_labeled_atom",
            "tracer_labeled_count",
            "tracer_infusion_rate",
            "tracer_infusion_concentration",
            "genotype",
            "body_weight",
            "age",
            "sex",
            "diet",
            "feeding_status",
            "treatment_id",
            "treatment",
            "treatment_category",
            "total_tissue",
            "total_sample",
            "total_msrun",
            "sample_owners",
            "studies",
            "study_id_name_list",
        ]
        anim_list_stats_df = anim_list_stats_df.reindex(columns=column_names)
        return anim_list_stats_df

    @classmethod
    def get_study_msrun_all_df(cls):
        """
        generate a DataFrame for study based summary data including animal, sample, and MSRun
        data fields
        """
        all_stud_anim_df = cls.get_study_animal_all_df()
        all_anim_msrun_df = cls.get_animal_msrun_all_df()

        # all_anim_msrun_df contains columns for studies, drop them
        all_anim_msrun_df1 = all_anim_msrun_df.drop(
            columns=["studies", "study_id_name_list"]
        )

        # merge DataFrames to get study based summary data
        all_stud_msrun_df = all_stud_anim_df.merge(
            all_anim_msrun_df1, on=["animal_id", "animal"]
        )

        # reindex with defined column names and column order
        column_names = [
            "study_id",
            "study",
            "study_description",
            "animal_id",
            "animal",
            "tracer_compound_id",
            "tracer",
            "tracer_labeled_atom",
            "tracer_labeled_count",
            "tracer_infusion_rate",
            "tracer_infusion_concentration",
            "genotype",
            "body_weight",
            "age",
            "sex",
            "diet",
            "feeding_status",
            "treatment_id",
            "treatment",
            "treatment_category",
            "tissue_id",
            "tissue",
            "sample_id",
            "sample",
            "sample_owner",
            "sample_date",
            "sample_time_collected",
            "collect_time_in_minutes",
            "msrun_id",
            "msrun_owner",
            "msrun_date",
            "msrun_protocol_id",
            "msrun_protocol",
        ]
        all_stud_msrun_df = all_stud_msrun_df.reindex(columns=column_names)
        return all_stud_msrun_df

    @classmethod
    def get_study_list_stats_df(cls):
        """
        generate a DataFrame to add columns to study list including counts or unique values
        for selected data fields grouped by a study
        """
        stud_list_df = cls.get_study_list_df()
        all_stud_msrun_df = cls.get_study_msrun_all_df()

        # add a column to join id and name for each tracer
        all_stud_msrun_df["tracer_id_name"] = (
            all_stud_msrun_df["tracer_compound_id"]
            .astype(str)
            .str.cat(all_stud_msrun_df["tracer"], sep="||")
        )
        # add a column to join treatment_id and treatment
        all_stud_msrun_df["treatment_id_name"] = (
            all_stud_msrun_df["treatment_id"]
            .astype(str)
            .str.cat(all_stud_msrun_df["treatment"], sep="||")
        )
        # generate a DataFrame containing stats columns grouped by study_id
        stud_gb_df = (
            all_stud_msrun_df.groupby("study_id")
            .agg(
                total_animal=("animal_id", "nunique"),
                total_tissue=("tissue", "nunique"),
                total_sample=("sample_id", "nunique"),
                total_msrun=("msrun_id", "nunique"),
                sample_owners=("sample_owner", "unique"),
                genotypes=("genotype", "unique"),
                tracer_id_name_list=("tracer_id_name", "unique"),
                treatment_id_name_list=("treatment_id_name", "unique"),
            )
            .reset_index()
        )
        # merge DataFrames to add stats to each row of study list
        stud_list_stats_df = stud_list_df.merge(stud_gb_df, on="study_id")

        # reindex with defined column names
        column_names = [
            "study_id",
            "study",
            "study_description",
            "total_animal",
            "total_tissue",
            "total_sample",
            "total_msrun",
            "sample_owners",
            "genotypes",
            "tracer_id_name_list",
            "treatment_id_name_list",
        ]
        stud_list_stats_df = stud_list_stats_df.reindex(columns=column_names)
        return stud_list_stats_df

    def get_per_study_msrun_df(self, study_id):
        """
        generate a DataFrame for summary data including animal, sample, and MSRun
        data fields for a study
        """
        all_stud_msrun_df = self.get_study_msrun_all_df()
        self.study_id = study_id
        per_stud_msrun_df = all_stud_msrun_df[all_stud_msrun_df["study_id"] == study_id]
        return per_stud_msrun_df

    def get_per_study_stat_df(self, study_id):
        """
        generate a DataFrame for summary data including animal, sample, and MSRun
        counts for a study
        """
        stud_list_stats_df = self.get_study_list_stats_df()
        self.study_id = study_id
        per_stud_stat_df = stud_list_stats_df[
            stud_list_stats_df["study_id"] == study_id
        ]
        return per_stud_stat_df

    def get_per_animal_msrun_df(self, animal_id):
        """
        generate a DataFrame for summary data including animal, sample, and MSRun
        data fields for an animal
        """
        all_anim_msrun_df = self.get_animal_msrun_all_df()
        self.animal_id = animal_id
        per_anim_msrun_df = all_anim_msrun_df[
            all_anim_msrun_df["animal_id"] == animal_id
        ]
        return per_anim_msrun_df
