import collections
import re
from collections import namedtuple
from datetime import datetime, timedelta

import dateutil.parser  # type: ignore
import pandas as pd
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

    def validate_sample_table(self, data, skip_researcher_check):
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
                    'input_researchers': input_researchers,
                    'new_researchers': new_researchers,
                    'db_researchers': db_researchers,
                }
                self.researcher_errors.append(error)

    def load_sample_table(self, data, skip_researcher_check, debug):
        self.debug = debug
        self.validate_sample_table(data, skip_researcher_check)
        for row in data:

            name = self.getRowVal(row, self.headers.TISSUE_NAME)

            # Skip BLANK rows
            if name == self.blank:
                print("Skipping row: Tissue field is empty, assuming blank sample")
                continue

            # Tissue
            created = False
            if name is not None:
                tissue, created = Tissue.objects.get_or_create(name=name)
            if created:
                print(f"Created new record: Tissue:{tissue}")
                try:
                    tissue.full_clean()
                    tissue.save()
                except Exception as e:
                    print(f"Error saving record: Tissue:{tissue}")
                    raise (e)

            # Study
            study_exists = False
            created = False
            name = self.getRowVal(row, self.headers.STUDY_NAME)
            if name is not None:
                study, created = Study.objects.get_or_create(name=name)
                study_exists = True
            if created:
                description = self.getRowVal(row, self.headers.STUDY_DESCRIPTION, hdr_required=False)
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
                genotype = self.getRowVal(row, self.headers.ANIMAL_GENOTYPE, hdr_required=False)
                if genotype is not None:
                    animal.genotype = genotype
                weight = self.getRowVal(row, self.headers.ANIMAL_WEIGHT, hdr_required=False)
                if weight is not None:
                    animal.body_weight = weight
                feedstatus = self.getRowVal(row, self.headers.ANIMAL_FEEDING_STATUS, hdr_required=False)
                if feedstatus is not None:
                    animal.feeding_status = feedstatus
                age = self.getRowVal(row, self.headers.ANIMAL_AGE, hdr_required=False)
                if age is not None:
                    animal.age = age
                diet = self.getRowVal(row, self.headers.ANIMAL_DIET, hdr_required=False)
                if diet is not None:
                    animal.diet = diet
                animal_sex_string = self.getRowVal(row, self.headers.ANIMAL_SEX, hdr_required=False)
                if animal_sex_string is not None:
                    if animal_sex_string in animal.SEX_CHOICES:
                        animal_sex = animal_sex_string
                    else:
                        animal_sex = value_from_choices_label(
                            animal_sex_string, animal.SEX_CHOICES
                        )
                    animal.sex = animal_sex
                treatment = self.getRowVal(row, self.headers.ANIMAL_TREATMENT, hdr_required=False, val_required=False)
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

                tracer_compound_name = self.getRowVal(row, self.headers.TRACER_COMPOUND_NAME, hdr_required=False)
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
                tracer_labeled_elem = self.getRowVal(row, self.headers.TRACER_LABELED_ELEMENT, hdr_required=False)
                if tracer_labeled_elem is not None:
                    tracer_labeled_atom = value_from_choices_label(
                        tracer_labeled_elem,
                        animal.TRACER_LABELED_ELEMENT_CHOICES,
                    )
                    animal.tracer_labeled_atom = tracer_labeled_atom
                tlc = self.getRowVal(row, self.headers.TRACER_LABELED_COUNT, hdr_required=False)
                if tlc is not None:
                    animal.tracer_labeled_count = int(tlc)
                tir = self.getRowVal(row, self.headers.TRACER_INFUSION_RATE, hdr_required=False)
                if tir is not None:
                    animal.tracer_infusion_rate = tir
                tic = self.getRowVal(row, self.headers.TRACER_INFUSION_CONCENTRATION, hdr_required=False)
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
                            time_collected=timedelta(
                                minutes=float(tc)
                            ),
                            animal=animal,
                            tissue=tissue,
                        )
                    sd = self.getRowVal(row, self.headers.SAMPLE_DATE, hdr_required=False)
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
            raise ResearcherError("\n".join(self.researcher_errors))

        assert not debug, "Debugging..."
    
    def getRowVal(self, row, header, hdr_required=True, val_required=True):
        """
        Gets a value from the row, indexed by the column header.  If the header is not required but the header key is
        defined, a lookup can still cause a key error and the missing header will be recorded.  If the value is not
        required, no missing header will be recorded.
        """
        val = None
        try:
            # If required, always do the lookup.  If not required, only look up the value if the header is defined
            if hdr_required or header:
                val = row[header]
        except KeyError:
            if val_required and header not in self.missing_headers:
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

        original_samples = [
            sample
            for sample in list(self.accucor_original_df)[original_minimum_sample_index:]
            if sample not in self.skip_samples
        ]
        corrected_samples = [
            sample
            for sample in list(self.accucor_corrected_df)[
                corrected_minimum_sample_index:
            ]
            if sample not in self.skip_samples
        ]

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

        # Make sure that the sheets have the same number of sample columns
        err_msg = (
            f"Number of samples in the original and corrected sheets differ. Original: [{orig_iter_err}] Corrected: "
            "[{corr_iter_err}]."
        )
        assert orig_iter == corr_iter, err_msg
        self.original_samples = original_samples

    def validate_compounds(self):
        dupe_dict = {}

        # for index, row in self.accucor_original_df.iterrows():
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
        for original_sample_name in self.original_samples:
            prefix_sample_name = f"{self.sample_name_prefix}{original_sample_name}"
            try:
                # cached it for later
                self.sample_dict[original_sample_name] = Sample.objects.get(
                    name=prefix_sample_name
                )
            except Sample.DoesNotExist:
                missing_samples.append(original_sample_name)

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
                        mapped_compound = Compound.objects.get(
                            name__iexact=compound_input
                        )
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


class HeaderError(Exception):
    def __init__(self, message, headers):
        super().__init__(message)
        self.header_list = headers


class ResearcherError(Exception):
    pass


class MissingSamplesError(Exception):
    def __init__(self, message, samples):
        super().__init__(message)
        self.sample_list = samples
