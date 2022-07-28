from collections import namedtuple
from datetime import timedelta

import dateutil.parser  # type: ignore
import pandas as pd
from django.conf import settings

from DataRepo.models import Animal, Infusate, Protocol, Sample, Study, Tissue
from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import (
    disable_autoupdates,
    enable_autoupdates,
    perform_buffered_updates,
)
from DataRepo.models.utilities import get_researchers, value_from_choices_label
from DataRepo.utils.exceptions import (
    HeaderConfigError,
    HeaderError,
    RequiredValueError,
    ResearcherError,
    ValidationDatabaseSetupError,
)
from DataRepo.utils.infusate_name_parser import parse_infusate_name

CONCENTRATIONS_DELIMITER = ";"


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
            "INFUSATE",
            "ANIMAL_INFUSION_RATE",
            "TRACER_CONCENTRATIONS",
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
        INFUSATE="Infusate",
        ANIMAL_INFUSION_RATE="Infusion Rate",
        TRACER_CONCENTRATIONS="Tracer Concentrations",
    )

    def __init__(
        self,
        sample_table_headers=DefaultSampleTableHeaders,
        database=None,
        validate=False,
    ):
        self.headers = sample_table_headers
        self.blank = ""
        self.researcher_errors = []
        self.header_errors = []
        self.missing_headers = []
        self.debug = False
        self.db = settings.TRACEBASE_DB
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

        disable_autoupdates()
        disable_caching_updates()
        animals_to_uncache = []

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
                # Assuming that both the default and validation databases each have all current tissues
                tissue = Tissue.objects.using(self.db).get(name=tissue_name)
            except Tissue.DoesNotExist as e:
                raise Tissue.DoesNotExist(
                    f"Invalid tissue type specified: '{tissue_name}'. Not found in database {self.db}."
                ) from e

            # Study
            study_exists = False
            created = False
            name = self.getRowVal(row, self.headers.STUDY_NAME)
            if name is not None:
                study, created = Study.objects.using(self.db).get_or_create(name=name)
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
                    study.save(using=self.db)
                except Exception as e:
                    print(f"Error saving record: Study:{study}")
                    raise (e)

            # Animal
            created = False
            name = self.getRowVal(row, self.headers.ANIMAL_NAME)
            if name is not None:
                animal, created = Animal.objects.using(self.db).get_or_create(name=name)
                if created and animal.caches_exist():
                    animals_to_uncache.append(animal)
                elif created and settings.DEBUG:
                    print(f"No cache exists for animal {animal.id}")
            """
            We do this here, and not in the "created" block below, in case the
            researcher is creating a new study from previously-loaded animals
            """
            if study_exists and animal not in study.animals.all():
                # Save the animal to the supplied database, because study may be in a different database
                animal.save(using=self.db)
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
                    animal.age = timedelta(weeks=int(age))
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
                                database=self.db,
                            )
                            action = "Found"
                            feedback = f"{animal.treatment.category} protocol "
                            f"{animal.treatment.id} '{animal.treatment.name}'"
                            if created:
                                action = "Created"
                                feedback += f" '{animal.treatment.description}'"
                            print(f"{action} {feedback}")

                # Get the tracer concentrations
                tracer_concs_str = self.getRowVal(
                    row, self.headers.TRACER_CONCENTRATIONS, hdr_required=False
                )
                try:
                    if tracer_concs_str is None:
                        tracer_concs = None
                    else:
                        # Not sure how the split results in a float, but my guess is that it's something in excel, thus
                        # if there do exist comma-delimited items, this should actually work
                        tracer_concs = [
                            float(x.strip())
                            for x in tracer_concs_str.split(CONCENTRATIONS_DELIMITER)
                        ]
                except AttributeError as ae:
                    if "object has no attribute 'split'" in str(ae):
                        tracer_concs = [float(tracer_concs_str)]
                    else:
                        raise ae

                # Create the infusate record and all its tracers and labels, then link to it from the animal
                infusate_str = self.getRowVal(
                    row, self.headers.INFUSATE, hdr_required=True
                )
                infusate = None
                if infusate_str is not None:
                    if tracer_concs is None:
                        raise NoConcentrations(
                            f"{self.headers.INFUSATE} [{infusate_str}] supplied without "
                            f"{self.headers.TRACER_CONCENTRATIONS}."
                        )
                    infusate_data_object = parse_infusate_name(
                        infusate_str, tracer_concs
                    )
                    (infusate, created) = Infusate.objects.get_or_create_infusate(
                        infusate_data_object
                    )
                    animal.infusate = infusate

                rate_required = infusate is not None

                # Get the infusion rate
                tir = self.getRowVal(
                    row, self.headers.ANIMAL_INFUSION_RATE, hdr_required=rate_required
                )
                if tir is not None:
                    animal.infusion_rate = tir

                try:
                    animal.full_clean()
                    animal.save(using=self.db)
                except Exception as e:
                    print(f"Error saving record: Animal:{animal}")
                    raise (e)

            # Sample
            sample_name = self.getRowVal(row, self.headers.SAMPLE_NAME)
            if sample_name is not None:
                try:
                    # Assuming that duplicates among the submission are handled in the checking of the file, so we must
                    # check against the tracebase database for pre-existing sample name duplicates
                    sample = Sample.objects.using(settings.TRACEBASE_DB).get(
                        name=sample_name
                    )
                    print(f"SKIPPING existing record: Sample:{sample_name}")
                except Sample.DoesNotExist:
                    # This loop encounters this code for the same sample multiple times, so during user data validation
                    # and when getting here because the sample doesn't exist in the tracebase-proper database, we still
                    # have to check the validation database before trying to create the sample so that we don't run
                    # afoul of the unique constraint
                    # In the case of actually just loading the tracebase database, this will result in a duplicate
                    # check & exception, but otherwise, it would result in dealing with duplicate code
                    try:
                        sample = Sample.objects.using(self.db).get(name=sample_name)
                    except Sample.DoesNotExist:
                        print(f"Creating new record: Sample:{sample_name}")
                        researcher = self.getRowVal(row, self.headers.SAMPLE_RESEARCHER)
                        tc = self.getRowVal(row, self.headers.TIME_COLLECTED)
                        sample_args = {
                            "name": sample_name,
                            "animal": animal,
                            "tissue": tissue,
                        }
                        if researcher is not None:
                            sample_args["researcher"] = researcher
                        if tc is not None:
                            sample_args["time_collected"] = timedelta(minutes=float(tc))
                        sample = Sample(**sample_args)
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
                            if self.db == settings.DEFAULT_DB:
                                sample.full_clean()
                            sample.save(using=self.db)
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

        enable_caching_updates()
        if debug:
            enable_autoupdates()

        # Throw an exception in debug mode to abort the load
        assert not debug, "Debugging..."

        perform_buffered_updates()
        enable_autoupdates()

        if settings.DEBUG:
            print("Expiring affected caches...")
        for animal in animals_to_uncache:
            if settings.DEBUG:
                print(f"Expiring animal {animal.id}'s cache")
            animal.delete_related_caches()
        if settings.DEBUG:
            print("Expiring done.")

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


class NoConcentrations(Exception):
    pass
