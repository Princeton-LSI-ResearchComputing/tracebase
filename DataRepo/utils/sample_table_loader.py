import traceback
from collections import defaultdict, namedtuple
from datetime import timedelta

import dateutil.parser  # type: ignore
import pandas as pd
from django.conf import settings
from django.db.utils import IntegrityError

from DataRepo.models import (
    Animal,
    AnimalLabel,
    FCirc,
    Infusate,
    Protocol,
    Sample,
    Study,
    Tissue,
)
from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import (
    clear_update_buffer,
    disable_autoupdates,
    enable_autoupdates,
    enable_buffering,
    perform_buffered_updates,
)
from DataRepo.models.researcher import get_researchers
from DataRepo.models.utilities import value_from_choices_label
from DataRepo.utils import parse_infusate_name, parse_tracer_concentrations
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    DryRun,
    HeaderConfigError,
    RequiredHeadersError,
    RequiredValuesError,
    SaveError,
    UnknownHeadersError,
    UnknownResearcherError,
    ValidationDatabaseSetupError,
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

    RequiredSampleTableHeaders = SampleTableHeaders(
        ANIMAL_NAME=True,
        SAMPLE_NAME=True,
        INFUSATE=True,
        ANIMAL_INFUSION_RATE=True,
        SAMPLE_RESEARCHER=True,
        TISSUE_NAME=True,
        TIME_COLLECTED=True,
        STUDY_NAME=True,
        SAMPLE_DATE=False,
        STUDY_DESCRIPTION=False,
        ANIMAL_WEIGHT=False,
        ANIMAL_AGE=False,
        ANIMAL_SEX=False,
        ANIMAL_GENOTYPE=False,
        ANIMAL_FEEDING_STATUS=False,
        ANIMAL_DIET=False,
        ANIMAL_TREATMENT=False,
        TRACER_CONCENTRATIONS=False,
    )

    RequiredSampleTableValues = SampleTableHeaders(
        ANIMAL_NAME=True,
        SAMPLE_NAME=True,
        INFUSATE=True,
        ANIMAL_INFUSION_RATE=True,
        SAMPLE_RESEARCHER=True,
        TIME_COLLECTED=True,
        STUDY_NAME=True,
        SAMPLE_DATE=True,
        ANIMAL_WEIGHT=True,
        ANIMAL_AGE=True,
        ANIMAL_SEX=True,
        ANIMAL_GENOTYPE=True,
        ANIMAL_FEEDING_STATUS=True,
        ANIMAL_DIET=True,
        TRACER_CONCENTRATIONS=True,
        TISSUE_NAME=False,  # Due to blank samples
        ANIMAL_TREATMENT=False,
        STUDY_DESCRIPTION=False,
    )

    def __init__(
        self,
        sample_table_headers=DefaultSampleTableHeaders,
        database=None,
        validate=False,
        verbosity=1,
        skip_researcher_check=False,
    ):
        self.headers = sample_table_headers
        self.missing_headers = []
        self.missing_values = defaultdict(list)
        self.headers_present = []
        self.animals_to_uncache = []
        self.errors = []
        self.skip_researcher_check = skip_researcher_check
        self.verbosity = verbosity
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
        self.input_researchers = []
        self.unknown_researchers = []
        self.known_researchers = get_researchers(database=self.db)

    def load_sample_table(self, data, dry_run=False):

        disable_autoupdates()
        disable_caching_updates()

        # Create a list to hold the csv reader data so that iterations from validating cleardoesn't leave the csv
        # reader empty/at-the-end upon the import loop
        sample_table_data = list(data)

        # If there is data to load
        if len(sample_table_data) > 0:
            # use the first row to check the headers
            self.check_headers(sample_table_data[0].keys())

        for rownum, row in enumerate(sample_table_data, start=1):

            tissue_rec, is_blank = self.get_tissue(rownum, row)

            if is_blank:
                continue

            infusate_rec = self.get_or_create_infusate(rownum, row)
            treatment_rec = self.get_treatment(rownum, row)
            animal_rec = self.get_or_create_animal(
                rownum, row, infusate_rec, treatment_rec
            )
            self.get_or_create_study(rownum, row, animal_rec)
            self.get_or_create_animallabel(animal_rec, infusate_rec)
            sample_rec = self.get_or_create_sample(rownum, row, animal_rec, tissue_rec)
            self.get_or_create_fcircs(infusate_rec, sample_rec)

        if (
            not self.skip_researcher_check
            and len(self.known_researchers) > 0
            and len(self.unknown_researchers) > 0
        ):
            self.errors.append(
                UnknownResearcherError(
                    self.unknown_researchers,
                    self.input_researchers,
                    self.known_researchers,
                    "the sample file",
                    "If all researchers are valid new researchers, add --skip-researcher-check to your command.",
                )
            )

        if len(self.missing_values.keys()) > 0:
            self.errors.append(RequiredValuesError(self.missing_values))

        if len(self.errors) > 0:
            if self.verbosity >= 5:
                for err in self.errors:
                    if self.verbosity >= 6:
                        if err.__traceback__:
                            traceback.print_exception(type(err), err, err.__traceback__)
                        else:
                            print("No trace available.")
                    print(f"{type(err).__name__}: {str(err)}")
            raise AggregatedErrors(self.errors)

        enable_caching_updates()
        if dry_run:
            # If we're in debug mode, we need to clear the update buffer so that the next call doesn't make auto-
            # updates on non-existent (or incorrect) records
            clear_update_buffer()
            # And before we leave, we must re-enable auto-updates
            enable_autoupdates()
            enable_buffering()
            raise DryRun()

        if self.verbosity >= 3:
            print("Expiring affected caches...")
        for animal_rec in self.animals_to_uncache:
            if self.verbosity >= 4:
                print(f"Expiring animal {animal_rec.id}'s cache")
            animal_rec.delete_related_caches()
        if self.verbosity >= 3:
            print("Expiring done.")

        # Cannot perform buffered updates of FCirc, Sample, or Animal's last serum tracer peak group because no peak
        # groups have been loaded yet, so only update the ones labeled "name".
        perform_buffered_updates(labels=["name"], using=self.db)
        # Since we only updated some of the buffered items, clear the rest of the buffer
        clear_update_buffer()
        enable_autoupdates()
        enable_buffering()

    def get_tissue(self, rownum, row):
        tissue_name = self.getRowVal(rownum, row, "TISSUE_NAME")
        tissue_rec = None
        is_blank = tissue_name == ""
        if is_blank:
            if self.verbosity >= 2:
                print("Skipping row: Tissue field is empty, assuming blank sample")
        else:
            try:
                # Assuming that both the default and validation databases each have all current tissues
                tissue_rec = Tissue.objects.using(self.db).get(name=tissue_name)
            except Tissue.DoesNotExist as e:
                self.errors.append(
                    Tissue.DoesNotExist(
                        f"Invalid tissue type specified: '{tissue_name}'. Not found in database {self.db}.  {str(e)}"
                    ).with_traceback(e.__traceback__)
                )
            except Exception as e:
                self.errors.append(TissueError(e).with_traceback(e.__traceback__))
        return tissue_rec, is_blank

    def get_or_create_study(self, rownum, row, animal_rec):
        study_name = self.getRowVal(rownum, row, "STUDY_NAME")
        study_desc = self.getRowVal(rownum, row, "STUDY_DESCRIPTION")

        study_created = False
        study_rec = None

        if study_name:
            try:
                try:
                    study_rec, study_created = Study.objects.using(
                        self.db
                    ).get_or_create(
                        name=study_name,
                        description=study_desc,
                    )
                except IntegrityError as ie:
                    estr = str(ie)
                    if "duplicate key value violates unique constraint" in estr:
                        self.errors.append(
                            ConflictingValueError(
                                Study.__name__,
                                "name",
                                study_name,
                                "description",
                                Study.objects.using(self.db).get(name=study_name),
                                study_desc,
                            ).with_traceback(ie.__traceback__)
                        )
                    else:
                        raise ie
            except Exception as e:
                study_rec = None
                self.errors.append(
                    SaveError(Study.__name__, study_name, self.db, e).with_traceback(
                        e.__traceback__
                    )
                )

        if study_created:
            if self.verbosity >= 2:
                print(f"Created new Study record: {study_rec}")
            try:
                # get_or_create doesn't do a full_clean
                # TODO: See issue #580.  This will allow full_clean to be called regardless of the database.
                if self.db == settings.TRACEBASE_DB:
                    # full_clean does not have a using parameter. It only supports the default database
                    study_rec.full_clean()
            except Exception as e:
                study_rec = None
                self.errors.append(
                    SaveError(Study.__name__, study_name, self.db, e).with_traceback(
                        e.__traceback__
                    )
                )

        # We do this here, and not in the "animal_created" block, in case the researcher is creating a new study
        # from previously-loaded animals
        if study_rec and animal_rec and animal_rec not in study_rec.animals.all():
            if self.verbosity >= 2:
                print("Adding animal to the study...")
            study_rec.animals.add(animal_rec)

        return study_rec

    def get_tracer_concentrations(self, rownum, row):
        tracer_concs_str = self.getRowVal(rownum, row, "TRACER_CONCENTRATIONS")
        return parse_tracer_concentrations(tracer_concs_str)

    def get_or_create_infusate(self, rownum, row):
        tracer_concs = self.get_tracer_concentrations(rownum, row)
        infusate_str = self.getRowVal(rownum, row, "INFUSATE")

        infusate_rec = None
        if infusate_str is not None:
            if tracer_concs is None:
                self.errors.append(
                    NoConcentrations(
                        f"{self.headers.INFUSATE} [{infusate_str}] supplied without "
                        f"{self.headers.TRACER_CONCENTRATIONS}."
                    ).with_traceback(traceback.format_exc())
                )
            infusate_data_object = parse_infusate_name(infusate_str, tracer_concs)
            infusate_rec = Infusate.objects.using(self.db).get_or_create_infusate(
                infusate_data_object
            )[0]
        return infusate_rec

    def get_treatment(self, rownum, row):
        treatment_name = self.getRowVal(rownum, row, "ANIMAL_TREATMENT")
        treatment_rec = None
        if treatment_name:
            # Animal Treatments are optional protocols
            try:
                assert treatment_name != ""
                assert treatment_name != pd.isnull(treatment_name)
            except AssertionError:
                if self.verbosity >= 2:
                    print("No animal treatments with empty/null values.")
            else:
                if self.verbosity >= 2:
                    print(
                        f"Finding {Protocol.ANIMAL_TREATMENT} protocol for '{treatment_name}'..."
                    )
                try:
                    treatment_rec = Protocol.objects.using(self.db).get(
                        name=treatment_name,
                        category=Protocol.ANIMAL_TREATMENT,
                    )
                    if self.verbosity >= 2:
                        action = "Found"
                        feedback = (
                            f"{treatment_rec.category} protocol id '{treatment_rec.id}' named '{treatment_rec.name}' "
                            f"with description '{treatment_rec.description}'"
                        )
                        print(f"{action} {feedback}")
                except Protocol.DoesNotExist as e:
                    self.errors.append(
                        Protocol.DoesNotExist(
                            f"Could not find '{Protocol.ANIMAL_TREATMENT}' protocol with name "
                            f"'{treatment_name}'"
                        ).with_traceback(e.__traceback__)
                    )
                except Exception as e:
                    self.errors.append(
                        TreatmentError(e).with_traceback(e.__traceback__)
                    )

        elif self.verbosity >= 2:
            print("No animal treatment found.")

        return treatment_rec

    def get_or_create_animal(self, rownum, row, infusate_rec, treatment_rec):
        animal_name = self.getRowVal(rownum, row, "ANIMAL_NAME")
        genotype = self.getRowVal(rownum, row, "ANIMAL_GENOTYPE")
        weight = self.getRowVal(rownum, row, "ANIMAL_WEIGHT")
        feedstatus = self.getRowVal(rownum, row, "ANIMAL_FEEDING_STATUS")
        age = self.getRowVal(rownum, row, "ANIMAL_AGE")
        diet = self.getRowVal(rownum, row, "ANIMAL_DIET")
        animal_sex_string = self.getRowVal(rownum, row, "ANIMAL_SEX")
        infusion_rate = self.getRowVal(rownum, row, "ANIMAL_INFUSION_RATE")

        animal_rec = None
        animal_created = False

        if animal_name:
            # An infusate is required to create an animal
            if infusate_rec:
                animal_rec, animal_created = Animal.objects.using(
                    self.db
                ).get_or_create(name=animal_name, infusate=infusate_rec)
            else:
                try:
                    animal_rec = Animal.objects.using(self.db).get(name=animal_name)
                except Animal.DoesNotExist:
                    return animal_rec, animal_created

        # animal_created block contains all the animal attribute updates if the animal was newly created
        if animal_created:
            # TODO: See issue #580.  The following hits the default database's cache table even if the validation
            #       database has been set in the animal object.  This is currently tolerable because the only
            #       effect is a cache deletion.
            if animal_rec.caches_exist():
                self.animals_to_uncache.append(animal_rec)
            elif self.verbosity >= 3:
                print(f"No cache exists for animal {animal_rec.id}")

            if self.verbosity >= 2:
                print(f"Created new record: Animal:{animal_rec}")

            changed = False

            if genotype:
                animal_rec.genotype = genotype
                changed = True
            if weight:
                animal_rec.body_weight = weight
                changed = True
            if feedstatus:
                animal_rec.feeding_status = feedstatus
                changed = True
            if age:
                animal_rec.age = timedelta(weeks=int(age))
                changed = True
            if diet:
                animal_rec.diet = diet
                changed = True
            if animal_sex_string:
                if animal_sex_string in animal_rec.SEX_CHOICES:
                    animal_sex = animal_sex_string
                else:
                    animal_sex = value_from_choices_label(
                        animal_sex_string, animal_rec.SEX_CHOICES
                    )
                animal_rec.sex = animal_sex
                changed = True
            if treatment_rec:
                animal_rec.treatment = treatment_rec
                changed = True
            if infusion_rate:
                animal_rec.infusion_rate = infusion_rate
                changed = True

            try:
                # Even if there wasn't a change, get_or_create doesn't do a full_clean
                if self.db == settings.TRACEBASE_DB:
                    # full_clean does not have a using parameter. It only supports the default database
                    animal_rec.full_clean()
                # If there was a change, save the record again
                if changed:
                    animal_rec.save(using=self.db)
            except Exception as e:
                self.errors.append(
                    SaveError(
                        Animal.__name__, str(animal_rec), self.db, e
                    ).with_traceback(e.__traceback__)
                )

        return animal_rec

    def get_or_create_animallabel(self, animal_rec, infusate_rec):
        # Infusate is required, but the missing headers are buffered to create an exception later
        if animal_rec and infusate_rec:
            # Animal Label - Load each unique labeled element among the tracers for this animal
            # This is where enrichment_fraction, enrichment_abundance, and normalized_labeling functions live
            for labeled_element in infusate_rec.tracer_labeled_elements():
                if self.verbosity >= 2:
                    print(
                        f"Finding or inserting animal label '{labeled_element}' for '{animal_rec}'..."
                    )
                AnimalLabel.objects.using(self.db).get_or_create(
                    animal=animal_rec,
                    element=labeled_element,
                )

    def get_or_create_sample(self, rownum, row, animal_rec, tissue_rec):
        sample_name = self.getRowVal(rownum, row, "SAMPLE_NAME")
        researcher = self.getRowVal(rownum, row, "SAMPLE_RESEARCHER")
        time_collected_str = self.getRowVal(rownum, row, "TIME_COLLECTED")
        sample_date_value = self.getRowVal(rownum, row, "SAMPLE_DATE")

        sample_rec = None

        # Creating a sample requires a tissue
        if sample_name and tissue_rec:
            try:
                # Assuming that duplicates among the submission are handled in the checking of the file, so we must
                # check against the tracebase database for pre-existing sample name duplicates
                sample_rec = Sample.objects.using(settings.TRACEBASE_DB).get(
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
                    sample_rec, sample_created = Sample.objects.using(
                        self.db
                    ).get_or_create(
                        name=sample_name,
                        animal=animal_rec,
                        tissue=tissue_rec,
                    )
                except Exception as e:
                    # This script is permissive, to generate as many actionable errors as possible in one run, but if
                    # either animal or tissue are None (required in a sample record), don't generate more pointless
                    # errors, just return None
                    if not animal_rec or not tissue_rec:
                        return None
                    self.errors.append(
                        SampleError(str(e)).with_traceback(e.__traceback__)
                    )

                if sample_created:
                    changed = False

                    if self.verbosity >= 2:
                        print(f"Creating new record: Sample:{sample_name}")

                    if researcher:
                        changed = True
                        sample_rec.researcher = researcher
                        if (
                            not self.skip_researcher_check
                            and researcher not in self.input_researchers
                        ):
                            self.input_researchers.append(researcher)
                            if researcher not in self.known_researchers:
                                self.unknown_researchers.append(researcher)

                    if time_collected_str:
                        changed = True
                        sample_rec.time_collected = timedelta(
                            minutes=float(time_collected_str)
                        )

                    if sample_date_value:
                        changed = True
                        # Pandas may have already parsed the date
                        try:
                            sample_date = dateutil.parser.parse(sample_date_value)
                        except TypeError:
                            sample_date = sample_date_value
                        sample_rec.date = sample_date

                    try:
                        # Even if there wasn't a change, get_or_create doesn't do a full_clean
                        if self.db == settings.TRACEBASE_DB:
                            # full_clean does not have a using parameter. It only supports the default database
                            sample_rec.full_clean()
                        if changed:
                            sample_rec.save(using=self.db)
                    except Exception as e:
                        self.errors.append(
                            SaveError(
                                Sample.__name__, str(sample_rec), self.db, e
                            ).with_traceback(e.__traceback__)
                        )
            except Exception as e:
                self.errors.append(SampleError(e).with_traceback(e.__traceback__))

        return sample_rec

    def get_or_create_fcircs(self, infusate_rec, sample_rec):
        if (
            sample_rec
            and infusate_rec
            and sample_rec.tissue
            and sample_rec.tissue.is_serum()
        ):
            # FCirc - Load each unique tracer and labeled element combo if this is a serum sample
            # These tables are where the appearance and disappearance calculation functions live
            for tracer_rec in infusate_rec.tracers.all():
                for label_rec in tracer_rec.labels.all():
                    if self.verbosity >= 2:
                        print(
                            f"\tFinding or inserting FCirc tracer '{tracer_rec.compound}' and label "
                            f"'{label_rec.element}' for '{sample_rec}' in database {self.db}..."
                        )
                    FCirc.objects.using(self.db).get_or_create(
                        serum_sample=sample_rec,
                        tracer=tracer_rec,
                        element=label_rec.element,
                    )

    def check_headers(self, headers):
        known_headers = []
        missing_headers = []
        unknown_headers = []
        misconfiged_headers = []

        rqd_hdr_tuple = self.RequiredSampleTableHeaders
        hdr_name_tuple = self.headers

        header_attrs = rqd_hdr_tuple._fields

        # For each header attribute
        for hdr_attr in header_attrs:
            hdr_name = getattr(hdr_name_tuple, hdr_attr)
            hdr_required = getattr(rqd_hdr_tuple, hdr_attr)
            if hdr_name:
                known_headers.append(hdr_name)
                # If the header is required
                if hdr_required:
                    if hdr_name not in headers:
                        missing_headers.append(hdr_name)
                if hdr_name in headers:
                    self.headers_present.append(hdr_name)
            elif hdr_required:
                misconfiged_headers.append(hdr_attr)

        # For each header in the headers argument
        for hdr_name in headers:
            if hdr_name not in known_headers:
                unknown_headers.append(hdr_name)

        if len(missing_headers) > 0:
            self.errors.append(RequiredHeadersError(missing_headers))
        if len(unknown_headers) > 0:
            self.errors.append(UnknownHeadersError(unknown_headers))
        if len(misconfiged_headers) > 0:
            self.errors.append(HeaderConfigError(misconfiged_headers))

    def getRowVal(self, rownum, row, header_attribute):
        # get the header value to use as a dict key for 'row'
        header = getattr(self.headers, header_attribute)
        val = None

        if header in self.headers_present:
            val = row[header]
            # We're ignoring missing headers.  Required headers check is covered in check_headers.
            val_required = getattr(self.RequiredSampleTableValues, header_attribute)
            if val_required and val is None:
                self.missing_values[header].append(rownum)

        return val


class NoConcentrations(Exception):
    pass


class UnanticipatedError(Exception):
    def __init__(self, e):
        message = f"UnanticipatedError: {type(e).__name__}: {str(e)}"
        super().__init__(message)


class SampleError(UnanticipatedError):
    pass


class TissueError(UnanticipatedError):
    pass


class TreatmentError(UnanticipatedError):
    pass
