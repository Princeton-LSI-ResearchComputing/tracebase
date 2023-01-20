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
    init_autoupdate_label_filters,
    perform_buffered_updates,
)
from DataRepo.models.researcher import (
    UnknownResearcherError,
    validate_researchers,
)
from DataRepo.models.utilities import value_from_choices_label
from DataRepo.utils import parse_infusate_name, parse_tracer_concentrations
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    DryRun,
    DuplicateHeadersError,
    HeaderConfigError,
    RequiredHeadersError,
    RequiredValuesError,
    SaveError,
    UnknownHeadersError,
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

    # Configure what the headers are in the file
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

    # Configure what headers are required
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

    # Configure what values are required
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
        defer_autoupdates=False,
    ):
        # Header config
        self.headers = sample_table_headers
        self.headers_present = []

        # Verbosity affects log prints and error verbosity (for debugging)
        self.verbosity = verbosity

        # Database config
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

        # How to handle mass autoupdates
        self.defer_autoupdates = defer_autoupdates

        # Caching overhead
        self.animals_to_uncache = []

        # Error-tracking
        self.errors = []
        self.missing_headers = []
        self.missing_values = defaultdict(list)

        # Researcher consistency tracking (also a part of error-tracking)
        self.skip_researcher_check = skip_researcher_check
        self.input_researchers = []

    def load_sample_table(self, data, dry_run=False):

        disable_autoupdates()
        disable_caching_updates()
        # Only auto-update fields whose update_label in the decorator is "name"
        init_autoupdate_label_filters(label_filters=["name"])

        try:
            self.load_data(data, dry_run)
        except Exception as e:
            # If we're stopping with an exception, we need to clear the update buffer so that the next call doesn't
            # make auto-updates on non-existent (or incorrect) records
            clear_update_buffer()
            # Re-initialize label filters to default
            init_autoupdate_label_filters()
            enable_caching_updates()
            enable_autoupdates()
            raise e

        # Re-initialize label filters to default
        init_autoupdate_label_filters()
        enable_caching_updates()
        enable_autoupdates()

    def load_data(self, data, dry_run=False):
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

        if not self.skip_researcher_check:
            try:
                validate_researchers(self.input_researchers, "--skip-researcher-check")
            except UnknownResearcherError as ure:
                self.buffer_exception(ure)

        if len(self.missing_values.keys()) > 0:
            self.buffer_exception(RequiredValuesError(self.missing_values))

        if len(self.errors) > 0:
            if self.verbosity >= 2:
                for err in self.errors:
                    if self.verbosity >= 3:
                        if err.__traceback__:
                            traceback.print_exception(type(err), err, err.__traceback__)
                        else:
                            print("No trace available.")
                    print(f"{type(err).__name__}: {str(err)}")
            # PR REVIEW NOTE: I think it may be worthwhile to encapsulate this logic in a method of a superclass of
            #                 SampleTableLoader and AccucorDataLoader, because I don't like that cull_warnings is in
            #                 the exception class. I could instead do something like: decide in the fly if a problem
            #                 should be a warning or an error when buffering the "exception" and decide in the super
            #                 class method whether to raise the AggregatedErrors exception.  Though I'm not sure where
            #                 to put the data members currently in the exception classes that AggregatedErrors looks at
            #                 to decide what to do.
            aes = AggregatedErrors(self.errors, verbosity=self.verbosity)
            # Split into fatal errors and warnings and decide whether the exception should be raised or not (will not
            # be raised if not in validate mode and)
            should_raise = aes.cull_warnings(self.validate)
            if should_raise:
                raise aes

        if dry_run:
            raise DryRun()

        if self.verbosity >= 2:
            print("Expiring affected caches...")
        for animal_rec in self.animals_to_uncache:
            if self.verbosity >= 3:
                print(f"Expiring animal {animal_rec.id}'s cache")
            animal_rec.delete_related_caches()
        if self.verbosity >= 2:
            print("Expiring done.")

        autoupdate_mode = not self.defer_autoupdates

        if autoupdate_mode:
            # No longer any need to explicitly filter based on labels, because only records containing fields with the
            # required labels are buffered now, and when the records are buffered, the label filtering that was in
            # effect at the time of buffering is saved so that only the fields matching the label filter will be
            # updated.  There are autoupdates for fields in Animal and Sample, but they're only needed for FCirc
            # calculations and will be triggered by a subsequent accucor load.
            perform_buffered_updates(using=self.db)
            # Since we only updated some of the buffered items, clear the rest of the buffer
            clear_update_buffer()

    def get_tissue(self, rownum, row):
        tissue_name = self.getRowVal(rownum, row, "TISSUE_NAME")
        tissue_rec = None
        is_blank = tissue_name is None
        if is_blank:
            if self.verbosity >= 2:
                print("Skipping row: Tissue field is empty, assuming blank sample")
        else:
            try:
                # Assuming that both the default and validation databases each have all current tissues
                tissue_rec = Tissue.objects.using(self.db).get(name=tissue_name)
            except Tissue.DoesNotExist as e:
                self.buffer_exception(
                    Tissue.DoesNotExist(
                        f"Invalid tissue type specified: '{tissue_name}'. Not found in database {self.db}.  {str(e)}"
                    )
                )
            except Exception as e:
                self.buffer_exception(TissueError(type(e).__name__, e))
        return tissue_rec, is_blank

    def get_or_create_study(self, rownum, row, animal_rec):
        study_name = self.getRowVal(rownum, row, "STUDY_NAME")
        study_desc = self.getRowVal(rownum, row, "STUDY_DESCRIPTION")

        study_created = False
        study_updated = False
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
                        study_rec = Study.objects.using(self.db).get(name=study_name)
                        orig_desc = study_rec.description
                        if orig_desc and study_desc:
                            self.buffer_exception(
                                ConflictingValueError(
                                    study_rec,
                                    "description",
                                    orig_desc,
                                    study_desc,
                                )
                            )
                        elif study_desc:
                            study_rec.description = study_desc
                            study_updated = True
                    else:
                        raise ie
            except Exception as e:
                study_rec = None
                self.buffer_exception(SaveError(Study.__name__, study_name, self.db, e))

        if study_created or study_updated:
            if self.verbosity >= 2:
                if study_created:
                    print(f"Created new Study record: {study_rec}")
                else:
                    print(f"Updated Study record: {study_rec}")
            try:
                # get_or_create does not perform a full clean
                # TODO: See issue #580.  This will allow full_clean to be called regardless of the database.
                if self.db == settings.TRACEBASE_DB:
                    # full_clean does not have a using parameter. It only supports the default database
                    study_rec.full_clean()
                # We only need to save if there was an update.  get_or_create does a save
                if study_updated:
                    study_rec.save(using=self.db)
            except Exception as e:
                study_rec = None
                self.buffer_exception(SaveError(Study.__name__, study_name, self.db, e))

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
                self.buffer_exception(
                    NoConcentrations(
                        f"{self.headers.INFUSATE} [{infusate_str}] supplied without "
                        f"{self.headers.TRACER_CONCENTRATIONS}."
                    )
                )
            infusate_data_object = parse_infusate_name(infusate_str, tracer_concs)
            infusate_rec = Infusate.objects.using(self.db).get_or_create_infusate(
                infusate_data_object,
            )[0]
        return infusate_rec

    def get_treatment(self, rownum, row):
        treatment_name = self.getRowVal(rownum, row, "ANIMAL_TREATMENT")
        treatment_rec = None
        if treatment_name:
            # Animal Treatments are optional protocols
            try:
                assert treatment_name is not None
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
                except Protocol.DoesNotExist:
                    self.buffer_exception(
                        Protocol.DoesNotExist(
                            f"Could not find '{Protocol.ANIMAL_TREATMENT}' protocol with name "
                            f"'{treatment_name}'"
                        )
                    )
                except Exception as e:
                    self.buffer_exception(TreatmentError(type(e).__name__, e))

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
                self.buffer_exception(
                    SaveError(Animal.__name__, str(animal_rec), self.db, e)
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
        sample_rec = None

        # Initialize raw values
        sample_name = self.getRowVal(rownum, row, "SAMPLE_NAME")
        researcher = self.getRowVal(rownum, row, "SAMPLE_RESEARCHER")
        time_collected = None
        time_collected_str = self.getRowVal(rownum, row, "TIME_COLLECTED")
        sample_date = None
        sample_date_value = self.getRowVal(rownum, row, "SAMPLE_DATE")

        # Convert/check values as necessary
        if researcher and researcher not in self.input_researchers:
            self.input_researchers.append(researcher)
        if time_collected_str:
            time_collected = timedelta(minutes=float(time_collected_str))
        if sample_date_value:
            # Pandas may have already parsed the date.  Note that the database returns a datetime.date, but the parser
            # returns a datetime.datetime.  To compare them, the parsed value is cast to a datetime.date.
            try:
                sample_date = dateutil.parser.parse(sample_date_value).date()
            except TypeError:
                sample_date = sample_date_value.date()

        # Create a sample record - requires a tissue and animal record
        if sample_name and tissue_rec and animal_rec:
            try:
                # Assuming that duplicates among the submission are handled in the checking of the file, so we must
                # check against the tracebase database for pre-existing sample name duplicates
                sample_rec = Sample.objects.using(settings.TRACEBASE_DB).get(
                    name=sample_name
                )

                # Now check that the values are consistent.  Adds conflicts to self.errors and returns update info for
                # null fields
                updates_dict = self.check_for_inconsistencies(
                    sample_rec,
                    {
                        "animal": animal_rec,
                        "tissue": tissue_rec,
                        "researcher": researcher,
                        "time_collected": time_collected,
                        "date": sample_date,
                    },
                )

                if len(updates_dict.keys()) > 0 and self.db == settings.TRACEBASE_DB:
                    if self.verbosity >= 2:
                        print(
                            f"Updating [{', '.join(updates_dict.keys())}] in Sample record: [{sample_name}]"
                        )
                    for f, v in updates_dict.items():
                        setattr(sample_rec, f, v)
                    try:
                        # Even if there wasn't a change, get_or_create doesn't do a full_clean
                        sample_rec.full_clean()
                        sample_rec.save(using=self.db)
                    except Exception as e:
                        self.buffer_exception(
                            SaveError(Sample.__name__, str(sample_rec), self.db, e)
                        )
                elif self.verbosity >= 2:
                    print(f"SKIPPING existing Sample record: {sample_name}")

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
                except IntegrityError:
                    # If we get here, it means that it tried to create because not all values matched, but upon
                    # creation, the unique sample name collided.  We just need to check_for_inconsistencies

                    sample_rec = Sample.objects.using(self.db).get(name=sample_name)

                    # Now check that the values are consistent.  Adds conflicts to self.errors and returns update info
                    # for null fields
                    updates_dict = self.check_for_inconsistencies(
                        sample_rec,
                        {
                            "animal": animal_rec,
                            "tissue": tissue_rec,
                            "time_collected": time_collected,
                            "date": sample_date,
                            "researcher": researcher,
                        },
                    )

                    if len(updates_dict.keys()) > 0:
                        if self.verbosity >= 2:
                            print(
                                f"Updating [{', '.join(updates_dict.keys())}] in Sample record: [{sample_name}]"
                            )
                        for f, v in updates_dict.items():
                            setattr(sample_rec, f, v)
                        try:
                            # Even if there wasn't a change, get_or_create doesn't do a full_clean
                            if self.db == settings.TRACEBASE_DB:
                                # full_clean does not have a using parameter. It only supports the default database
                                sample_rec.full_clean()
                            sample_rec.save(using=self.db)
                        except Exception as e:
                            sample_rec = None
                            sample_created = False
                            self.buffer_exception(
                                SaveError(Sample.__name__, str(sample_rec), self.db, e)
                            )
                    else:
                        sample_rec = None
                        sample_created = False

                except Exception as e:
                    sample_rec = None
                    sample_created = False
                    self.buffer_exception(SampleError(type(e).__name__, e))

                if sample_created:
                    changed = False

                    if self.verbosity >= 2:
                        print(f"Creating new record: Sample:{sample_name}")

                    if researcher:
                        changed = True
                        sample_rec.researcher = researcher

                    if time_collected:
                        changed = True
                        sample_rec.time_collected = time_collected

                    if sample_date_value:
                        changed = True
                        sample_rec.date = sample_date

                    try:
                        # Even if there wasn't a change, get_or_create doesn't do a full_clean
                        if self.db == settings.TRACEBASE_DB:
                            # full_clean does not have a using parameter. It only supports the default database
                            sample_rec.full_clean()
                        if changed:
                            sample_rec.save(using=self.db)
                    except Exception as e:
                        self.buffer_exception(
                            SaveError(Sample.__name__, str(sample_rec), self.db, e)
                        )
            except Exception as e:
                self.buffer_exception(SampleError(type(e).__name__, e))

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

    def check_for_inconsistencies(self, rec, value_dict):
        updates_dict = {}
        for field, new_value in value_dict.items():
            orig_value = getattr(rec, field)
            if orig_value is None and new_value is not None:
                updates_dict[field] = new_value
            elif orig_value != new_value:
                self.buffer_exception(
                    ConflictingValueError(
                        rec,
                        field,
                        orig_value,
                        new_value,
                    )
                )
        return updates_dict

    def check_headers(self, headers):
        known_headers = []
        missing_headers = []
        unknown_headers = []
        misconfiged_headers = []  # header required but no header string set
        dupe_headers = {}

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
                    if hdr_name in self.headers_present:
                        if hdr_name in dupe_headers.keys():
                            dupe_headers[hdr_name] += 1
                        else:
                            dupe_headers[hdr_name] = 2
                    else:
                        self.headers_present.append(hdr_name)
            elif hdr_required:
                misconfiged_headers.append(hdr_attr)

        # For each header in the headers argument
        for hdr_name in headers:
            if hdr_name not in known_headers:
                unknown_headers.append(hdr_name)

        if len(missing_headers) > 0:
            self.buffer_exception(RequiredHeadersError(missing_headers))
        if len(unknown_headers) > 0:
            self.buffer_exception(UnknownHeadersError(unknown_headers))
        if len(misconfiged_headers) > 0:
            self.buffer_exception(HeaderConfigError(misconfiged_headers))
        if len(dupe_headers.keys()) > 0:
            self.buffer_exception(DuplicateHeadersError(dupe_headers))

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

            # This will make later checks of values easier
            if val == "":
                val = None

        return val

    def buffer_exception(self, exception):
        if hasattr(exception, "__traceback__"):
            self.errors.append(exception)
        else:
            try:
                raise exception
            except Exception as e:
                self.errors.append(e)


class NoConcentrations(Exception):
    pass


class UnanticipatedError(Exception):
    def __init__(self, type, e):
        message = f"{type}: {str(e)}"
        super().__init__(message)


class SampleError(UnanticipatedError):
    pass


class TissueError(UnanticipatedError):
    pass


class TreatmentError(UnanticipatedError):
    pass
