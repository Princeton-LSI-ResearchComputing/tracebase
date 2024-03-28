import re
from collections import defaultdict, namedtuple
from datetime import timedelta

import dateutil.parser  # type: ignore
import pandas as pd
from django.db import IntegrityError, transaction

from DataRepo.models import (
    Animal,
    AnimalLabel,
    FCirc,
    Infusate,
    MaintainedModel,
    Protocol,
    Sample,
    Study,
    Tissue,
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
from DataRepo.models.utilities import value_from_choices_label
from DataRepo.utils import (
    get_column_dupes,
    parse_infusate_name,
    parse_tracer_concentrations,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AllMissingTissues,
    AllMissingTreatments,
    ConflictingValueError,
    DryRun,
    DuplicateValues,
    HeaderConfigError,
    LCMSDBSampleMissing,
    MissingTissue,
    MissingTreatment,
    NoConcentrations,
    RequiredHeadersError,
    RequiredSampleValuesError,
    SampleError,
    SaveError,
    SheetMergeError,
    TissueError,
    TreatmentError,
    UnitsWrong,
    UnknownHeadersError,
)
from DataRepo.utils.lcms_metadata_parser import (
    lcms_df_to_dict,
    lcms_metadata_to_samples,
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
        validate=False,  # Only affects what is/isn't a warning
        verbosity=1,
        skip_researcher_check=False,
        defer_autoupdates=False,
        defer_rollback=False,  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in atomic transact in caller)
        dry_run=False,
        update_caches=True,
        lcms_metadata_df=None,
    ):
        # Header config
        self.headers = sample_table_headers
        self.headers_present = []

        # Modes
        self.verbosity = verbosity
        self.dry_run = dry_run
        self.validate = validate
        # How to handle mass autoupdates
        self.defer_autoupdates = defer_autoupdates
        # Whether to rollback upon error or keep the changes and defer rollback to the caller
        self.defer_rollback = defer_rollback

        # Caching overhead
        # Making this True causes existing caches associated with loaded records to be deleted
        self.update_caches = update_caches
        self.animals_to_uncache = []

        # Error-tracking
        self.aggregated_errors_object = AggregatedErrors()
        self.missing_headers = []
        self.missing_values = defaultdict(dict)

        # Skip rows that have errors
        self.units_errors = {}
        self.infile_sample_dupe_rows = []
        self.empty_animal_rows = []

        # Arrange the LCMS samples
        lcms_metadata = lcms_df_to_dict(lcms_metadata_df, self.aggregated_errors_object)
        self.lcms_samples = lcms_metadata_to_samples(lcms_metadata)

        # Obtain known researchers before load
        self.known_researchers = get_researchers()

        # Researcher consistency tracking (also a part of error-tracking)
        self.skip_researcher_check = skip_researcher_check
        self.input_researchers = []

        # This is used by strip_units to decide on whether to issue an error or warning.  Case insensitive.
        self.expected_units = {
            "ANIMAL_WEIGHT": ["g", "gram", "grams"],
            "ANIMAL_AGE": ["w", "week", "weeks"],
            "ANIMAL_INFUSION_RATE": [
                "ul/m/g",
                "ul/min/g",
                "ul/min/gram",
                "ul/minute/g",
                "ul/minute/gram",
            ],
            "TRACER_CONCENTRATIONS": ["mM", "millimolar"],
            "TIME_COLLECTED": ["m", "min", "mins", "minute", "minutes"],
        }

    @MaintainedModel.defer_autoupdates(
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def load_sample_table(self, data):
        # Chaching updates are not necessary when just adding data, so disabling dramatically speeds things up
        disable_caching_updates()

        try:
            saved_aes = None

            with transaction.atomic():
                try:
                    self._load_data(data)
                except AggregatedErrors as aes:
                    if self.defer_rollback:
                        saved_aes = aes
                    else:
                        # If we're not working for the validation interface, raise here to cause a rollback
                        raise aes

            # If we were directed to defer rollback in the event of an error, raise the exception here (outside of
            # the atomic transaction block).  This assumes that the caller is handling rollback in their own atomic
            # transaction blocl.
            if saved_aes is not None:
                raise saved_aes

        except Exception as e:
            enable_caching_updates()
            raise e

        enable_caching_updates()

    def _load_data(self, data):
        # This will be used to validate the lcms samples:
        all_sample_names = []

        # Create a list to hold the csv reader data so that iterations from validating doesn't leave the csv reader
        # empty/at-the-end upon the import loop
        sample_table_data = list(data)

        # If there are headers
        if len(sample_table_data) > 0:
            # Use the first row to check the headers
            self.check_headers(sample_table_data[0].keys())

        # Check for empty animal values - because it will screw up the pandas sheet merge
        self.identify_empty_animal_rows(sample_table_data)

        # Check the in-file uniqueness of the samples. With the database, you cannot tell if the sample uniqueness
        # issue pre-existed the study this describes or is within this study.  This check clarifies that.
        # This skips rows with empty animals identified above.
        self.identify_infile_sample_dupe_rows(sample_table_data)

        for rowidx, row in enumerate(sample_table_data):
            rownum = rowidx + 1

            self.check_required_values(rowidx, row)

            tissue_rec, is_blank = self.get_tissue(rownum, row)

            if is_blank:
                continue

            infusate_rec = self.get_or_create_infusate(rownum, row)
            treatment_rec = self.get_treatment(row)
            animal_rec = self.get_or_create_animal(
                rownum, row, infusate_rec, treatment_rec
            )
            self.get_or_create_study(rownum, row, animal_rec)
            self.get_or_create_animallabel(animal_rec, infusate_rec)
            # If the row has an issue (e.g. not unique in the file), skip it so there will not be pointless errors
            if rowidx not in self.infile_sample_dupe_rows:
                sample_rec = self.get_or_create_sample(
                    rownum, row, animal_rec, tissue_rec
                )
                # Sample rec will be none if there was a problem/exception
                if sample_rec is not None:
                    all_sample_names.append(sample_rec.name)
                self.get_or_create_fcircs(infusate_rec, sample_rec)
            elif self.verbosity >= 2:
                print(
                    f"SKIPPING sample load on row {rownum} due to duplicate sample name."
                )

        self.check_lcms_samples(all_sample_names)

        if not self.skip_researcher_check:
            try:
                validate_researchers(
                    self.input_researchers,
                    known_researchers=self.known_researchers,
                    skip_flag="--skip-researcher-check",
                )
            except UnknownResearcherError as ure:
                self.aggregated_errors_object.buffer_exception(
                    ure,
                    is_error=not self.validate,  # Error in load mode, warning in validate mode
                    is_fatal=True,  # Always raise the AggErrs exception
                )

        missing_tissue_errors = self.aggregated_errors_object.remove_exception_type(
            MissingTissue
        )
        if len(missing_tissue_errors) > 0:
            self.aggregated_errors_object.buffer_error(
                AllMissingTissues(missing_tissue_errors)
            )

        missing_treatment_errors = self.aggregated_errors_object.remove_exception_type(
            MissingTreatment
        )
        if len(missing_treatment_errors) > 0:
            self.aggregated_errors_object.buffer_error(
                AllMissingTreatments(missing_treatment_errors)
            )

        if len(self.missing_values.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                RequiredSampleValuesError(
                    self.missing_values, animal_hdr=getattr(self.headers, "ANIMAL_NAME")
                )
            )

        if len(self.units_errors.keys()) > 0:
            self.aggregated_errors_object.buffer_error(UnitsWrong(self.units_errors))

        if self.aggregated_errors_object.should_raise():
            raise self.aggregated_errors_object

        if self.dry_run:
            raise DryRun()

        if self.update_caches is True:
            if self.verbosity >= 2:
                print("Expiring affected caches...")
            for animal_rec in self.animals_to_uncache:
                if self.verbosity >= 3:
                    print(f"Expiring animal {animal_rec.id}'s cache")
                animal_rec.delete_related_caches()
            if self.verbosity >= 2:
                print("Expiring done.")

    def check_lcms_samples(self, all_load_samples):
        """
        Makes sure every sample in the LCMS dataframe is present in the animal sample table
        """
        lcms_samples_missing = []

        for lcms_sample in self.lcms_samples:
            if lcms_sample not in all_load_samples:
                lcms_samples_missing.append(lcms_sample)

        if len(lcms_samples_missing) > 0:
            self.aggregated_errors_object.buffer_error(
                LCMSDBSampleMissing(lcms_samples_missing)
            )

    def get_tissue(self, rownum, row):
        tissue_name = self.getRowVal(row, "TISSUE_NAME")
        tissue_rec = None
        is_blank = tissue_name is None
        if is_blank:
            if self.verbosity >= 2:
                print("Skipping row: Tissue field is empty, assuming blank sample.")
        else:
            try:
                tissue_rec = Tissue.objects.get(name=tissue_name)
            except Tissue.DoesNotExist:
                self.aggregated_errors_object.buffer_error(
                    MissingTissue(
                        tissue_name,
                        column=self.headers.TISSUE_NAME,
                        rownum=row.name + 2,
                    ),
                )
            except Exception as e:
                self.aggregated_errors_object.buffer_error(
                    TissueError(type(e).__name__, e)
                )
        return tissue_rec, is_blank

    def get_or_create_study(self, rownum, row, animal_rec):
        study_name = self.getRowVal(row, "STUDY_NAME")
        study_desc = self.getRowVal(row, "STUDY_DESCRIPTION")

        study_created = False
        study_updated = False
        study_rec = None

        if study_name:
            try:
                study_rec, study_created = Study.objects.get_or_create(
                    name=study_name,
                    description=study_desc,
                )
            except IntegrityError as ie:
                estr = str(ie)
                if "duplicate key value violates unique constraint" in estr:
                    study_rec = Study.objects.get(name=study_name)
                    orig_desc = study_rec.description
                    if orig_desc and study_desc:
                        self.aggregated_errors_object.buffer_error(
                            ConflictingValueError(
                                study_rec,
                                {
                                    "description": {
                                        "orig": orig_desc,
                                        "new": study_desc,
                                    },
                                },
                                rownum,
                            )
                        )
                    elif study_desc:
                        study_rec.description = study_desc
                        study_updated = True
                else:
                    raise ie
            except Exception as e:
                study_rec = None
                self.aggregated_errors_object.buffer_error(
                    SaveError(Study.__name__, study_name, e)
                )

        if study_created or study_updated:
            if self.verbosity >= 2:
                if study_created:
                    print(f"Created new Study record: {study_rec}")
                else:
                    print(f"Updated Study record: {study_rec}")
            try:
                # get_or_create does not perform a full clean
                study_rec.full_clean()
                # We only need to save if there was an update.  get_or_create does a save
                if study_updated:
                    study_rec.save()
            except Exception as e:
                study_rec = None
                self.aggregated_errors_object.buffer_error(
                    SaveError(Study.__name__, study_name, e)
                )

        # We do this here, and not in the "animal_created" block, in case the researcher is creating a new study
        # from previously-loaded animals
        if study_rec and animal_rec and animal_rec not in study_rec.animals.all():
            if self.verbosity >= 2:
                print("Adding animal to the study...")
            study_rec.animals.add(animal_rec)

        return study_rec

    def get_tracer_concentrations(self, rownum, row):
        tracer_concs_str = self.getRowVal(row, "TRACER_CONCENTRATIONS")
        stripped_tracer_concs_str = self.strip_units(
            tracer_concs_str, "TRACER_CONCENTRATIONS", rownum
        )
        return parse_tracer_concentrations(stripped_tracer_concs_str)

    def get_or_create_infusate(self, rownum, row):
        tracer_concs = self.get_tracer_concentrations(rownum, row)
        infusate_str = self.getRowVal(row, "INFUSATE")

        infusate_rec = None
        if infusate_str is not None:
            if tracer_concs is None:
                self.aggregated_errors_object.buffer_error(
                    NoConcentrations(
                        f"{self.headers.INFUSATE} [{infusate_str}] supplied without "
                        f"{self.headers.TRACER_CONCENTRATIONS}."
                    )
                )
            infusate_data_object = parse_infusate_name(infusate_str, tracer_concs)
            infusate_rec = Infusate.objects.get_or_create_infusate(
                infusate_data_object,
            )[0]
        return infusate_rec

    def get_treatment(self, row):
        treatment_name = self.getRowVal(row, "ANIMAL_TREATMENT")
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
                    treatment_rec = Protocol.objects.get(
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
                    self.aggregated_errors_object.buffer_error(
                        MissingTreatment(
                            treatment_name,
                            column=self.headers.ANIMAL_TREATMENT,
                            rownum=row.name + 2,
                        )
                    )
                except Exception as e:
                    self.aggregated_errors_object.buffer_error(
                        TreatmentError(type(e).__name__, e)
                    )

        elif self.verbosity >= 2:
            print("No animal treatment found.")

        return treatment_rec

    def strip_units(self, val, hdr_attr, rowidx):
        """
        This method takes a numeric value with accompanying units is string format and returns the numerical value
        without the units, also in string format.  It buffers a warning exception, because the value could be
        malformed, so the user should be alerted about it to potentially fix it.
        """
        if not isinstance(val, str):
            # Assume that if it's not a string, it already doesn't contain units, because pandas converted it
            return val

        stripped_val = val

        specific_units_pat = None
        if hdr_attr in self.expected_units.keys():
            specific_units_pat = re.compile(
                r"^("
                + r"|".join(re.escape(units) for units in self.expected_units[hdr_attr])
                + r")$",
                re.IGNORECASE,
            )

        united_val_pattern = re.compile(
            r"^(?P<val>[\d\.eE]+)\s*(?P<units>[a-z][a-z\/]*)$", re.IGNORECASE
        )
        match = re.search(united_val_pattern, val)

        # If the value matches a units pattern
        if match:
            # We will strip the units in either case to avoid subsequent errors, but the population of
            # self.units_errors will fail the load
            stripped_val = match.group("val")
            the_units = match.group("units")
            header = getattr(self.headers, hdr_attr)

            s_match = re.search(specific_units_pat, the_units)

            # If the units don't match any expected units
            if s_match is None:
                if header in self.units_errors:
                    self.units_errors[header]["rows"].append(rowidx + 2)
                else:
                    self.units_errors[header] = {
                        "example_val": val,
                        "expected": self.expected_units[hdr_attr][0],
                        "rows": [rowidx + 2],
                        "units": the_units,
                    }

        return stripped_val

    def get_or_create_animal(self, rownum, row, infusate_rec, treatment_rec):
        animal_name = self.getRowVal(row, "ANIMAL_NAME")
        genotype = self.getRowVal(row, "ANIMAL_GENOTYPE")
        raw_weight = self.getRowVal(row, "ANIMAL_WEIGHT")
        weight = self.strip_units(raw_weight, "ANIMAL_WEIGHT", rownum)
        feedstatus = self.getRowVal(row, "ANIMAL_FEEDING_STATUS")
        raw_age = self.getRowVal(row, "ANIMAL_AGE")
        age = self.strip_units(raw_age, "ANIMAL_AGE", rownum)
        diet = self.getRowVal(row, "ANIMAL_DIET")
        animal_sex_string = self.getRowVal(row, "ANIMAL_SEX")
        raw_infusion_rate = self.getRowVal(row, "ANIMAL_INFUSION_RATE")
        infusion_rate = self.strip_units(
            raw_infusion_rate, "ANIMAL_INFUSION_RATE", rownum
        )

        animal_rec = None
        animal_created = False

        if animal_name:
            # An infusate is required to create an animal
            if infusate_rec:
                animal_rec, animal_created = Animal.objects.get_or_create(
                    name=animal_name, infusate=infusate_rec
                )
            else:
                try:
                    animal_rec = Animal.objects.get(name=animal_name)
                except Animal.DoesNotExist:
                    return animal_rec

        # animal_created block contains all the animal attribute updates if the animal was newly created
        if animal_created:
            if self.update_caches is True:
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
                animal_rec.full_clean()
                # If there was a change, save the record again
                if changed:
                    animal_rec.save()
            except Exception as e:
                self.aggregated_errors_object.buffer_error(
                    SaveError(Animal.__name__, str(animal_rec), e)
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
                AnimalLabel.objects.get_or_create(
                    animal=animal_rec,
                    element=labeled_element,
                )

    def get_or_create_sample(self, rownum, row, animal_rec, tissue_rec):
        sample_rec = None

        # Initialize raw values
        sample_name = self.getRowVal(row, "SAMPLE_NAME")
        researcher = self.getRowVal(row, "SAMPLE_RESEARCHER")
        time_collected = None
        raw_time_collected_str = self.getRowVal(row, "TIME_COLLECTED")
        time_collected_str = self.strip_units(
            raw_time_collected_str, "TIME_COLLECTED", rownum
        )
        sample_date = None
        sample_date_value = self.getRowVal(row, "SAMPLE_DATE")

        # Convert/check values as necessary
        if researcher and researcher not in self.input_researchers:
            self.input_researchers.append(researcher)
        if time_collected_str:
            time_collected = timedelta(minutes=float(time_collected_str))
        if sample_date_value:
            # Pandas may have already parsed the date.  Note that the database returns a datetime.date, but the parser
            # returns a datetime.datetime.  To compare them, the parsed value is cast to a datetime.date.
            try:
                sample_date = dateutil.parser.parse(str(sample_date_value)).date()
            except TypeError:
                sample_date = sample_date_value.date()

        # Create a sample record - requires a tissue and animal record
        if sample_name and tissue_rec and animal_rec:
            # TODO: This strategy should be refactored to do the get_or_create with this other (and all) data first and
            # intelligently handle exceptions, but I didn't want to do that much refactoring in 1 go.  This would mean
            # "check_for_inconsistencies" would become obsolete and will need to be replaced using the strategy I
            # employed in the compounds loader.  It will simplify this code.
            try:
                # It's worth noting that this loop encounters this code for the same sample multiple times

                # Assuming that duplicates among the submission are handled in the checking of the file, but not
                # against the database, so we must check against the database for pre-existing sample name duplicates
                sample_rec = Sample.objects.get(name=sample_name)

                # Now check that the values are consistent.  Buffers exceptions.
                self.check_for_inconsistencies(
                    sample_rec,
                    {
                        "animal": animal_rec,
                        "tissue": tissue_rec,
                        "researcher": researcher,
                        "time_collected": time_collected,
                        "date": sample_date,
                    },
                    rownum + 1,
                )

                if self.verbosity >= 2:
                    print(f"SKIPPING existing Sample record: {sample_name}")

            except Sample.DoesNotExist:
                try:
                    sample_rec, sample_created = Sample.objects.get_or_create(
                        name=sample_name,
                        animal=animal_rec,
                        tissue=tissue_rec,
                    )
                except IntegrityError:
                    # If we get here, it means that it tried to create because not all values matched, but upon
                    # creation, the unique sample name collided.  We just need to check_for_inconsistencies

                    sample_rec = Sample.objects.get(name=sample_name)

                    # Now check that the values are consistent.  Buffers exceptions.
                    self.check_for_inconsistencies(
                        sample_rec,
                        {
                            "animal": animal_rec,
                            "tissue": tissue_rec,
                            "time_collected": time_collected,
                            "date": sample_date,
                            "researcher": researcher,
                        },
                        rownum + 1,
                    )

                    # There was an error - clear this record value so we can continue processing
                    sample_rec = None
                    sample_created = False

                except Exception as e:
                    sample_rec = None
                    sample_created = False
                    self.aggregated_errors_object.buffer_error(
                        SampleError(type(e).__name__, e)
                    )

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
                        sample_rec.full_clean()
                        if changed:
                            sample_rec.save()
                    except Exception as e:
                        self.aggregated_errors_object.buffer_error(
                            SaveError(Sample.__name__, str(sample_rec), e)
                        )
            except Exception as e:
                self.aggregated_errors_object.buffer_error(
                    SampleError(type(e).__name__, e)
                )

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
                            f"'{label_rec.element}' for '{sample_rec}'..."
                        )
                    FCirc.objects.get_or_create(
                        serum_sample=sample_rec,
                        tracer=tracer_rec,
                        element=label_rec.element,
                    )

    def check_for_inconsistencies(self, rec, value_dict, rownum=None):
        updates_dict = {}
        differences = {}
        for field, new_value in value_dict.items():
            orig_value = getattr(rec, field)
            if orig_value is None and new_value is not None:
                updates_dict[field] = new_value
            elif orig_value != new_value:
                differences[field] = {
                    "orig": orig_value,
                    "new": new_value,
                }
        if len(differences.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                ConflictingValueError(
                    rec,
                    differences,
                    rownum,
                )
            )
        return updates_dict

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
            self.aggregated_errors_object.buffer_error(
                RequiredHeadersError(missing_headers)
            )
        if len(unknown_headers) > 0:
            self.aggregated_errors_object.buffer_error(
                UnknownHeadersError(unknown_headers)
            )
        if len(misconfiged_headers) > 0:
            self.aggregated_errors_object.buffer_error(
                HeaderConfigError(misconfiged_headers)
            )

    def identify_infile_sample_dupe_rows(self, data):
        """
        An animal can belong to multiple studies.  As such, a sample from an animal can also belong to multiple
        studies, and with the animal and sample sheet merge, the same sample will exist on 2 different rows after the
        merge.  Therefore, we need to check that the combination of sample name and study name are unique instead of
        just sample name.  For an example of this, look at:
        DataRepo/data/examples/test_dataframes/animal_sample_table_df_test1.xlsx.
        """
        sample_name_header = getattr(self.headers, "SAMPLE_NAME")
        study_name_header = getattr(self.headers, "STUDY_NAME")
        sample_dupes, row_idxs = get_column_dupes(
            data, [sample_name_header, study_name_header], self.empty_animal_rows
        )
        if len(sample_dupes.keys()) > 0:
            # Custom message to explain the case with Study name
            dupdeets = []
            for combo_val, l in sample_dupes.items():
                sample = sample_dupes[combo_val]["vals"][sample_name_header]
                dupdeets.append(
                    f"{sample} (rows*: {', '.join(list(map(lambda i: str(i + 2), l['rowidxs'])))})"
                )
            nltab = "\n\t"
            message = (
                f"{len(sample_dupes.keys())} values in the {sample_name_header} column were found to have duplicate "
                "occurrences on the indicated rows (*note, row numbers could reflect a sheet merge and may be "
                f"inaccurate):{nltab}{nltab.join(dupdeets)}\nNote, a sample can be a part of multiple studies, so if "
                "the same sample is in this list more than once, it means it's duplicated in multiple studies."
            )

            self.aggregated_errors_object.buffer_error(
                DuplicateValues(sample_dupes, sample_name_header, message=message)
            )
            self.infile_sample_dupe_rows = row_idxs

    def identify_empty_animal_rows(self, data):
        """
        If the animal name is empty on a row but the row has non-empty values, the pandas sheet merge will be screwed
        up and lots of meaningless errors will be spit out.  This method identifies and stores the row numbers
        (indexes) where the animal name is empty **in the Animals sheet only**, but the row has at least 1 actual
        value, so those rows can be skipped in later processing.

        Note, this **DOES NOT** catch sample sheet rows with missing animal IDs in 1 specific use case that meets 2
        conditions:
          1. the Animal ID is missing on a row in the Samples sheet
          2. There are no empty rows between populated rows in the Animals sheet
        This is because the sheet merge completely ignores those Samples sheet rows.  If the Anoimals sheet **DOES
        HAVE** empty rows between populated rows, the Samples sheet rows with missing Animal IDs will be merged with
        the empty Animals sheet row and produce the SheetMergeError raised inside this method.

        What this means is that there is a silent case where Sample sheet rows with missing Animal IDs are sometimes
        silently ignored.
        """
        animal_name_header = getattr(self.headers, "ANIMAL_NAME")
        empty_animal_rows = []
        empty_animal_rows_with_vals = []

        for rowidx, row in enumerate(data):
            val = row[animal_name_header]
            row_has_vals = (
                len([v for v in row.values() if v is not None and v != ""]) > 0
            )
            if val is None or val == "":
                empty_animal_rows.append(rowidx)
                if row_has_vals:
                    empty_animal_rows_with_vals.append(rowidx)

        # This will allow us to skip rows that do not have an animal ID (which includes entirely empty rows)
        if len(empty_animal_rows) > 0:
            self.empty_animal_rows = empty_animal_rows

        # This will allow us to identify invalid rows (i.e. entirely empty is valid)
        if len(empty_animal_rows_with_vals) > 0:
            self.aggregated_errors_object.buffer_error(
                SheetMergeError(empty_animal_rows_with_vals, animal_name_header)
            )

    def check_required_values(self, rowidx, row):
        """
        Due to some rows being skipped in specific (but not precise) instances, required values must be checked first.
        C.I.P. A malformed file wasn't reporting problems because the rows were being skipped due to the fact that the
        tissue field was empty.
        """
        rqd_vals_tuple = self.RequiredSampleTableValues
        hdr_name_tuple = self.headers
        header_attrs = rqd_vals_tuple._fields

        # We will skip problematic rows that are either completely empty or are missing an animal ID
        if rowidx in self.empty_animal_rows:
            return

        # For each header attribute
        for hdr_attr in header_attrs:
            hdr_name = getattr(hdr_name_tuple, hdr_attr)
            val_reqd = getattr(rqd_vals_tuple, hdr_attr)

            # If the header is present in the row and it is required
            if hdr_name in self.headers_present and val_reqd:
                val = row[hdr_name]
                if val is None or val == "":
                    animal = self.getRowVal(row, "ANIMAL_NAME")
                    if "rows" in self.missing_values[hdr_name].keys():
                        self.missing_values[hdr_name]["rows"].append(rowidx + 1)
                        if animal not in self.missing_values[hdr_name]["animals"]:
                            self.missing_values[hdr_name]["animals"].append(animal)
                    else:
                        self.missing_values[hdr_name]["rows"] = [rowidx + 1]
                        self.missing_values[hdr_name]["animals"] = [animal]

    def getRowVal(self, row, header_attribute):
        # get the header value to use as a dict key for 'row'
        header = getattr(self.headers, header_attribute)
        val = None

        if header in self.headers_present:
            val = row[header]

            # This will make later checks of values easier
            if val == "":
                val = None

        return val
