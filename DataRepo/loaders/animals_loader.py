from collections import namedtuple
from datetime import timedelta
from typing import Dict, Optional

from django.db import transaction

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.infusates_loader import InfusatesLoader
from DataRepo.loaders.protocols_loader import ProtocolsLoader
from DataRepo.loaders.studies_loader import StudiesLoader
from DataRepo.models import (
    Animal,
    AnimalLabel,
    Infusate,
    MaintainedModel,
    Protocol,
    Study,
)
from DataRepo.models.utilities import value_from_choices_label
from DataRepo.utils.exceptions import (
    MissingStudies,
    MissingTreatments,
    RecordDoesNotExist,
    RollbackException,
)
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs

AnimalStudy = Animal.studies.through


class AnimalsLoader(TableLoader):
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    NAME_KEY = "NAME"
    AGE_KEY = "AGE"
    SEX_KEY = "SEX"
    GENOTYPE_KEY = "GENOTYPE"
    WEIGHT_KEY = "WEIGHT"
    INFUSATE_KEY = "INFUSATE"
    INFUSIONRATE_KEY = "INFUSIONRATE"
    DIET_KEY = "DIET"
    FEEDINGSTATUS_KEY = "FEEDINGSTATUS"
    TREATMENT_KEY = "TREATMENT"
    STUDY_KEY = "STUDY"

    DataSheetName = "Animals"

    StudyDelimiter = ";"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "NAME",
            "AGE",
            "SEX",
            "GENOTYPE",
            "WEIGHT",
            "INFUSATE",
            "INFUSIONRATE",
            "DIET",
            "FEEDINGSTATUS",
            "TREATMENT",
            "STUDY",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        NAME="Animal Name",
        AGE="Age",
        SEX="Sex",
        GENOTYPE="Genotype",
        WEIGHT="Weight",
        INFUSATE="Infusate",
        INFUSIONRATE="Infusion Rate",
        DIET="Diet",
        FEEDINGSTATUS="Feeding Status",
        TREATMENT="Treatment",
        STUDY="Study",
    )

    # List of required header keys
    DataRequiredHeaders = [NAME_KEY, GENOTYPE_KEY, INFUSATE_KEY, STUDY_KEY]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes: Dict[str, type] = {
        NAME_KEY: str,
        AGE_KEY: int,
        SEX_KEY: str,
        GENOTYPE_KEY: str,
        WEIGHT_KEY: float,
        INFUSATE_KEY: str,
        INFUSIONRATE_KEY: float,
        DIET_KEY: str,
        FEEDINGSTATUS_KEY: str,
        TREATMENT_KEY: str,
        STUDY_KEY: str,
    }

    # No DataDefaultValues needed

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [[NAME_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        Animal.__name__: {
            "name": NAME_KEY,
            "infusate": INFUSATE_KEY,
            "infusion_rate": INFUSIONRATE_KEY,
            "genotype": GENOTYPE_KEY,
            "body_weight": WEIGHT_KEY,
            "age": AGE_KEY,
            "sex": SEX_KEY,
            "diet": DIET_KEY,
            "feeding_status": FEEDINGSTATUS_KEY,
            "studies": STUDY_KEY,
            "treatment": TREATMENT_KEY,
        },
    }

    FieldToDataValueConverter = {
        Animal.__name__: {
            "age": lambda val: val.total_seconds() // 604800,
        },
    }

    DataColumnMetadata = DataTableHeaders(
        NAME=TableColumn.init_flat(
            name=DataHeaders.NAME,
            field=Animal.name,
            # TODO: Replace "Samples" and "Sample" below with a reference to its loader's DataSheetName and the
            # corresponding column, respectively
            # Cannot reference the SamplesLoader here (to include the name of its sheet and its tracer name column)
            # due to circular import
            reference=ColumnReference(
                header="Sample",
                sheet="Samples",
            ),
        ),
        INFUSIONRATE=TableColumn.init_flat(
            name=DataHeaders.INFUSIONRATE,
            field=Animal.infusion_rate,
            format="Units: ul/min/g (microliters/min/gram)",
        ),
        GENOTYPE=TableColumn.init_flat(
            name=DataHeaders.GENOTYPE, field=Animal.genotype, current_choices=True
        ),
        WEIGHT=TableColumn.init_flat(
            name=DataHeaders.WEIGHT,
            field=Animal.body_weight,
            format="Units: grams.",
        ),
        AGE=TableColumn.init_flat(
            name=DataHeaders.AGE,
            field=Animal.age,
            type=float,
            format="Units: weeks (integer or decimal).",
        ),
        SEX=TableColumn.init_flat(name=DataHeaders.SEX, field=Animal.sex),
        DIET=TableColumn.init_flat(
            name=DataHeaders.DIET, field=Animal.diet, current_choices=True
        ),
        FEEDINGSTATUS=TableColumn.init_flat(
            name=DataHeaders.FEEDINGSTATUS,
            field=Animal.feeding_status,
            guidance=(
                "Note that the drop-downs are populated by existing values in the database, to encourage consistency.  "
                "You may add new values."
            ),
            current_choices=True,
        ),
        INFUSATE=TableColumn.init_flat(
            name=DataHeaders.INFUSATE,
            field=Animal.infusate,
            type=str,
            dynamic_choices=ColumnReference(
                loader_class=InfusatesLoader,
                loader_header_key=InfusatesLoader.NAME_KEY,
            ),
        ),
        STUDY=TableColumn.init_flat(
            name=DataHeaders.STUDY,
            field=Animal.studies,
            format=(
                "Note that an animal can belong to multiple studies.  As such, this is a delimited field.  Multiple "
                f"{DataHeaders.STUDY} records can be entered using the delimiter: '{StudyDelimiter}', but the "
                "dropdowns only work for individual studies, thus multiple studies must be manually entered."
            ),
            type=str,
            dynamic_choices=ColumnReference(
                loader_class=StudiesLoader,
                loader_header_key=StudiesLoader.NAME_KEY,
                # TODO: See if an option can be supplied to auto-fill combinations with a delimiter
            ),
        ),
        TREATMENT=TableColumn.init_flat(
            name=DataHeaders.TREATMENT,
            field=Animal.treatment,
            type=str,
            dynamic_choices=ColumnReference(
                loader_class=ProtocolsLoader,
                header=ProtocolsLoader.DataHeadersExcel.NAME,
            ),
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Animal, AnimalLabel, AnimalStudy]

    def __init__(self, *args, **kwargs):
        """Constructor.

        Args:
            Superclass Args:
                df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
                dry_run (Optional[boolean]) [False]: Dry run mode.
                defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT
                    MUST HANDLE THE ROLLBACK.
                data_sheet (Optional[str]): Sheet name (for error reporting).
                defaults_sheet (Optional[str]): Sheet name (for error reporting).
                file (Optional[str]): File path.
                filename (Optional[str]): Filename (for error reporting).
                user_headers (Optional[dict]): Header names by header key.
                defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
                defaults_file (Optional[str]): Defaults file name (None if the same as infile).
                headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
                defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.
                extra_headers (Optional[List[str]]): Use for dynamic headers (different in every file).  To allow any
                    unknown header, supply an empty list.
                _validate (bool): If true, runs in validate mode, perhaps better described as "non-curator mode".  This
                    is intended for use by the web validation interface.  It's similar to dry-run mode, in that it never
                    commits anything, but it also raises warnings as fatal (so they can be reported through the web
                    interface and seen by researchers, among other behaviors specific to non-privileged users).
            Derived (this) class Args:
                study_delimiter (Optional[str]) [;]: Study name string delimiter.
        Exceptions:
            None
        Returns:
            None
        """
        self.study_delimiter = kwargs.pop("study_delimiter", self.StudyDelimiter)
        super().__init__(*args, **kwargs)

    @MaintainedModel.defer_autoupdates()
    def load_data(self):
        """Loads the Infusate and InfusateTracer tables from the dataframe (self.df).

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        for _, row in self.df.iterrows():
            animal = None

            # Get the existing infusate and treatment
            infusate = self.get_infusate(row)
            treatment = self.get_treatment(row)

            # Get or create the animal record
            try:
                animal, _ = self.get_or_create_animal(row, infusate, treatment)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

            # Get or create the AnimalLabel records
            for element in self.get_labeled_elements(infusate):
                try:
                    self.get_or_create_animal_label(animal, element)
                except RollbackException:
                    # Exception handling was handled in get_or_create_*
                    # Continue processing rows to find more errors
                    pass

            # Link the animal to the study(/ies)
            for study in self.get_studies(row):
                try:
                    self.get_or_create_animal_study_link(animal, study)
                except RollbackException:
                    # Exception handling was handled in get_or_create_*
                    # Continue processing rows to find more errors
                    pass

        self.repackage_exceptions()

    @transaction.atomic
    def get_or_create_animal(
        self, row, infusate: Infusate, treatment: Optional[Protocol] = None
    ):
        """Get or create an Animal record.

        Args:
            row (pd.Series)
            infusate (Infusate)
            treatment (Optional[Protocol])
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                None
        Returns:
            rec (Animal)
            created (boolean)
        """
        created = False
        rec = None

        name = self.get_row_val(row, self.headers.NAME)
        infusion_rate = self.get_row_val(row, self.headers.INFUSIONRATE)
        genotype = self.get_row_val(row, self.headers.GENOTYPE)
        weight = self.get_row_val(row, self.headers.WEIGHT)
        age = self.get_row_val(row, self.headers.AGE)
        sex = self.get_row_val(row, self.headers.SEX)
        diet = self.get_row_val(row, self.headers.DIET)
        feeding_status = self.get_row_val(row, self.headers.FEEDINGSTATUS)

        if infusate is None or self.is_skip_row():
            self.skipped(Animal.__name__)
            return rec, created

        # Required fields
        rec_dict = {
            "name": name,
            "genotype": genotype,
            "infusate": infusate,
        }

        errored = False

        # Optional fields
        if infusion_rate is not None:
            # TODO: Make it possible to parse and use units for infusion_rate
            rec_dict["infusion_rate"] = infusion_rate
        if genotype is not None:
            rec_dict["genotype"] = genotype
        if weight is not None:
            # TODO: Make it possible to parse and use units for weight
            rec_dict["body_weight"] = weight
        if age is not None:
            try:
                # TODO: Make it possible to parse and use units for age(/duration)
                rec_dict["age"] = timedelta(weeks=age)
            except Exception as e:
                self.buffer_infile_exception(e, column=self.headers.AGE)
                errored = True
                # Press on to find more errors...
        if sex is not None:
            try:
                rec_dict["sex"] = value_from_choices_label(sex, Animal.SEX_CHOICES)
            except Exception as e:
                self.buffer_infile_exception(e, column=self.headers.SEX)
                errored = True
                # Press on to find more errors...
        if diet is not None:
            rec_dict["diet"] = diet
        if feeding_status is not None:
            rec_dict["feeding_status"] = feeding_status
        if treatment is not None:
            rec_dict["treatment"] = treatment

        try:
            rec, created = Animal.objects.get_or_create(**rec_dict)
            if errored:
                self.errored(Animal.__name__)
            elif created:
                rec.full_clean()
                self.created(Animal.__name__)
            else:
                self.existed(Animal.__name__)
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Animal, rec_dict)
            self.errored(Animal.__name__)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise RollbackException()

        if errored:
            # The timedelta error should cause a rollback, if no other exception occurred.
            raise RollbackException()

        return rec, created

    def get_infusate(self, row):
        """Get an Infusate record.

        Args:
            row (pd.Series)
        Exceptions:
            None
        Returns:
            rec (Optional[Infusate])
        """
        rec = None
        name = self.get_row_val(row, self.headers.INFUSATE)

        if name is None:
            # There should have been a RequiredColumnHeader/Value error already, if we get here, so just return None
            return rec

        query_dict = {"name": name}

        try:
            # This will only work for pre-existing records, but it produces a simpler error.  Records created during the
            # current load will not have names yet (due to deferred autoupdates).
            rec = Infusate.objects.get(**query_dict)
        except Exception as e:
            try:
                # The names from the sheet are populated by the database, but the user can enter their own.  The
                # database enters concentrations using significant figures (see
                # Infusate.CONCENTRATION_SIGNIFICANT_FIGURES).  If the user entered their own data, they could have used
                # more than the significant digits than were saved in the name when the infusates were loaded, so this
                # is a fallback that uses the number entered in the name compared to the actual data in the database (as
                # opposed to the formatted name in the database).
                infusate_data = parse_infusate_name_with_concs(name)
                rec = Infusate.objects.get_infusate(infusate_data)
                if rec is None:
                    self.handle_load_db_errors(e, Infusate, query_dict)
                    self.add_skip_row_index()
            except Exception as e2:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                # This also updates the skip row indexes
                self.handle_load_db_errors(e2, Infusate, query_dict)
                self.add_skip_row_index()
        return rec

    def get_treatment(self, row):
        """Get a Protocol (treatment) record.

        Args:
            row (pd.Series)
        Exceptions:
            None
        Returns:
            rec (Optional[Protocol])
        """
        rec = None
        name = self.get_row_val(row, self.headers.TREATMENT)

        if name is None:
            # The treatment column is optional
            return rec

        query_dict = {"name": name}

        try:
            rec = Protocol.objects.get(**query_dict)
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Protocol, query_dict)
            # Treatment is not a required field, so no need to add to skip rows

        return rec

    def get_studies(self, row):
        """Get Study records by name.

        Args:
            row (pd.Series)
        Exceptions:
            None
        Returns:
            recs (List[Optional[Study]])
        """
        recs = []
        names_str = self.get_row_val(row, self.headers.STUDY)

        if names_str is None:
            # Appending None so that the skipped count will get incremented once.
            recs.append(None)
            return recs

        for name in names_str.split(self.StudyDelimiter):
            query_dict = {"name": name.strip()}

            try:
                rec = Study.objects.get(**query_dict)
                recs.append(rec)
            except Exception as e:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                # This also updates the skip row indexes
                self.handle_load_db_errors(e, Study, query_dict)
                # Append None so the stats will get updated
                recs.append(None)

        return recs

    @transaction.atomic
    def get_or_create_animal_study_link(
        self, animal: Optional[Animal], study: Optional[Study]
    ):
        """Get or create an AnimalStudy record.

        Args:
            animal (Optional[Animal])
            study (Optional[Study])
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                None
        Returns:
            rec (Optional[AnimalStudy])
            created (boolean)
        """
        rec = None
        created = False

        if animal is None or study is None or self.is_skip_row():
            self.skipped(AnimalStudy.__name__)
            return rec, created

        try:
            rec, created = animal.get_or_create_study_link(study)
        except Exception as e:
            # This is the effective rec_dict
            rec_dict = {
                "animal": animal,
                "study": study,
            }
            self.handle_load_db_errors(e, AnimalStudy, rec_dict)
            self.errored(AnimalStudy.__name__)
            raise RollbackException()

        if created:
            self.created(AnimalStudy.__name__)
            # No need to call full clean.
        else:
            self.existed(AnimalStudy.__name__)

        return rec, created

    def get_labeled_elements(self, infusate: Optional[Infusate]):
        """Retrieve all the labeled elements from the supplied infusate.

        Args:
            infusate Optional[Infusate]
        Exceptions:
            None
        Returns:
            elements (List[Optional[str]])
        """
        # Including None when empty so that the skipped count will get incremented once.
        return [None] if infusate is None else infusate.tracer_labeled_elements

    @transaction.atomic
    def get_or_create_animal_label(
        self, animal: Optional[Animal], element: Optional[str]
    ):
        """Get or create an AnimalLabel record.

        Args:
            animal (Optional[Animal])
            element (Optional[str])
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                None
        Returns:
            rec (Optional[AnimalLabel])
            created (bool)
        """
        rec = None
        created = False

        if animal is None or element is None or self.is_skip_row():
            self.skipped(AnimalLabel.__name__)
            return rec, created

        rec_dict = {
            "animal": animal,
            "element": element,
        }

        try:
            rec, created = AnimalLabel.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(AnimalLabel.__name__)
            else:
                self.existed(AnimalLabel.__name__)
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, AnimalLabel, rec_dict)
            self.errored(AnimalLabel.__name__)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise RollbackException()

        return rec, created

    def repackage_exceptions(self):
        """Summarize missing treatments.

        Args:
            None
        Exceptions:
            Raises:
                None
            Buffers:
                MissingTreatments
        Returns:
            None
        """
        # Summarize missing tissues
        tdnes = self.aggregated_errors_object.remove_matching_exceptions(
            RecordDoesNotExist, "model", Protocol
        )
        if len(tdnes) > 0:
            cross_sheet_col_ref = (
                self.DataColumnMetadata.TREATMENT.value.dynamic_choices
            )
            sheet = self.sheet
            if sheet is None:
                sheet = self.DataSheetName
            self.aggregated_errors_object.buffer_error(
                MissingTreatments(
                    tdnes,
                    suggestion=(
                        f"{self.headers.TREATMENT}s in the '{sheet}' sheet must be loaded into the database prior to "
                        f"animal loading.  Please be sure to add each missing '{self.headers.TREATMENT}' to the "
                        f"'{cross_sheet_col_ref.header}' column in the '{cross_sheet_col_ref.sheet}' sheet in your "
                        "submission."
                    ),
                ),
            )

        # Summarize missing studies
        sdnes = self.aggregated_errors_object.remove_matching_exceptions(
            RecordDoesNotExist, "model", Study
        )
        if len(sdnes) > 0:
            cross_sheet_col_ref = self.DataColumnMetadata.STUDY.value.dynamic_choices
            sheet = self.sheet
            if sheet is None:
                sheet = self.DataSheetName
            self.aggregated_errors_object.buffer_error(
                MissingStudies(
                    sdnes,
                    suggestion=(
                        f"Please be sure to add each missing '{self.headers.STUDY}' to the "
                        f"'{cross_sheet_col_ref.header}' column in the '{cross_sheet_col_ref.sheet}' sheet in your "
                        "submission."
                    ),
                ),
            )
