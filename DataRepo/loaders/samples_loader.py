from collections import namedtuple
from datetime import date, timedelta
from typing import Dict

from django.db import transaction

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.tissues_loader import TissuesLoader
from DataRepo.models import Animal, MaintainedModel, Sample, Tissue
from DataRepo.models.fcirc import FCirc
from DataRepo.models.researcher import (
    could_be_variant_researcher,
    get_researchers,
)
from DataRepo.utils.exceptions import (
    DateParseError,
    DurationError,
    MissingTissues,
    NewResearcher,
    NoTracers,
    RecordDoesNotExist,
    RollbackException,
)
from DataRepo.utils.file_utils import datetime_to_string, string_to_date

today = date.today


class SamplesLoader(TableLoader):
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    SAMPLE_KEY = "SAMPLE"
    DATE_KEY = "DATE"
    HANDLER_KEY = "HANDLER"
    TISSUE_KEY = "TISSUE"
    DAYS_INFUSED_KEY = "DAYS_INFUSED"
    ANIMAL_KEY = "ANIMAL"

    DataSheetName = "Samples"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "SAMPLE",
            "DATE",
            "HANDLER",
            "TISSUE",
            "DAYS_INFUSED",
            "ANIMAL",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        SAMPLE="Sample",
        DATE="Date Collected",
        HANDLER="Researcher Name",
        TISSUE="Tissue",
        DAYS_INFUSED="Collection Time",
        ANIMAL="Animal",
    )

    # List of required header keys
    DataRequiredHeaders = [
        SAMPLE_KEY,
        DATE_KEY,
        HANDLER_KEY,
        TISSUE_KEY,
        DAYS_INFUSED_KEY,
        ANIMAL_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = [
        SAMPLE_KEY,
        DATE_KEY,
        HANDLER_KEY,
        TISSUE_KEY,
        # DAYS_INFUSED_KEY,  # Not required in model
        ANIMAL_KEY,
    ]

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes: Dict[str, type] = {
        SAMPLE_KEY: str,
        # DATE_KEY: str,  # Pandas can automatically detect dates (though this also accepts a str)
        HANDLER_KEY: str,
        TISSUE_KEY: str,
        DAYS_INFUSED_KEY: float,
        ANIMAL_KEY: str,
    }

    # No DataDefaultValues needed

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [[SAMPLE_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        Sample.__name__: {
            "name": SAMPLE_KEY,
            "date": DATE_KEY,
            "researcher": HANDLER_KEY,
            "tissue": TISSUE_KEY,
            "time_collected": DAYS_INFUSED_KEY,
            "animal": ANIMAL_KEY,
        },
    }

    FieldToDataValueConverter = {
        Sample.__name__: {
            "time_collected": lambda val: val.total_seconds() // 60,
            "date": lambda val: datetime_to_string(val),
        },
    }

    DataColumnMetadata = DataTableHeaders(
        SAMPLE=TableColumn.init_flat(
            name=DataHeaders.SAMPLE,
            field=Sample.name,
            # TODO: Replace "Peak Annotation Details" and "Sample Name" with a loader class reference
            reference=ColumnReference(
                sheet="Peak Annotation Details",
                header="Sample Name",
            ),
        ),
        HANDLER=TableColumn.init_flat(
            name=DataHeaders.HANDLER, field=Sample.researcher, current_choices=True
        ),
        DATE=TableColumn.init_flat(
            name=DataHeaders.DATE,
            field=Sample.date,
            format="Format: YYYY-MM-DD.",
        ),
        DAYS_INFUSED=TableColumn.init_flat(
            name=DataHeaders.DAYS_INFUSED,
            field=Sample.time_collected,
            format="Units: minutes.",
        ),
        TISSUE=TableColumn.init_flat(
            name=DataHeaders.TISSUE,
            field=Sample.tissue,
            type=str,
            dynamic_choices=ColumnReference(
                loader_class=TissuesLoader,
                loader_header_key=TissuesLoader.NAME_KEY,
            ),
        ),
        ANIMAL=TableColumn.init_flat(
            name=DataHeaders.ANIMAL,
            field=Sample.animal,
            type=str,
            dynamic_choices=ColumnReference(
                loader_class=AnimalsLoader,
                loader_header_key=AnimalsLoader.NAME_KEY,
            ),
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [FCirc, Sample]

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
                None
        Exceptions:
            None
        Returns:
            None
        """
        self.known_researchers = get_researchers()
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
            # Get the existing animal and tissue
            animal = self.get_animal(row)
            tissue = self.get_tissue(row)
            sample = None

            # Get or create the animal record
            try:
                sample, _ = self.get_or_create_sample(row, animal, tissue)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

            if (
                sample is not None
                and sample._is_serum_sample()
                and sample.animal.infusate is not None
            ):
                count = 0
                for tracer in sample.animal.infusate.tracers.all():
                    for label in tracer.labels.all():
                        count += 1
                        try:
                            self.get_or_create_fcirc(sample, tracer, label.element)
                        except RollbackException:
                            # Exception handling was handled in get_or_create_*
                            # Continue processing rows to find more errors
                            pass
                if (
                    count == 0
                    and not self.aggregated_errors_object.exception_type_exists(
                        NoTracers
                    )
                ):
                    self.aggregated_errors_object.buffer_warning(
                        NoTracers(
                            message=(
                                "Unable to add FCirc records for serun samples because there are either no tracers or "
                                "no tracer label records associated with the source animal (e.g. animal "
                                f"'{sample.animal}')."
                            )
                        )
                    )

        self.repackage_exceptions()

    @transaction.atomic
    def get_or_create_sample(self, row, animal: Animal, tissue: Tissue):
        """Get or create a Sample record.

        Args:
            row (pd.Series)
            animal (Animal)
            tissue (Tissue)
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                None
        Returns:
            rec (Sample)
            created (boolean)
        """
        created = False
        rec = None

        name = self.get_row_val(row, self.headers.SAMPLE)

        researcher = self.get_row_val(row, self.headers.HANDLER)
        if researcher is not None and could_be_variant_researcher(
            researcher, known_researchers=self.known_researchers
        ):
            # Raised if in validate mode (so the web user will see it).  Just printed otherwise.
            self.aggregated_errors_object.buffer_warning(
                NewResearcher(
                    researcher,
                    known=self.known_researchers,
                    file=self.friendly_file,
                    sheet=self.sheet,
                    rownum=self.rownum,
                    column=self.headers.HANDLER,
                ),
                is_fatal=self.validate,
            )
            self.warned(Sample.__name__)

        date_str = self.get_row_val(row, self.headers.DATE)
        try:
            # A None value will have already caused a skip since the field is required (which will be checked by calling
            # is_skip_row below), but we're going to try this anyway here/now, despite the potential skip, so we can
            # check for more errors, in case it does have a value.
            if date_str is not None:
                date = string_to_date(date_str)
        except DateParseError as dpe:
            # This is a required field, so since we're buffering an exception, let's set a placeholder value and see if
            # we can catch more errors
            date = today()
            dpe.set_formatted_message(
                file=self.friendly_file,
                sheet=self.sheet,
                rownum=self.rownum,
                column=self.headers.DATE,
                suggestion=f"Setting '{date}' temporarily.",
            )
            self.aggregated_errors_object.buffer_exception(
                dpe,
                orig_exception=dpe.ve_exc,
            )
        except ValueError as ve:
            # This is a required field, so since we've buffered an exception, let's set a placeholder value and see if
            # we can catch more errors
            date = today()
            self.buffer_infile_exception(
                ve,
                column=self.headers.DATE,
                suggestion=f"Setting '{date}' temporarily.",
            )

        time_collected_str = str(self.get_row_val(row, self.headers.DAYS_INFUSED))
        try:
            time_collected = None
            # A None value will have already caused a skip since the field is required (which will be checked by calling
            # is_skip_row below), but we're going to try this anyway here/now, despite the potential skip, so we can
            # check for more errors, in case it does have a value.
            if time_collected_str not in self.none_vals:
                time_collected = timedelta(minutes=float(time_collected_str))
        except Exception as e:
            # This is a required field, so since we're buffering an exception, let's set a placeholder value and see if
            # we can catch more errors
            phv = 0
            time_collected = timedelta(minutes=phv)
            self.aggregated_errors_object.buffer_error(
                DurationError(
                    time_collected_str,
                    "minutes",
                    e,
                    file=self.friendly_file,
                    sheet=self.DataSheetName,
                    rownum=self.rownum,
                    column=self.headers.DAYS_INFUSED,
                    suggestion=f"Setting a default of {phv} minutes.",
                ),
            )

        if animal is None or tissue is None or self.is_skip_row():
            # An animal or tissue being None would have already buffered a required value error
            self.skipped(Sample.__name__)
            return rec, created

        # Required fields
        rec_dict = {
            "name": name,
            "date": date,
            "researcher": researcher,
            "tissue": tissue,
            "time_collected": time_collected,
            "animal": animal,
        }

        try:
            rec, created = Sample.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(Sample.__name__)
            else:
                self.existed(Sample.__name__)
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Sample, rec_dict)
            self.errored(Sample.__name__)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise RollbackException()

        return rec, created

    def get_animal(self, row):
        """Get an Animal record.

        Args:
            row (pd.Series)
        Exceptions:
            None
        Returns:
            rec (Optional[Animal])
        """
        rec = None
        name = self.get_row_val(row, self.headers.ANIMAL)

        if name is None:
            # There should have been a RequiredColumnHeader/Value error already, if we get here, so just return None
            return rec

        query_dict = {"name": name}

        try:
            rec = Animal.objects.get(**query_dict)
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(
                e, Animal, query_dict, columns=self.headers.ANIMAL
            )
            self.add_skip_row_index()

        return rec

    def get_tissue(self, row):
        """Get a Tissue record.

        Args:
            row (pd.Series)
        Exceptions:
            None
        Returns:
            rec (Optional[Protocol])
        """
        rec = None
        name = self.get_row_val(row, self.headers.TISSUE)

        if name is None:
            # There should have been a RequiredColumnHeader/Value error already, if we get here, so just return None
            return rec

        query_dict = {"name": name}

        try:
            rec = Tissue.objects.get(**query_dict)
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(
                e, Tissue, query_dict, columns=self.headers.TISSUE
            )
            self.add_skip_row_index()

        return rec

    @transaction.atomic
    def get_or_create_fcirc(self, sample, tracer, element):
        """Get or create an FCirc record.

        Args:
            sample (Sample)
            tracer (Tracer)
            element (str)
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                None
        Returns:
            rec (Sample)
            created (boolean)
        """
        rec = None
        created = False

        rec_dict = {
            "serum_sample": sample,
            "tracer": tracer,
            "element": element,
        }

        try:
            rec, created = FCirc.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(FCirc.__name__)
            else:
                self.existed(FCirc.__name__)
        except Exception as e:
            self.handle_load_db_errors(e, FCirc, rec_dict)
            self.errored(FCirc.__name__)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise RollbackException

        return rec, created

    def repackage_exceptions(self):
        """Summarize missing tissues.

        Args:
            None
        Exceptions:
            Raises:
                None
            Buffers:
                MissingTissues
        Returns:
            None
        """
        # Summarize missing tissues
        dnes = self.aggregated_errors_object.remove_matching_exceptions(
            RecordDoesNotExist, "model", Tissue
        )
        if len(dnes) > 0:
            cross_sheet_col_ref = self.DataColumnMetadata.TISSUE.value.dynamic_choices
            sheet = self.sheet
            if sheet is None:
                sheet = self.DataSheetName
            self.aggregated_errors_object.buffer_error(
                MissingTissues(
                    dnes,
                    suggestion=(
                        f"{self.headers.TISSUE}s in the '{sheet}' sheet must be loaded into the database prior to "
                        f"sample loading.  Please be sure to add each missing '{self.headers.TISSUE}' to the "
                        f"'{cross_sheet_col_ref.header}' in the '{cross_sheet_col_ref.sheet}' in your submission."
                    ),
                ),
            )
