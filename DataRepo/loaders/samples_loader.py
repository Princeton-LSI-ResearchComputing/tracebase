from collections import namedtuple
from datetime import datetime, timedelta
from typing import Dict

from django.db import transaction

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.tissues_loader import TissuesLoader
from DataRepo.models import Animal, MaintainedModel, Sample, Tissue
from DataRepo.models.researcher import (
    could_be_variant_researcher,
    get_researchers,
)
from DataRepo.utils.exceptions import (
    DateParseError,
    NewResearcher,
    RollbackException,
)
from DataRepo.utils.file_utils import string_to_datetime


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
    DataRequiredValues = DataRequiredHeaders

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

    DataColumnMetadata = DataTableHeaders(
        SAMPLE=TableColumn.init_flat(name=DataHeaders.SAMPLE, field=Sample.name),
        HANDLER=TableColumn.init_flat(
            name=DataHeaders.HANDLER, field=Sample.researcher
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
            guidance=(
                f"Select a {DataHeaders.TISSUE} from the dropdowns in this column.  The dropdowns are populated "
                f"by the {TissuesLoader.DataHeaders.NAME} column in the {TissuesLoader.DataSheetName} sheet."
            ),
            type=str,
            dynamic_choices=ColumnReference(
                loader_class=TissuesLoader,
                loader_header_key=TissuesLoader.NAME_KEY,
            ),
        ),
        ANIMAL=TableColumn.init_flat(
            name=DataHeaders.ANIMAL,
            field=Sample.animal,
            guidance=(
                f"Select a {DataHeaders.ANIMAL} from the dropdowns in this column.  The dropdowns are populated "
                f"by the {AnimalsLoader.DataHeaders.NAME} column in the {AnimalsLoader.DataSheetName} sheet."
            ),
            type=str,
            dynamic_choices=ColumnReference(
                loader_class=AnimalsLoader,
                loader_header_key=AnimalsLoader.NAME_KEY,
            ),
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Sample]

    def __init__(self, *args, **kwargs):
        """Constructor.

        Args:
            Superclass Args:
                df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
                dry_run (Optional[boolean]) [False]: Dry run mode.
                defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT
                    MUST HANDLE THE ROLLBACK.
                data_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                defaults_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                file (Optional[str]) [None]: File name (for error reporting).
                user_headers (Optional[dict]): Header names by header key.
                defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
                defaults_file (Optional[str]) [None]: Defaults file name (None if the same as infile).
                headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
                defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.
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

            # Get or create the animal record
            try:
                self.get_or_create_sample(row, animal, tissue)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

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
                    file=self.file,
                    sheet=self.sheet,
                    rownum=self.rownum,
                    column=self.headers.HANDLER,
                ),
                is_fatal=self.validate,
            )
            self.warned(Sample.__name__)

        date_str = self.get_row_val(row, self.headers.DATE)
        try:
            # A None value will cause a skip (at the is_skip_row check) below, since the field is required
            if date_str is not None:
                date = string_to_datetime(date_str)
        except DateParseError as dpe:
            # This is a required field, so since we've buffered an exception, let's set a placeholder value and see if
            # we can catch more errors
            date = datetime.now()
            dpe.set_formatted_message(
                file=self.file,
                sheet=self.sheet,
                rownum=self.rownum,
                column=self.headers.DATE,
                suggestion=(
                    f"Setting a placeholder value of {date}, for now.  Note, this could cause a "
                    f"ConflictingValueError.  If so, you can ignore the {self.headers.DATE} conflicts."
                ),
            )
            self.aggregated_errors_object.buffer_exception(
                dpe,
                orig_exception=dpe.ve_exc,
            )
        except ValueError as ve:
            # This is a required field, so since we've buffered an exception, let's set a placeholder value and see if
            # we can catch more errors
            date = datetime.now()
            self.buffer_infile_exception(
                ve,
                column=self.headers.DATE,
                suggestion=(
                    f"Setting a placeholder value of {date}, for now.  Note, this could cause a "
                    f"ConflictingValueError.  If so, you can ignore the {self.headers.DATE} conflicts."
                ),
            )

        time_collected_str = self.get_row_val(row, self.headers.DAYS_INFUSED)
        try:
            # A None value will cause a skip (at the is_skip_row check) below, since the field is required
            if time_collected_str is not None:
                time_collected = timedelta(minutes=time_collected_str)
        except Exception as e:
            # This is a required field, so since we've buffered an exception, let's set a placeholder value and see if
            # we can catch more errors
            time_collected = timedelta(minutes=0)
            self.buffer_infile_exception(
                e,
                column=self.headers.DAYS_INFUSED,
                suggestion=(
                    f"Setting a placeholder value of {time_collected}, for now.  Note, this could cause a "
                    f"ConflictingValueError.  If so, you can ignore the {self.headers.DAYS_INFUSED} conflicts."
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
