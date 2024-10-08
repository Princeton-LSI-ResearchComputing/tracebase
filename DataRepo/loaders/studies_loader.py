from collections import namedtuple
from typing import Dict

from django.db import transaction

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.models import Study
from DataRepo.utils.exceptions import RollbackException


class StudiesLoader(TableLoader):
    """Loads the Study Model from a dataframe (obtained from a table-like file).

    NOTE: This DOES NOT load an entire study (or multiple entire studies).
    """

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    NAME_KEY = "NAME"
    DESC_KEY = "DESCRIPTION"

    DataSheetName = "Study"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "NAME",
            "DESCRIPTION",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        NAME="Name",
        DESCRIPTION="Description",
    )

    # List of required header keys
    DataRequiredHeaders = [
        NAME_KEY,
        DESC_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = [
        NAME_KEY,
    ]

    # No DataDefaultValues needed

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes: Dict[str, type] = {
        NAME_KEY: str,
        DESC_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [[NAME_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        Study.__name__: {
            "name": NAME_KEY,
            "description": DESC_KEY,
        },
    }

    # No FieldToDataValueConverter needed

    DataColumnMetadata = DataTableHeaders(
        NAME=TableColumn.init_flat(field=Study.name, name=DataHeaders.NAME),
        DESCRIPTION=TableColumn.init_flat(
            field=Study.description,
            name=DataHeaders.DESCRIPTION,
            # TODO: Replace "Animals" and "Study" with class references once circular import has been figured out
            reference=ColumnReference(
                sheet="Animals",
                header="Study",
            ),
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Study]

    def load_data(self):
        """Loads the study table from the dataframe.

        Args:
            None
        Raises:
            Nothing (explicitly)
        Returns:
            Nothing
        """
        for _, row in self.df.iterrows():
            try:
                self.get_or_create_study(row)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

    @transaction.atomic
    def get_or_create_study(self, row):
        """Get or create a study record and buffer exceptions before raising.
        Args:
            row (pandas dataframe row)
        Raises:
            RollbackException
        Returns:
            Nothing
        """
        rec_dict = None

        try:
            name = self.get_row_val(row, self.headers.NAME)
            description = self.get_row_val(row, self.headers.DESCRIPTION)

            # missing required values update the skip_row_indexes before load_data is even called, and get_row_val sets
            # the current row index
            if self.is_skip_row():
                self.errored()
                return

            rec_dict = {
                "name": name,
                "description": description,
            }

            study_rec, created = Study.objects.get_or_create(**rec_dict)

            if created:
                study_rec.full_clean()
                self.created()
            else:
                self.existed()

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Study, rec_dict)
            self.errored()
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise RollbackException()
