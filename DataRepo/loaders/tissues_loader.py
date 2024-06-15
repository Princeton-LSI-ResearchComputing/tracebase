from collections import namedtuple
from typing import Dict

from DataRepo.loaders.base.table_column import TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.models import Tissue
from DataRepo.utils.exceptions import RollbackException


class TissuesLoader(TableLoader):
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    NAME_KEY = "NAME"
    DESC_KEY = "DESCRIPTION"

    DataSheetName = "Tissues"

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
        NAME="Tissue",
        DESCRIPTION="Description",
    )

    # List of required header keys
    DataRequiredHeaders = [
        NAME_KEY,
        DESC_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes: Dict[str, type] = {
        NAME_KEY: str,
        DESC_KEY: str,
    }

    # No DataDefaultValues needed

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [[NAME_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        Tissue.__name__: {
            "name": NAME_KEY,
            "description": DESC_KEY,
        },
    }

    DataColumnMetadata = DataTableHeaders(
        NAME=TableColumn.init_flat(field=Tissue.name),
        DESCRIPTION=TableColumn.init_flat(field=Tissue.description),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Tissue]

    def load_data(self):
        """Loads the tissue table from the dataframe.

        Args:
            None

        Raises:
            Nothing (explicitly)

        Returns:
            Nothing
        """
        for _, row in self.df.iterrows():
            try:
                self.get_or_create_tissue(row)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

    def get_or_create_tissue(self, row):
        """Get or create a tissue record and buffer exceptions before raising.
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

            tissue, created = Tissue.objects.get_or_create(**rec_dict)

            if created:
                tissue.full_clean()
                self.created()
            else:
                self.existed()

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Tissue, rec_dict)
            self.errored()
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise RollbackException()
