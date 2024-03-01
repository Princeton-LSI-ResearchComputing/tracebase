from collections import namedtuple

from DataRepo.models import Tissue
from DataRepo.utils.table_loader import TraceBaseLoader


class TissuesLoader(TraceBaseLoader):
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

    # Whether each column is required to be present of not
    DataRequiredHeaders = DataTableHeaders(
        NAME=True,
        DESCRIPTION=True,
    )

    # Whether a value for an row in a column is required or not (note that defined DataDefaultValues will satisfy this)
    DataRequiredValues = DataRequiredHeaders

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes = {
        NAME_KEY: str,
        DESC_KEY: str,
    }

    # No DataDefaultValues needed

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [[NAME_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        "Tissue": {
            "name": NAME_KEY,
            "description": DESC_KEY,
        },
    }

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
            except Exception:
                # Exception handling was handled in get_or_create_protocol
                # Continue processing rows to find more errors
                pass

    def get_or_create_tissue(self, row):
        """Get or create a study record and buffer exceptions before raising.

        Args:
            row (pandas dataframe row)

        Raises:
            Nothing (explicitly)

        Returns:
            Nothing
        """
        rec_dict = None

        try:
            name = self.get_row_val(row, self.headers.NAME)
            description = self.get_row_val(row, self.headers.DESCRIPTION)

            rec_dict = {
                "name": name,
                "description": description,
            }

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row():
                self.errored()
                return

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
            # Now that the exception has been handled, trigger a roolback of this record load attempt
            raise e
