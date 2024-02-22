from collections import namedtuple

from DataRepo.models import Protocol
from DataRepo.utils.loader import TraceBaseLoader


class ProtocolsLoader(TraceBaseLoader):
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    NAME_KEY = "NAME"
    CAT_KEY = "CATEGORY"
    DESC_KEY = "DESCRIPTION"

    # The tuple used to store different kinds of data per column at the class level
    TableHeaders = namedtuple(
        "TableHeaders",
        [
            "NAME",
            "CATEGORY",
            "DESCRIPTION",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DefaultHeaders = TableHeaders(
        NAME="Name",
        CATEGORY="Category",
        DESCRIPTION="Description",
    )

    # Whether each column is required to be present of not
    RequiredHeaders = TableHeaders(
        NAME=True,
        CATEGORY=False,
        DESCRIPTION=True,
    )

    # Default values to use when a row in the given column doesn;t have a value in it
    DefaultValues = TableHeaders(
        NAME=None,
        CATEGORY=Protocol.ANIMAL_TREATMENT,
        DESCRIPTION=None,
    )

    # Whether a value for an row in a column is required or not (note that defined DefaultValues will satisfy this)
    RequiredValues = TableHeaders(
        NAME=True,
        CATEGORY=True,  # Required by the model field, but effectively not reqd, bec. it's defaulted
        DESCRIPTION=False,
    )

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    ColumnTypes = {
        NAME_KEY: str,
        CAT_KEY: str,
        DESC_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    UniqueColumnConstraints = [[NAME_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToHeaderKey = {
        "Protocol": {
            "name": NAME_KEY,
            "category": CAT_KEY,
            "description": DESC_KEY,
        },
    }

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Protocol]

    def load_data(self):
        """Loads the tissue table from the dataframe.

        Args:
            None

        Raises:
            Nothing (see TraceBaseLoader._loader() wrapper for exceptions raised by the automatically applied wrapping
                method)

        Returns:
            Nothing (see TraceBaseLoader._loader() wrapper for return value from the automatically applied wrapping
                method)
        """
        for _, row in self.df.iterrows():
            rec_dict = None

            try:
                name = self.get_row_val(row, self.headers.NAME)
                category = self.get_row_val(row, self.headers.CATEGORY)
                description = self.get_row_val(row, self.headers.DESCRIPTION)
                print(f"category: {category}")
                rec_dict = {
                    "name": name,
                    "category": category,
                    "description": description,
                }

                # get_row_val can add to skip_row_indexes when there is a missing required value
                if self.is_skip_row():
                    continue

                # Try and get the protocol
                rec, created = Protocol.objects.get_or_create(**rec_dict)

                # If no protocol was found, create it
                if created:
                    rec.full_clean()
                    self.created()
                else:
                    self.existed()

            except Exception as e:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                self.handle_load_db_errors(e, Protocol, rec_dict)
                self.errored()
