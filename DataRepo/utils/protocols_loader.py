from collections import namedtuple

from DataRepo.models import Protocol
from DataRepo.utils.loader import TraceBaseLoader


class ProtocolsLoader(TraceBaseLoader):
    """
    Load the Protocols table
    """

    NAME_KEY = "NAME"
    CAT_KEY = "CATEGORY"
    DESC_KEY = "DESCRIPTION"

    TableHeaders = namedtuple(
        "TableHeaders",
        [
            "NAME",
            "CATEGORY",
            "DESCRIPTION",
        ],
    )
    DefaultHeaders = TableHeaders(
        NAME="Name",
        CATEGORY="Category",
        DESCRIPTION="Description",
    )
    RequiredHeaders = TableHeaders(
        NAME=True,
        CATEGORY=False,
        DESCRIPTION=True,
    )
    DefaultValues = TableHeaders(
        NAME=None,
        CATEGORY=Protocol.ANIMAL_TREATMENT,
        DESCRIPTION=None,
    )
    RequiredValues = TableHeaders(
        NAME=True,
        CATEGORY=True,  # Required by the model field, but effectively not reqd, bec. it's defaulted
        DESCRIPTION=False,
    )
    ColumnTypes = {
        NAME_KEY: str,
        CAT_KEY: str,
        DESC_KEY: str,
    }
    UniqueColumnConstraints = [[NAME_KEY]]
    FieldToHeaderKey = {
        "Protocol": {
            "name": NAME_KEY,
            "category": CAT_KEY,
            "description": DESC_KEY,
        },
    }
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
