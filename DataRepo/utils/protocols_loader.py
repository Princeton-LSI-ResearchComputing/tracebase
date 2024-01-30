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
            NAME_KEY,
            CAT_KEY,
            DESC_KEY,
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
        CATEGORY=True,  # Header not reqd, bec. can be defaulted
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

    def __init__(
        self,
        protocols,
        headers=None,
        defaults=None,
        dry_run=True,
        defer_rollback=False,
        sheet=None,
        file=None,
    ):
        # Data
        self.protocols = protocols

        super().__init__(
            protocols,
            headers=headers,
            defaults=defaults,
            dry_run=dry_run,
            defer_rollback=defer_rollback,
            sheet=sheet,
            file=file,
            models=[Protocol],
        )

    @TraceBaseLoader.loader
    def load_protocol_data(self):
        for index, row in self.protocols.iterrows():
            if index in self.get_skip_row_indexes():
                continue

            # Index starts at 0, headers are on row 1
            rownum = index + 2

            try:
                rec_dict = {
                    "name": self.getRowVal(row, self.headers.NAME),
                    "category": self.getRowVal(row, self.headers.CATEGORY),
                    "description": self.getRowVal(row, self.headers.DESCRIPTION),
                }

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
                self.handle_load_db_errors(e, Protocol, rec_dict, rownum)
                self.errored()
