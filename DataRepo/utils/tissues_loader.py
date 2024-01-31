from collections import namedtuple

from DataRepo.models import Tissue
from DataRepo.utils.loader import TraceBaseLoader


class TissuesLoader(TraceBaseLoader):
    NAME_KEY = "NAME"
    DESC_KEY = "DESCRIPTION"

    TableHeaders = namedtuple(
        "TableHeaders",
        [
            NAME_KEY,
            DESC_KEY,
        ],
    )
    DefaultHeaders = TableHeaders(
        NAME="Tissue",
        DESCRIPTION="Description",
    )
    RequiredHeaders = TableHeaders(
        NAME=True,
        DESCRIPTION=True,
    )
    RequiredValues = RequiredHeaders
    ColumnTypes = {
        NAME_KEY: str,
        DESC_KEY: str,
    }
    UniqueColumnConstraints = [[NAME_KEY]]
    FieldToHeaderKey = {
        "Tissue": {
            "name": NAME_KEY,
            "description": DESC_KEY,
        },
    }

    def __init__(
        self,
        tissues,
        headers=None,
        dry_run=True,
        defer_rollback=False,  # DO NOT USE MANUALLY - A PARENT SCRIPT MUST HANDLE THE ROLLBACK.
        sheet=None,
        file=None,
    ):
        # Data
        self.tissues = tissues

        super().__init__(
            tissues,
            headers=headers,
            dry_run=dry_run,
            defer_rollback=defer_rollback,
            sheet=sheet,
            file=file,
            models=[Tissue],
        )

    @TraceBaseLoader.loader
    def load_data(self):
        for index, row in self.tissues.iterrows():
            self.set_row_index(index)

            if index in self.get_skip_row_indexes():
                continue

            try:
                rec_dict = {
                    "name": self.getRowVal(row, self.headers.NAME),
                    "description": self.getRowVal(row, self.headers.DESCRIPTION),
                }

                # getRowVal can add to skip_row_indexes when there is a missing required value
                if index in self.get_skip_row_indexes():
                    continue

                tissue, created = Tissue.objects.get_or_create(**rec_dict)

                if created:
                    tissue.full_clean()
                    self.created()
                else:
                    self.existed()

            except Exception as e:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                self.handle_load_db_errors(e, Tissue, rec_dict)
                self.errored()
