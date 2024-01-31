from collections import namedtuple

from DataRepo.models import Study
from DataRepo.utils.loader import TraceBaseLoader


class StudyTableLoader(TraceBaseLoader):
    TableHeaders = namedtuple(
        "TableHeaders",
        [
            "CODE",
            "NAME",
            "DESCRIPTION",
        ],
    )
    DefaultHeaders = TableHeaders(
        CODE="Study ID",
        NAME="Name",
        DESCRIPTION="Description",
    )
    RequiredHeaders = TableHeaders(
        CODE=True,
        NAME=True,
        DESCRIPTION=True,
    )
    RequiredValues = RequiredHeaders
    UniqueColumnConstraints = [["CODE"]], ["NAME"]
    FieldToHeaderKey = {
        "Study": {
            "code": "CODE",
            "name": "NAME",
            "description": "DESCRIPTION",
        },
    }

    def __init__(
        self,
        study_table_df,
        headers=None,
        sheet=None,
        file=None,
        dry_run=True,
        defer_rollback=False,  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in atomic transact in caller)
    ):
        # Data
        self.study_table_df = study_table_df

        super().__init__(
            study_table_df,
            headers=headers,
            dry_run=dry_run,
            defer_rollback=defer_rollback,
            sheet=sheet,
            file=file,
            models=[Study],
        )

    @TraceBaseLoader.loader
    def load_study_table(self):
        for index, row in self.study_table_df.iterrows():
            self.set_row_index(index)

            try:
                rec_dict = {
                    "code": self.getRowVal(row, self.headers.CODE),
                    "name": self.getRowVal(row, self.headers.NAME),
                    "description": self.getRowVal(row, self.headers.DESCRIPTION),
                }

                # getRowVal can add to skip_row_indexes when there is a missing required value
                if index in self.get_skip_row_indexes():
                    continue

                study_rec, created = Study.objects.get_or_create(**rec_dict)

                if created:
                    study_rec.full_clean()
                    self.created()
                else:
                    self.existed()

            except Exception as e:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                self.handle_load_db_errors(e, Study, rec_dict)
                self.errored()
