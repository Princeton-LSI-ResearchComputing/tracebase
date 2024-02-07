from DataRepo.models import Study
from DataRepo.utils.loader import TraceBaseLoader


class StudyTableLoader(TraceBaseLoader):
    CODE_HEADER = "Study ID"
    NAME_HEADER = "Name"
    DESC_HEADER = "Description"
    ALL_HEADERS = [CODE_HEADER, NAME_HEADER, DESC_HEADER]
    REQUIRED_HEADERS = ALL_HEADERS
    REQUIRED_VALUES = ALL_HEADERS
    UNIQUE_COLUMN_CONSTRAINTS = [[CODE_HEADER], [NAME_HEADER]]
    FLD_TO_COL = {
        "code": CODE_HEADER,
        "name": NAME_HEADER,
        "description": DESC_HEADER,
    }

    def __init__(
        self,
        study_table_df,
        sheet=None,
        file=None,
        dry_run=True,
        defer_rollback=False,  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in atomic transact in caller)
    ):
        # Data
        self.study_table_df = study_table_df

        super().__init__(
            study_table_df,
            all_headers=self.ALL_HEADERS,
            reqd_headers=self.REQUIRED_HEADERS,
            reqd_values=self.REQUIRED_VALUES,
            unique_constraints=self.UNIQUE_COLUMN_CONSTRAINTS,
            dry_run=dry_run,
            defer_rollback=defer_rollback,
            sheet=sheet,
            file=file,
            models=[Study],
        )

    @TraceBaseLoader.loader
    def load_study_table(self):
        for index, row in self.study_table_df.iterrows():
            if index in self.get_skip_row_indexes():
                continue

            # Index starts at 0, headers are on row 1
            rownum = index + 2

            try:
                rec_dict = {
                    "code": self.getRowVal(row, self.CODE_HEADER),
                    "name": self.getRowVal(row, self.NAME_HEADER),
                    "description": self.getRowVal(row, self.DESC_HEADER),
                }

                study_rec, created = Study.objects.get_or_create(**rec_dict)

                if created:
                    study_rec.full_clean()
                    self.created()
                else:
                    self.existed()

            except Exception as e:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                self.handle_load_db_errors(
                    e, Study, rec_dict, rownum=rownum, fld_to_col=self.FLD_TO_COL
                )
                self.errored()
