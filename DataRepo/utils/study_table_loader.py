from DataRepo.models import Study
from DataRepo.utils import TraceBaseLoader


class StudyTableLoader(TraceBaseLoader):
    ALL_HEADERS = ["study id", "name", "description"]
    REQUIRED_HEADERS = ALL_HEADERS
    REQUIRED_VALUES = ALL_HEADERS
    UNIQUE_CONSTRAINTS = [["study id"], ["name"]]
    FLD_TO_COL = {
        "code": "study id",
        "name": "name",
        "description": "Description",
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
            unique_constraints=self.UNIQUE_CONSTRAINTS,
            dry_run=dry_run,
            defer_rollback=defer_rollback,
            sheet=sheet,
            file=file,
        )

    @TraceBaseLoader.loader
    def load_study_table(self):
        for index, row in self.study_table_df.iterrows():
            # Index starts at 0, headers are on row 1
            rownum = index + 2

            try:
                rec_dict = {
                    "code": self.getRowVal(row, "study id"),
                    "name": self.getRowVal(row, "name"),
                    "description": self.getRowVal(row, "description"),
                }

                study_rec, created = Study.objects.get_or_create(**rec_dict)

                if created:
                    study_rec.full_clean()
                    study_rec.save()
                    self.created()
                else:
                    self.existed()

            except Exception as e:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                self.handle_load_db_errors(e, Study, rec_dict, rownum=rownum, fld_to_col=self.FLD_TO_COL)
                self.errored()
