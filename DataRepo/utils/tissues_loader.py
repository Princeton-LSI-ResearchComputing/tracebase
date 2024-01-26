from DataRepo.models import Tissue
from DataRepo.utils.loader import TraceBaseLoader


class TissuesLoader(TraceBaseLoader):
    NAME_HEADER = "Tissue"
    DESC_HEADER = "Description"
    ALL_HEADERS = [NAME_HEADER, DESC_HEADER]
    REQUIRED_HEADERS = ALL_HEADERS
    REQUIRED_VALUES = ALL_HEADERS
    UNIQUE_COLUMN_CONSTRAINTS = [[NAME_HEADER]]
    FLD_TO_COL = {
        "name": NAME_HEADER,
        "description": DESC_HEADER,
    }

    def __init__(
        self,
        tissues,
        dry_run=True,
        defer_rollback=False,  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in atomic transact in caller)
        sheet=None,
        file=None,
    ):
        # Data
        self.tissues = tissues

        super().__init__(
            tissues,
            all_headers=self.ALL_HEADERS,
            reqd_headers=self.REQUIRED_HEADERS,
            reqd_values=self.REQUIRED_VALUES,
            unique_constraints=self.UNIQUE_COLUMN_CONSTRAINTS,
            dry_run=dry_run,
            defer_rollback=defer_rollback,
            sheet=sheet,
            file=file,
            models=[Tissue],
        )

    @TraceBaseLoader.loader
    def load_tissue_data(self):
        for index, row in self.tissues.iterrows():
            if index in self.get_skip_row_indexes():
                continue

            # Index starts at 0, headers are on row 1
            rownum = index + 2

            try:
                rec_dict = {
                    "name": self.getRowVal(row, self.NAME_HEADER),
                    "description": self.getRowVal(row, self.DESC_HEADER),
                }

                tissue, created = Tissue.objects.get_or_create(**rec_dict)

                if created:
                    tissue.full_clean()
                    self.created()
                else:
                    self.existed()

            except Exception as e:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                self.handle_load_db_errors(
                    e, Tissue, rec_dict, rownum=rownum, fld_to_col=self.FLD_TO_COL
                )
                self.errored()
