from DataRepo.models import Tissue
from DataRepo.utils import TraceBaseLoader


class TissuesLoader(TraceBaseLoader):
    ALL_HEADERS = ["Tissue", "Description"]
    REQUIRED_HEADERS = ALL_HEADERS
    REQUIRED_VALUES = ALL_HEADERS
    UNIQUE_CONSTRAINTS = [["Tissue"]]
    FLD_TO_COL = {
        "name": "Tissue",
        "description": "Description",
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
            unique_constraints=self.UNIQUE_CONSTRAINTS,
            dry_run=dry_run,
            defer_rollback=defer_rollback,
            sheet=sheet,
            file=file,
        )

    @TraceBaseLoader.loader
    def load_tissue_data(self):
        for index, row in self.tissues.iterrows():
            # Index starts at 0, headers are on row 1
            rownum = index + 2

            try:
                rec_dict = {
                    "name": self.getRowVal(row, "Tissue"),
                    "description": self.getRowVal(row, "Description"),
                }

                # We will assume that the validation DB has up-to-date tissues
                tissue, created = Tissue.objects.get_or_create(**rec_dict)

                if created:
                    tissue.full_clean()
                    tissue.save()
                    self.created()
                else:
                    self.existed()

            except Exception as e:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                self.handle_load_db_errors(e, Tissue, rec_dict, rownum=rownum, fld_to_col=self.FLD_TO_COL)
                self.errored()
