from DataRepo.models import Tissue
from DataRepo.utils import TraceBaseLoader
from DataRepo.utils.exceptions import InfileDatabaseError


class TissuesLoader(TraceBaseLoader):
    ALL_HEADERS = ["Tissue", "Description"]
    REQUIRED_HEADERS = ALL_HEADERS
    REQUIRED_VALUES = ALL_HEADERS
    UNIQUE_CONSTRAINTS = [["Tissue"]]

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
                # Package IntegrityErrors and ValidationErrors with relevant details
                if not self.handle_load_db_errors(e, Tissue, rec_dict, rownum=rownum):
                    # If the error was not handled, buffer the original error
                    self.aggregated_errors_object.buffer_error(
                        InfileDatabaseError(
                            e, rec_dict, rownum=rownum, sheet=self.sheet, file=self.file
                        )
                    )
                self.errored()
