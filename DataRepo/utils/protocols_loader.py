from django.core.exceptions import ValidationError

from DataRepo.models import Protocol
from DataRepo.utils.exceptions import InfileDatabaseError
from DataRepo.utils.loader import TraceBaseLoader


class ProtocolsLoader(TraceBaseLoader):
    """
    Load the Protocols table
    """

    NAME_HEADER = "Name"
    DESC_HEADER = "Description"
    CTGY_HEADER = "Category"
    ALL_HEADERS = [NAME_HEADER, CTGY_HEADER, DESC_HEADER]
    REQUIRED_HEADERS = [NAME_HEADER, DESC_HEADER]
    REQUIRED_VALUES = REQUIRED_HEADERS
    DEFAULT_VALUES = {
        CTGY_HEADER: Protocol.ANIMAL_TREATMENT,
    }
    UNIQUE_COLUMN_CONSTRAINTS = [[NAME_HEADER]]
    FLD_TO_COL = {
        "name": NAME_HEADER,
        "category": CTGY_HEADER,
        "description": DESC_HEADER,
    }

    def __init__(
        self,
        protocols,
        category=None,
        dry_run=True,
        defer_rollback=False,
        sheet=None,
        file=None,
    ):
        # Data
        self.protocols = protocols
        self.category = category

        super().__init__(
            protocols,
            all_headers=self.ALL_HEADERS,
            reqd_headers=self.REQUIRED_HEADERS,
            reqd_values=self.REQUIRED_VALUES,
            unique_constraints=self.UNIQUE_COLUMN_CONSTRAINTS,
            dry_run=dry_run,
            defer_rollback=defer_rollback,
            sheet=sheet,
            file=file,
            models=[Protocol],
        )

    @TraceBaseLoader.loader
    def load_protocol_data(self):
        batch_cat_err_occurred = False

        for index, row in self.protocols.iterrows():
            rownum = index + 2

            try:
                name = self.getRowVal(row, self.NAME_HEADER)
                category = self.getRowVal(row, self.CTGY_HEADER)
                description = self.getRowVal(row, self.DESC_HEADER)

                # The command line option overrides what's in the file
                if self.category:
                    category = self.category

                rec_dict = {
                    "name": name,
                    "category": category,
                    "description": description,
                }

                # Try and get the protocol
                protocol_rec, protocol_created = Protocol.objects.get_or_create(
                    **rec_dict
                )

                # If no protocol was found, create it
                if protocol_created:
                    protocol_rec.full_clean()
                    self.created()
                else:
                    self.existed()

            except ValidationError as ve:
                vestr = str(ve)
                if (
                    self.category is not None
                    and "category" in vestr
                    and "is not a valid choice" in vestr
                ):
                    # Only include a batch category error once
                    if not batch_cat_err_occurred:
                        self.aggregated_errors_object.buffer_error(
                            InfileDatabaseError(ve, rec_dict, rownum=rownum)
                        )
                    batch_cat_err_occurred = True
                else:
                    # Package errors (like IntegrityError and ValidationError) with relevant details
                    self.handle_load_db_errors(
                        ve,
                        Protocol,
                        rec_dict,
                        rownum=rownum,
                        fld_to_col=self.FLD_TO_COL,
                    )
                    self.errored()
                self.errored()
            except Exception as e:
                # Package errors (like IntegrityError and ValidationError) with relevant details
                self.handle_load_db_errors(
                    e, Protocol, rec_dict, rownum=rownum, fld_to_col=self.FLD_TO_COL
                )
                self.errored()
