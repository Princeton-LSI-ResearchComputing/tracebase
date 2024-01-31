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

    def __init__(self, *args, **kwargs):
        """Constructor.

        Args:
            df (pandas dataframe): Data, e.g. as parsed from a table-like file.
            headers (Optional[Tableheaders namedtuple]) [DefaultHeaders]: Header names by header key.
            defaults (Optional[Tableheaders namedtuple]) [DefaultValues]: Default values by header key.
            dry_run (Optional[boolean]) [False]: Dry run mode.
            defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT MUST
                HANDLE THE ROLLBACK.
            sheet (Optional[str]) [None]: Sheet name (for error reporting).
            file (Optional[str]) [None]: File name (for error reporting).

        Raises:
            Nothing

        Returns:
            Nothing
        """

        kwargs["models"] = [Tissue]
        super().__init__(*args, **kwargs)

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
        for index, row in self.df.iterrows():
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
