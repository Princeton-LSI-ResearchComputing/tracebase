from collections import namedtuple

from DataRepo.models import Tissue
from DataRepo.utils.loader import TraceBaseLoader


class TissuesLoader(TraceBaseLoader):
    NAME_KEY = "NAME"
    DESC_KEY = "DESCRIPTION"

    TableHeaders = namedtuple(
        "TableHeaders",
        [
            "NAME",
            "DESCRIPTION",
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
    # No DefaultValues needed
    UniqueColumnConstraints = [[NAME_KEY]]
    FieldToHeaderKey = {
        "Tissue": {
            "name": NAME_KEY,
            "description": DESC_KEY,
        },
    }
    Models = [Tissue]

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
        for _, row in self.df.iterrows():
            rec_dict = None

            try:
                name = self.get_row_val(row, self.headers.NAME)
                description = self.get_row_val(row, self.headers.DESCRIPTION)

                rec_dict = {
                    "name": name,
                    "description": description,
                }

                # get_row_val can add to skip_row_indexes when there is a missing required value
                if self.is_skip_row():
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
