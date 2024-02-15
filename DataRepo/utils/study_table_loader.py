from collections import namedtuple

from DataRepo.models import Study
from DataRepo.utils.loader import TraceBaseLoader


class StudyTableLoader(TraceBaseLoader):

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    CODE_KEY = "CODE"
    NAME_KEY = "NAME"
    DESC_KEY = "DESCRIPTION"

    # The tuple used to store different kinds of data per column at the class level
    TableHeaders = namedtuple(
        "TableHeaders",
        [
            "CODE",
            "NAME",
            "DESCRIPTION",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DefaultHeaders = TableHeaders(
        CODE="Study ID",
        NAME="Name",
        DESCRIPTION="Description",
    )

    # Whether each column is required to be present of not
    RequiredHeaders = TableHeaders(
        CODE=True,
        NAME=True,
        DESCRIPTION=True,
    )

    # Whether a value for an row in a column is required or not (note that defined DefaultValues will satisfy this)
    RequiredValues = RequiredHeaders

    # No DefaultValues needed
    # No ColumnTypes needed

    # Combinations of columns whose values must be unique in the file
    UniqueColumnConstraints = [[CODE_KEY], [NAME_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToHeaderKey = {
        "Study": {
            "code": CODE_KEY,
            "name": NAME_KEY,
            "description": DESC_KEY,
        },
    }

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Study]

    def load_data(self):
        """Loads the study table from the dataframe.

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
                code = self.get_row_val(row, self.headers.CODE)
                name = self.get_row_val(row, self.headers.NAME)
                description = self.get_row_val(row, self.headers.DESCRIPTION)

                rec_dict = {
                    "code": code,
                    "name": name,
                    "description": description,
                }

                # get_row_val can add to skip_row_indexes when there is a missing required value
                if self.is_skip_row():
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
