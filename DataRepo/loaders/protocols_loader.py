from collections import namedtuple

from django.db import transaction

from DataRepo.loaders.table_loader import TableLoader
from DataRepo.models import Protocol
from DataRepo.utils.file_utils import is_excel


class ProtocolsLoader(TableLoader):
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    NAME_KEY = "NAME"
    CAT_KEY = "CATEGORY"
    DESC_KEY = "DESCRIPTION"

    DataSheetName = "Treatments"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "NAME",
            "CATEGORY",
            "DESCRIPTION",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        NAME="Name",
        CATEGORY="Category",
        DESCRIPTION="Description",
    )

    # default XLXS template headers
    DataHeadersExcel = DataTableHeaders(
        NAME="Animal Treatment",
        CATEGORY="Category",  # Unused
        DESCRIPTION="Treatment Description",
    )

    # Whether each column is required to be present of not
    DataRequiredHeaders = DataTableHeaders(
        NAME=True,
        CATEGORY=False,
        DESCRIPTION=True,
    )

    # Default values to use when a row in the given column doesn;t have a value in it
    DataDefaultValues = DataTableHeaders(
        NAME=None,
        CATEGORY=Protocol.ANIMAL_TREATMENT,
        DESCRIPTION=None,
    )

    # Whether a value for an row in a column is required or not (note that defined DataDefaultValues will satisfy this)
    DataRequiredValues = DataTableHeaders(
        NAME=True,
        CATEGORY=True,  # Required by the model field, but effectively not reqd, bec. it's defaulted
        DESCRIPTION=False,
    )

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes = {
        NAME_KEY: str,
        CAT_KEY: str,
        DESC_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [[NAME_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        "Protocol": {
            "name": NAME_KEY,
            "category": CAT_KEY,
            "description": DESC_KEY,
        },
    }

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Protocol]

    def get_pretty_headers(self):
        """Override of the base class to conditionally change the headers based on infile type.

        Generates a string describing the headers, with appended asterisks if required, and a message about the
        asterisks.

        Args:
            None

        Raises:
            Nothing

        Returns:
            pretty_headers (string)
        """
        if hasattr(self, "headers"):
            headers = self.headers
        else:
            headers = self.DataHeaders

        pretty_headers = []
        for hk in list(headers._asdict().keys()):
            reqd = getattr(self.DataRequiredHeaders, hk)
            pretty_header = getattr(headers, hk)
            if reqd:
                pretty_header += "*"
            pretty_headers.append(pretty_header)

        pretty_excel_headers = []
        for hk in list(headers._asdict().keys()):
            if hk == self.CAT_KEY:
                # The category has a default when an excel file is submitted
                continue
            reqd = getattr(self.DataRequiredHeaders, hk)
            pretty_excel_header = getattr(self.DataHeadersExcel, hk)
            if reqd:
                pretty_excel_header += "*"
            pretty_excel_headers.append(pretty_excel_header)

        msg = "(* = Required)"

        return (
            f"[{', '.join(pretty_headers)}] (or, if the input file is an excel file: "
            f"[{', '.join(pretty_excel_headers)}]) {msg}"
        )

    def set_headers(self, custom_headers=None):
        """Override of the base class to conditionally change the headers based on infile type.

        Args:
            custom_headers (namedtupe of loader_class.DataTableHeaders): Header names by header key

        Raises:
            Nothing

        Returns:
            headers (namedtupe of loader_class.DataTableHeaders): Header names by header key
        """
        if custom_headers is not None:
            # This is only needed if called from elsewhere (e.g. another derived class of this class)
            return super().set_headers(custom_headers=custom_headers)
        # Different headers if an excel file is provided
        if is_excel(self.file):
            return super().set_headers(custom_headers=self.DataHeadersExcel._asdict())
        return super().set_headers()

    def load_data(self):
        """Loads the tissue table from the dataframe.

        Args:
            None

        Raises:
            Nothing

        Returns:
            Nothing
        """
        for _, row in self.df.iterrows():
            try:
                self.get_or_create_protocol(row)
            except Exception:
                # Exception handling was handled in get_or_create_protocol
                # Continue processing rows to find more errors
                pass

    @transaction.atomic
    def get_or_create_protocol(self, row):
        """Get or create a protocol record and buffer exceptions before raising.

        Args:
            row (pandas dataframe row)

        Raises:
            Nothing (explicitly)

        Returns:
            Nothing
        """
        try:
            rec_dict = None
            rec = None
            created = False

            name = self.get_row_val(row, self.headers.NAME)
            category = self.get_row_val(row, self.headers.CATEGORY)
            description = self.get_row_val(row, self.headers.DESCRIPTION)

            rec_dict = {
                "name": name,
                "category": category,
                "description": description,
            }

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row():
                self.errored()
                return

            # Try and get the protocol
            rec, created = Protocol.objects.get_or_create(**rec_dict)

            # If no protocol was found, create it
            if created:
                rec.full_clean()
                self.created()
            else:
                self.existed()

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Protocol, rec_dict)
            self.errored()
            # Now that the exception has been handled, trigger a roolback of this record load attempt
            raise e
