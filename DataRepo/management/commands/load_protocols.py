from DataRepo.management.commands.load_table import LoadFromTableCommand
from DataRepo.utils import ProtocolsLoader
from DataRepo.utils.file_utils import is_excel


class Command(LoadFromTableCommand):
    """Command to load the Protocol model from a table-like file."""

    help = "Loads data from a protocol table into the database"
    loader_class = ProtocolsLoader
    data_sheet_default = "Treatments"

    # default XLXS template headers
    TREATMENTS_NAME_HEADER = "Animal Treatment"
    TREATMENTS_DESC_HEADER = "Treatment Description"

    def set_headers(self, custom_headers=None):
        """Override of the base class to conditionally change the headers based on infile type.

        Args:
            custom_headers (namedtupe of loader_class.TableHeaders): Header names by header key

        Raises:
            Nothing

        Returns:
            headers (namedtupe of loader_class.TableHeaders): Header names by header key
        """
        if custom_headers is not None:
            # This is only needed if called from elsewhere (e.g. another derived class of this class)
            return super().set_headers(custom_headers=custom_headers)
        # Different headers if an excel file is provided
        if is_excel(self.get_infile()):
            # An excel sheet is assumed to be for treatment protocols
            excel_headers = {
                ProtocolsLoader.NAME_KEY: self.TREATMENTS_NAME_HEADER,
                ProtocolsLoader.DESC_KEY: self.TREATMENTS_DESC_HEADER,
            }
            return super().set_headers(custom_headers=excel_headers)
        return super().set_headers()

    def handle(self, *args, **options):
        """Code to run when the command is called from the command line.

        This code is automatically wrapped by LoadFromTableCommand._handler, which handles:
            - Retrieving the base-class-provided option values (and fills in the defaults provided by the loader_class)
            - Atomic transactions with optionally deferred rollback
            - Exception handling:
                - DryRun Exceptions
                - Contextualization of exceptions to the associated input in the file
            - Validation
                - Header and data type
                - Unique file constraints

        Args:
            options (dict of strings): String values provided on the command line by option name.

        Raises:
            Nothing (See LoadFromTableCommand._handler for exceptions in the wrapper)

        Returns:
            Nothing
        """
        # Force a re-evaluation of the header names.  They can change based on the options, given the custom code above.
        # When the object is instantiated, they are set based on defaults, and will not reset automatically.
        self.set_headers()
        self.load_data()
