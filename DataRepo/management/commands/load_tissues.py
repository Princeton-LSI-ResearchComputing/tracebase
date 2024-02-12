from DataRepo.management.commands.load_table import LoadFromTableCommand
from DataRepo.utils import TissuesLoader


class Command(LoadFromTableCommand):
    """Command to load the Tissue model from a table-like file."""

    help = "Loads data from a tissue table into the database"
    loader_class = TissuesLoader
    data_sheet_default = "Tissues"

    def handle(self, *args, **options):
        """Code to run when the command is called from the command line.

        This code is automatically wrapped by LoadFromTableCommand._handler, which handles:
            - DryRun Exceptions
            - Contextualization of exceptions to the associated input in the file
            - Atomic transactions with optionally deferred rollback
            - Header and data type validation
            - Unique file constraints

        Args:
            options (dict of strings): String values provided on the command line by option name.

        Raises:
            Nothing (See LoadFromTableCommand._handler for exceptions in the wrapper)

        Returns:
            Nothing
        """
        self.load_data()
