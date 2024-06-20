from typing import Type

from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.study_table_loader import StudyTableLoader
from DataRepo.management.commands.load_table import LoadTableCommand


class Command(LoadTableCommand):
    """Command to load the Study model from a table-like file."""

    help = "Loads data from a study table (e.g. study code, name, and description) into the database."
    loader_class: Type[TableLoader] = StudyTableLoader

    def handle(self, *args, **options):
        """Code to run when the command is called from the command line.

        This code is automatically wrapped by LoadTableCommand._handler, which handles:
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
            Nothing (See LoadTableCommand._handler for exceptions in the wrapper)

        Returns:
            Nothing
        """
        self.load_data()
