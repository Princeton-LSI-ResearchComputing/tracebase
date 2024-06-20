from typing import Type

from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.samples_loader import SamplesLoader
from DataRepo.management.commands.load_table import LoadTableCommand


# TODO: Rename this file to "load_samples.py" after the old one is deleted
class Command(LoadTableCommand):
    """Command to load the Sample model from a table-like file."""

    help = "Loads samples from a table-like file into the database"
    loader_class: Type[TableLoader] = SamplesLoader

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
            options (dict): Values provided on the command line.
        Exceptions:
            None
        Returns:
            None
        """
        self.load_data()
