from DataRepo.management.commands.load_table import LoadFromTableCommand
from DataRepo.utils import TissuesLoader


class Command(LoadFromTableCommand):
    """Command to load the Tissue model from a table-like file."""

    help = "Loads data from a tissue table into the database"
    loader_class = TissuesLoader
    default_sheet = "Tissues"

    def handle(self, *args, **options):
        """Code to run when the command is called from the command line.

        This code is automatically wrapped by LoadFromTableCommand._handler, which handles:
            - DryRun Exceptions
            - Contextualization of exceptions to the associated input in the file
            - Atomic transactions with optionally deferred rollback
            - Header and data type validation
            - Unique file constraints

        Args:
            None

        Raises:
            Nothing (See LoadFromTableCommand._handler for exceptions in the wrapper)

        Returns:
            Nothing
        """
        self.set_loader(
            TissuesLoader(
                self.get_dataframe(),
                headers=self.get_headers(),
                defaults=self.get_defaults(),
                dry_run=self.get_dry_run(),
                defer_rollback=self.get_defer_rollback(),
                sheet=self.get_sheet(),
                file=self.get_infile(),
            )
        )

        self.load_data()
