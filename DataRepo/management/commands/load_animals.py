from typing import Type

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.management.commands.load_table import LoadTableCommand


class Command(LoadTableCommand):
    """Command to load the Animal, AnimalLabel, and Animal_studies models from a table-like file."""

    help = "Loads animals from a table-like file into the database"
    loader_class: Type[TableLoader] = AnimalsLoader

    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        # Add additional options for this specific script
        parser.add_argument(
            "--study-delimiter",
            type=str,
            help=(
                f"Character delimiting multiple Studies in the {AnimalsLoader.DataHeaders.STUDY} column (default "
                "'%(default)s')"
            ),
            default=self.loader_class.StudyDelimiter,
            required=False,
        )

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
        # The AnimalsLoader class constructor has 1 custom argument
        # The TableLoader superclass arguments are controlled by the LoadTableCommand superclass
        self.init_loader(study_delimiter=options["study_delimiter"])
        self.load_data()
