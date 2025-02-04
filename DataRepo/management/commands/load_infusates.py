from typing import Type

from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.infusates_loader import InfusatesLoader
from DataRepo.management.commands.load_table import LoadTableCommand


class Command(LoadTableCommand):
    """Command to load the Infusate and InfusateTracer models from a table-like file."""

    help = "Loads data from an infusate table into the database"
    loader_class: Type[TableLoader] = InfusatesLoader

    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        # Add additional options for this specific script
        parser.add_argument(
            "--tracer-delimiter",
            type=str,
            help="Character delimiting multiple label positions in the label positions column (default '%(default)s')",
            default=self.loader_class.TRACER_DELIMITER,
            required=False,
        )

    def handle(self, *args, **options):
        """Code to run when the command is called from the command line.

        This code is automatically wrapped by LoadTableCommand._handler.

        Args:
            options (dict of strings): String values provided on the command line by option name.
        Exceptions:
            None
        Returns:
            None
        """
        self.load_data()
