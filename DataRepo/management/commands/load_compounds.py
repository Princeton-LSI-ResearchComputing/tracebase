from DataRepo.management.commands.load_table import LoadFromTableCommand
from DataRepo.utils import CompoundsLoader


class Command(LoadFromTableCommand):
    """Command to load the Compound and CompoundSynonym models from a table-like file."""

    help = "Loads data from a compound table into the database"
    loader_class = CompoundsLoader
    data_sheet_default = "Compounds"

    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        # Add additional options for this specific script
        parser.add_argument(
            "--synonym-separator",
            type=str,
            help="Character separating multiple synonyms in 'Synonyms' column (default '%(default)s')",
            default=";",
            required=False,
        )

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
        self.load_data(
            # Specific to this loader.  All other args are extracted from the command line automatically.
            synonym_separator=options["synonym_separator"],
        )
