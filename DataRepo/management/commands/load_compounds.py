from DataRepo.management.commands.load_table import LoadFromTableCommand
from DataRepo.utils import CompoundsLoader


class Command(LoadFromTableCommand):
    """Command to load the Compound and CompoundSynonym models from a table-like file."""

    help = "Loads data from a compound table into the database"
    loader_class = CompoundsLoader
    default_sheet = "Compounds"

    def add_arguments(self, parser):
        """Adds command line options.

        Args:
            parser (argparse object)

        Raises:
            Nothing

        Returns:
            Nothing
        """
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
            CompoundsLoader(
                self.get_dataframe(),
                headers=self.get_headers(),
                defaults=self.get_defaults(),
                synonym_separator=options["synonym_separator"],
                dry_run=self.get_dry_run(),
                defer_rollback=self.get_defer_rollback(),
                sheet=self.get_sheet(),
                file=self.get_infile(),
            )
        )

        self.load_data()
