import argparse
from typing import Type

from django.core.management import CommandError

from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.samples_loader import SamplesLoader
from DataRepo.management.commands.load_table import LoadTableCommand


class Command(LoadTableCommand):
    """Command to load the Sample model from a table-like file."""

    help = "Loads samples from a table-like file into the database"
    loader_class: Type[TableLoader] = SamplesLoader

    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        parser.add_argument(
            # Legacy support - catch this option and issue an error if it is used.
            "--sample-table-filename",
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
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
        if options["sample_table_filename"]:
            raise CommandError(
                "By supplying --sample-table-filename, it looks like you're trying to call the old version of this "
                "script.  This script has been renamed.  Use `pythong manage.py legacy_load_samples ...` instead."
                f"{options}"
            )

        self.load_data()
