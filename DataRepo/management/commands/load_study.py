import argparse
from typing import Type

from django.core.management import CommandError

from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.study_loader import StudyLoader
from DataRepo.management.commands.load_table import LoadTableCommand


class Command(LoadTableCommand):
    """Command to load all sheets of an entire study doc.

    NOTE: loader_class takes a derived class of TableLoader named StudyLoader, but anywhere it refers to columns and
    headers, in this context, it's referring to sheets and tabs.
    """

    help = "Loads all data from a study doc (e.g. Animals, Samples, Compounds, etc) into the database."
    loader_class: Type[TableLoader] = StudyLoader

    # TODO: Remove this after all dependent code has been updated for the new version of this script
    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        parser.add_argument(
            # Legacy support - catch this option and issue an error if it is used.
            "study_params",
            type=argparse.FileType("r"),
            nargs="?",
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
            options (dict of strings): String values provided on the command line by option name.
        Raises:
            None
        Returns:
            None
        """
        # TODO: Remove this after all dependent code has been updated for the new version of this script
        if options["study_params"] is not None:
            raise CommandError(
                "By supplying a positional argument, it looks like you're trying to call the old version of this "
                "script.  This script has been renamed.  Use `pythong manage.py legacy_load_study ...` instead."
                f"{options}"
            )

        self.load_data()
