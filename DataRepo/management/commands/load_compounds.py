import argparse

from django.core.management import BaseCommand

from DataRepo.utils import CompoundsLoader, DryRun
from DataRepo.utils.file_utils import read_from_file


class Command(BaseCommand):
    # Show this when the user types help
    help = "Loads data from a compound list into the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--compounds",
            type=str,
            help=(
                "Path to tab-delimited file containing headers of "
                "'Compound','Formula', 'HMDB ID', and 'Synonyms'; required."
            ),
            required=True,
        )

        parser.add_argument(
            "--synonym-separator",
            type=str,
            help="Character separating multiple synonyms in 'Synonyms' column (default '%(default)s')",
            default=";",
            required=False,
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help=(
                "Dry Run mode. If specified, command will not change the database, "
                "but simply report back potential work or issues."
            ),
        )

        # Intended for use by load_study to prevent rollback of changes in the event of an error so that for example,
        # subsequent loading scripts can validate with all necessary data present
        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in outer atomic transact)
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        action = "Loading"
        if options["dry_run"]:
            action = "Validating"
        self.stdout.write(self.style.MIGRATE_HEADING(f"{action} compound data"))

        self.compounds_df = read_from_file(options["compounds"], sheet="Compounds")

        # Initialize loader class
        loader = CompoundsLoader(
            compounds_df=self.compounds_df,
            synonym_separator=options["synonym_separator"],
            dry_run=options["dry_run"],
            defer_rollback=options["defer_rollback"],
        )

        try:
            loader.load_compound_data()
        except DryRun:
            pass

        self.stdout.write(self.style.SUCCESS(f"{action} compound data completed"))
