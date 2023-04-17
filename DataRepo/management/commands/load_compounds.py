import argparse

import pandas as pd
from django.core.management import BaseCommand

from DataRepo.utils import CompoundsLoader, DryRun


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
        # Used internally by the DataValidationView
        parser.add_argument(
            "--validate",
            required=False,
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        action = "Loading"
        if options["dry_run"]:
            action = "Validating"
        self.stdout.write(self.style.MIGRATE_HEADING(f"{action} compound data"))

        self.extract_compounds_from_tsv(options)

        # Initialize loader class
        loader = CompoundsLoader(
            compounds_df=self.compounds_df,
            synonym_separator=options["synonym_separator"],
            validate=options["validate"],
            dry_run=options["dry_run"],
        )

        try:
            loader.load_compounds()
        except DryRun:
            pass

        self.stdout.write(self.style.SUCCESS(f"{action} compound data completed"))

    def extract_compounds_from_tsv(self, options):

        self.compounds_df = pd.read_csv(
            options["compounds"], sep="\t", keep_default_na=False
        )
