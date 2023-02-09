import argparse

import pandas as pd
from django.core.management import BaseCommand, CommandError

from DataRepo.utils import CompoundsLoader


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
        # optional "do work" argument; otherwise, only reports of possible work
        parser.add_argument(
            "--validate-only",
            action="store_true",
            default=False,
            help=(
                "Validation mode. If specified, command will not change the database, "
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
        # Used internally to load necessary data into the validation database
        parser.add_argument(
            "--database",
            required=False,
            type=str,
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        action = "Loading"
        if options["validate_only"]:
            action = "Validating"
        self.stdout.write(self.style.MIGRATE_HEADING(f"{action} compound data"))

        self.extract_compounds_from_tsv(options)

        # Initialize loader class
        loader = CompoundsLoader(
            compounds_df=self.compounds_df,
            synonym_separator=options["synonym_separator"],
            database=options["database"],
            validate=options["validate"],
        )

        loader.load_compounds()

        if options["verbosity"] >= 2:
            for msg in loader.validation_debug_messages:
                self.stdout.write(self.style.NOTICE(msg))

        if options["verbosity"] >= 1:
            for msg in loader.validation_warning_messages:
                self.stdout.write(self.style.WARNING(msg))

        # If validation failed, raise an exception
        if len(loader.validation_error_messages) >= 1:
            # report on what errors were discovered by the loader
            for err_msg in loader.validation_error_messages:
                self.stdout.write(self.style.ERROR(err_msg))
            raise CommandError(
                "Validation errors when loading compounds, no compounds were loaded"
            )

        # report on what what work would be done by the loader
        self.stdout.write(
            self.style.MIGRATE_HEADING(
                f"Work to be done: {len(loader.validated_new_compounds_for_insertion)} "
                "new compounds will be inserted and all names/synonyms from the file "
                "will either be found or inserted."
            )
        )
        if len(loader.validated_new_compounds_for_insertion) >= 1:
            self.stdout.write(
                self.style.MIGRATE_HEADING("New compounds to be inserted:")
            )
            for compound in loader.validated_new_compounds_for_insertion:
                self.stdout.write(self.style.MIGRATE_LABEL(str(compound)))

        # Load compounds
        if not options["validate_only"]:
            for msg in loader.summary_messages:
                self.stdout.write(self.style.MIGRATE_HEADING(msg))

        self.stdout.write(self.style.SUCCESS(f"{action} compound data completed"))

    def extract_compounds_from_tsv(self, options):

        self.compounds_df = pd.read_csv(
            options["compounds"], sep="\t", keep_default_na=False
        )
