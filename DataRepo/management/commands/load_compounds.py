import pandas as pd
from django.core.management import BaseCommand

from DataRepo.utils import CompoundsLoader


class Command(BaseCommand):
    # Show this when the user types help
    help = "Loads data from a compound list into the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--compounds",
            type=str,
            help="Path to tab-delimited file containing headers of 'Compound','Formula', 'HMDB ID', and 'Synonyms'; "
            "required.",
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
            help="Validation mode. If specified, command will not change the database, "
            "but simply report back potential work or issues.",
        )

    def handle(self, *args, **options):
        action = "Loading"
        if options["validate_only"]:
            action = "Validating"
        print(f"{action} compound data")

        self.extract_compounds_from_tsv(options)

        loader = CompoundsLoader(
            compounds_df=self.compounds_df,
            synonym_separator=options["synonym_separator"],
            validate_only=options["validate_only"],
            verbosity=int(options["verbosity"]),
        )

        loader.load_data()

    def extract_compounds_from_tsv(self, options):

        self.compounds_df = pd.read_csv(
            options["compounds"], sep="\t", keep_default_na=False
        )
