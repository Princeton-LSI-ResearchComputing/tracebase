import argparse
from csv import DictReader

import yaml  # type: ignore
from django.core.management import BaseCommand

from DataRepo.utils import SampleTableLoader


class Command(BaseCommand):
    # Show this when the user types help
    help = (
        "Loads data from a sample table into the database."
        "Rows where 'Tissue' is empty will be skipped "
        "(assumed to be blank samples)."
    )

    def add_arguments(self, parser):
        parser.add_argument("sample_table_filename", type=str)
        parser.add_argument(
            "--sample-table-headers",
            type=str,
            help="YAML file defining headers to be used",
        )
        # optional skip researcher check argument.  Since a file can have multiple researchers, you can't check the
        # opposite way unless we added an option that takes a list of new researchers.
        parser.add_argument(
            "--skip-researcher-check",
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )
        # optional database argument.  This was added specifically for user data validation without changing the
        # production database.
        parser.add_argument(
            "--database",
            required=False,
            type=str,
            default="default",
            help=f"Supply 'validation' for the user data validation database : default",
        )

    def handle(self, *args, **options):
        print("Reading header definition")
        if options["sample_table_headers"]:
            with open(options["sample_table_headers"]) as headers_file:
                header_def = yaml.safe_load(headers_file)
                headers = SampleTableLoader.SampleTableHeaders(**header_def, database=options["database"])
        else:
            headers = SampleTableLoader.DefaultSampleTableHeaders
        print(f"{headers}")
        print("Loading sample table")
        loader = SampleTableLoader(sample_table_headers=headers)
        loader.load_sample_table(
            DictReader(
                open(options["sample_table_filename"]),
                dialect="excel-tab",
            ),
            skip_researcher_check=options["skip_researcher_check"],
        )
        print("Done loading sample table")
