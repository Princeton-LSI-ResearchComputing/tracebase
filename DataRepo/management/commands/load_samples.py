from csv import DictReader

import yaml
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

    def handle(self, *args, **options):
        print("Reading header definition")
        if options["sample_table_headers"]:
            with open(options["sample_table_headers"]) as headers_file:
                header_def = yaml.safe_load(headers_file)
                headers = SampleTableLoader.SampleTableHeaders(**header_def)
        else:
            headers = SampleTableLoader.DefaultSampleTableHeaders
        print(f"{headers}")
        print("Loading sample table")
        loader = SampleTableLoader(sample_table_headers=headers)
        loader.load_sample_table(
            DictReader(
                open(options["sample_table_filename"]),
                dialect="excel-tab",
            )
        )
        print("Done loading sample table")
