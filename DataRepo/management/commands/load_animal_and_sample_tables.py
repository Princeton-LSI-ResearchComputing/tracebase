import pandas as pd
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
        parser.add_argument(
            "--sample-table-filename",
            required=True,
            type=str,
            help="file containing the sample-specific annotations",
        )
        parser.add_argument(
            "--animal-table-filename",
            required=True,
            type=str,
            help="file containing the animal-specific annotations",
        )
        parser.add_argument(
            "--table-headers",
            type=str,
            help="YAML file defining headers to be used",
        )

    def handle(self, *args, **options):
        print("Reading header definition")
        if options["table_headers"]:
            with open(options["table_headers"]) as headers_file:
                header_def = yaml.safe_load(headers_file)
                headers = SampleTableLoader.SampleTableHeaders(**header_def)
        else:
            headers = SampleTableLoader.DefaultSampleTableHeaders
        print(f"{headers}")

        print("Merging animal and samples tables...")
        samples = pd.read_table(
            options["sample_table_filename"],
            dtype={headers.ANIMAL_NAME: str},
            keep_default_na=False,
        )

        animals = pd.read_table(
            options["animal_table_filename"],
            dtype={headers.ANIMAL_NAME: str, headers.ANIMAL_TREATMENT: str},
            keep_default_na=False,
        )

        # merge the two files/dataframes together on Animal ID
        merged = pd.merge(left=samples, right=animals, on=headers.ANIMAL_NAME)

        print("Loading sample table")
        loader = SampleTableLoader(sample_table_headers=headers)
        loader.load_sample_table(merged.to_dict("records"))

        print("Done loading sample table")
