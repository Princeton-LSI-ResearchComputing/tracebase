import pandas as pd
import yaml
from django.core.management import BaseCommand

from DataRepo.utils import SampleTableLoader


class Command(BaseCommand):

    examples_dir = "DataRepo/example_data/"
    example_animals = examples_dir + "obob_animals_table.tsv"
    example_samples = examples_dir + "obob_samples_table.tsv"
    example_yaml = examples_dir + "sample_and_animal_tables_headers.yaml"

    # Show this when the user types help
    help = (
        "Loads data from animal and sample tables into the database. "
        "Rows where 'Tissue' is empty will be skipped "
        "(assumed to be blank samples). "
        f"Example usage : manage.py load_animal_and_sample_tables --sample-table-filename {example_samples}"
        f" --animal-table-filename {example_animals} --table-headers {example_yaml}"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--sample-table-filename",
            required=True,
            type=str,
            help=f"file containing the sample-specific annotations, for example : {self.example_samples}",
        )
        parser.add_argument(
            "--animal-table-filename",
            required=True,
            type=str,
            help=f"file containing the animal-specific annotations, for example : {self.example_animals}",
        )
        parser.add_argument(
            "--table-headers",
            type=str,
            help=f"YAML file defining headers to be used, for example : {self.example_yaml}",
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
