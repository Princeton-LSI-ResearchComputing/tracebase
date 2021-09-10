import argparse
import os

import jsonschema
import yaml  # type: ignore
from django.apps import apps
from django.core.management import BaseCommand, call_command


class Command(BaseCommand):

    # Show this when the user types help
    help = (
        "Loads compounds, animals, samples, and accucor data using a YAML "
        "file to specify parameters."
        "Example usage : manage.py load_study my_study/my_study.yaml"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "study_params",
            type=argparse.FileType("r"),
            help="file containing the parameters used to load the study data.",
        )

    def get_current_app_path(self):
        return apps.get_app_config("DataRepo").path

    def handle(self, *args, **options):

        # Read load study parameters
        study_params = yaml.safe_load(options["study_params"])
        study_dir = os.path.dirname(os.path.realpath(options["study_params"].name))

        # Read load study config schema
        app_path = self.get_current_app_path()
        schema_path = os.path.join(app_path, "schemas", "load_study.yaml")

        with open(schema_path, "r") as stream:
            schema = yaml.safe_load(stream)

        # Validate the configuration
        jsonschema.validate(study_params, schema)
        self.stdout.write(
            self.style.SUCCESS(
                f"Validated study parameters {options['study_params'].name}"
            )
        )
        if "compounds" in study_params:
            compounds_file = os.path.join(study_dir, study_params["compounds"])
            self.stdout.write(
                self.style.MIGRATE_HEADING(f"Loading compounds from {compounds_file}")
            )
            call_command("load_compounds", compounds_file)

        if "animals_samples_treatments" in study_params:
            # Read in animals and samples file
            animals_samples_table_file = os.path.join(
                study_dir, study_params["animals_samples_treatments"]["table"]
            )
            self.stdout.write(
                self.style.MIGRATE_HEADING(
                    f"Loading animals and samples from {animals_samples_table_file}"
                )
            )
            if "headers" in study_params["animals_samples_treatments"]:
                headers_file = os.path.join(
                    study_dir, study_params["animals_samples_treatments"]["headers"]
                )
            else:
                headers_file = None
            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename=animals_samples_table_file,
                table_headers=headers_file,
            )

        if "accucor_data" in study_params:

            # Get parameters for all accucor files
            protocol = study_params["accucor_data"]["msrun_protocol"]
            date = study_params["accucor_data"]["date"]
            researcher = study_params["accucor_data"]["researcher"]
            new_researcher = study_params["accucor_data"]["new_researcher"]
            skip_samples = None
            if "skip_samples" in study_params["accucor_data"]:
                skip_samples = study_params["accucor_data"]["skip_samples"]
            sample_name_prefix = None
            if "sample_name_prefix" in study_params["accucor_data"]:
                sample_name_prefix = study_params["accucor_data"]["sample_name_prefix"]

            # Read in accucor data files
            for accucor_file in study_params["accucor_data"]["accucor_files"]:
                accucor_file_name = accucor_file["name"]
                self.stdout.write(
                    self.style.MIGRATE_HEADING(
                        f"Loading accucor_data from {accucor_file_name}"
                    )
                )
                # Get parameters specific to each accucor file
                if "protocol" in accucor_file:
                    protocol = accucor_file["protocol"]
                if "date" in accucor_file:
                    date = accucor_file["date"]
                if "researcher" in accucor_file:
                    researcher = accucor_file["researcher"]
                if "new_researcher" in accucor_file:
                    new_researcher = accucor_file["new_researcher"]
                if "skip_samples" in accucor_file:
                    skip_samples = accucor_file["skip_samples"]
                if "sample_name_prefix" in accucor_file:
                    sample_name_prefix = accucor_file["sample_name_prefix"]

                call_command(
                    "load_accucor_msruns",
                    accucor_file=os.path.join(study_dir, accucor_file_name),
                    protocol=protocol,
                    date=date,
                    researcher=researcher,
                    new_researcher=new_researcher,
                    skip_samples=skip_samples,
                    sample_name_prefix=sample_name_prefix,
                )

        self.stdout.write(self.style.SUCCESS("Done loading study"))
