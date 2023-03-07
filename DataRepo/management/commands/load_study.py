import argparse
import os

import jsonschema
import yaml  # type: ignore
from django.apps import apps
from django.conf import settings
from django.core.management import BaseCommand, call_command

from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import (
    UncleanBufferError,
    buffer_size,
    clear_update_buffer,
    disable_autoupdates,
    enable_autoupdates,
    perform_buffered_updates,
)
from DataRepo.utils.exceptions import ValidationDatabaseSetupError


class Command(BaseCommand):

    # Path to example config file
    example_configfile = os.path.relpath(
        os.path.join(
            apps.get_app_config("DataRepo").path,
            "example_data" "small_dataset" "small_obob_study_params.yaml",
        )
    )

    # Path to config file schema
    schema_path = os.path.relpath(
        os.path.join(
            apps.get_app_config("DataRepo").path,
            "schemas",
            "load_study.yaml",
        )
    )

    # Show this when the user types help
    help = (
        "Loads tissues, compounds, animals, samples, and accucor data using a YAML "
        "file to specify parameters. "
        "Example usage: manage.py load_study config_file.yaml "
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "study_params",
            type=argparse.FileType("r"),
            help=(
                "File containing parameters used to load study data "
                f"EXAMPLE {self.example_configfile} : "
                f"SCHEMA {self.schema_path}"
            ),
        )
        # Used internally by the DataValidationView
        parser.add_argument(
            "--validate",
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
        # Used internally to load necessary data into the validation database
        parser.add_argument(
            "--clear-buffer",
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):

        if options["clear_buffer"]:
            clear_update_buffer()
        elif buffer_size() > 0:
            raise UncleanBufferError(
                "The auto-update buffer is unexpectedly populated.  Add --clear-buffer to your command to flush the "
                "buffer and proceed with the load."
            )

        # Read load study parameters
        study_params = yaml.safe_load(options["study_params"])
        study_dir = os.path.dirname(os.path.realpath(options["study_params"].name))

        with open(self.schema_path, "r") as stream:
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
            call_command(
                "load_compounds",
                compounds=compounds_file,
                database=options["database"],
                validate=options["validate"],
            )

        if "protocols" in study_params:
            protocols_file = os.path.join(study_dir, study_params["protocols"])
            self.stdout.write(
                self.style.MIGRATE_HEADING(f"Loading protocols from {protocols_file}")
            )
            call_command(
                "load_protocols",
                protocols=protocols_file,
                database=options["database"],
                validate=options["validate"],
                verbosity=options["verbosity"],
            )

        if "tissues" in study_params:
            tissues_file = os.path.join(study_dir, study_params["tissues"])
            self.stdout.write(
                self.style.MIGRATE_HEADING(f"Loading tissues from {tissues_file}")
            )
            call_command(
                "load_tissues",
                tissues=tissues_file,
                database=options["database"],
                validate=options["validate"],
                verbosity=options["verbosity"],
            )

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
            skip_researcher_check = study_params["animals_samples_treatments"].get(
                "skip_researcher_check", False
            )
            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename=animals_samples_table_file,
                table_headers=headers_file,
                skip_researcher_check=skip_researcher_check,
                database=options["database"],
                verbosity=options["verbosity"],
                defer_autoupdates=True,
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
                isocorr_format = False
                self.stdout.write(
                    self.style.MIGRATE_HEADING(
                        f"Loading accucor_data from {accucor_file_name}"
                    )
                )
                # Get parameters specific to each accucor file
                if "msrun_protocol" in accucor_file:
                    protocol = accucor_file["msrun_protocol"]
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
                    print(f"PREFIX: {sample_name_prefix}")
                if "isocorr_format" in accucor_file:
                    isocorr_format = accucor_file["isocorr_format"]

                call_command(
                    "load_accucor_msruns",
                    accucor_file=os.path.join(study_dir, accucor_file_name),
                    protocol=protocol,
                    date=date,
                    researcher=researcher,
                    new_researcher=new_researcher,
                    skip_samples=skip_samples,
                    sample_name_prefix=sample_name_prefix,
                    database=options["database"],
                    validate=options["validate"],
                    isocorr_format=isocorr_format,
                    defer_autoupdates=True,
                )

        # Database config
        db = settings.TRACEBASE_DB
        # If a database was explicitly supplied
        if options["database"] is not None:
            db = options["database"]
        else:
            if options["validate"]:
                if settings.VALIDATION_ENABLED:
                    db = settings.VALIDATION_DB
                else:
                    raise ValidationDatabaseSetupError()

        # Since defer_autoupdates is supplied as True to the sample and accucor load commands, we can do all the mass
        # autoupdates in 1 go.
        disable_autoupdates()
        disable_caching_updates()
        perform_buffered_updates(using=db)
        # The buffer should be clear, but just for good measure...
        clear_update_buffer()
        enable_caching_updates()
        enable_autoupdates()

        self.stdout.write(self.style.SUCCESS("Done loading study"))
