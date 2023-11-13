import argparse
import os
from collections import defaultdict

import jsonschema
import yaml  # type: ignore
from django.apps import apps
from django.core.management import BaseCommand, call_command
from django.db import transaction

from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AllMissingCompounds,
    AllMissingSamples,
    AllMissingTissues,
    DryRun,
    MissingCompounds,
    MissingSamplesError,
    MissingTissues,
    MultiLoadStatus,
)


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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.missing_samples = defaultdict(list)
        self.missing_tissues = defaultdict(dict)
        self.missing_compounds = defaultdict(dict)
        self.load_statuses = MultiLoadStatus()

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
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Dry run mode.  Nothing will be committed to the database.",
        )
        # Used internally to decide whether an exception is a warning or an error, and whether any exception should
        # cause an AggregatedErrors exception to be raised or not (because the validation interface conveys warnings
        # to the validation view by raising warnings as exceptions)
        parser.add_argument(
            "--validate",
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )
        # Used internally by the validation view, as temporary data should not trigger cache deletions
        parser.add_argument(
            "--skip-cache-updates",
            required=False,
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )

    @MaintainedModel.defer_autoupdates(
        disable_opt_names=["validate", "dry_run"],
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def handle(self, *args, **options):
        self.missing_samples = defaultdict(list)
        self.missing_tissues = defaultdict(dict)
        self.missing_compounds = defaultdict(dict)
        self.load_statuses = MultiLoadStatus()
        self.verbosity = options["verbosity"]
        self.validate = options["validate"]
        self.dry_run = options["dry_run"]
        self.skip_cache_updates = (options["skip_cache_updates"],)

        # Read load study parameters
        study_params = yaml.safe_load(options["study_params"])
        study_dir = os.path.dirname(os.path.realpath(options["study_params"].name))

        with open(self.schema_path, "r") as stream:
            schema = yaml.safe_load(stream)

        # Validate the configuration
        jsonschema.validate(study_params, schema)
        if self.verbosity > 0:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Validated study parameters {options['study_params'].name}"
                )
            )

        with transaction.atomic():
            if "compounds" in study_params:
                compound_file_basename = study_params["compounds"]
                compounds_file = os.path.join(study_dir, compound_file_basename)
                self.load_statuses.init_load(compounds_file)

                if self.verbosity > 1:
                    self.stdout.write(
                        self.style.MIGRATE_HEADING(
                            f"Loading compounds from {compounds_file}"
                        )
                    )

                try:
                    call_command(
                        "load_compounds",
                        compounds=compounds_file,
                        defer_rollback=True,  # Until after we exit THIS atomic block
                        # Validate is not needed - it changes nothing
                    )
                except Exception as e:
                    self.package_group_exceptions(e, compounds_file)

            if "protocols" in study_params:
                protocol_file_basename = study_params["protocols"]
                protocols_file = os.path.join(study_dir, protocol_file_basename)
                self.load_statuses.init_load(protocols_file)

                if self.verbosity > 1:
                    self.stdout.write(
                        self.style.MIGRATE_HEADING(
                            f"Loading protocols from {protocols_file}"
                        )
                    )

                try:
                    call_command(
                        "load_protocols",
                        protocols=protocols_file,
                        verbosity=self.verbosity,
                        defer_rollback=True,  # Until after we exit THIS atomic block
                        # Validate is not needed - it changes nothing
                    )
                except Exception as e:
                    self.package_group_exceptions(e, protocols_file)

            if "tissues" in study_params:
                tissue_file_basename = study_params["tissues"]
                tissues_file = os.path.join(study_dir, tissue_file_basename)
                self.load_statuses.init_load(tissues_file)

                if self.verbosity > 1:
                    self.stdout.write(
                        self.style.MIGRATE_HEADING(
                            f"Loading tissues from {tissues_file}"
                        )
                    )

                try:
                    call_command(
                        "load_tissues",
                        tissues=tissues_file,
                        verbosity=self.verbosity,
                        defer_rollback=True,  # Until after we exit THIS atomic block
                        # Validate is not needed - it changes nothing
                    )
                except Exception as e:
                    self.package_group_exceptions(e, tissues_file)

            if "animals_samples_treatments" in study_params:
                # Read in animals and samples file
                sample_file_basename = study_params["animals_samples_treatments"][
                    "table"
                ]
                animals_samples_table_file = os.path.join(
                    study_dir, sample_file_basename
                )
                # If the protocols load was from the animal sample table file, don't overwrite the errors that
                # already came from it
                if animals_samples_table_file not in self.load_statuses.statuses.keys():
                    self.load_statuses.init_load(animals_samples_table_file)
                headers_basename = study_params["animals_samples_treatments"].get(
                    "headers", None
                )
                headers_file = None
                if headers_basename is not None:
                    headers_file = os.path.join(study_dir, headers_basename)
                skip_researcher_check = study_params["animals_samples_treatments"].get(
                    "skip_researcher_check", False
                )

                if self.verbosity > 1:
                    self.stdout.write(
                        self.style.MIGRATE_HEADING(
                            f"Loading animals and samples from {animals_samples_table_file}"
                        )
                    )

                try:
                    call_command(
                        "load_animals_and_samples",
                        animal_and_sample_table_filename=animals_samples_table_file,
                        table_headers=headers_file,
                        skip_researcher_check=skip_researcher_check,
                        verbosity=self.verbosity,
                        # Validate mode affects what is or isn't a warning. It essentially represents "end user context"
                        # and changes errors to warnings when the user has no means to fix it themselves.
                        validate=self.validate,
                        defer_autoupdates=True,
                        defer_rollback=True,  # Until after we exit THIS atomic block
                        skip_cache_updates=self.skip_cache_updates,
                    )
                except Exception as e:
                    self.package_group_exceptions(e, animals_samples_table_file)

            if "accucor_data" in study_params:
                # Get parameters for all accucor files
                study_protocol = study_params["accucor_data"]["msrun_protocol"]
                study_date = study_params["accucor_data"]["date"]
                study_researcher = study_params["accucor_data"]["researcher"]
                study_new_researcher = study_params["accucor_data"]["new_researcher"]
                study_skip_samples = study_params["accucor_data"].get(
                    "skip_samples", None
                )
                study_sample_name_prefix = study_params["accucor_data"].get(
                    "sample_name_prefix", None
                )

                # Read in accucor data files
                for accucor_info_dict in study_params["accucor_data"]["accucor_files"]:
                    # Get parameters specific to each accucor file
                    accucor_file_basename = accucor_info_dict["name"]
                    accucor_file = os.path.join(study_dir, accucor_file_basename)
                    self.load_statuses.init_load(accucor_file)
                    protocol = accucor_info_dict.get("msrun_protocol", study_protocol)
                    date = accucor_info_dict.get("date", study_date)
                    researcher = accucor_info_dict.get("researcher", study_researcher)
                    new_researcher = accucor_info_dict.get(
                        "new_researcher", study_new_researcher
                    )
                    skip_samples = accucor_info_dict.get(
                        "skip_samples", study_skip_samples
                    )
                    sample_name_prefix = accucor_info_dict.get(
                        "sample_name_prefix", study_sample_name_prefix
                    )
                    isocorr_format = accucor_info_dict.get("isocorr_format", False)

                    if self.verbosity > 1:
                        self.stdout.write(
                            self.style.MIGRATE_HEADING(
                                f"Loading accucor_data from {accucor_file_basename}"
                            )
                        )

                    if sample_name_prefix is not None and self.verbosity > 0:
                        print(f"PREFIX: {sample_name_prefix}")

                    try:
                        call_command(
                            "load_accucor_msruns",
                            accucor_file=accucor_file,
                            protocol=protocol,
                            date=date,
                            researcher=researcher,
                            new_researcher=new_researcher,
                            skip_samples=skip_samples,
                            sample_name_prefix=sample_name_prefix,
                            isocorr_format=isocorr_format,
                            skip_cache_updates=self.skip_cache_updates,
                            validate=self.validate,
                            dry_run=self.dry_run,
                        )
                        if self.verbosity > 1:
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f"STATUS: SUCCESS {accucor_file_basename} - commit pending"
                                )
                            )
                    except DryRun:
                        pass
                    except Exception as e:
                        self.package_group_exceptions(e, accucor_file)

            self.create_grouped_exceptions()

            # If we're in validate mode, raise the MultiLoadStatus Exception whether there were errors or not, so
            # that we can roll back all changes and pass all the status data to the validation interface via this
            # exception.
            if self.validate:
                # If we are in validate mode, we raise the entire load_statuses object whether the load failed or
                # not, so that we can report the load status of all load files, including successful loads.  It's
                # like Dry Run mode, but exclusively for the validation interface.
                raise self.load_statuses

            # If there were actual errors, raise an AggregatedErrorsSet exception inside the atomic block to cause
            # a rollback of everything
            if not self.load_statuses.get_success_status():
                raise self.load_statuses.get_final_exception()

            if self.dry_run:
                self.print_load_status()
                raise DryRun()

        self.print_load_status()

    def package_group_exceptions(self, exception, filepath):
        """
        Repackages an exception for consolidated reporting
        """

        # Just report the file names, not the paths, in the errors
        filename = os.path.basename(filepath)

        # Compile group-level errors/warnings relevant specifically to a study load that should be reported in one
        # consolidated error based on exceptions contained in AggregatedErrors.  Note, this could potentially change
        # individual load file statuses from fatal errors to non-fatal warnings.  This is because the consolidated
        # representation will be the "fatal error" and the errors in the files will be kept to cross-reference with the
        # group level error.  See handle_exceptions.
        if isinstance(exception, AggregatedErrors):
            # Consolidate related cross-file exceptions, like missing samples
            # Note, this can change whether the AggregatedErrors for this file are fatal or not
            missing_sample_exceptions = exception.get_exception_type(
                MissingSamplesError
            )
            for missing_sample_exception in missing_sample_exceptions:
                # Look through the sample names saved in the exception and add then to the master list
                for sample in missing_sample_exception.samples:
                    self.missing_samples[sample].append(filename)

            missing_tissue_exceptions = exception.get_exception_type(MissingTissues)
            for missing_tissue_exception in missing_tissue_exceptions:
                # Look through the sample names saved in the exception and add then to the master list
                for tissue in missing_tissue_exception.tissues_dict.keys():
                    if tissue not in self.missing_tissues["tissues"].keys():
                        self.missing_tissues["tissues"][tissue] = defaultdict(list)
                    self.missing_tissues["tissues"][tissue][
                        filename
                    ] = missing_tissue_exception.tissues_dict[tissue]
                    if "existing" not in self.missing_tissues.keys():
                        self.missing_tissues[
                            "existing"
                        ] = missing_tissue_exception.existing

            # Consolidate related cross-file exceptions, like missing compounds
            # Note, this can change whether the AggregatedErrors for this file are fatal or not
            # Example result: self.missing_compounds = {
            #     "some missing compound name": {
            #         "formula": "C2O1H4",
            #         "files": {
            #             "accucor1.lxsx": [1, 8, 20],  # Rows where the compound is found
            #             "isocor5.xlsx": [99, 100],
            #         },
            #     },
            # }
            # This is the dict structure required by AllMissingCompounds's constructor.
            missing_compound_exceptions = exception.get_exception_type(MissingCompounds)
            for missing_compound_exception in missing_compound_exceptions:
                for compound in missing_compound_exception.compounds_dict.keys():
                    self.missing_compounds[compound][
                        "formula"
                    ] = missing_compound_exception.compounds_dict[compound]["formula"]
                    if "files" in self.missing_compounds[compound].keys():
                        self.missing_compounds[compound]["files"][
                            filename
                        ] = missing_compound_exception.compounds_dict[compound][
                            "rownums"
                        ]
                    else:
                        self.missing_compounds[compound]["files"] = {
                            filename: missing_compound_exception.compounds_dict[
                                compound
                            ]["rownums"]
                        }

        self.load_statuses.set_load_exception(exception, filepath)

    def create_grouped_exceptions(self):
        """
        This method compiles group-level exceptions, raises an AggregatedErrorsSet exception if fatal errors have been
        aggregated for any load file.
        """

        # Collect all the missing samples in 1 error to add to the animal sample table file
        if len(self.missing_samples) > 0:
            self.load_statuses.set_load_exception(
                AllMissingSamples(self.missing_samples),
                "All Samples Present in Sample Table File",
                top=True,
            )

        # Collect all the missing compounds in 1 error to add to the compounds file
        if len(self.missing_tissues) > 0:
            self.load_statuses.set_load_exception(
                AllMissingTissues(self.missing_tissues),
                "All Tissues Exist in the Database",
                top=True,
            )

        # Collect all the missing compounds in 1 error to add to the compounds file
        if len(self.missing_compounds) > 0:
            self.load_statuses.set_load_exception(
                AllMissingCompounds(self.missing_compounds),
                "All Compounds Exist in the Database",
                top=True,
            )

    def print_load_status(self):
        if self.verbosity > 1:
            for dct in self.load_statuses.get_status_messages():
                self.print_message(dct["message"], dct["state"])

        if self.verbosity > 0:
            message, state = self.load_statuses.get_status_message()
            self.print_message(message, state)

    def print_message(self, message, state):
        if state == "PASSED":
            self.stdout.write(self.style.SUCCESS(message))
        elif state == "WARNING":
            self.stdout.write(self.style.WARNING(message))
        elif state == "FAILED":
            self.stdout.write(self.style.ERROR(message))
