import argparse
import os

from django.core.exceptions import ValidationError
from django.core.management import BaseCommand, call_command

from DataRepo.utils.exceptions import AggregatedErrors


class Command(BaseCommand):

    # Show this when the user types help
    help = (
        "Loads a set of studies using the load_study command. Input is a file of "
        "filenames listing load_study YAML config files, one per line."
        "Example usage: manage.py load_study_set list_of_study_config_files.txt"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "study_set_list",
            type=argparse.FileType("r"),
            help=("File of load_study config filenames, one per line"),
        )

    def handle(self, *args, **options):

        studies_loaded = list()
        studies_skipped = list()
        studies_failed = list()
        study_set_dir = os.path.dirname(
            os.path.realpath(options["study_set_list"].name)
        )
        with options["study_set_list"] as study_set:
            for study in study_set:
                try:
                    study_path = os.path.join(study_set_dir, study.strip())
                    study_dir_name = os.path.dirname(study)
                    self.stdout.write(
                        self.style.MIGRATE_HEADING(f"Loading study using {study_path}")
                    )
                    call_command(
                        "load_study", study_path, verbosity=options["verbosity"]
                    )
                    studies_loaded.append(study_dir_name)
                except AggregatedErrors as aes:
                    # The first error that should be encountered if a study was already loaded is a ValidationError
                    # containing the following string.  The theory is that it would have been rolled back if the
                    # previous load had failed and there wouldn't have been this error.
                    if (
                        aes.num_errors == 1
                        and isinstance(aes.exceptions[0], ValidationError)
                        and "Peak group set with this Filename already exists."
                        in str(aes.exceptions[0])
                    ):
                        self.stdout.write(
                            self.style.WARNING(
                                f"It appears that study {study_path} has already been loaded.  Skipping."
                            )
                        )
                        studies_skipped.append(study_dir_name)
                        continue
                    else:
                        self.stdout.write(
                            self.style.ERROR(
                                f"Study {study_path} had {aes.num_errors} errors and failed to load."
                            )
                        )
                        studies_failed.append(study_dir_name)
                        continue

        self.stdout.write(
            self.style.SUCCESS(f"Completed loading of {len(studies_loaded)} studies")
        )

        nlindent = "\n\t"
        if len(studies_skipped) > 0:
            self.stdout.write(
                self.style.WARNING(
                    f"Skipped loading of {len(studies_skipped)} studies:\n"
                    f"\t{nlindent.join(studies_skipped)}"
                )
            )
        if len(studies_failed) > 0:
            self.stdout.write(
                self.style.ERROR(
                    f"Failed loading of {len(studies_failed)} studies:\n"
                    f"\t{nlindent.join(studies_failed)}"
                )
            )
