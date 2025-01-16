import argparse
import os
import sys

from django.core.management import BaseCommand, call_command

from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.utils.exceptions import AggregatedErrorsSet


class Command(BaseCommand):
    help = (
        "Loads a set of studies using the load_study command. Input is a file of filenames listing study doc files, "
        "one per line.\n"
        "Example usage: python manage.py load_study_set list_of_study_docs.txt"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "study_set_list",
            type=argparse.FileType("r"),
            help=("File of study doc filenames, one per line."),
        )

    @MaintainedModel.defer_autoupdates(
        # There is no dry-run or validate mode in this script, so mass autoupdate and buffering will never be disabled
        # here.
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def handle(self, *args, **options):
        set_file = str(options["study_set_list"])
        studies_loaded = list()
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

                    call_command("load_study", infile=study_path)

                    studies_loaded.append(study_dir_name)

                    self.stdout.write(
                        self.style.SUCCESS(f"Loaded study '{study_dir_name}'")
                    )
                except AggregatedErrorsSet as aes:
                    self.stdout.write(
                        self.style.ERROR(
                            f"Study {study_path} failed with {aes.num_errors} errors and {aes.num_warnings} warnings."
                        )
                    )
                    studies_failed.append(study_dir_name)
                    continue

        msg = f"'{set_file}' Done.\n"

        if len(studies_loaded) == 0 and len(studies_failed) == 0:
            msg += "Study set file empty.\n"
            self.stdout.write(self.style.WARNING(msg))
        else:
            msg += f"Studies loaded: [{len(studies_loaded)}], failed: [{len(studies_failed)}].\n"

            # Overall stats
            if len(studies_failed) > 0:
                self.stdout.write(self.style.SUCCESS(msg))
            else:
                self.stdout.write(self.style.ERROR(msg))

            # Details
            nlindent = "\n\t"
            if len(studies_loaded) > 0:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Loaded studies:\n\t{nlindent.join(studies_loaded)}"
                    )
                )
            if len(studies_failed) > 0:
                self.stdout.write(
                    self.style.ERROR(
                        f"Failed studies:\n\t{nlindent.join(studies_failed)}"
                    )
                )

        if len(studies_loaded) == 0:
            sys.exit(1)
