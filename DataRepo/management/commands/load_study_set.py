import argparse
import os
import sys

from django.core.management import BaseCommand, call_command

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

    def handle(self, *args, **options):
        set_file_handle = options["study_set_list"]
        set_file = set_file_handle.name
        studies_loaded = list()
        studies_skipped = list()
        studies_failed = list()
        study_set_dir = os.path.dirname(os.path.realpath(set_file))
        with set_file_handle as study_set:
            study_doc_str: str
            for study_doc_str in study_set:

                try:
                    study_doc_str = study_doc_str.strip()
                    study_path = os.path.join(study_set_dir, study_doc_str)
                    study_dir_name = os.path.dirname(study_doc_str)

                    if study_doc_str.startswith("#"):
                        skipped_dir_name = study_dir_name.lstrip("# ")
                        self.stdout.write(
                            self.style.MIGRATE_HEADING(
                                f"Skipping commented study '{skipped_dir_name}'"
                            )
                        )
                        studies_skipped.append(skipped_dir_name)
                        continue

                    if not os.path.isfile(study_path):
                        self.stdout.write(
                            self.style.ERROR(
                                f"FileNotFound: No such file or directory: '{study_path}'"
                            )
                        )
                        studies_failed.append(study_dir_name)
                        continue

                    self.stdout.write(
                        self.style.MIGRATE_HEADING(f"Loading study '{study_dir_name}'")
                    )

                    call_command(
                        "load_study",
                        infile=study_path,
                    )

                    studies_loaded.append(study_dir_name)

                    self.stdout.write(
                        self.style.SUCCESS(f"Loaded study '{study_dir_name}'")
                    )
                except AggregatedErrorsSet as aes:
                    self.stdout.write(
                        self.style.ERROR(
                            f"Study '{study_dir_name}' failed with {aes.num_errors} errors and {aes.num_warnings} "
                            "warnings."
                        )
                    )
                    studies_failed.append(study_dir_name)
                    continue

        msg = f"\n'{set_file}' Done.\n"

        if len(studies_loaded) == 0 and len(studies_failed) == 0:
            msg += "Study set file empty.\n"
            self.stdout.write(self.style.WARNING(msg))
        else:
            msg += (
                f"Studies loaded: [{len(studies_loaded)}], failed: [{len(studies_failed)}], "
                f"skipped: [{len(studies_skipped)}].\n"
            )

            # Overall stats
            if len(studies_failed) > 0:
                self.stdout.write(self.style.ERROR(msg))
            else:
                self.stdout.write(self.style.SUCCESS(msg))

            # Details
            nlindent = "\n\t"
            if len(studies_loaded) > 0:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Loaded studies:\n\t{nlindent.join(studies_loaded)}"
                    )
                )
            if len(studies_skipped) > 0:
                self.stdout.write(
                    self.style.WARNING(
                        f"Skipped studies:\n\t{nlindent.join(studies_skipped)}"
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
