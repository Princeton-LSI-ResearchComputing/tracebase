import argparse
import os

from django.core.management import BaseCommand, call_command


class Command(BaseCommand):

    # Show this when the user types help
    help = (
        "Loads a set of study using the load_study command. Input is a file of "
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
        study_set_dir = os.path.dirname(
            os.path.realpath(options["study_set_list"].name)
        )
        with options["study_set_list"] as study_set:
            for study in study_set:
                study_path = os.path.join(study_set_dir, study.strip())
                self.stdout.write(
                    self.style.MIGRATE_HEADING(f"Loading study using {study_path}")
                )
                call_command("load_study", study_path)
                studies_loaded.append(study_path)

        self.stdout.write(
            self.style.SUCCESS(f"Completed loading of {len(studies_loaded)} studies")
        )
