import argparse
import os
import sys

import yaml  # type: ignore
from django.core.exceptions import ValidationError
from django.core.management import BaseCommand, call_command

from DataRepo.models.maintained_model import (
    UncleanBufferError,
    buffer_size,
    clear_update_buffer,
    disable_autoupdates,
    enable_autoupdates,
    perform_buffered_updates,
)
from DataRepo.utils.exceptions import AggregatedErrorsSet


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
        # Certain errors will prompt the user to supply this flag if the contents of the buffer are determined to be
        # stale
        parser.add_argument(
            "--clear-buffer",
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):

        # The buffer can only exist as long as the existence of the process, but since this method can be called from
        # code, who knows what has been done before.  So the clear_buffer option allows the load_study_set method to be
        # called in code with an option to explicitly clean the buffer.
        if options["clear_buffer"]:
            clear_update_buffer()
        elif buffer_size() > 0:
            raise UncleanBufferError(
                "The auto-update buffer is unexpectedly populated.  Add --clear-buffer to your command to flush the "
                "buffer and proceed with the load."
            )

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
                    # TODO: This was intended to be called using defer_autoupdates=True, however I realized that if
                    # there is an error in 1 study after N successfully loaded studies, there's currently no means to
                    # clear only the autoupdates of just the failed study load from the autoupdate buffer.  Until that
                    # has been implemented, each study, at the end of its load, will either clear the buffer or process
                    # its autoupdates.
                    call_command(
                        "load_study", study_path, verbosity=options["verbosity"]
                    )
                    studies_loaded.append(study_dir_name)
                except AggregatedErrorsSet as aes:
                    # Grab the parameters of this study to be able to tell whether the study was previously loaded or
                    # if this was a failed load due to some new error
                    with open(study_path, "r") as stream:
                        study_params = yaml.safe_load(stream)
                    # Basically, we just need the number of accucor + isocorr files
                    num_peak_files = len(study_params["accucor_data"]["accucor_files"])
                    # We need to assure that there's only 1 error in each load file
                    num_peak_files_with_1_err = len(list(
                        x
                        for x in list(aes.aggregated_errors_dict.values())
                        if x.num_errors == 1
                    ))
                    # And that that one error is a ValidationError that says "Peak group set with this Filename already
                    # exists."
                    first_pgse_exceptions = list(
                        x.exceptions[0]
                        for x in list(aes.aggregated_errors_dict.values())
                        if (
                            x.num_errors == 1
                            and isinstance(x.exceptions[0], ValidationError)
                            and "Peak group set with this Filename already exists."
                            in str(x.exceptions[0])
                        )
                    )
                    # The first and only error (in each peak file) that should be encountered if a study was already
                    # loaded is a ValidationError containing "Peak group set with this Filename already exists.".  If a
                    # previous attempt to load this study had failed this error wouldn't exist.
                    if (
                        aes.num_errors == num_peak_files
                        and num_peak_files_with_1_err == num_peak_files
                        and len(first_pgse_exceptions) == num_peak_files
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
                    f"Skipped loading of {len(studies_skipped)} (already loaded) studies:\n"
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
            sys.exit(1)

    def perform_autoupdates(self):
        disable_autoupdates()
        disable_caching_updates()
        perform_buffered_updates()
        # The buffer should be clear, but just for good measure...
        clear_update_buffer()
        enable_caching_updates()
        enable_autoupdates()
