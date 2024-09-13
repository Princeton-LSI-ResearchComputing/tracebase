import cProfile
import os
from typing import Optional

from django.core.management import BaseCommand

from DataRepo.views.upload.submission import BuildSubmissionView


def profile(study_doc, peak_annot_files: Optional[list] = None):

    # Set up all the input files
    study_doc_name = None
    peak_annot_filenames = []
    dvv = BuildSubmissionView()
    if study_doc is not None:
        _, study_doc_name = os.path.split(study_doc)
    if peak_annot_files is None:
        peak_annot_files = []
    else:
        peak_annot_filenames = [
            fn for _, fn in [os.path.split(f) for f in peak_annot_files]
        ]

    dvv.set_files(
        study_doc,
        study_doc_name,
        peak_annot_files,
        peak_annot_filenames,
    )

    # Now process them
    dvv.get_download_data()


class Command(BaseCommand):
    # Show this when the user types help
    help = "Profiles the submission page processing"

    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        parser.add_argument(
            "--study-file",
            type=str,
            help="The input study doc.",
            default=None,
            required=False,
        )
        parser.add_argument(
            "--annot-files",
            type=str,
            help="The input peak annotation files.",
            default=None,
            required=False,
            nargs="*",
        )

    def handle(self, **options):
        p = cProfile.Profile()
        p.runcall(profile, options["study_file"], options["annot_files"])
        p.print_stats()
