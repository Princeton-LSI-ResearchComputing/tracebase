from django.conf import settings
from django.core.management import BaseCommand

from DataRepo.utils.exports_organizer import ExportsOrganizer


class Command(BaseCommand):
    # Show this when the user types help
    help = (
        "Organize exported peak data, peak groups, fcirc, and mzXML formats into zip archive bundles by study and data "
        "type for each host and export date where something changed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--export-dir",
            required=False,
            default=settings.DOWNLOADS_DIR,
            help=f"[{settings.DOWNLOADS_DIR}] Directory to organize exported files into zip archives.",
        )
        parser.add_argument(
            "--staging-mode",
            action="store_true",
            default=False,
            help=(
                "Append a '.staged' extension to output files.  Note, this will not create staged files for existing "
                "unstaged files.  See --overwrite."
            ),
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            default=False,
            help="Overwrite existing zip packages (not study exports).",
        )

    def handle(self, *args, **options):
        eo = ExportsOrganizer()
        eo.organize(
            options["export_dir"],
            staging_mode=options["staging_mode"],
            overwrite=options["overwrite"],
        )
