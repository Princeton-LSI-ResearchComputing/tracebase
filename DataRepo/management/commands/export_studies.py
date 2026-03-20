from django.conf import settings
from django.core.management import BaseCommand

from DataRepo.utils.studies_exporter import StudiesExporter


class Command(BaseCommand):
    # Show this when the user types help
    help = "Export peak data, peak groups, and fcirc formats, organized by study."

    def add_arguments(self, parser):
        parser.add_argument(
            "--outdir",
            required=True,
            default=settings.DOWNLOADS_DIR,
            help=f"[{settings.DOWNLOADS_DIR}] Directory to create and save exported files.",
        )
        parser.add_argument(
            "--data-type",
            required=False,
            choices=StudiesExporter.all_data_types,
            default=StudiesExporter.all_data_types,
            nargs="*",
            help="[All] Data types to export per study.",
        )
        parser.add_argument(
            "--studies",
            required=False,
            default=[],
            nargs="*",
            help="[All] Study names or record IDs.",
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            default=False,
            help="Overwrite existing files (not directories).",
        )

    def handle(self, *args, **options):
        se = StudiesExporter(
            outdir=options["outdir"],
            study_targets=options["studies"],
            data_types=options["data_type"],
            overwrite=options["overwrite"],
        )
        se.export()
