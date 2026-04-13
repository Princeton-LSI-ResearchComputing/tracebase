from datetime import datetime

from django.conf import settings
from django.core.management import BaseCommand

from DataRepo.utils.studies_exporter import StudiesExporter


class Command(BaseCommand):
    # Show this when the user types help
    help = "Export peak data, peak groups, fcirc, and mzxml formats."

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
        parser.add_argument(
            "--host",
            required=False,
            default=StudiesExporter.default_instance,
            help=(
                f"[{StudiesExporter.default_instance} Host name to include in the exported file names.  "
                "Use this so users can identify files exported from different TraceBase instances."
            ),
        )
        parser.add_argument(
            "--date",
            type=datetime.fromisoformat,
            help="Date of export.  ISO format: YYYY-MM-DDTHH:MM:SS.",
            default=datetime.now(),
        )

    def handle(self, *args, **options):
        se = StudiesExporter(
            outdir=options["outdir"],
            study_targets=options["studies"],
            data_types=options["data_type"],
            overwrite=options["overwrite"],
            host=options["host"],
            date=(
                datetime.fromisoformat(options["date"])
                if isinstance(options["date"], str) and options["date"] != ""
                else None
            ),
        )
        se.export()
