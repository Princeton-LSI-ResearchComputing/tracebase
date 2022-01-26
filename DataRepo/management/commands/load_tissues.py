import pandas as pd
from django.core.management import BaseCommand, CommandError

from DataRepo.utils import DryRun, LoadingError, TissuesLoader


class Command(BaseCommand):
    # Show this when the user types help
    help = "Loads data from a tissue list into the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--tissues",
            type=str,
            help=(
                "Path to tab-delimited file containing the headers "
                "'Tissue','Description'"
            ),
            required=True,
        )

        # optional "do work" argument; otherwise, only reports of possible work
        parser.add_argument(
            "-n",
            "--dry-run",
            action="store_true",
            default=False,
            help=("Dry-run. If specified, nothing will be saved to the database. "),
        )

    def handle(self, *args, **options):
        if options["dry_run"]:
            self.stdout.write(
                self.style.MIGRATE_HEADING("DRY-RUN, NO CHANGES WILL BE SAVED")
            )

        new_tissues = pd.read_csv(options["tissues"], sep="\t", keep_default_na=False)

        self.tissue_loader = TissuesLoader(
            tissues=new_tissues, dry_run=options["dry_run"]
        )

        try:
            self.tissue_loader.load()
        except DryRun:
            if options["verbosity"] >= 2:
                self.print_notices()
            self.stdout.write(self.style.SUCCESS("DRY-RUN complete, no tissues loaded"))
        except LoadingError:
            if options["verbosity"] >= 2:
                self.print_notices()
            for exception in self.tissue_loader.errors:
                self.stdout.write(self.style.ERROR(exception))
            raise CommandError(
                f"{len(self.tissue_loader.errors)} errors loading tissue records from "
                f"{options['tissues']} - NO RECORDS SAVED"
            )
        else:
            if options["verbosity"] >= 2:
                self.print_notices()
            self.stdout.write(
                self.style.SUCCESS(
                    f"Complete, loaded {len(self.tissue_loader.created)} new tissues and found "
                    f"{len(self.tissue_loader.existing)} matching tissues from {options['tissues']}"
                )
            )

    def print_notices(self):
        for tissue in self.tissue_loader.created:
            self.stdout.write(
                f"Created tissue record - {tissue.name}:{tissue.description}"
            )
        for tissue in self.tissue_loader.existing:
            self.stdout.write(
                f"Skipping existing tissue record - {tissue.name}:{tissue.description}"
            )
