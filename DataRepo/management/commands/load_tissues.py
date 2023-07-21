import argparse

import pandas as pd
from django.core.management import BaseCommand

from DataRepo.utils import DryRun, TissuesLoader


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

        # Used internally by the DataValidationView
        parser.add_argument(
            "--validate",
            required=False,
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        # Keeping `na` to differentiate between intentional empty descriptions and spaces in the first column that were
        # intended to be tab characters
        new_tissues = pd.read_csv(options["tissues"], sep="\t", keep_default_na=True)

        self.tissue_loader = TissuesLoader(
            tissues=new_tissues,
            dry_run=options["dry_run"],
            validate=options["validate"],
        )

        try:
            self.tissue_loader.load()
        except DryRun:
            pass

        self.print_notices(
            self.tissue_loader.get_stats(), options["tissues"], options["verbosity"]
        )

    def print_notices(self, stats, opt, verbosity):
        if verbosity >= 2:
            for stat in stats["created"]:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Created tissue record - {stat['tissue']}:{stat['description']}"
                    )
                )
            for stat in stats["skipped"]:
                self.stdout.write(
                    f"Skipped tissue record - {stat['tissue']}:{stat['description']}"
                )

        smry = "Complete"
        smry += f", loaded {len(stats['created'])} new tissues and found "
        smry += f"{len(stats['skipped'])} matching tissues"
        smry += f" from {opt}"

        self.stdout.write(self.style.SUCCESS(smry))
