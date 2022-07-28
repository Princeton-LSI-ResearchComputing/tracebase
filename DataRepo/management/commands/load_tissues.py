import argparse

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

        # Used internally by the DataValidationView
        parser.add_argument(
            "--validate",
            required=False,
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )

        # Used internally to load necessary data into the validation database
        parser.add_argument(
            "--database",
            required=False,
            type=str,
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        if options["dry_run"]:
            self.stdout.write(
                self.style.MIGRATE_HEADING("DRY-RUN, NO CHANGES WILL BE SAVED")
            )

        # Keeping `na` to differentiate between intentional empty descriptions and spaces in the first column that were
        # intended to be tab characters
        new_tissues = pd.read_csv(options["tissues"], sep="\t", keep_default_na=True)

        self.tissue_loader = TissuesLoader(
            tissues=new_tissues,
            dry_run=options["dry_run"],
            database=options["database"],
            validate=options["validate"],
        )

        try:
            self.tissue_loader.load()
        except DryRun:
            if options["verbosity"] >= 2:
                self.print_notices(
                    self.tissue_loader.get_stats(),
                    options["tissues"],
                    options["verbosity"],
                )
            self.stdout.write(self.style.SUCCESS("DRY-RUN complete, no tissues loaded"))
        except LoadingError:
            if options["verbosity"] >= 2:
                self.print_notices(
                    self.tissue_loader.get_stats(),
                    options["tissues"],
                    options["verbosity"],
                )
            for exception in self.tissue_loader.errors:
                self.stdout.write(self.style.ERROR("ERROR: " + exception))
            raise CommandError(
                f"{len(self.tissue_loader.errors)} errors loading tissue records from "
                f"{options['tissues']} - NO RECORDS SAVED"
            )
        else:
            self.print_notices(
                self.tissue_loader.get_stats(), options["tissues"], options["verbosity"]
            )

    def print_notices(self, stats, opt, verbosity):

        if verbosity >= 2:
            for db in stats.keys():
                for stat in stats[db]["created"]:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Created {db} tissue record - {stat['tissue']}:{stat['description']}"
                        )
                    )
                for stat in stats[db]["skipped"]:
                    self.stdout.write(
                        f"Skipped {db} tissue record - {stat['tissue']}:{stat['description']}"
                    )

        smry = "Complete"
        for db in stats.keys():
            smry += f", loaded {len(stats[db]['created'])} new tissues and found "
            smry += f"{len(stats[db]['skipped'])} matching tissues "
            smry += f"in database [{db}]"
        smry += f" from {opt}"

        self.stdout.write(self.style.SUCCESS(smry))
