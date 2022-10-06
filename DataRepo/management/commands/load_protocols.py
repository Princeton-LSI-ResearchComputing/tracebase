from DataRepo.utils import DryRun, LoadingError, ProtocolsLoader
from django.core.management import BaseCommand, CommandError
import pandas as pd
import argparse


class Command(BaseCommand):
    # Show this when the user types help
    help = "Loads data from a protocol list into the database"

    name_header = "Name"
    category_header = "Category"
    description_header = "Description"

    def add_arguments(self, parser):
        parser.add_argument(
            "--protocols",
            type=str,
            help=(
                "Path to tab-delimited file containing the headers "
                f"'{self.name_header}','{self.category_header}','{self.description_header}'"
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
        new_protocols = pd.read_csv(options["protocols"], sep="\t", keep_default_na=True)
        # rename template columns to ProtocolLoader expectations
        new_protocols.rename(
            inplace=True,
            columns={
                str(self.name_header): "name",
                str(self.category_header): "category",
                str(self.description_header): "description",
            },
        )

        self.protocol_loader = ProtocolsLoader(
            protocols=new_protocols,
            dry_run=options["dry_run"],
            database=options["database"],
            validate=options["validate"],
        )

        try:
            self.protocol_loader.load()
        except DryRun:
            if options["verbosity"] >= 2:
                self.print_notices(
                    self.protocol_loader.get_stats(),
                    options["protocols"],
                    options["verbosity"],
                )
            self.stdout.write(self.style.SUCCESS("DRY-RUN complete, no protocols loaded"))
        except LoadingError:
            if options["verbosity"] >= 2:
                self.print_notices(
                    self.protocol_loader.get_stats(),
                    options["protocols"],
                    options["verbosity"],
                )
            for exception in self.protocol_loader.errors:
                self.stdout.write(self.style.ERROR("ERROR: " + exception))
            raise CommandError(
                f"{len(self.protocol_loader.errors)} errors loading protocol records from "
                f"{options['protocols']} - NO RECORDS SAVED"
            )
        else:
            self.print_notices(
                self.protocol_loader.get_stats(), options["protocols"], options["verbosity"]
            )

    def print_notices(self, stats, opt, verbosity):

        if verbosity >= 2:
            for db in stats.keys():
                for stat in stats[db]["created"]:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Created {db} protocol record - {stat['protocol']}:{stat['description']}"
                        )
                    )
                for stat in stats[db]["skipped"]:
                    self.stdout.write(
                        f"Skipped {db} protocol record - {stat['protocol']}:{stat['description']}"
                    )

        smry = "Complete"
        for db in stats.keys():
            smry += f", loaded {len(stats[db]['created'])} new protocols and found "
            smry += f"{len(stats[db]['skipped'])} matching protocols "
            smry += f"in database [{db}]"
        smry += f" from {opt}"

        self.stdout.write(self.style.SUCCESS(smry))
