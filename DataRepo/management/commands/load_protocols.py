import argparse
import pathlib

import pandas as pd
from django.core.management import BaseCommand, CommandError

from DataRepo.models.protocol import Protocol
from DataRepo.utils import DryRun, LoadingError, ProtocolsLoader


class Command(BaseCommand):
    # Show this when the user types help
    help = "Loads data from a protocol list into the database"

    name_header = "Name"
    category_header = "Category"
    description_header = "Description"

    # default XLXS template headers and assumed category value
    TREATMENT_SHEET_NAME = "Treatments"
    TREATMENTS_NAME_HEADER = "Animal Treatment"
    TREATMENTS_DESC_HEADER = "Treatment Description"
    TREATMENTS_CTGR_VALUE = Protocol.ANIMAL_TREATMENT

    # Used (e.g.) to set the category for a 2-column (name/desc) "treatments" tab in an xlsx file
    batch_category = None

    def add_arguments(self, parser):
        parser.add_argument(
            "--protocols",
            type=str,
            help=(
                "Path to EITHER a tab-delimited file containing the headers "
                f"'{self.name_header}','{self.category_header}','{self.description_header}' "
                f"OR a path to an xlsx workbook file containing a sheet named '{self.TREATMENT_SHEET_NAME}' "
                f"with the headers '{self.TREATMENTS_NAME_HEADER}','{self.TREATMENTS_DESC_HEADER}'"
            ),
            required=True,
        )

        # Optional flag to only show what would be loaded. Does not load in this mode
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

        # Used to explicitly load the supplied database.  This option is hidden and mentioned in error messages to
        # resolve synch issues.
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

        self.read_from_file(options["protocols"])

        loader_args = {
            "protocols": self.new_protocols_df,
            "dry_run": options["dry_run"],
            "database": options["database"],
            "validate": options["validate"],
        }
        if self.batch_category:
            loader_args["category"] = self.batch_category

        self.protocol_loader = ProtocolsLoader(**loader_args)

        try:
            self.protocol_loader.load()
        except DryRun:
            if options["verbosity"] >= 2:
                self.print_notices(
                    self.protocol_loader.get_stats(),
                    options["protocols"],
                    options["verbosity"],
                )
            self.stdout.write(
                self.style.SUCCESS("DRY-RUN complete, no protocols loaded")
            )
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
                self.protocol_loader.get_stats(),
                options["protocols"],
                options["verbosity"],
            )

    def read_from_file(self, filename, format=None):
        """
        Read protocols from a file and buffer their contents in the instance object to be loaded later
        """
        format_choices = ("tsv", "xlsx")
        if format is None:
            format = pathlib.Path(filename).suffix.strip(".")
        if format == "tsv":
            self.read_protocols_tsv(filename)
        elif format == "xlsx":
            self.extract_treatments(filename)
        else:
            raise CommandError(
                'Invalid file format reading samples: "%s", expected one of [%s].',
                format,
                ", ".join(format_choices),
            )

    def read_protocols_tsv(self, protocols_tsv):

        # Keeping `na` to differentiate between intentional empty descriptions and spaces in the first column that were
        # intended to be tab characters
        new_protocols = pd.read_table(protocols_tsv, dtype=str, keep_default_na=True)
        # rename template columns to ProtocolLoader expectations
        new_protocols.rename(
            inplace=True,
            columns={
                str(self.name_header): "name",
                str(self.category_header): "category",
                str(self.description_header): "description",
            },
        )

        self.new_protocols_df = new_protocols

    def extract_treatments(self, xlxs_file_containing_treatments_sheet):
        self.stdout.write(self.style.MIGRATE_HEADING("Loading animal treatments..."))
        nh = self.TREATMENTS_NAME_HEADER
        dh = self.TREATMENTS_DESC_HEADER

        treatments = pd.read_excel(
            xlxs_file_containing_treatments_sheet,
            sheet_name=self.TREATMENT_SHEET_NAME,
            dtype={
                nh: str,
                dh: str,
            },
            keep_default_na=False,
            engine="openpyxl",
        )

        # rename template columns to ProtocolLoader expectations
        treatments.rename(
            inplace=True,
            columns={
                str(nh): "name",
                str(dh): "description",
            },
        )
        self.new_protocols_df = treatments
        self.batch_category = self.TREATMENTS_CTGR_VALUE

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
