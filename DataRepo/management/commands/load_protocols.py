import argparse
import pathlib

import numpy as np
import pandas as pd
from django.core.management import BaseCommand, CommandError

from DataRepo.models.protocol import Protocol
from DataRepo.utils import AggregatedErrors, DryRun, ProtocolsLoader


class Command(BaseCommand):
    # Show this when the user types help
    help = "Loads data from a protocol list into the database"

    name_header = "Name"
    category_header = "Category"
    description_header = "Description"

    # default XLXS template headers and assumed category value
    TREATMENTS_SHEET_NAME = "Treatments"
    TREATMENTS_NAME_HEADER = "Animal Treatment"
    TREATMENTS_DESC_HEADER = "Treatment Description"
    TREATMENTS_CATEGORY_VALUE = Protocol.ANIMAL_TREATMENT

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_category = None  # Set from the excel spreadsheet

    def add_arguments(self, parser):
        parser.add_argument(
            "--protocols",
            type=str,
            help=(
                "Path to EITHER a tab-delimited file containing the headers "
                f"'{self.name_header}','{self.category_header}','{self.description_header}' "
                f"OR a path to an xlsx workbook file containing a sheet named '{self.TREATMENTS_SHEET_NAME}' "
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

        # Intended for use by load_study to prevent rollback of changes in the event of an error so that for example,
        # subsequent loading scripts can validate with all necessary data present
        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in outer atomic transact)
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        if options["dry_run"]:
            self.stdout.write(
                self.style.MIGRATE_HEADING("DRY-RUN, NO CHANGES WILL BE SAVED")
            )

        self.protocols_df, self.batch_category = self.read_from_file(
            options["protocols"]
        )

        self.protocol_loader = ProtocolsLoader(
            protocols=self.protocols_df,
            category=self.batch_category,
            dry_run=options["dry_run"],
            verbosity=options["verbosity"],
            defer_rollback=options["defer_rollback"],
        )

        try:
            self.protocol_loader.load_protocol_data()

            self.stdout.write(
                self.style.SUCCESS(
                    f"Load complete, inserted {self.protocol_loader.created} and skipped "
                    f"{self.protocol_loader.existing} existing records"
                )
            )
        except DryRun:
            self.stdout.write(
                self.style.SUCCESS(
                    f"DRY-RUN complete, would have inserted {self.protocol_loader.created} and skipped "
                    f"{self.protocol_loader.existing} existing records"
                )
            )
        except AggregatedErrors as aes:
            aes.print_summary()
            raise aes

    def read_from_file(self, filename, format=None):
        """
        Read protocols from a file and buffer their contents in the instance object to be loaded later
        """
        format_choices = ("tsv", "xlsx")
        batch_category = None

        if format is None:
            format = pathlib.Path(filename).suffix.strip(".")

        if format == "tsv":
            protocols_df = self.read_protocols_tsv(filename)
        elif format == "xlsx":
            protocols_df, batch_category = self.read_protocols_xlsx(filename)
        else:
            raise CommandError(
                f"Invalid file format reading samples: [{format}], expected one of [{', '.join(format_choices)}].",
            )

        # Tidy the data up
        protocols_df = protocols_df.replace(np.nan, "", regex=True)
        for col in protocols_df.columns:
            protocols_df[col] = protocols_df[col].str.strip()

        return protocols_df, batch_category

    def read_protocols_tsv(self, protocols_tsv):
        # Keeping `na` to differentiate between intentional empty descriptions and spaces in the first column that were
        # intended to be tab characters
        protocols_df = pd.read_table(
            protocols_tsv,
            dtype=object,
            keep_default_na=False,
            na_values="",
        )
        # rename template columns to ProtocolsLoader expectations
        protocols_df.rename(
            inplace=True,
            columns={
                str(self.name_header): ProtocolsLoader.STANDARD_NAME_HEADER,
                str(self.category_header): ProtocolsLoader.STANDARD_CATEGORY_HEADER,
                str(
                    self.description_header
                ): ProtocolsLoader.STANDARD_DESCRIPTION_HEADER,
            },
        )

        return protocols_df

    def read_protocols_xlsx(self, xlxs_file_containing_treatments_sheet):
        self.stdout.write(self.style.MIGRATE_HEADING("Loading animal treatments..."))
        name_header = self.TREATMENTS_NAME_HEADER
        description_header = self.TREATMENTS_DESC_HEADER

        protocols_df = pd.read_excel(
            xlxs_file_containing_treatments_sheet,
            sheet_name=self.TREATMENTS_SHEET_NAME,
            dtype={
                name_header: str,
                description_header: str,
            },
            keep_default_na=False,
            engine="openpyxl",
        )

        # rename template columns to ProtocolsLoader expectations
        protocols_df.rename(
            inplace=True,
            columns={
                str(name_header): ProtocolsLoader.STANDARD_NAME_HEADER,
                str(description_header): ProtocolsLoader.STANDARD_DESCRIPTION_HEADER,
            },
        )

        return protocols_df, self.TREATMENTS_CATEGORY_VALUE
