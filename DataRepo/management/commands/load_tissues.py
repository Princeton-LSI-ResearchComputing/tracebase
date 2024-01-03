import argparse

from django.core.management import BaseCommand

from DataRepo.utils import (
    AggregatedErrors,
    DryRun,
    TissuesLoader,
    read_from_file,
)


class Command(BaseCommand):
    # Show this when the user types help
    help = "Loads data from a tissue list into the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--tissues",
            type=str,
            help=(
                "Path to either a tab-delimited file or excel file with a sheet named 'Tissues'.  "
                "Required headers: 'Tissue' & 'Description'"
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

        # Intended for use by load_study to prevent rollback of changes in the event of an error so that for example,
        # subsequent loading scripts can validate with all necessary data present
        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in outer atomic transact)
            action="store_true",
            help=argparse.SUPPRESS,
        )

    def handle(self, *args, **options):
        try:
            # Keeping `na` to differentiate between intentional empty descriptions and spaces in the first column that
            # were intended to be tab characters
            new_tissues = read_from_file(
                options["tissues"], sheet="Tissues", keep_default_na=True
            )

            self.tissue_loader = TissuesLoader(
                tissues=new_tissues,
                dry_run=options["dry_run"],
                defer_rollback=options["defer_rollback"],
            )

            self.tissue_loader.load_tissue_data()
        except DryRun:
            pass
        except AggregatedErrors as aes:
            aes.print_summary()
            raise aes
        except Exception as e:
            aes2 = AggregatedErrors()
            aes2.buffer_error(e)
            if aes2.should_raise():
                aes2.print_summary()
                raise aes2

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
