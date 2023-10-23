import argparse
from csv import DictReader

import yaml  # type: ignore
from django.core.management import BaseCommand

from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.utils import SampleTableLoader


class Command(BaseCommand):
    # Show this when the user types help
    help = (
        "Loads data from a sample table into the database."
        "Rows where 'Tissue' is empty will be skipped "
        "(assumed to be blank samples)."
    )

    def add_arguments(self, parser):
        parser.add_argument("sample_table_filename", type=str)
        parser.add_argument(
            "--sample-table-headers",
            type=str,
            help="YAML file defining headers to be used",
        )
        # optional skip researcher check argument.  Since a file can have multiple researchers, you can't check the
        # opposite way unless we added an option that takes a list of new researchers.
        parser.add_argument(
            "--skip-researcher-check",
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )
        # Used internally by the DataValidationView
        parser.add_argument(
            "--validate",  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK UPON ERROR (handle in outer atomic transact)
            required=False,
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )
        # Intended for use by load_study to prevent individual loader autoupdates and buffer clearing, then perform all
        # mass autoupdates/buffer-clearings after all load scripts are complete
        parser.add_argument(
            "--defer-autoupdates",
            action="store_true",
            help=argparse.SUPPRESS,
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Dry run mode. Will not change the database.",
        )
        parser.add_argument(
            "--skip-cache-updates",
            required=False,
            action="store_true",
            default=False,
            help="Do not delete stale cache values associated with inserted records.",
        )

    @MaintainedModel.defer_autoupdates(
        label_filters=["name"],
        disable_opt_names=["validate", "dry_run"],
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def handle(self, *args, **options):
        print("Reading header definition")
        if options["sample_table_headers"]:
            with open(options["sample_table_headers"]) as headers_file:
                header_def = yaml.safe_load(headers_file)
                headers = SampleTableLoader.SampleTableHeaders(
                    **header_def,
                )
        else:
            headers = SampleTableLoader.DefaultSampleTableHeaders
        print(f"{headers}")
        print("Loading sample table")
        loader = SampleTableLoader(
            sample_table_headers=headers,
            validate=options[
                "validate"
            ],  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK UPON ERROR
            skip_researcher_check=options["skip_researcher_check"],
            verbosity=options["verbosity"],
            dry_run=options["dry_run"],
            update_caches=not options["skip_cache_updates"],
        )
        loader.load_sample_table(
            DictReader(
                open(options["sample_table_filename"]),
                dialect="excel-tab",
            ),
        )
        print("Done loading sample table")
