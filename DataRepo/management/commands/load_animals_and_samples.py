import argparse

import yaml  # type: ignore
from django.core.management import BaseCommand, CommandError

from DataRepo.loaders.sample_table_loader import SampleTableLoader
from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.utils.file_utils import merge_dataframes, read_from_file


class Command(BaseCommand):
    examples_dir = "DataRepo/data/examples/obob_fasted/"
    example_animals = examples_dir + "obob_animals_table.tsv"
    example_samples = examples_dir + "obob_samples_table.tsv"
    example_yaml = examples_dir + "sample_and_animal_tables_headers.yaml"
    example_lcms = "DataRepo/data/tests/small_obob_lcms_metadata/glucose.xlsx"

    # Show this when the user types help
    help = (
        "Loads data from animal and sample tables into the database. "
        "Rows where 'Tissue' is empty will be skipped "
        "(assumed to be blank samples). "
        f"Example usage : manage.py load_animals_and_samples --sample-table-filename {example_samples}"
        f" --animal-table-filename {example_animals} --table-headers {example_yaml}"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--animal-and-sample-table-filename",
            required=False,
            type=str,
            help=(
                f"Excel file containing the sample-specific annotations, (e.g. {self.example_samples}).  Required "
                "sheet names: 'Animals' and 'Samples'.  Supported extensions: ['tsv', 'xlsx']."
            ),
        )
        parser.add_argument(
            "--sample-table-filename",
            required=False,
            type=str,
            help=(
                "Excel or tab-delimited file containing the sample-specific annotations, (e.g. "
                f"{self.example_samples}).  Required sheet name for excel: 'Samples'.  Supported extensions: ['tsv', "
                "'xlsx']."
            ),
        )
        parser.add_argument(
            "--animal-table-filename",
            required=False,
            type=str,
            help=(
                "Excel or tab-delimited file containing the animal-specific annotations, (e.g. "
                f"{self.example_animals}).  Required sheet name for excel: 'Animals'.  Supported extensions: ['tsv', "
                "'xlsx']."
            ),
        )
        parser.add_argument(
            "--lcms-file",
            type=str,
            help=(
                "Excel or tab-delimited file containing metadata associated with the liquid chromatography and mass "
                f"spec instrument run, (e.g. {self.example_lcms}).  If an excel file is used, it will use the sheet "
                "named 'LCMS Metadata' or the first sheet."
            ),
            default=None,
            required=False,
        )
        parser.add_argument(
            "--table-headers",
            type=str,
            help=f"YAML file defining headers to be used, for example : {self.example_yaml}",
        )
        # optional skip researcher check argument.  Since a file can have multiple researchers, you can't check the
        # opposite way unless we added an option that takes a list of new researchers.
        parser.add_argument(
            "--skip-researcher-check",
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            # This issues a DryRun error, to abort the transaction
            help="Dry run mode. Will not change the database.",
        )
        # Used internally by the DataValidationView
        parser.add_argument(
            "--validate",  # Only affects what is/isn't a warning
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
        # Intended for use by load_study to prevent rollback of changes in the event of an error so that for example,
        # subsequent loading scripts can validate with all necessary data present
        parser.add_argument(
            "--defer-rollback",  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in outer atomic transact)
            action="store_true",
            help=argparse.SUPPRESS,
        )
        # Used internally by the validation view, as temporary data should not trigger cache deletions
        parser.add_argument(
            "--skip-cache-updates",
            required=False,
            action="store_true",
            default=False,
            help=argparse.SUPPRESS,
        )

    @MaintainedModel.defer_autoupdates(
        label_filters=["name"],
        disable_opt_names=["validate", "dry_run"],
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def handle(self, *args, **options):
        lcms_metadata_df = None
        if options["lcms_file"] is not None:
            lcms_metadata_df = read_from_file(
                options["lcms_file"],
                sheet="LCMS Metadata",
            )

        self.stdout.write(self.style.MIGRATE_HEADING("Reading header definition..."))
        if options["table_headers"]:
            with open(options["table_headers"]) as headers_file:
                header_def = yaml.safe_load(headers_file)
                headers = SampleTableLoader.SampleTableHeaders(**header_def)
        else:
            headers = SampleTableLoader.DefaultSampleTableHeaders
        self.stdout.write(yaml.dump(dict(headers._asdict()), explicit_start=True))

        if options["animal_and_sample_table_filename"]:
            sample_table_filename = options["animal_and_sample_table_filename"]
            animal_table_filename = options["animal_and_sample_table_filename"]
        elif options["sample_table_filename"] and options["animal_table_filename"]:
            sample_table_filename = options["sample_table_filename"]
            animal_table_filename = options["animal_table_filename"]
        else:
            raise CommandError(
                "You must specify either:\n"
                "\t--animal-and-sample-table-filename or\n"
                "\t --animal-table-filename and --sample-table-filename"
            )

        self.stdout.write(self.style.MIGRATE_HEADING("Loading animals..."))
        animals = read_from_file(
            animal_table_filename,
            dtype={headers.ANIMAL_NAME: str, headers.ANIMAL_TREATMENT: str},
            sheet="Animals",
        )

        self.stdout.write(self.style.MIGRATE_HEADING("Loading samples..."))
        samples = read_from_file(
            filepath=sample_table_filename,
            dtype={headers.ANIMAL_NAME: str},
            sheet="Samples",
        )

        # merge the two files/dataframes together on Animal ID
        self.stdout.write(
            self.style.MIGRATE_HEADING("Merging animal and samples tables...")
        )
        merged = merge_dataframes(left=samples, right=animals, on=headers.ANIMAL_NAME)

        self.stdout.write(
            self.style.MIGRATE_HEADING("Importing animals and samples...")
        )
        loader = SampleTableLoader(
            sample_table_headers=headers,
            validate=options["validate"],
            skip_researcher_check=options["skip_researcher_check"],
            verbosity=options["verbosity"],
            defer_rollback=options["defer_rollback"],
            dry_run=options["dry_run"],
            update_caches=not options["skip_cache_updates"],
            lcms_metadata_df=lcms_metadata_df,
        )
        loader.load_sample_table(
            merged.to_dict("records"),
        )

        self.stdout.write(self.style.SUCCESS("Done loading sample table"))
