from argparse import RawTextHelpFormatter

from django.core.management import BaseCommand

from DataRepo.models.hier_cached_model import (
    CACHING_RETRIEVALS,
    CACHING_UPDATES,
    THROW_CACHE_ERRORS,
    HierCachedModel,
    delete_all_caches,
    disable_caching_errors,
    disable_caching_retrievals,
    disable_caching_updates,
    enable_caching_errors,
    enable_caching_retrievals,
    enable_caching_updates,
    get_cached_method_names,
)

# This builds a string to use in the help text when the user supplied -h
NLT = "\n  "
NLTT = "\n    "
FUNCS_STR = NLT.join(
    [f"{k}\n    {NLTT.join(v)}" for k, v in get_cached_method_names().items()]
)


class Command(BaseCommand):
    help = (
        "Builds missing cached values for all model fields with the following cached_functions:\n"
        f"{NLT}{FUNCS_STR}\n"
        "\n"
        "To monitor cache building progress in another terminal window, you can run `python manage.py shell` and use "
        "any of the following commands:\n"
        f"{NLT}from DataRepo.models.hier_cached_model import get_cache_table_size, HierCachedModel\n"
        f"{NLT}get_cache_table_size()"
        f"{NLT}HierCachedModel.get_final_cache_table_size()"
        f"{NLT}HierCachedModel.get_cache_table_size_per_model()  # This one is slower, but more detailed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--models",
            required=False,
            default=[],
            nargs="+",
            help=(
                "Only update cached fields of these models.  Supply model names as space-demilited, e.g. `--models "
                "model1 model2 model3`.  See -h for a list of model names containing cached functions."
            ),
        )
        parser.add_argument(
            "--functions",
            required=False,
            default=[],
            nargs="+",
            help=(
                "Only update cached fields whose update functions exactly match any of these function names.  Supply "
                "function names as space-demilited, e.g. `--functions name1 name2 name3`.  See -h for a list of "
                "function names available per model."
            ),
        )
        parser.add_argument(
            "--clear",
            action="store_true",
            default=False,
            help=(
                "Clear all existing cached values first.  Note, the default behavior is only to set a cached value if "
                "one does not already exist."
            ),
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            default=False,
            help=(
                "Overwrite existing cache values.  Only use this if using --models and/or --functions.  Otherwise use "
                "--clear."
            ),
        )
        parser.add_argument(
            "--errors",
            action="store_true",
            default=False,
            help=(
                "Enable caching errors.  The default behavior is to ignore errors that arised from the "
                "cached_functions.  Used for debugging."
            ),
        )
        parser.add_argument(
            "--new-only",
            action="store_true",
            default=False,
            help=(
                "Only build cached_function values for model records whose primary key is greater than the max key for "
                "that model in the cache table.  This is intended to be run immediately after a study load, to only "
                "update cached values for the new data.  WARNING: Cache builds can happen randomly (as data is "
                "displayed) if the new data is browsed on the site.  If that happens, note that this option may not "
                "build caches for all the new data."
            ),
        )

    def handle(self, *args, **options):
        save_updates = CACHING_UPDATES
        save_retrievals = CACHING_RETRIEVALS
        save_errors = THROW_CACHE_ERRORS
        if not CACHING_RETRIEVALS:
            enable_caching_retrievals()
        if not CACHING_UPDATES:
            enable_caching_updates()

        if options["overwrite"] and CACHING_RETRIEVALS:
            disable_caching_retrievals()
        elif not options["overwrite"] and not CACHING_RETRIEVALS:
            enable_caching_retrievals()

        if options["errors"] and not THROW_CACHE_ERRORS:
            enable_caching_updates()
        elif not options["errors"] and THROW_CACHE_ERRORS:
            disable_caching_errors()

        if options["clear"]:
            delete_all_caches()

        try:
            HierCachedModel.build_cached_fields(
                model_names=options["models"],
                func_names=options["functions"],
                new_only=options["new_only"],
            )
        finally:
            if not save_updates:
                disable_caching_updates()

            if save_retrievals is not CACHING_RETRIEVALS:
                if save_retrievals:
                    enable_caching_retrievals()
                else:
                    disable_caching_retrievals()

            if save_errors is not THROW_CACHE_ERRORS:
                if save_errors:
                    enable_caching_errors()
                else:
                    disable_caching_errors()

    def create_parser(self, *args, **kwargs):
        """This extends the superclass method to allow multi-line help text.
        See: https://stackoverflow.com/a/35470682
        """
        parser = super().create_parser(*args, **kwargs)
        parser.formatter_class = RawTextHelpFormatter
        return parser
