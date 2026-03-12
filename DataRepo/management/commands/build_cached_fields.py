from argparse import RawTextHelpFormatter

from django.core.management import BaseCommand

from DataRepo.models.hier_cached_model import (
    HierCachedModel,
    caching_retrievals,
    caching_updates,
    delete_all_caches,
    disable_caching_errors,
    disable_caching_retrievals,
    disable_caching_updates,
    enable_caching_errors,
    enable_caching_retrievals,
    enable_caching_updates,
    get_cached_method_names,
    throw_cache_errors,
)

# This builds a string to use in the help text when the user supplied -h
nlt = "\n  "
nltt = "\n    "
funcs_str = nlt.join(
    [f"{k}\n    {nltt.join(v)}" for k, v in get_cached_method_names().items()]
)


class Command(BaseCommand):
    help = (
        "Builds missing cached values for all model fields with the following cached_functions:\n"
        f"{nlt}{funcs_str}"
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
        save_updates = caching_updates
        save_retrievals = caching_retrievals
        save_errors = throw_cache_errors

        if not caching_updates:
            enable_caching_updates()

        if options["overwrite"] and caching_retrievals:
            disable_caching_retrievals()
        elif not options["overwrite"] and not caching_retrievals:
            enable_caching_retrievals()

        if options["errors"] and not throw_cache_errors:
            enable_caching_updates()
        elif not options["errors"] and throw_cache_errors:
            disable_caching_errors

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

            if save_retrievals is not caching_retrievals:
                if save_retrievals:
                    enable_caching_retrievals()
                else:
                    disable_caching_retrievals()

            if save_errors is not throw_cache_errors:
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
