from argparse import RawTextHelpFormatter

from django.core.management import BaseCommand

from DataRepo.models.hier_cached_model import (
    HierCachedModel,
    caching_retrievals,
    caching_updates,
    delete_all_caches,
    disable_caching_retrievals,
    disable_caching_updates,
    enable_caching_retrievals,
    enable_caching_updates,
    get_cached_method_names,
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
            nargs="*",
            help="Only update cached fields of these models.",
        )
        parser.add_argument(
            "--functions",
            required=False,
            default=[],
            nargs="*",
            help="Only update cached fields whose update functions exactly match any of these function names.",
        )
        parser.add_argument(
            "--clear",
            action="store_true",
            default=False,
            help="Clear existing cached values first.",
        )

    def handle(self, *args, **options):
        save_retrievals = caching_retrievals
        save_updates = caching_updates
        if not caching_retrievals:
            enable_caching_retrievals()
        if not caching_updates:
            enable_caching_updates()

        if options["clear"]:
            delete_all_caches()

        try:
            HierCachedModel.build_cached_fields(
                model_names=options["models"],
                func_names=options["functions"],
            )
        except Exception as e:
            if not save_updates:
                disable_caching_updates()
            if not save_retrievals:
                disable_caching_retrievals()
            raise e

        if not save_updates:
            disable_caching_updates()
        if not save_retrievals:
            disable_caching_retrievals()

    def create_parser(self, *args, **kwargs):
        """This extends the superclass method to allow multi-line help text.
        See: https://stackoverflow.com/a/35470682
        """
        parser = super().create_parser(*args, **kwargs)
        parser.formatter_class = RawTextHelpFormatter
        return parser
