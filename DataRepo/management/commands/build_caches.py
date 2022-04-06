from django.core.cache import cache
from django.core.management import BaseCommand

from DataRepo.hier_cached_model import (
    enable_caching_errors,
    enable_caching_retrievals,
    enable_caching_updates,
    get_cached_method_names,
)
from DataRepo.models.animal import Animal  # noqa: F401
from DataRepo.models.peak_data import PeakData  # noqa: F401
from DataRepo.models.peak_group import PeakGroup  # noqa: F401
from DataRepo.models.sample import Sample  # noqa: F401

# ^^^ Must import every HierCachedModel (because it's eval'd below)


def build_caches(clear):
    enable_caching_errors()
    enable_caching_retrievals()
    enable_caching_updates()
    func_name_lists = get_cached_method_names()

    if clear:
        cache.clear()

    for class_name in func_name_lists.keys():
        cls = eval(class_name)
        for cfunc_name in func_name_lists[class_name]:
            print(f"Building {class_name}.{cfunc_name} caches")
            cached_function_call(cls, cfunc_name)


def cached_function_call(cls, cfunc_name):
    """
    Iterates over every record in the database and caches the value for the supplied cached_function if it's not cached
    """
    for rec in cls.objects.all():
        try:
            getattr(rec, cfunc_name)
        except Exception as e:
            print(e)
    return True


class Command(BaseCommand):

    # Show this when the user types help
    help = "Builds cache values for all cached_functions"

    def add_arguments(self, parser):
        parser.add_argument(
            "--clear",
            required=False,
            action="store_true",
            default=False,
            help="Clear existing caches.  Default behavior is to only fill in missing cache values.",
        )

    def handle(self, *args, **options):
        build_caches(options["clear"])
