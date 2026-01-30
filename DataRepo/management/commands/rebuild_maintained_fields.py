from django.core.management import BaseCommand

from DataRepo.models.hier_cached_model import (
    caching_updates,
    caching_retrievals,
    disable_caching_retrievals,
    disable_caching_updates,
    enable_caching_retrievals,
    enable_caching_updates,
)
from DataRepo.models.maintained_model import MaintainedModel


class Command(BaseCommand):
    help = (
        "Update all maintained fields for every record in the database containing maintained fields.\n\nNote: This "
        "assumes that no @MaintainedModel.setter function uses a maintained field in its calculation, because that "
        "would make the auto-updates order-dependent."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--labels",
            required=False,
            default=[],
            nargs="*",
            help="Only update maintained fields of records whose decorators are labeled with one of these labels.",
        )
        parser.add_argument(
            "--exclude",
            action="store_true",
            default=False,
            help="Invert the --labels option.  I.e. Exclude records whose decorators contain the labels specified by "
            "--labels.",
        )

    def handle(self, *args, **options):
        try:
            # Maintained field rebuilds are accomplished by calling .save() on every record.  This should not be done
            # with caching updates enabled.  Caching retrievals are also disabled because, although the maintained model
            # setter methods should not use cached values (and vice-versa), if they do, always calculating the cached
            # value is guaranteed to be accurate and this makes their updates not be order-dependent.
            saved_caching_updates = caching_updates
            saved_caching_retrievals = caching_retrievals
            if caching_retrievals:
                disable_caching_retrievals()
            if caching_updates:
                disable_caching_updates()

            MaintainedModel.rebuild_maintained_fields(
                "DataRepo.models",  # optional - should work without this, but supplying anyway
                label_filters=options["labels"],
                filter_in=not options["exclude"],
            )
        finally:
            if saved_caching_updates:
                enable_caching_updates()
            if saved_caching_retrievals:
                enable_caching_retrievals()
