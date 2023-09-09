from django.core.management import BaseCommand

from DataRepo.models.maintained_model import MaintainedModel


class Command(BaseCommand):
    # Show this when the user types help
    help = "Update all maintained fields for every record in the database containing maintained fields.  Note that "
    "this assumes that no @MaintainedModel.setter function uses a maintained field in its calculation, because that "
    "would make the auto-updates order-dependent."

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
        MaintainedModel.rebuild_maintained_fields(
            "DataRepo.models",  # optional - should work without this, but supplying anyway
            label_filters=options["labels"],
            filter_in=not options["exclude"],
        )
