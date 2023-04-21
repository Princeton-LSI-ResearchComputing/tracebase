from django.core.management import BaseCommand

from DataRepo.models.maintained_model import (
    AutoUpdateFailed,
    clear_update_buffer,
    disable_autoupdates,
    disable_mass_autoupdates,
    enable_autoupdates,
    enable_mass_autoupdates,
    get_all_updaters,
    get_classes,
    get_max_generation,
    updater_list_has_labels,
)


def rebuild_maintained_fields(label_filters=[]):
    """
    Performs a mass update of all fields of every record in a breadth-first fashion without repeated updates to the
    same record over and over.
    """
    disable_autoupdates()
    enable_mass_autoupdates()

    # Get the largest generation value
    youngest_generation = get_max_generation(get_all_updaters(), label_filters)
    # Track what's been updated to prevent repeated updates triggered by multiple child updates
    updated = {}
    has_filters = len(label_filters) > 0

    # For every generation from the youngest leaves/children to root/parent
    for gen in sorted(range(youngest_generation + 1), reverse=True):

        # For every MaintainedModel derived class with decorated functions
        for cls in get_classes("DataRepo.models", gen, label_filters):
            class_name = cls.__name__

            try:
                updater_dicts = cls.get_my_updaters()
            except Exception as e:
                raise MissingMaintainedModelDerivedClass(class_name, e)

            # Leave the loop when the max generation present changes so that we can update the updated buffer with the
            # parent-triggered updates that were locally buffered during the execution of this loop
            max_gen = get_max_generation(updater_dicts, label_filters)
            if max_gen < gen:
                break

            # No need to perform updates if none of the updaters match the label filters
            if has_filters and not updater_list_has_labels(
                updater_dicts, label_filters
            ):
                break

            # For each record in the database for this model
            for rec in cls.objects.all():
                # Track updated records to avoid repeated updates
                key = f"{class_name}.{rec.pk}"

                # Try to perform the update. It could fail if the affected record was deleted
                try:
                    if key not in updated:
                        # Saving the record while performing_mass_autoupdates is True, causes auto-updates of every
                        # field included among the model's decorated functions.  It does not only update the fields
                        # indicated in decorators that contain the labels indicated in the label_filters.  The filters
                        # are only used to decide which records should be updated.  Currently, this is not an issue
                        # because we only have 1 update_label in use.  And if/when we add another label, it will only
                        # end up causing extra repeated updates of the same record.
                        rec.save()

                        # keep track that this record was updated
                        updated[key] = True

                except Exception as e:
                    raise AutoUpdateFailed(rec, e, updater_dicts)

    # We're done performing buffered updates
    disable_mass_autoupdates()
    enable_autoupdates()
    # Clear the buffer for good measure
    clear_update_buffer()


class Command(BaseCommand):

    # Show this when the user types help
    help = "Update all maintained fields for every record in the database containing maintained fields."

    def add_arguments(self, parser):
        parser.add_argument(
            "--labels",
            required=False,
            default=[],
            nargs="*",
            help="Only update maintained fields of records whose decorators are labeled with one of these labels.",
        )

    def handle(self, *args, **options):
        rebuild_maintained_fields(options["labels"])


class MissingMaintainedModelDerivedClass(Exception):
    def __init__(self, cls, err):
        message = f"The {cls} class must be imported so that its eval works in this script.  {err}"
        super().__init__(message)
        self.cls = cls
