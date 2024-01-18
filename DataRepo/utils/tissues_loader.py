from django.core.exceptions import ValidationError
from django.db import transaction

from DataRepo.models import Tissue
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    DryRun,
    LoadFileError,
)


class TissuesLoader:
    """
    Load the Tissues table
    """

    def __init__(
        self,
        tissues,
        dry_run=True,
        defer_rollback=False,  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in atomic transact in caller)
    ):
        self.aggregated_errors_object = AggregatedErrors()
        try:
            # Data
            self.tissues = tissues
            self.tissues.columns = self.tissues.columns.str.lower()

            # Tracking stats
            self.notices = []  # List of strings that note what was done
            self.created = []
            self.existing = []  # Pre-existing, matching tissues

            # Modes
            self.dry_run = dry_run
            self.defer_rollback = defer_rollback

        except Exception as e:
            self.aggregated_errors_object.buffer_error(e)
            raise self.aggregated_errors_object

    def load_tissue_data(self):
        saved_aes = None

        with transaction.atomic():
            try:
                self._load_data()
            except AggregatedErrors as aes:
                if self.defer_rollback:
                    saved_aes = aes
                else:
                    # Raise here to cause a rollback
                    raise aes

        # If we were directed to defer rollback (in the event of an error), raise the exception here (outside of the
        # atomic transaction block).  This assumes that the caller is handling rollback in their own atomic transaction
        # block.
        if saved_aes:
            # Raise here to NOT cause a rollback
            raise saved_aes

    def _load_data(self):
        for index, row in self.tissues.iterrows():
            print(f"Loading tissues row {index+1}")
            try:
                with transaction.atomic():
                    name = row["name"]
                    description = row["description"]
                    # Note, the tsv parser returns a "nan" object when there's no value, which is evaluated as "nan" in
                    # string context, so change back to None
                    if str(name) == "nan":
                        name = None
                    if str(description) == "nan":
                        description = None
                    # To aid in debugging the case where an editor entered spaces instead of a tab...
                    if " " in str(name) and description is None:
                        raise ValidationError(
                            f"Tissue with name '{name}' cannot contain a space unless a description is provided.  "
                            "Either the space(s) must be changed to a tab character or a description must be provided."
                        )
                    if description is None:
                        description = ""
                    # We will assume that the validation DB has up-to-date tissues
                    tissue, created = Tissue.objects.get_or_create(name=name)
                    if created:
                        tissue.description = description
                        tissue.full_clean()
                        tissue.save()
                        self.created.append(tissue)
                        self.notices.append(
                            f"Created new tissue {tissue}:{description}"
                        )
                    elif tissue.description == description:
                        self.existing.append(tissue)
                        self.notices.append(
                            f"Matching tissue {tissue} already exists, skipping"
                        )
                    else:
                        self.aggregated_errors_object.buffer_error(
                            ConflictingValueError(
                                tissue,
                                differences={
                                    "description": {
                                        "orig": tissue.description,
                                        "new": description,
                                    },
                                },
                                rownum=index + 2,
                            )
                        )
            except Exception as e:
                self.aggregated_errors_object.buffer_error(LoadFileError(e, index + 2))

        if self.aggregated_errors_object.should_raise():
            raise self.aggregated_errors_object

        if self.dry_run:
            raise DryRun()

    def get_stats(self):
        stats = {}

        created = []
        for tissue in self.created:
            created.append({"tissue": tissue.name, "description": tissue.description})

        skipped = []
        for tissue in self.existing:
            skipped.append({"tissue": tissue.name, "description": tissue.description})

        stats = {
            "created": created,
            "skipped": skipped,
        }

        return stats
