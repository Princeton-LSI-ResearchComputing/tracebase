from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction

from DataRepo.models import Tissue
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    DryRun,
    LoadFileError,
    LoadingError,
)


class TissuesLoader:
    """
    Load the Tissues table
    """

    def __init__(self, tissues, dry_run=True, validate=False):
        self.aggregated_errors_object = AggregatedErrors()
        try:
            self.tissues = tissues
            self.tissues.columns = self.tissues.columns.str.lower()
            self.dry_run = dry_run
            # List of strings that note what was done
            self.notices = []
            # Newly create tissues
            self.created = []
            # Pre-existing, matching tissues
            self.existing = []
            self.validate = validate
        except Exception as e:
            self.aggregated_errors_object.buffer_error(e)
            raise self.aggregated_errors_object

    def load(self):
        self.load_database()

    @transaction.atomic
    def load_database(self):
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
                        raise ValidationError(
                            f"Tissue with name = '{name}' but a different description already exists:\n"
                            f"Existing description = '{tissue.description}'\n"
                            f"New description = '{description}'"
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
