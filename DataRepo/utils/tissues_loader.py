from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction

from DataRepo.models import Tissue
from DataRepo.utils.exceptions import (
    DryRun,
    LoadingError,
    ValidationDatabaseSetupError,
)


class TissuesLoader:
    """
    Load the Tissues table
    """

    def __init__(self, tissues, dry_run=True, database=None, validate=False):
        self.tissues = tissues
        self.tissues.columns = self.tissues.columns.str.lower()
        self.dry_run = dry_run
        # List of exceptions
        self.errors = []
        # List of strings that note what was done
        self.notices = []
        # Newly create tissues
        self.created = {}
        # Pre-existing, matching tissues
        self.existing = {}
        self.db = settings.TRACEBASE_DB
        self.loading_mode = "both"
        # If a database was explicitly supplied
        if database is not None:
            self.validate = False
            self.db = database
            self.loading_mode = "one"
        else:
            self.validate = validate
            if validate:
                if settings.VALIDATION_ENABLED:
                    self.db = settings.VALIDATION_DB
                else:
                    raise ValidationDatabaseSetupError()
                self.loading_mode = "one"
            else:
                self.loading_mode = "both"

    def load(self):
        # "both" is the normal loading mode - always loads both the validation and tracebase databases, unless the
        # database is explicitly supplied or --validate is supplied
        if self.loading_mode == "both":
            self.load_database(settings.TRACEBASE_DB)
            if settings.VALIDATION_ENABLED:
                self.load_database(settings.VALIDATION_DB)
        elif self.loading_mode == "one":
            self.load_database(self.db)
        else:
            raise Exception(
                f"Internal error: Invalid loading_mode: [{self.loading_mode}]"
            )

    @transaction.atomic
    def load_database(self, db):
        for index, row in self.tissues.iterrows():
            try:
                with transaction.atomic():
                    name = row["name"]
                    description = row["description"]
                    # To aid in debugging the case where an editor entered spaces instead of a tab...
                    if " " in name and description is None:
                        raise ValidationError(
                            f"Tissue with name '{name}' cannot contain a space unless a description is provided.  "
                            "Either the space(s) be changed to a tab character or a tab character must be appended to "
                            "the line."
                        )
                    if description is None:
                        description = ""
                    # We will assume that the validation DB has up-to-date tissues
                    tissue, created = Tissue.objects.using(db).get_or_create(name=name)
                    if created:
                        tissue.description = description
                        # full_clean cannot validate (e.g. uniqueness) using a non-default database
                        if db == settings.DEFAULT_DB:
                            tissue.full_clean()
                        tissue.save(using=db)
                        if db in self.created:
                            self.created[db].append(tissue)
                        else:
                            self.created[db] = [tissue]
                        self.notices.append(
                            f"Created new tissue {tissue}:{description} in the {db} database"
                        )
                    elif tissue.description == description:
                        if db in self.existing:
                            self.existing[db].append(tissue)
                        else:
                            self.existing[db] = [tissue]
                        self.notices.append(
                            f"Matching tissue {tissue} already exists, skipping"
                        )
                    else:
                        raise ValidationError(
                            f"Tissue with name = '{name}' but a different description already exists: "
                            f"Existing description = '{tissue.description}' "
                            f"New description = '{description}'"
                        )
            except (IntegrityError, ValidationError) as e:
                self.errors.append(f"Error in line {index}: {e}")
        if len(self.errors) > 0:
            raise LoadingError("Errors during tissue loading")
        if self.dry_run:
            raise DryRun("DRY-RUN successful")

    def get_stats(self):
        dbs = [settings.TRACEBASE_DB]
        if settings.VALIDATION_ENABLED:
            dbs.append(settings.VALIDATION_DB)
        stats = {}
        for db in dbs:

            created = []
            if db in self.created:
                for tissue in self.created[db]:
                    created.append(
                        {"tissue": tissue.name, "description": tissue.description}
                    )

            skipped = []
            if db in self.existing:
                for tissue in self.existing[db]:
                    skipped.append(
                        {"tissue": tissue.name, "description": tissue.description}
                    )

            stats[db] = {
                "created": created,
                "skipped": skipped,
            }

        return stats
