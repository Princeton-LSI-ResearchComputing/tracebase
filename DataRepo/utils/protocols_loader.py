from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction

from DataRepo.models import Protocol
from DataRepo.utils.exceptions import (
    DryRun,
    LoadingError,
    ValidationDatabaseSetupError,
)


class ProtocolsLoader:
    """
    Load the Protocols table
    """

    def __init__(self, protocols, category=None, database=None, validate=False, dry_run=True):
        self.protocols = protocols
        self.protocols.columns = self.protocols.columns.str.lower()
        self.dry_run = dry_run
        self.category = category
        # List of exceptions
        self.errors = []
        # List of strings that note what was done
        self.notices = []
        # Newly create protocols
        self.created = {}
        # Pre-existing, matching protocols
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
        for index, row in self.protocols.iterrows():
            print(f"Loading protocols row {index+1}")
            try:
                with transaction.atomic():
                    name = row["name"].strip()
                    description = row["description"].strip()
                    # prefer the file/dataframe-specified category, but user the
                    # loader initialization category, as a fallback
                    if "category" in row:
                        category = row["category"]
                    else:
                        category = self.category
                    category = category.strip()
                    # Note, the tsv parser returns a "nan" object when there's
                    # no value, which is evaluated as "nan" in string context,
                    # so change back to None
                    if str(name) == "nan":
                        name = None
                    if str(description) == "nan":
                        description = None
                    if str(category) == "nan":
                        category = None
                    # To aid in debugging the case where an editor entered spaces instead of a tab...
                    if " " in str(name) and description is None:
                        raise ValidationError(
                            f"Protocol with name '{name}' cannot contain a space unless a description is provided.  "
                            "Either the space(s) must be changed to a tab character or a description must be provided."
                        )
                    if category is None:
                        raise ValidationError(
                            f"Protocol with name '{name}' is missing a specified/defined category."
                        )
                    if description is None:
                        description = ""

                    # We will assume that the validation DB has up-to-date protocols
                    protocol, created = Protocol.objects.using(db).get_or_create(
                        name=name, category=category
                    )
                    if created:
                        protocol.description = description
                        # full_clean cannot validate (e.g. uniqueness) using a non-default database
                        if db == settings.DEFAULT_DB:
                            protocol.full_clean()
                        protocol.save(using=db)
                        if db in self.created:
                            self.created[db].append(protocol)
                        else:
                            self.created[db] = [protocol]
                        self.notices.append(
                            f"Created new protocol {protocol}:{description} in the {db} database"
                        )
                    elif protocol.description == description:
                        if db in self.existing:
                            self.existing[db].append(protocol)
                        else:
                            self.existing[db] = [protocol]
                        self.notices.append(
                            f"Matching protocol {protocol} already exists, skipping"
                        )
                    else:
                        raise ValidationError(
                            f"Protocol with name = '{name}' but a different description already exists: "
                            f"Existing description = '{protocol.description}' "
                            f"New description = '{description}'"
                        )
            except (IntegrityError, ValidationError) as e:
                self.errors.append(f"Error in row {index + 1}: {e}")
        if len(self.errors) > 0:
            message = ""
            for err in self.errors:
                message += f"{err}\n"

            raise LoadingError(f"Errors during protocol loading :\n {message}")
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
                for protocol in self.created[db]:
                    created.append(
                        {"protocol": protocol.name, "description": protocol.description}
                    )

            skipped = []
            if db in self.existing:
                for protocol in self.existing[db]:
                    skipped.append(
                        {"protocol": protocol.name, "description": protocol.description}
                    )

            stats[db] = {
                "created": created,
                "skipped": skipped,
            }

        return stats
