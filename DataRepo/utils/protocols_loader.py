from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction

from DataRepo.models import Protocol
from DataRepo.utils.exceptions import DryRun, LoadingError


class ProtocolsLoader:
    """
    Load the Protocols table
    """

    STANDARD_NAME_HEADER = "name"
    STANDARD_DESCRIPTION_HEADER = "description"
    STANDARD_CATEGORY_HEADER = "category"

    def __init__(self, protocols, category=None, validate=False, dry_run=True):
        self.protocols = protocols
        self.protocols.columns = self.protocols.columns.str.lower()
        req_cols = [self.STANDARD_NAME_HEADER, self.STANDARD_DESCRIPTION_HEADER]
        missing_columns = list(set(req_cols) - set(self.protocols.columns))
        if missing_columns:
            raise KeyError(
                f"ProtocolsLoader missing required headers {missing_columns}"
            )
        self.dry_run = dry_run
        self.category = category
        # List of exceptions
        self.errors = []
        # List of strings that note what was done
        self.notices = []
        # Newly create protocols
        self.created = []
        # Pre-existing, matching protocols
        self.existing = []
        self.validate = validate

    def load(self):
        self.load_database()

    @transaction.atomic
    def load_database(self):
        for index, row in self.protocols.iterrows():
            try:
                name = row[self.STANDARD_NAME_HEADER]
                description = row[self.STANDARD_DESCRIPTION_HEADER]
                # prefer the file/dataframe-specified category, but user the
                # loader initialization category, as a fallback
                if self.STANDARD_CATEGORY_HEADER in row:
                    category = row[self.STANDARD_CATEGORY_HEADER]
                else:
                    category = self.category

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

                # Try and get the protocol
                protocol_rec, protocol_created = Protocol.objects.get_or_create(
                    name=name, category=category
                )
                # If no protocol was found, create it
                if protocol_created:
                    protocol_rec.description = description
                    print("Saving protocol with description")
                    protocol_rec.full_clean()
                    protocol_rec.save()
                    self.created.append(protocol_rec)
                    self.notices.append(
                        f"Created new protocol {protocol_rec}:{description}"
                    )
                elif protocol_rec.description == description:
                    self.existing.append(protocol_rec)
                    self.notices.append(
                        f"Matching protocol {protocol_rec} already exists, skipping"
                    )
                else:
                    raise ValidationError(
                        f"Protocol with name = '{name}' but a different description already exists: "
                        f"Existing description = '{protocol_rec.description}' "
                        f"New description = '{description}'"
                    )
            except (IntegrityError, ValidationError) as e:
                self.errors.append(
                    f"{type(e).__name__} in the database on data row {index + 1}, creating {category} record for "
                    f"protocol '{name}' with description '{description}': {e}"
                )
            except KeyError:
                raise ValidationError(
                    "ProtocolLoader requires a dataframe with 'name' and 'description' headers/keys."
                ) from None

        if len(self.errors) > 0:
            message = ""
            for err in self.errors:
                message += f"{err}\n"

            raise LoadingError(f"Errors during protocol loading :\n {message}")

        if self.dry_run:
            raise DryRun()

    def get_stats(self):
        stats = {}

        created = []
        for protocol in self.created:
            created.append(
                {"protocol": protocol.name, "description": protocol.description}
            )

        skipped = []
        for protocol in self.existing:
            skipped.append(
                {"protocol": protocol.name, "description": protocol.description}
            )

        stats = {
            "created": created,
            "skipped": skipped,
        }

        return stats
