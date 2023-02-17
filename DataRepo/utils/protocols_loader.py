from collections import defaultdict

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction

from DataRepo.models import Protocol
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    DryRun,
    RequiredHeadersError,
    RequiredValuesError,
)


class ProtocolsLoader:
    """
    Load the Protocols table
    """

    STANDARD_NAME_HEADER = "name"
    STANDARD_DESCRIPTION_HEADER = "description"
    STANDARD_CATEGORY_HEADER = "category"

    def __init__(
        self, protocols, category=None, validate=False, dry_run=True, verbosity=1
    ):
        self.protocols = protocols
        self.protocols.columns = self.protocols.columns.str.lower()
        self.req_cols = [self.STANDARD_NAME_HEADER, self.STANDARD_DESCRIPTION_HEADER]
        self.missing_columns = list(set(self.req_cols) - set(self.protocols.columns))
        if self.missing_columns:
            raise RequiredHeadersError(self.missing_columns)
        self.dry_run = dry_run
        self.category = category
        self.created = 0
        self.existing = 0
        self.space_no_desc = []
        self.missing_reqd_vals = defaultdict(list)
        self.validate = validate
        self.verbosity = verbosity
        self.aggregated_errors_object = AggregatedErrors()

    def load(self):
        self.load_database()

    @transaction.atomic
    def load_database(self):
        for index, row in self.protocols.iterrows():
            try:
                name = self.getRowVal(row, self.STANDARD_NAME_HEADER)
                category = self.getRowVal(row, self.STANDARD_CATEGORY_HEADER)
                description = self.getRowVal(row, self.STANDARD_DESCRIPTION_HEADER)

                # The command line option overrides what's in the file
                if self.category:
                    category = self.category

                # Validate the values provided in the file
                # To aid in debugging the case where an editor entered spaces instead of a tab...
                if " " in str(name) and description is None:
                    self.aggregated_errors_object.buffer_error(
                        NoSpaceAllowedWhenOneColumn(name)
                    )
                    continue
                if category is None:
                    self.missing_reqd_vals[self.STANDARD_CATEGORY_HEADER].append(index)
                    continue

                rec_dict = {
                    "name": name,
                    "category": category,
                    "description": description,
                }

                # Try and get the protocol
                protocol_rec, protocol_created = Protocol.objects.get_or_create(
                    **rec_dict
                )

                # If no protocol was found, create it
                if protocol_created:
                    protocol_rec.full_clean()
                    protocol_rec.save()
                    self.created += 1
                else:
                    self.existing += 1

            except IntegrityError as ie:
                iestr = str(ie)
                if "duplicate key value violates unique constraint" in iestr:
                    self.aggregated_errors_object.buffer_error(
                        ConflictingValueError(
                            protocol_rec,
                            "description",
                            protocol_rec.description,
                            description,
                            index + 2,
                        )
                    )
                else:
                    self.aggregated_errors_object.buffer_error(
                        InfileDatabaseError(ie, index + 2, rec_dict)
                    )
            except ValidationError as ve:
                self.aggregated_errors_object.buffer_error(
                    InfileDatabaseError(ve, index + 2, rec_dict)
                )

        if len(self.missing_reqd_vals.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                RequiredValuesError(self.missing_reqd_vals)
            )

        if self.aggregated_errors_object.should_raise():
            raise self.aggregated_errors_object

        if self.dry_run:
            raise DryRun()

    def getRowVal(self, row, header):
        val = None

        if header in row.keys():
            val = row[header]

            # This will make later checks of values easier
            if val == "":
                val = None

        return val


class NoSpaceAllowedWhenOneColumn(Exception):
    def __init__(self, name):
        message = (
            f"Protocol with name '{name}' cannot contain a space unless a description is provided.  "
            "Either the space(s) must be changed to a tab character or a description must be provided."
        )
        super().__init__(message)
        self.name = name


class InfileDatabaseError(Exception):
    def __init__(self, exception, line_num, rec_dict):
        nltab = "\n\t"
        deets = [f"{k}: {v}" for k, v in rec_dict]
        message = (
            f"{type(exception).__name__} on infile line {line_num}, creating record:\n\t{nltab.join(deets)}\n"
            f"{str(exception)}"
        )
        super().__init__(message)
        self.exception = exception
        self.line_num = line_num
        self.rec_dict = rec_dict
