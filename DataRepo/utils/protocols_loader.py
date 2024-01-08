from collections import defaultdict

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction

from DataRepo.models import Protocol
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    DryRun,
    InfileDatabaseError,
    NoSpaceAllowedWhenOneColumn,
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
        self,
        protocols,
        category=None,
        dry_run=True,
        verbosity=1,
        defer_rollback=False,
    ):
        # Data
        self.protocols = protocols
        self.category = category

        # Check supplied protocols for the basics
        self.protocols.columns = self.protocols.columns.str.lower()
        self.req_cols = [self.STANDARD_NAME_HEADER, self.STANDARD_DESCRIPTION_HEADER]
        self.missing_columns = list(set(self.req_cols) - set(self.protocols.columns))
        if self.missing_columns:
            raise RequiredHeadersError(self.missing_columns)

        # Modes
        self.dry_run = dry_run
        self.verbosity = verbosity
        # Whether to rollback upon error or keep the changes and defer rollback to the caller
        self.defer_rollback = defer_rollback

        # Tracking stats (num created/existing)
        self.created = 0
        self.existing = 0

        # Error tracking
        self.space_no_desc = []
        self.missing_reqd_vals = defaultdict(list)
        self.aggregated_errors_object = AggregatedErrors()

    def load_protocol_data(self):
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
        if saved_aes is not None:
            # Raise here to NOT cause a rollback
            raise saved_aes

    def _load_data(self):
        batch_cat_err_occurred = False
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
                # Check required values
                if name is None or category is None:
                    if name is None:
                        self.missing_reqd_vals[self.STANDARD_NAME_HEADER].append(index)
                    if category is None:
                        self.missing_reqd_vals[self.STANDARD_CATEGORY_HEADER].append(
                            index
                        )
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
                    # Retrieve the protocol with the conflicting value(s) that caused the unique constraint error
                    protocol_rec = Protocol.objects.get(name__exact=rec_dict["name"])
                    self.aggregated_errors_object.buffer_error(
                        ConflictingValueError(
                            protocol_rec,
                            "description",
                            protocol_rec.description,
                            description,
                            index + 2,
                            "treatments",
                        )
                    )
                else:
                    self.aggregated_errors_object.buffer_error(
                        InfileDatabaseError(ie, rec_dict, rownum=index + 2)
                    )
            except ValidationError as ve:
                vestr = str(ve)
                if (
                    self.category is not None
                    and "category" in vestr
                    and "is not a valid choice" in vestr
                ):
                    # Only include a batch category error once
                    if not batch_cat_err_occurred:
                        self.aggregated_errors_object.buffer_error(
                            InfileDatabaseError(ve, rec_dict, rownum=index + 2)
                        )
                    batch_cat_err_occurred = True
                else:
                    self.aggregated_errors_object.buffer_error(
                        InfileDatabaseError(ve, rec_dict, rownum=index + 2)
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
