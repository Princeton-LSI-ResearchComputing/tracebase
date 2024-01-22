from django.core.exceptions import ValidationError
from django.db import transaction

from DataRepo.models import Tissue
from DataRepo.models.utilities import handle_load_db_errors
from DataRepo.utils.exceptions import AggregatedErrors, DryRun, LoadFileError


class TissuesLoader:
    """
    Load the Tissues table
    """

    def __init__(
        self,
        tissues,
        dry_run=True,
        defer_rollback=False,  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in atomic transact in caller)
        sheet=None,
        file=None,
    ):
        self.aggregated_errors_object = AggregatedErrors()
        try:
            # Data
            self.tissues = tissues
            self.tissues.columns = self.tissues.columns.str.lower()

            # Tracking stats
            self.created = 0
            self.existing = 0
            self.erroneous = 0

            # Error tracking
            self.conflicting_value_errors = []

            # Error reporting
            self.sheet = sheet
            self.file = file

            # Modes
            self.dry_run = dry_run
            self.defer_rollback = defer_rollback

        except Exception as e:
            self.aggregated_errors_object.buffer_error(e)
            raise self.aggregated_errors_object

    def load_tissue_data(self):
        with transaction.atomic():
            try:
                self._load_data()
            except Exception as e:
                # Add this unanticipated error to the other buffered errors
                self.aggregated_errors_object.buffer_error(e)

            if (
                self.aggregated_errors_object.should_raise()
                and not self.defer_rollback
            ):
                # Raise here to cause a rollback
                raise self.aggregated_errors_object

            if self.dry_run:
                # Raise here to cause a rollback
                raise DryRun()

        if self.aggregated_errors_object.should_raise():
            # Raise here to NOT cause a rollback
            raise self.aggregated_errors_object

        return self.created, self.existing, self.erroneous

    def _load_data(self):
        none_vals = ["", "nan"]

        for index, row in self.tissues.iterrows():
            # Index starts at 0, headers are on row 1
            rownum = index + 2
            try:
                name = (
                    row["name"].strip()
                    if row["name"].strip() not in none_vals
                    else None
                )
                description = (
                    row["description"].strip()
                    if row["description"].strip() not in none_vals
                    else None
                )

                rec_dict = {
                    "name": name,
                    "description": description,
                }

                # We will assume that the validation DB has up-to-date tissues
                tissue, created = Tissue.objects.get_or_create(**rec_dict)
                if created:
                    tissue.full_clean()
                    tissue.save()
                    self.created += 1
                else:
                    self.existing += 1

            except Exception as e:
                print(f"VALUES: {row['name']} {row['description']} TYPES: {type(row['name'])} {type(row['description'])}")
                # Package IntegrityErrors and ValidationErrors with relevant details
                if not handle_load_db_errors(
                    # Data needed to yield useful errors to users
                    e,
                    Tissue,
                    rec_dict,
                    # What to do with the errors
                    aes=self.aggregated_errors_object,
                    conflicts_list=self.conflicting_value_errors,
                    # How to report the location of the data causing the error
                    rownum=rownum,
                    sheet=self.sheet,
                    file=self.file,
                ):
                    # If the error was not handled, buffer the original error
                    self.aggregated_errors_object.buffer_error(
                        LoadFileError(e, rownum, sheet=self.sheet, file=self.file)
                    )
                self.erroneous += 1
