from django.db import transaction

from DataRepo.models import Study
from DataRepo.models.utilities import handle_load_db_errors
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueErrors,
    DryRun,
    LoadFileError,
)


class StudyTableLoader:
    """
    Load the Study table
    """

    def __init__(
        self,
        study_table_df,
        sheet=None,
        file=None,
        dry_run=True,
        defer_rollback=False,  # DO NOT USE MANUALLY
    ):
        self.aggregated_errors_object = AggregatedErrors()
        try:
            # Data
            self.study_table_df = study_table_df
            self.study_table_df.columns = self.study_table_df.columns.str.lower()

            # Modes
            self.dry_run = dry_run
            self.defer_rollback = defer_rollback

            # Error tracking
            self.conflicting_value_errors = []

            # Error reporting
            self.sheet = sheet
            self.file = file

        except Exception as e:
            self.aggregated_errors_object.buffer_error(e)
            if self.aggregated_errors_object.should_raise():
                raise self.aggregated_errors_object

    def load_study_table(self):
        with transaction.atomic():
            try:
                self._load_data()
            except Exception as e:
                # Add this unanticipated error to the other buffered errors
                self.aggregated_errors_object.buffer_error(e)

            if self.aggregated_errors_object.should_raise() and not self.defer_rollback:
                # Raise here to cause a rollback
                raise self.aggregated_errors_object

            if self.dry_run:
                raise DryRun()

        if self.aggregated_errors_object.should_raise():
            # Raise here to NOT cause a rollback
            raise self.aggregated_errors_object

    @transaction.atomic
    def _load_data(self):
        # Convert these values to None
        none_vals = ["", "nan"]

        for index, row in self.study_table_df.iterrows():
            try:
                code = (
                    row["study id"].strip()
                    if row["study id"].strip() not in none_vals
                    else None
                )
                name = (
                    row["name"].strip()
                    if row["name"].strip() not in none_vals
                    else None
                )
                desc = (
                    row["description"].strip()
                    if row["description"].strip() not in none_vals
                    else None
                )

                study_dict = {
                    "code": code,
                    "name": name,
                    "description": desc,
                }
            except Exception as exception:
                self.aggregated_errors_object.buffer_error(
                    LoadFileError(exception, index + 2)
                )

            try:
                study_rec, created = Study.objects.get_or_create(**study_dict)
                if created:
                    study_rec.full_clean()
                    study_rec.save()
            except Exception as exception:
                # Package IntegrityErrors and ValidationErrors with relevant details
                if not handle_load_db_errors(
                    # Data needed to yield useful errors to users
                    exception,
                    Study,
                    study_dict,
                    # What to do with the errors
                    aes=self.aggregated_errors_object,
                    conflicts_list=self.conflicting_value_errors,
                    # How to report the location of the data causing the error
                    rownum=index + 2,
                    sheet=self.sheet,
                    file=self.file,
                ):
                    # If the error was not handled, buffer the original error
                    self.aggregated_errors_object.buffer_error(exception)

        if len(self.conflicting_value_errors) > 0:
            self.aggregated_errors_object.buffer_error(
                ConflictingValueErrors(
                    model_name="PeakGroup",
                    conflicting_value_errors=self.conflicting_value_errors,
                ),
            )
