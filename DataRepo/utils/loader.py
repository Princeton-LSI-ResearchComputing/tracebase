import re
from collections import defaultdict
from typing import Optional

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Q

from DataRepo.models.utilities import (
    get_enumerated_fields,
    get_unique_constraint_fields,
    get_unique_fields,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    ConflictingValueErrors,
    DryRun,
    DuplicateValueErrors,
    DuplicateValues,
    InfileDatabaseError,
    RequiredHeadersError,
    RequiredValueError,
    RequiredValueErrors,
    UnknownHeadersError,
)
from DataRepo.utils.file_utils import get_column_dupes, get_one_column_dupes


class TraceBaseLoader:
    def __init__(
        self,
        df,
        all_headers=None,
        reqd_headers=None,
        reqd_values=None,
        unique_constraints=None,
        dry_run=False,
        defer_rollback=False,  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in atomic transact in caller)
        sheet=None,
        file=None,
        models=None,
    ):
        # File data
        self.df = df

        # Metadata
        self.all_headers = all_headers
        self.reqd_headers = reqd_headers
        self.reqd_values = reqd_values
        self.unique_constraints = unique_constraints
        self.models = models

        # Load stats
        self.record_counts = defaultdict(lambda: defaultdict(int))
        if models is not None:
            for mdl in models:
                self.record_counts[mdl.__name__]["created"] = 0
                self.record_counts[mdl.__name__]["existed"] = 0
                self.record_counts[mdl.__name__]["errored"] = 0

        # Running Modes
        self.dry_run = dry_run
        self.defer_rollback = defer_rollback

        # Error tracking
        self.skip_row_indexes = []
        self.aggregated_errors_object = AggregatedErrors()
        self.conflicting_value_errors = []
        self.required_value_errors = []

        # For error reporting
        self.file = file
        self.sheet = sheet

    def check_headers(self):
        if self.all_headers is not None:
            unknown_headers = []
            for file_header in self.df.columns:
                if file_header not in self.all_headers:
                    unknown_headers.append(file_header)
            if len(unknown_headers) > 0:
                self.aggregated_errors_object.buffer_error(
                    UnknownHeadersError(
                        unknown_headers, file=self.file, sheet=self.sheet
                    )
                )

        if self.reqd_headers is not None:
            missing_headers = []
            for rqd_header in self.reqd_headers:
                if rqd_header not in self.df.columns:
                    missing_headers.append(rqd_header)
            if len(missing_headers) > 0:
                # Cannot proceed, so not buffering
                raise RequiredHeadersError(
                    missing_headers, file=self.file, sheet=self.sheet
                )

    def check_unique_constraints(self):
        """Check in-file unique constraints

        Handling unique constraints by catching IntegrityErrors lacks context.  Did the load encounter pre-existing data
        or was the data in the file not unique?  There's no way to tell the user from catching the IntegrityError where
        the duplicate is.  Handling the unique constraints at the file level allows the user to tell where all the
        duplicate values are.

        Exceptions Buffered:
            DuplicateValues

        Returns:
            Nothing
        """
        if self.unique_constraints is None:
            return
        for unique_combo in self.unique_constraints:
            # A single field unique requirements is much cleaner to display than unique combos, so handle differently
            if len(unique_combo) == 1:
                dupes, row_idxs = get_one_column_dupes(self.df, unique_combo[0])
            else:
                dupes, row_idxs = get_column_dupes(self.df, unique_combo)
            self.add_skip_row_index(index_list=row_idxs)
            if len(dupes) > 0:
                self.aggregated_errors_object.buffer_error(
                    DuplicateValues(
                        dupes, unique_combo, sheet=self.sheet, file=self.file
                    )
                )

    def add_skip_row_index(
        self, index: Optional[int] = None, index_list: Optional[list] = None
    ):
        if index is None and index_list is None:
            # Raise programming errors (data errors are buffered)
            raise ValueError("Either an index or index_list argument is required.")
        if index is not None and index not in self.skip_row_indexes:
            self.skip_row_indexes.append(index)
        if index_list is not None:
            for idx in index_list:
                if idx not in self.skip_row_indexes:
                    self.skip_row_indexes.append(idx)

    def get_skip_row_indexes(self):
        return self.skip_row_indexes

    def getRowVal(self, row, header):
        none_vals = ["", "nan"]
        val = None

        if header in row:
            val = row[header]
            if type(val) == str:
                val = val.strip()
            if val in none_vals:
                val = None
        elif self.all_headers is not None and header not in self.all_headers:
            # Missing headers are addressed way before this. If we get here, it's a programming issue, so raise instead
            # of buffer
            raise ValueError(
                f"Incorrect header supplied: [{header}].  Must be one of: {self.all_headers}"
            )

        return val

    @staticmethod
    def loader(fn):
        def load_wrapper(self, *args, **kwargs):
            with transaction.atomic():
                try:
                    self.check_headers()
                    self.check_unique_constraints()

                    fn(self, *args, **kwargs)

                except Exception as e:
                    # Add this unanticipated error to the other buffered errors
                    self.aggregated_errors_object.buffer_error(e)

                if len(self.conflicting_value_errors) > 0:
                    self.aggregated_errors_object.buffer_error(
                        ConflictingValueErrors(self.conflicting_value_errors)
                    )

                if len(self.required_value_errors) > 0:
                    self.aggregated_errors_object.buffer_error(
                        RequiredValueErrors(self.required_value_errors)
                    )

                # Summarize any DuplicateValues reported
                dupe_errs = self.aggregated_errors_object.get_exception_type(
                    DuplicateValues, remove=True
                )
                if len(dupe_errs) > 0:
                    self.aggregated_errors_object.buffer_error(
                        DuplicateValueErrors(dupe_errs)
                    )

                if (
                    self.aggregated_errors_object.should_raise()
                    and not self.defer_rollback
                ):
                    # Raise here to cause a rollback
                    raise self.aggregated_errors_object

                if self.dry_run:
                    raise DryRun()

            if self.aggregated_errors_object.should_raise():
                # Raise here to NOT cause a rollback
                raise self.aggregated_errors_object

            return self.record_counts

        return load_wrapper

    def get_model_name(self, model_name):
        if model_name is not None:
            return model_name
        if self.models is not None and len(self.models) == 1:
            return self.models[0].__name__
        # Raise a programming error
        raise ValueError(
            "A model name is required when there is not exactly 1 model initialized in the constructor."
        )

    def created(self, model_name: Optional[str] = None):
        self.record_counts[self.get_model_name(model_name)]["created"] += 1

    def existed(self, model_name: Optional[str] = None):
        self.record_counts[self.get_model_name(model_name)]["existed"] += 1

    def errored(self, model_name: Optional[str] = None):
        self.record_counts[self.get_model_name(model_name)]["errored"] += 1

    def get_load_stats(self):
        return self.record_counts

    def check_for_inconsistencies(self, rec, rec_dict, rownum=None):
        """
        This function compares the supplied database model record with the dict that was used to (get or) create a
        record that resulted (or will result) in an IntegrityError (i.e. a unique constraint violation).  Call this
        method inside an `except IntegrityError` block, e.g.:
            try:
                rec_dict = {field values for record creation}
                rec, created = Model.objects.get_or_create(**rec_dict)
            except IntegrityError as ie:
                rec = Model.objects.get(name="unique value")
                conflicts.extend(check_for_inconsistencies(rec, rec_dict))
        (It can also be called pre-emptively by querying for only a record's unique field and supply the record and a
        dict for record creation.  E.g.:
            rec_dict = {field values for record creation}
            rec = Model.objects.get(name="unique value")
            new_conflicts = check_for_inconsistencies(rec, rec_dict)
            if len(new_conflicts) == 0:
                Model.objects.create(**rec_dict)
        The purpose of this function is to provide helpful information in an exception (i.e. repackage an
        IntegrityError) so that users working to resolve the error can quickly identify and resolve the issue.
        """
        conflicting_value_errors = []
        differences = {}
        for field, new_value in rec_dict.items():
            orig_value = getattr(rec, field)
            if orig_value != new_value:
                differences[field] = {
                    "orig": orig_value,
                    "new": new_value,
                }
        if len(differences.keys()) > 0:
            conflicting_value_errors.append(
                ConflictingValueError(
                    rec,
                    differences,
                    rec_dict=rec_dict,
                    rownum=rownum,
                    sheet=self.sheet,
                    file=self.file,
                )
            )
        return conflicting_value_errors

    def handle_load_db_errors(
        self,
        exception,
        model,
        rec_dict,
        rownum=None,
        fld_to_col=None,
        handle_all=True,
    ):
        """Handles IntegrityErrors and ValidationErrors raised during database loading.  Put in `except` block.

        The purpose of this function is to provide helpful information in an exception (i.e. repackage an IntegrityError
        or a ValidationError) so that users working to resolve errors can quickly identify and resolve data issues.  It
        calls check_for_inconsistencies.

        This function evaluates whether the supplied exception is the result of either a field value conflict or a
        validation error (triggered by a full_clean).  It will either buffer a ConflictingValue error in either the
        supplied conflicts_list or AggregatedErrors object, or raise an exception.

        Args:
            exception (Exception): Exception, e.g. obtained from `except` block
            model (Model): Model being loaded when the exception occurred
            rec_dict (dict): Fields and their values that were passed to either `create` or `get_or_create`
            aes (AggregatedErrors): Aggregated errors object
            conflicts_list (list): List to which ConflictingValueError exception objects will be added.
            missing_list (list): List to which RequiredValuesError exception objects will be added.
            rownum (int): Line or row number of the file that was being loaded when the exception occurred.
            sheet (str): Name of the Excel sheet that was being loaded when the exception occurred.
            file (str): Name (path optional) of the file that was being loaded when the exception occurred.
            fld_to_col (dict): Supply if you want to map field names (keys from the rec_dict) to column header names in
                the file.  Field names parsed from IntegrityErrors will be used for the column name value (which you can
                customize in a string for example, to represent multiple columns that were used to produce the field
                value).

        Raises (or buffers):
            ValueError
            InfileDatabaseError
            ConflictingValueError
            RequiredValuesError

        Returns:
            boolean indicating whether an error was handled(/buffered).
        """
        if self.aggregated_errors_object is None and (
            self.required_value_errors is None or self.conflicting_value_errors is None
        ):
            raise ValueError(
                "Either an AggregatedErrors object or both a required_value_errors and conflicting_value_errors list "
                "is required."
            )
        elif handle_all and self.aggregated_errors_object is None:
            raise ValueError(
                "An AggregatedErrors object is required when handle_all is True."
            )

        # We may or may not use estr and exc, but we're pre-making them here to reduce code duplication
        estr = str(exception)
        exc = InfileDatabaseError(
            exception, rec_dict, rownum=rownum, sheet=self.sheet, file=self.file
        )

        if isinstance(exception, IntegrityError):
            if "duplicate key value violates unique constraint" in estr:
                # Create a list of lists of unique fields and unique combos of fields
                # First, get unique fields and force them into a list of lists (so that we only need to loop once)
                unique_combos = [
                    [f] for f in get_unique_fields(model, fields=rec_dict.keys())
                ]
                # Now add in the unique field combos from the model's unique constraints
                unique_combos.extend(get_unique_constraint_fields(model))

                # Create a set of the fields in the dict causing the error so that we can only check unique its fields
                field_set = set(rec_dict.keys())

                # We're going to loop over unique records until we find one that conflicts with the dict
                for combo_fields in unique_combos:
                    # Only proceed if we have all the values
                    combo_set = set(combo_fields)
                    if not combo_set.issubset(field_set):
                        continue

                    # Retrieve the record with the conflicting value(s) that caused the unique constraint error using
                    # the unique fields
                    q = Q()
                    for uf in combo_fields:
                        q &= Q(**{f"{uf}__exact": rec_dict[uf]})
                    qs = model.objects.filter(q)

                    # If there was a record found using a unique field (combo)
                    if qs.count() == 1:
                        rec = qs.first()
                        errs = self.check_for_inconsistencies(
                            rec, rec_dict, rownum=rownum
                        )
                        if len(errs) > 0:
                            if self.conflicting_value_errors is not None:
                                self.conflicting_value_errors.extend(errs)
                                return True
                            elif self.aggregated_errors_object:
                                for err in errs:
                                    self.aggregated_errors_object.buffer_error(err)
                                return True

            elif "violates not-null constraint" in estr:
                regexp = re.compile(r"^null value in column \"(?P<fldname>[^\"]+)\"")
                match = re.search(regexp, estr)
                if match:
                    fldname = match.group("fldname")
                    colname = fldname
                    # Convert the database column name to the file column header, if available
                    if fld_to_col is not None and colname in fld_to_col.keys():
                        colname = fld_to_col[fldname]
                    err = RequiredValueError(
                        column=colname,
                        rownum=rownum,
                        model_name=model.__name__,
                        field_name=fldname,
                        sheet=self.sheet,
                        file=self.file,
                        rec_dict=rec_dict,
                    )
                    if self.required_value_errors is not None:
                        self.required_value_errors.append(err)
                        return True
                    elif self.aggregated_errors_object:
                        for err in errs:
                            self.aggregated_errors_object.buffer_error(err)
                        return True

        elif isinstance(exception, ValidationError):
            if "is not a valid choice" in estr:
                choice_fields = get_enumerated_fields(model, fields=rec_dict.keys())
                for choice_field in choice_fields:
                    if choice_field in estr and rec_dict[choice_field] is not None:
                        if self.aggregated_errors_object is not None:
                            # Only include error once
                            if not self.aggregated_errors_object.exception_type_exists(
                                InfileDatabaseError
                            ):
                                self.aggregated_errors_object.buffer_error(exc)
                            else:
                                # NOTE: This is not perfect.  There can be multiple field values with issues.  Repeated
                                # calls to this function could potentially reference the same record and contain an
                                # error about a different field.  We only buffer/raise one of them because the details
                                # include the entire record and dict causing the issue(s).
                                already_buffered = False
                                for (
                                    existing_exc
                                ) in self.aggregated_errors_object.get_exception_type(
                                    InfileDatabaseError
                                ):
                                    if existing_exc.rec_dict != rec_dict:
                                        already_buffered = True
                                if not already_buffered:
                                    self.aggregated_errors_object.buffer_error(exc)
                            # Whether we buffered or not, the error was identified and handled (by either buffering or
                            # ignoring a duplicate)
                            return True

        if handle_all and self.aggregated_errors_object is not None:
            self.aggregated_errors_object.buffer_error(exc)
            return True

        # If we get here, we did not identify the error as one we knew what to do with
        return False
