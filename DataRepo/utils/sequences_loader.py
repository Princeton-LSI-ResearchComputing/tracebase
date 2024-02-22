from collections import namedtuple
from datetime import datetime
from typing import Dict

from django.db import transaction

from DataRepo.models import LCMethod, MSRunSequence
from DataRepo.utils.exceptions import RequiredColumnValueWhenNovel
from DataRepo.utils.loader import TraceBaseLoader


class SequencesLoader(TraceBaseLoader):
    # TODO: 1. Implement a sequence accession composed of study code and sequence number
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    STUDY_CODE_KEY = "STUDY_CODE"  # See TODO 1 above
    ID_KEY = "ID"  # See TODO 1 above
    OPERATOR_KEY = "OPERATOR"
    DATE_KEY = "DATE"
    INSTRUMENT_KEY = "INSTRUMENT"
    LC_PROTOCOL_KEY = "LC_PROTOCOL"
    LC_RUNLEN_KEY = "LC_RUNLEN"
    LC_DESC_KEY = "LC_DESC"
    NOTES_KEY = "NOTES"

    # The tuple used to store different kinds of data per column at the class level
    TableHeaders = namedtuple(
        "TableHeaders",
        [
            "STUDY_CODE",  # See TODO 1 above
            "ID",  # See TODO 1 above
            "OPERATOR",
            "DATE",
            "INSTRUMENT",
            "LC_PROTOCOL",
            "LC_RUNLEN",
            "LC_DESC",
            "NOTES",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DefaultHeaders = TableHeaders(
        STUDY_CODE="Study Code",  # See TODO 1 above
        ID="Sequence Number",  # See TODO 1 above
        OPERATOR="Operator",
        DATE="Date",
        INSTRUMENT="Instrument",
        LC_PROTOCOL="LC Protocol",
        LC_RUNLEN="LC Run Length",
        LC_DESC="LC Description",
        NOTES="Notes",
    )

    # Whether each column is required to be present of not
    RequiredHeaders = TableHeaders(
        STUDY_CODE=False,  # See TODO 1 above
        ID=True,  # See TODO 1 above
        OPERATOR=True,
        DATE=True,
        INSTRUMENT=True,
        LC_PROTOCOL=True,
        LC_RUNLEN=True,
        LC_DESC=False,
        NOTES=False,
    )

    # Whether a value for an row in a column is required or not (note that defined DefaultValues will satisfy this)
    RequiredValues = TableHeaders(
        STUDY_CODE=True,  # Study code and ID combined are an "accession number" for a Sequence
        ID=True,  # See TODO 1 above
        OPERATOR=True,
        DATE=True,
        INSTRUMENT=True,
        LC_PROTOCOL=True,
        LC_RUNLEN=True,
        LC_DESC=True,
        NOTES=False,
    )

    # No DefaultValues needed

    ColumnTypes: Dict[str, type] = {
        STUDY_CODE_KEY: str,  # See TODO 1 above
        ID_KEY: int,  # See TODO 1 above
        OPERATOR_KEY: str,
        DATE_KEY: str,
        INSTRUMENT_KEY: str,
        LC_PROTOCOL_KEY: str,
        LC_RUNLEN_KEY: int,
        LC_DESC_KEY: str,
        NOTES_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file  # See TODO 1 above
    UniqueColumnConstraints = [
        [STUDY_CODE_KEY, ID_KEY],
        [OPERATOR_KEY, DATE_KEY, INSTRUMENT_KEY, LC_PROTOCOL_KEY, LC_RUNLEN_KEY],
    ]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToHeaderKey = {
        "MSRunSequence": {
            "researcher": OPERATOR_KEY,
            "date": DATE_KEY,
            "instrument": INSTRUMENT_KEY,
            "notes": NOTES_KEY,
        },
        "LCMethod": {
            "type": LC_PROTOCOL_KEY,
            "runlength": LC_RUNLEN_KEY,
            "description": LC_DESC_KEY,
        },
    }

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [MSRunSequence, LCMethod]

    def load_data(self):
        """Loads the MSRunSequence and LCMethod tables from the dataframe.

        Args:
            None

        Raises:
            Nothing (see TraceBaseLoader._loader() wrapper for exceptions raised by the automatically applied wrapping
                method)

        Returns:
            Nothing (see TraceBaseLoader._loader() wrapper for return value from the automatically applied wrapping
                method)
        """
        for _, row in self.df.iterrows():
            try:
                lc_rec = self.get_or_create_lc_method(row)
            except Exception as e:
                # get_or_create_lc_method raises database exceptions in order to roll back the 1 attempted record load
                # We catch here so that we can proceed unencumbered
                if self.aggregated_errors_object.exception_type_exists(type(e)):
                    continue
                # If the exception wasn't buffered, this is a programming error, so raise immediately
                raise e

            if self.is_skip_row() or lc_rec is None:
                continue

            try:
                self.get_or_create_sequence(row, lc_rec)
            except Exception as e:
                # get_or_create_lc_method raises database exceptions in order to roll back the 1 attempted record load
                # We catch here so that we can proceed unencumbered
                if self.aggregated_errors_object.exception_type_exists(type(e)):
                    continue
                # If the exception wasn't buffered, this is a programming error, so raise immediately
                raise e

    @transaction.atomic
    def get_or_create_lc_method(self, row):
        """Gets or creates an LCMethod record from the supplied row.

        This method is decorated with transaction.atomic so that any specific record creation that causes an exception
        will be rolled back and loading can proceed (in order to catch more errors).  That exception is buffered and
        will eventually cause all loaded data to be rolled back.

        Args:
            row (pandas dataframe row)

        Raises:
            Nothing specific, but any database exception that is raised by the ORM:
                IntegrityError
                ValidationError

        Returns:
            lc_rec (LCMethod)
        """
        lc_rec_dict = None

        try:
            type = self.get_row_val(row, self.headers.LC_PROTOCOL)
            run_length = self.get_row_val(row, self.headers.LC_RUNLEN)
            description = self.get_row_val(row, self.headers.LC_DESC)
            name = LCMethod.create_name(type=type, run_length=run_length)

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row():
                return None

            lc_rec_dict = {
                "type": type,
                "run_length": run_length,
                "name": name,
            }
        except Exception as e:
            # Package errors with relevant details
            self.handle_load_db_errors(e, LCMethod, lc_rec_dict)
            self.errored(LCMethod.__name__)
            return None

        try:
            # The LC_DESC column is optional WHEN the LC Method *exists*.  Required Otherwise.
            if description is None:
                qs = LCMethod.objects.filter(**lc_rec_dict)
                if qs.count() != 1:
                    self.add_skip_row_index(self.rownum)
                    self.aggregated_errors_object.buffer_error(
                        RequiredColumnValueWhenNovel(
                            column="description", model_name=LCMethod.__name__
                        )
                    )
                    return None
                lc_rec = qs.first()
            else:
                lc_rec_dict["description"] = description
                lc_rec, lc_created = LCMethod.objects.get_or_create(**lc_rec_dict)

            if lc_created:
                lc_rec.full_clean()
                self.created(LCMethod.__name__)
            else:
                self.existed(LCMethod.__name__)

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            self.handle_load_db_errors(e, LCMethod, lc_rec_dict)
            self.errored(LCMethod.__name__)

            # Any database exception should trigger a raise in order to roll back
            # This will be caught in the loop in load_data so that we can proceed
            # (We can proceed, because the exception was also buffered and the skip row indexes updated.)
            raise e

        return lc_rec

    @transaction.atomic
    def get_or_create_sequence(self, row, lc_rec) -> None:
        """Gets or creates an MSRunSequence record from the supplied row and LCMethod record.

        This method is decorated with transaction.atomic so that any specific record creation that causes an exception
        will be rolled back and loading can proceed (in order to catch more errors).  That exception is buffered and
        will eventually cause all loaded data to be rolled back.

        Args:
            row (pandas dataframe row)
            lc_rec (LCMethod)

        Raises:
            Nothing specific, but any database exception that is raised by the ORM:
                IntegrityError
                ValidationError

        Returns:
            Nothing
        """
        sqnc_rec_dict = None

        try:
            # See TODO 1 above
            researcher = self.get_row_val(row, self.headers.OPERATOR)
            date_str = self.get_row_val(row, self.headers.DATE)
            instrument = self.get_row_val(row, self.headers.INSTRUMENT)
            notes = self.get_row_val(row, self.headers.NOTES)

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row():
                return None

            sqnc_rec_dict = {
                "researcher": researcher,
                "date": datetime.strptime(date_str.strip(), "%Y-%m-%d"),
                "instrument": instrument,
                "notes": notes,
                "lc_method": lc_rec,
            }
        except Exception as e:
            # Package errors with relevant details
            self.handle_load_db_errors(e, MSRunSequence, sqnc_rec_dict)
            self.errored(MSRunSequence.__name__)
            return None

        try:
            sqnc_rec, sqnc_created = MSRunSequence.objects.get_or_create(
                **sqnc_rec_dict
            )

            if sqnc_created:
                lc_rec.full_clean()
                self.created(MSRunSequence.__name__)
            else:
                self.existed(MSRunSequence.__name__)

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            self.handle_load_db_errors(e, MSRunSequence, sqnc_rec_dict)
            self.errored(MSRunSequence.__name__)

            # Any database exception should trigger a raise in order to roll back
            # This will be caught in the loop in load_data so that we can proceed
            # (We can proceed, because the exception was also buffered and the skip row indexes updated.)
            raise e
