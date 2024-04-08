from collections import namedtuple
from datetime import timedelta
from typing import Dict

from django.db import transaction

from DataRepo.loaders.table_column import TableColumn
from DataRepo.loaders.table_loader import TableLoader
from DataRepo.models import LCMethod, MSRunSequence
from DataRepo.utils.exceptions import RequiredColumnValueWhenNovel
from DataRepo.utils.file_utils import string_to_datetime


class SequencesLoader(TableLoader):
    # TODO: 1. Implement a sequence accession composed of study code and sequence number
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    ID_KEY = "ID"  # See TODO 1 above
    OPERATOR_KEY = "OPERATOR"
    DATE_KEY = "DATE"
    INSTRUMENT_KEY = "INSTRUMENT"
    LC_PROTOCOL_KEY = "LC_PROTOCOL"
    LC_RUNLEN_KEY = "LC_RUNLEN"
    LC_DESC_KEY = "LC_DESC"
    NOTES_KEY = "NOTES"

    DataSheetName = "Sequences"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
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
    DataHeaders = DataTableHeaders(
        ID="Sequence Number",  # See TODO 1 above
        OPERATOR="Operator",
        DATE="Date",
        INSTRUMENT="Instrument",
        LC_PROTOCOL="LC Protocol",
        LC_RUNLEN="LC Run Length",
        LC_DESC="LC Description",
        NOTES="Notes",
    )

    # List of required header keys
    DataRequiredHeaders = [
        ID_KEY,  # See TODO 1 above
        OPERATOR_KEY,
        DATE_KEY,
        INSTRUMENT_KEY,
        LC_PROTOCOL_KEY,
        LC_RUNLEN_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # No DataDefaultValues needed

    DataColumnTypes: Dict[str, type] = {
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
    DataUniqueColumnConstraints = [
        # See TODO 1 above
        # [STUDY_CODE_KEY, ID_KEY],
        [ID_KEY],
        [OPERATOR_KEY, DATE_KEY, INSTRUMENT_KEY, LC_PROTOCOL_KEY, LC_RUNLEN_KEY],
    ]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        MSRunSequence.__name__: {
            "researcher": OPERATOR_KEY,
            "date": DATE_KEY,
            "instrument": INSTRUMENT_KEY,
            "notes": NOTES_KEY,
        },
        LCMethod.__name__: {
            "type": LC_PROTOCOL_KEY,
            "runlength": LC_RUNLEN_KEY,
            "description": LC_DESC_KEY,
        },
    }

    DataColumnMetadata = DataTableHeaders(
        ID=TableColumn.init_flat(
            name="Sequence Number",
            help_text="Sequential integer starting from 1.",
            guidance="This column is used to populate Sequence Number choices in the Peak Annotation Details sheet.",
            type=int,
        ),
        OPERATOR=TableColumn.init_flat(
            name="Operator",
            help_text="Researcher who operated the Mass Spec instrument.",
        ),
        DATE=TableColumn.init_flat(
            field=MSRunSequence.date,
            format="Format: YYYY-MM-DD.",
        ),
        INSTRUMENT=TableColumn.init_flat(field=MSRunSequence.instrument),
        LC_PROTOCOL=TableColumn.init_flat(field=LCMethod.type),
        LC_RUNLEN=TableColumn.init_flat(
            field=LCMethod.run_length,
            format="Units: minutes.",
        ),
        LC_DESC=TableColumn.init_flat(field=LCMethod.description),
        NOTES=TableColumn.init_flat(field=MSRunSequence.notes),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [MSRunSequence, LCMethod]

    def load_data(self):
        """Loads the MSRunSequence and LCMethod tables from the dataframe.

        Args:
            None

        Raises:
            Nothing

        Returns:
            Nothing
        """
        for _, row in self.df.iterrows():
            try:
                lc_rec, _ = self.get_or_create_lc_method(row)
            except Exception:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                lc_rec = None

            if self.is_skip_row() or lc_rec is None:
                self.skipped(MSRunSequence.__name__)
                continue

            try:
                self.get_or_create_sequence(row, lc_rec)
            except Exception:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

    @transaction.atomic
    def get_or_create_lc_method(self, row):
        """Gets or creates an LCMethod record from the supplied row.

        This method is decorated with transaction.atomic so that any specific record creation that causes an exception
        will be rolled back and loading can proceed (in order to catch more errors).  That exception is buffered and
        will eventually cause all loaded data to be rolled back.

        Args:
            row (pandas dataframe row)

        Raises:
            Nothing (explicitly)

        Returns:
            rec (Optional[LCMethod])
            created (boolean): Only returned for use in tests
        """
        rec_dict = None
        rec = None
        created = False

        try:
            type = self.get_row_val(row, self.headers.LC_PROTOCOL)
            raw_run_length = self.get_row_val(row, self.headers.LC_RUNLEN)
            description = self.get_row_val(row, self.headers.LC_DESC)

            # In case run_length was None, let's prevent an exception at the timedelta
            if self.is_skip_row():
                self.errored(LCMethod.__name__)
                return rec, created

            # run_length is a required value (see DataRequiredValues), and is typed to be an int (see DataColumnTypes)
            run_length = timedelta(minutes=raw_run_length)

            name = LCMethod.create_name(type=type, run_length=run_length)

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row():
                self.errored(LCMethod.__name__)
                return rec, created

            rec_dict = {
                "type": type,
                "run_length": run_length,
                "name": name,
            }

            # The LC_DESC column is optional WHEN the LC Method *exists*.  Required Otherwise.
            if description is None:
                qs = LCMethod.objects.filter(**rec_dict)
                if qs.count() != 1:
                    self.add_skip_row_index()
                    self.aggregated_errors_object.buffer_error(
                        RequiredColumnValueWhenNovel(
                            column="description", model_name=LCMethod.__name__
                        )
                    )
                    return rec, created
                rec = qs.first()
                created = False
            else:
                rec_dict["description"] = description
                rec, created = LCMethod.objects.get_or_create(**rec_dict)

            if created:
                rec.full_clean()
                self.created(LCMethod.__name__)
            else:
                self.existed(LCMethod.__name__)

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, LCMethod, rec_dict)
            self.errored(LCMethod.__name__)
            # Now that the exception has been handled, trigger a roolback of this record load attempt
            raise e

        return rec, created

    @transaction.atomic
    def get_or_create_sequence(self, row, lc_rec):
        """Gets or creates an MSRunSequence record from the supplied row and LCMethod record.

        This method is decorated with transaction.atomic so that any specific record creation that causes an exception
        will be rolled back and loading can proceed (in order to catch more errors).  That exception is buffered and
        will eventually cause all loaded data to be rolled back.

        Args:
            row (pandas dataframe row)
            lc_rec (LCMethod)

        Raises:
            Nothing (explicitly)

        Returns:
            rec (Optional[MSRunSequence])
            created (boolean): Only returned for use in tests
        """
        rec_dict = None
        rec = None
        created = False

        try:
            # See TODO 1 above, and read in study_code and seq_id
            researcher = self.get_row_val(row, self.headers.OPERATOR)
            date_str = self.get_row_val(row, self.headers.DATE)
            date = string_to_datetime(
                date_str,
                file=self.file,
                sheet=self.sheet,
                rownum=self.rownum,
                column=self.headers.DATE,
            )
            instrument = self.get_row_val(row, self.headers.INSTRUMENT)
            notes = self.get_row_val(row, self.headers.NOTES)

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row():
                self.errored(MSRunSequence.__name__)
                return rec, created

            rec_dict = {
                "researcher": researcher,
                "date": date,
                "instrument": instrument,
                "notes": notes,
                "lc_method": lc_rec,
            }

            rec, created = MSRunSequence.objects.get_or_create(**rec_dict)

            if created:
                lc_rec.full_clean()
                self.created(MSRunSequence.__name__)
            else:
                self.existed(MSRunSequence.__name__)

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, MSRunSequence, rec_dict)
            self.errored(MSRunSequence.__name__)
            # Now that the exception has been handled, trigger a roolback of this record load attempt
            raise e

        return rec, created
