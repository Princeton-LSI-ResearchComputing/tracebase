from collections import namedtuple
from typing import Dict

from django.db import transaction

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.lcprotocols_loader import LCProtocolsLoader
from DataRepo.models import LCMethod, MSRunSequence
from DataRepo.models.researcher import (
    could_be_variant_researcher,
    get_researchers,
)
from DataRepo.utils.exceptions import (
    InfileError,
    NewResearcher,
    PlaceholderDetected,
    RecordDoesNotExist,
    RollbackException,
)
from DataRepo.utils.file_utils import string_to_date


class SequencesLoader(TableLoader):
    # For the conversion of study doc version 2 to the current version, v2 has no sequence data, so this note is set in
    # the notes field.  If we see this value, the load of that record will be skipped.  The records will have to be
    # addressed before any of the peak annotation files can be loaded.
    V2_PLACEHOLDER_NOTE = "This is a temporary placeholder record.  Edit it."

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    SEQNAME_KEY = "SEQNAME"
    OPERATOR_KEY = "OPERATOR"
    DATE_KEY = "DATE"
    INSTRUMENT_KEY = "INSTRUMENT"
    LCNAME_KEY = "LCNAME"
    NOTES_KEY = "NOTES"

    DataSheetName = "MS Runs"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "SEQNAME",
            "OPERATOR",
            "LCNAME",
            "INSTRUMENT",
            "DATE",
            "NOTES",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        SEQNAME="MS Run Name",
        OPERATOR="Operator",
        LCNAME="LC Protocol Name",
        INSTRUMENT="Instrument",
        DATE="Date",
        NOTES="Notes",
    )

    # List of required header keys
    DataRequiredHeaders = [
        SEQNAME_KEY,
        OPERATOR_KEY,
        DATE_KEY,
        INSTRUMENT_KEY,
        LCNAME_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # No DataDefaultValues needed

    DataColumnTypes: Dict[str, type] = {
        SEQNAME_KEY: str,
        OPERATOR_KEY: str,
        DATE_KEY: str,
        INSTRUMENT_KEY: str,
        LCNAME_KEY: str,
        NOTES_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        [SEQNAME_KEY],
        [OPERATOR_KEY, DATE_KEY, INSTRUMENT_KEY, LCNAME_KEY],
    ]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        MSRunSequence.__name__: {
            "researcher": OPERATOR_KEY,
            "lc_method": LCNAME_KEY,
            "date": DATE_KEY,
            "instrument": INSTRUMENT_KEY,
            "notes": NOTES_KEY,
        },
    }

    # No FieldToDataValueConverter needed

    DataColumnMetadata = DataTableHeaders(
        SEQNAME=TableColumn.init_flat(
            name=DataHeaders.SEQNAME,
            readonly=True,
            help_text=(
                "A unique MS Run (Sequence) identifier.\n\nNote that an MS Run Sequence is unique to a researcher, "
                "protocol, instrument (model), and date.  If a researcher performs multiple such Mass Spec Runs on the "
                "same day, this MS Run Sequence record will represent multiple runs."
            ),
            # TODO: Replace "Peak Annotation Details" and "Default MS Run" below with a reference to the loader
            # TODO: Make "reference" be a List[ColumnReference], because there are multiple sheets that reference this
            # column
            reference=ColumnReference(
                sheet="Peak Annotation Files",
                header="Default MS Run",
            ),
            type=str,
            format=(
                "Comma-delimited string combining the values from these columns in this order:\n"
                f"- {DataHeaders.OPERATOR}\n"
                f"- {DataHeaders.LCNAME}\n"
                f"- {DataHeaders.INSTRUMENT}\n"
                f"- {DataHeaders.DATE}"
            ),
            # TODO: Create the method that applies the formula to the SEQNAME column on every row
            # Excel formula that creates f"{OPERATOR}, {LCNAME}, {INSTRUMENT}, {DATE}" using the spreadsheet columns on
            # the current row.  The header keys will be replaced by the excel column letters.  Simplified example:
            # 'CONCATENATE(INDIRECT("B" & ROW()), ", ", INDIRECT("C" & ROW()), ", ", INDIRECT("D" & ROW()), ", ",
            # INDIRECT("E" & ROW()))'
            # NOTE: The inclusion of function prefixes like `_xlfn.` is documented as necessary in xlsxwriter.  If not
            # included, the excel formulas will behave erratically.  The process used to discover the prefixes necessary
            # is documented here:
            # https://xlsxwriter.readthedocs.io/working_with_formulas.html#dealing-with-formula-errors
            # But basically:
            # 1. Manually paste the (unprefixed) formula into an exported sheet (which should fix the formula, unless
            #    you have a syntax error).
            # 2. Save the file.
            # 3. unzip myfile.xlsx -d myfile
            # 4. xmllint --format myfile/xl/worksheets/sheet8.xml | grep '</f>'
            # 5. Update the prefixes in the formula below to match the prefixes in the working formula that was manually
            #    fixed.
            formula=(
                "=IF("
                "OR("
                f'NOT(ISBLANK(INDIRECT("{{{OPERATOR_KEY}}}" & ROW()))),'
                f'NOT(ISBLANK(INDIRECT("{{{LCNAME_KEY}}}" & ROW()))),'
                f'NOT(ISBLANK(INDIRECT("{{{INSTRUMENT_KEY}}}" & ROW()))),'
                f'NOT(ISBLANK(INDIRECT("{{{DATE_KEY}}}" & ROW()))),'
                "),"
                f'_xlfn.TEXTJOIN("{MSRunSequence.SEQNAME_DELIMITER} ", FALSE, '
                f'INDIRECT("{{{OPERATOR_KEY}}}" & ROW()), '
                f'INDIRECT("{{{LCNAME_KEY}}}" & ROW()), '
                f'INDIRECT("{{{INSTRUMENT_KEY}}}" & ROW()), '
                # If the date is blank, return empty string
                f'IF(ISBLANK(INDIRECT("{{{DATE_KEY}}}" & ROW())),"",'
                # Otherwise, format the date (because excel returns an encoded number)
                f'TEXT(INDIRECT("{{{DATE_KEY}}}" & ROW()),"yyyy-mm-dd"))'
                '),"")'
            ),
        ),
        OPERATOR=TableColumn.init_flat(
            field=MSRunSequence.researcher,
            name=DataHeaders.OPERATOR,
            help_text="Researcher who operated the Mass Spec instrument.",
            current_choices=True,
        ),
        DATE=TableColumn.init_flat(
            name=DataHeaders.DATE,
            field=MSRunSequence.date,
            format="Format: YYYY-MM-DD.",
        ),
        INSTRUMENT=TableColumn.init_flat(field=MSRunSequence.instrument),
        LCNAME=TableColumn.init_flat(
            name=DataHeaders.LCNAME,
            field=LCMethod.name,
            # TODO: Implement the method which creates the dropdowns in the excel spreadsheet
            dynamic_choices=ColumnReference(
                loader_class=LCProtocolsLoader,
                loader_header_key=LCProtocolsLoader.NAME_KEY,
            ),
        ),
        NOTES=TableColumn.init_flat(field=MSRunSequence.notes),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling.
    Models = [MSRunSequence]

    def load_data(self):
        """Loads the MSRunSequence table from the dataframe.

        Args:
            None

        Raises:
            Nothing

        Returns:
            Nothing
        """
        known_researchers = get_researchers()

        for _, row in self.df.iterrows():
            note = self.get_row_val(row, self.headers.NOTES)

            if note == self.V2_PLACEHOLDER_NOTE:
                if not self.validate:
                    self.buffer_infile_exception(
                        PlaceholderDetected(),
                        is_error=False,
                        is_fatal=False,
                        suggestion=(
                            f"The {self.DataSheetName} sheet row will have to be updated before associated peak "
                            "annotation files can be loaded."
                        ),
                    )
                    self.add_skip_row_index()

            try:
                lc_rec = self.get_lc_method(self.get_row_val(row, self.headers.LCNAME))
            except Exception:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                lc_rec = None

            # Check the researcher being added, and buffer a warning if new
            # Doing this before skipping intentionally, so we check all in the file.
            operator = self.get_row_val(row, self.headers.OPERATOR)
            if operator is not None and could_be_variant_researcher(
                operator, known_researchers=known_researchers
            ):
                # Raised if in validate mode (so the web user will see it).  Just printed otherwise.
                self.aggregated_errors_object.buffer_warning(
                    NewResearcher(operator, known=known_researchers),
                    is_fatal=self.validate,
                )
                self.warned(MSRunSequence.__name__)

            if self.is_skip_row() or lc_rec is None:
                self.skipped(MSRunSequence.__name__)
                continue

            try:
                self.get_or_create_sequence(row, lc_rec, operator, note)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

    def get_lc_method(self, name):
        """Gets an LCMethod record from the supplied row.
        Args:
            row (pandas dataframe row)
        Exceptions:
            Raises:
                None
            Buffers:
                InfileError
        Returns:
            rec (Optional[LCMethod])
        """
        rec = None
        query_dict = {"name": name}
        try:
            rec = LCMethod.objects.get(**query_dict)
        except LCMethod.DoesNotExist:
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(
                    model=LCMethod,
                    query_obj=query_dict,
                    rownum=self.rownum,
                    sheet=self.sheet,
                    file=self.friendly_file,
                )
            )
        except Exception as e:
            # Package other errors with file-location metadata
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    str(e),
                    rownum=self.rownum,
                    sheet=self.sheet,
                    file=self.friendly_file,
                )
            )
        return rec

    @transaction.atomic
    def get_or_create_sequence(self, row, lc_rec, researcher, notes):
        """Gets or creates an MSRunSequence record from the supplied row, LCMethod record, and operator/researcher.

        This method is decorated with transaction.atomic so that any specific record creation that causes an exception
        will be rolled back and loading can proceed (in order to catch more errors).  That exception is buffered and
        will eventually cause all loaded data to be rolled back.

        Args:
            row (pandas dataframe row)
            lc_rec (LCMethod)
            operator (str)
            notes (str)
        Raises:
            RollbackException
        Returns:
            rec (Optional[MSRunSequence])
            created (boolean): Only returned for use in tests
        """
        rec_dict = None
        rec = None
        created = False

        try:
            date_str = self.get_row_val(row, self.headers.DATE)
            date = string_to_date(
                date_str,
                file=self.friendly_file,
                sheet=self.sheet,
                rownum=self.rownum,
                column=self.headers.DATE,
            )
            instrument = self.get_row_val(row, self.headers.INSTRUMENT)

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
            raise RollbackException()

        return rec, created
