from collections import namedtuple
from datetime import timedelta
from typing import Dict

from django.db import transaction

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.models import LCMethod
from DataRepo.utils.exceptions import ConflictingValueError, RollbackException


class LCProtocolsLoader(TableLoader):
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    NAME_KEY = "NAME"
    TYPE_KEY = "TYPE"
    RUNLEN_KEY = "RUNLEN"
    DESC_KEY = "DESC"

    DataSheetName = "LC Protocols"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "NAME",
            "TYPE",
            "RUNLEN",
            "DESC",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        NAME="Name",
        TYPE="LC Protocol",
        RUNLEN="Run Length",
        DESC="Description",
    )

    # List of required header keys
    DataRequiredHeaders = [
        NAME_KEY,
        TYPE_KEY,
        RUNLEN_KEY,
        DESC_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = [
        NAME_KEY,
        TYPE_KEY,
        DESC_KEY,
    ]

    # No DataDefaultValues needed

    DataColumnTypes: Dict[str, type] = {
        NAME_KEY: str,
        TYPE_KEY: str,
        RUNLEN_KEY: int,
        DESC_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        [NAME_KEY],
        [TYPE_KEY, RUNLEN_KEY],
    ]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        LCMethod.__name__: {
            "name": NAME_KEY,
            "type": TYPE_KEY,
            "run_length": RUNLEN_KEY,
            "description": DESC_KEY,
        },
    }

    FieldToDataValueConverter = {
        LCMethod.__name__: {
            "run_length": lambda val: val.total_seconds() // 60,
        },
    }

    DataColumnMetadata = DataTableHeaders(
        NAME=TableColumn.init_flat(
            field=LCMethod.name,
            name=DataHeaders.NAME,
            type=str,
            readonly=True,
            format=(
                "While this column is automatically populated by excel formula, the following describes the formula "
                "output, if you wish to manually enter it.\n"
                "\n"
                f"E.g. '{DataHeaders.TYPE}'-'{DataHeaders.RUNLEN}'-min"
            ),
            reference=ColumnReference(
                sheet="Sequences",
                header="Sequence Name",
            ),
            # TODO: Create the method that applies the formula to the NAME column on every row
            # Excel formula that creates f"{type}-{run_length}-min" using the spreadsheet columns on the current row
            # The header keys will be replaced by the excel column letters.  Simplified example:
            # E.g. 'CONCATENATE(INDIRECT("B" & ROW()), "-", INDIRECT("C" & ROW()), "-min")'
            formula=(
                # If either the type of runlen is blank, return an empty string
                "=IF("
                "OR("
                f'ISBLANK(INDIRECT("{{{TYPE_KEY}}}" & ROW())),'
                f'ISBLANK(INDIRECT("{{{RUNLEN_KEY}}}" & ROW()))'
                '),"",'
                # Otherwise, construct the protocol name, using concatenation
                f"CONCATENATE("
                f'INDIRECT("{{{TYPE_KEY}}}" & ROW()),'
                '"-",'
                f'INDIRECT("{{{RUNLEN_KEY}}}" & ROW()),'
                '"-min"'
                "))"
            ),
        ),
        TYPE=TableColumn.init_flat(
            field=LCMethod.type, name=DataHeaders.TYPE, current_choices=True
        ),
        RUNLEN=TableColumn.init_flat(
            field=LCMethod.run_length,
            name=DataHeaders.RUNLEN,
            format="Units: minutes.",
            current_choices=True,
            current_choices_converter=FieldToDataValueConverter[LCMethod.__name__][
                "run_length"
            ],
        ),
        DESC=TableColumn.init_flat(field=LCMethod.description, name=DataHeaders.DESC),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling.
    Models = [LCMethod]

    def load_data(self):
        """Loads the MSRunSequence and LCMethod tables from the dataframe.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        for _, row in self.df.iterrows():
            try:
                self.get_or_create_lc_method(row)
            except RollbackException:
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
            RollbackException
        Returns:
            rec (Optional[LCMethod])
            created (boolean): Only returned for use in tests
        """
        rec_dict = None
        rec = None
        created = False

        try:
            name = self.get_row_val(row, self.headers.NAME)
            type = self.get_row_val(row, self.headers.TYPE)
            raw_run_length = self.get_row_val(row, self.headers.RUNLEN)
            run_length = None
            description = self.get_row_val(row, self.headers.DESC)

            # This row is added to skip_row_indexes (by get_row_val) when run_length is None, because it's a required
            # value (see DataRequiredValues).  So we skip before instantiating a timedelta object to avoid buffering an
            # unnecessary exception, as a RequiredColumnValue exception would have already been buffered.
            if self.is_skip_row():
                self.errored(LCMethod.__name__)
                return rec, created

            if raw_run_length is not None:
                run_length = timedelta(minutes=raw_run_length)

            computed_name = LCMethod.create_name(type=type, run_length=run_length)

            # We're not going to use the name from the file.  The name column is only used for the creation of drop-
            # down lists for columns in other sheets, which is why it is a readonly column, but if the user does
            # unexpectedly change the value in the column, we should warn them that the result will not be what they
            # expect.
            if name is not None and name != computed_name:
                self.aggregated_errors_object.buffer_warning(
                    ConflictingValueError(
                        rec=None,
                        differences={
                            "name": {
                                "orig": computed_name,
                                "new": name,
                            },
                        },
                        rownum=self.rownum,
                        sheet=self.sheet,
                        file=self.friendly_file,
                    )
                )
                self.warned(LCMethod.__name__)

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row():
                self.errored(LCMethod.__name__)
                return rec, created

            rec_dict = {
                "name": computed_name,
                "type": type,
                "run_length": run_length,
                "description": description,
            }

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
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise RollbackException()

        return rec, created
