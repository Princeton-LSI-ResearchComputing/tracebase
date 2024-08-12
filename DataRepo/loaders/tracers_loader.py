from collections import defaultdict, namedtuple
from typing import Dict

from django.core.exceptions import ObjectDoesNotExist
from django.db import ProgrammingError, transaction

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.compounds_loader import CompoundsLoader
from DataRepo.models import Compound, MaintainedModel, Tracer, TracerLabel
from DataRepo.utils.exceptions import (
    CompoundDoesNotExist,
    InfileError,
    summarize_int_list,
)
from DataRepo.utils.infusate_name_parser import (
    IsotopeData,
    TracerData,
    parse_tracer_string,
)


class TracersLoader(TableLoader):
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    ID_KEY = "ID"
    COMPOUND_KEY = "COMPOUND"
    ELEMENT_KEY = "ELEMENT"
    MASSNUMBER_KEY = "MASSNUMBER"
    LABELCOUNT_KEY = "LABELCOUNT"
    LABELPOSITIONS_KEY = "LABELPOSITIONS"
    NAME_KEY = "NAME"

    POSITIONS_DELIMITER = TracerLabel.POSITIONS_DELIMITER

    DataSheetName = "Tracers"

    # The tuple used to store different kinds of data per column at the class level
    # NOTE: The order of these headers is required for the excel formula that generates the NAME column value
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "ID",
            "COMPOUND",
            "MASSNUMBER",
            "ELEMENT",
            "LABELCOUNT",
            "LABELPOSITIONS",
            "NAME",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        ID="Tracer Row Group",
        COMPOUND="Compound Name",
        ELEMENT="Element",
        MASSNUMBER="Mass Number",
        LABELCOUNT="Label Count",
        LABELPOSITIONS="Label Positions",
        NAME="Tracer Name",
    )

    # List of required header keys
    DataRequiredHeaders = [
        [
            # Either tracer number, compound, element, mass, and count - or tracer name
            [
                ID_KEY,
                COMPOUND_KEY,
                ELEMENT_KEY,
                MASSNUMBER_KEY,
                LABELCOUNT_KEY,
            ],
            NAME_KEY,
        ],
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes: Dict[str, type] = {
        ID_KEY: int,
        COMPOUND_KEY: str,
        ELEMENT_KEY: str,
        MASSNUMBER_KEY: int,
        LABELCOUNT_KEY: int,
        LABELPOSITIONS_KEY: str,
        NAME_KEY: str,
    }

    # No DataDefaultValues needed

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        [
            ID_KEY,
            NAME_KEY,
            COMPOUND_KEY,
            ELEMENT_KEY,
            MASSNUMBER_KEY,
            LABELCOUNT_KEY,
            LABELPOSITIONS_KEY,
        ],
    ]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    # Notes:
    # The purpose of FieldToDataHeaderKey is to interpret database errors that reference a field name and then associate
    # it with a problematic column value.  The value types do not need to be the same.  It is just for reporting, so:
    # - Tracer.compound is a foreign key, so even though the column value is a string, errors that reference the foreign
    #   key are directly applicable to the value in the column.
    # - Tracer.name is OK to have here for the future, but since it is both null=True and a Maintained Field, it is not
    #   allowed to be included in an ORM create command, and thus cannot be referenced in a database error.
    # - TracerLabel.positions is an ArrayField, not a string, so even though the column value is a delimited string,
    #   errors that reference the array value are directly applicable to the value in the column.
    # - TracerLabel.name is not mapped because it does not exist as a single column.  It is also a Maintained Field, so
    #   it would not exist in an error from the database anyway.
    FieldToDataHeaderKey = {
        Tracer.__name__: {
            "name": NAME_KEY,
            "compound": COMPOUND_KEY,
        },
        TracerLabel.__name__: {
            "element": ELEMENT_KEY,
            "mass_number": MASSNUMBER_KEY,
            "count": LABELCOUNT_KEY,
            "positions": LABELPOSITIONS_KEY,
        },
    }

    # No FieldToDataValueConverter needed

    DataColumnMetadata = DataTableHeaders(
        ID=TableColumn.init_flat(
            name=DataHeaders.ID,
            help_text=(
                "Arbitrary number that identifies every row containing a label that belongs to a tracer.  Each row "
                "defines 1 label and this value links them together."
            ),
            guidance=(
                "The values in this column are not loaded into the database.  It is only used to populate the "
                f"{DataHeaders.NAME} column using an excel formula.  All rows having the same {DataHeaders.ID} are "
                f"used to build the {DataHeaders.NAME} column values."
            ),
            type=int,
        ),
        COMPOUND=TableColumn.init_flat(
            name=DataHeaders.COMPOUND,
            field=Tracer.compound,
            help_text="Primary name of the compound for which this is a tracer.",
            guidance=(
                f"The dropdown menus in this column are populated by the {CompoundsLoader.DataHeaders.NAME} column in "
                f"the {CompoundsLoader.DataSheetName} sheet."
            ),
            type=str,
            # TODO: Implement the method which creates the dropdowns in the excel spreadsheet
            dynamic_choices=ColumnReference(
                loader_class=CompoundsLoader,
                loader_header_key=CompoundsLoader.NAME_KEY,
            ),
        ),
        ELEMENT=TableColumn.init_flat(
            name=DataHeaders.ELEMENT,
            field=TracerLabel.element,
            type=str,
        ),
        MASSNUMBER=TableColumn.init_flat(
            name=DataHeaders.MASSNUMBER,
            field=TracerLabel.mass_number,
            type=int,
        ),
        LABELCOUNT=TableColumn.init_flat(
            name=DataHeaders.LABELCOUNT,
            field=TracerLabel.count,
            type=int,
        ),
        LABELPOSITIONS=TableColumn.init_flat(
            name=DataHeaders.LABELPOSITIONS,
            field=TracerLabel.positions,
            format="Comma-delimited string of integers.",
            type=str,
        ),
        NAME=TableColumn.init_flat(
            name=DataHeaders.NAME,
            field=Tracer.name,
            readonly=True,  # This is a "soft" read-only.  Users can enter values, but use of the formula is encouraged.
            # TODO: Replace "Infusates" and "Tracer Name" below with a reference to its loader's DataSheetName and the
            # corresponding column, respectively
            # Cannot reference the InfusatesLoader here (to include the name of its sheet and its tracer name column)
            # due to circular import
            guidance=(
                "This column is automatically filled in using an excel formula and its values are used to populate "
                "Tracer Name choices in the Infusates sheet."
            ),
            type=str,
            # TODO: Create the method that applies the formula to the NAME column on every row
            # Excel formula that creates the name using the spreadsheet columns on the rows containing the ID on the
            # current row.  The header keys will be replaced by the excel column letters.
            # Simplified example:
            # =CONCATENATE(
            #   INDIRECT("B" & ROW()),
            #   "-[",
            #   TEXTJOIN(",",TRUE,
            #     BYROW(
            #       FILTER(C:E,A:A=INDIRECT("A" & ROW()),""),
            #       LAMBDA(row, CONCAT(row))
            #     )
            #   ),
            #   "]"
            # )
            # NOTE: The inclusion of function prefixes like `_xlpm.` is documented as necessary for the LAMBDA variables
            # in xlsxwriter.  If not included, the export of the excel document will corrupt the formulas.
            # Other errors however occur without other prefixes.  The process used to discover the prefixes necessary is
            # documented here: https://xlsxwriter.readthedocs.io/working_with_formulas.html#dealing-with-formula-errors
            # But basically:
            # 1. Manually paste the (unprefixed) formula into an exported sheet (which should fix the formula, unless
            #    you have a syntax error).
            # 2. Save the file.
            # 3. unzip myfile.xlsx -d myfile
            # 4. xmllint --format myfile/xl/worksheets/sheet8.xml | grep '</f>'
            # 5. Update the prefixes in the formula below to match the prefixes in the working formula that was manually
            #    fixed.
            formula=(
                # If all columns are empty, return an empty string
                "=IF("
                "AND("
                f'ISBLANK(INDIRECT("{{{COMPOUND_KEY}}}" & ROW())),'
                f'ISBLANK(INDIRECT("{{{MASSNUMBER_KEY}}}" & ROW())),'
                f'ISBLANK(INDIRECT("{{{ELEMENT_KEY}}}" & ROW())),'
                f'ISBLANK(INDIRECT("{{{LABELCOUNT_KEY}}}" & ROW())),'
                f'ISBLANK(INDIRECT("{{{LABELPOSITIONS_KEY}}}" & ROW())),'
                f'ISBLANK(INDIRECT("{{{ID_KEY}}}" & ROW()))'
                '),"",'
                # Otherwise, build the tracer name
                "CONCATENATE("
                # Start with the compound
                f'INDIRECT("{{{COMPOUND_KEY}}}" & ROW()),'
                # Wrap the labels in "-[]"
                '"-[",'
                # Join all the label strings with ","
                '_xlfn.TEXTJOIN(",",TRUE,_xlfn._xlws.SORT(_xlfn.MAP('
                # Include mass number from every row for this group
                f"_xlfn._xlws.FILTER({{{MASSNUMBER_KEY}}}:{{{MASSNUMBER_KEY}}},"
                f'{{{ID_KEY}}}:{{{ID_KEY}}}=INDIRECT("{{{ID_KEY}}}" & ROW()), ""),'
                # Include element from every row for this group
                f"_xlfn._xlws.FILTER({{{ELEMENT_KEY}}}:{{{ELEMENT_KEY}}},"
                f'{{{ID_KEY}}}:{{{ID_KEY}}}=INDIRECT("{{{ID_KEY}}}" & ROW()), ""),'
                # Include count from every row for this group
                f"_xlfn._xlws.FILTER({{{LABELCOUNT_KEY}}}:{{{LABELCOUNT_KEY}}},"
                f'{{{ID_KEY}}}:{{{ID_KEY}}}=INDIRECT("{{{ID_KEY}}}" & ROW()), ""),'
                # Include positions from every row for this group
                f"_xlfn._xlws.FILTER({{{LABELPOSITIONS_KEY}}}:{{{LABELPOSITIONS_KEY}}},"
                f'{{{ID_KEY}}}:{{{ID_KEY}}}=INDIRECT("{{{ID_KEY}}}" & ROW()), ""),'
                # Build each label string using a lambda
                "_xlfn._xlws.LAMBDA(_xlpm.mass, _xlpm.elem, _xlpm.cnt, _xlpm.poss, "
                # Concatenate the label elements
                "CONCATENATE("
                # If the positions string is empty, return an empty string, otherwise, return the positions string
                'IF(ISBLANK(_xlpm.poss),"",_xlpm.poss), IF(ISBLANK(_xlpm.poss),"","-"), '
                # And just straight-up join the other columns as-is
                "_xlpm.mass, _xlpm.elem, _xlpm.cnt))"
                "))),"
                # Close off the square bracket in the encompassing "-[]"
                '"]"'
                "))"
            ),
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Tracer, TracerLabel]

    def __init__(self, *args, **kwargs):
        """Constructor.

        Args:
            Superclass Args:
                df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
                dry_run (Optional[boolean]) [False]: Dry run mode.
                defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT
                    MUST HANDLE THE ROLLBACK.
                data_sheet (Optional[str]): Sheet name (for error reporting).
                defaults_sheet (Optional[str]): Sheet name (for error reporting).
                file (Optional[str]): File path.
                filename (Optional[str]): Filename (for error reporting).
                user_headers (Optional[dict]): Header names by header key.
                defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
                defaults_file (Optional[str]): Defaults file name (None if the same as infile).
                headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
                defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.
                extra_headers (Optional[List[str]]): Use for dynamic headers (different in every file).  To allow any
                    unknown header, supply an empty list.
                _validate (bool): If true, runs in validate mode, perhaps better described as "non-curator mode".  This
                    is intended for use by the web validation interface.  It's similar to dry-run mode, in that it never
                    commits anything, but it also raises warnings as fatal (so they can be reported through the web
                    interface and seen by researchers, among other behaviors specific to non-privileged users).
            Derived (this) class Args:
                positions_delimiter (Optional[str]) [;]: Isotope positions delimiter.
        Exceptions:
            None
        Returns:
            None
        """
        self.positions_delimiter = kwargs.pop(
            "positions_delimiter", self.POSITIONS_DELIMITER
        )
        super().__init__(*args, **kwargs)

    def init_load(self):
        """Initializes load-related metadata.  Called before any load occurs (in load_data()).

        Args:
            None

        Exceptions:
            None

        Returns:
            Nothing
        """
        self.auto_tracer_number = 0
        self.tracers_dict = defaultdict(dict)
        self.tracer_name_to_number = defaultdict(lambda: defaultdict(list))
        self.valid_tracers = {}

        self.inconsistent_compounds = defaultdict(lambda: defaultdict(list))
        self.inconsistent_names = defaultdict(lambda: defaultdict(list))
        self.inconsistent_numbers = defaultdict(lambda: defaultdict(list))

    @MaintainedModel.defer_autoupdates()
    def load_data(self):
        """Loads the Tracer and TracerLabel tables from the dataframe (self.df).

        Args:
            None

        Exceptions:
            None (explicitly)

        Returns:
            Nothing
        """
        # Gather all the data needed for the tracers (a tracer can span multiple rows)
        self.build_tracers_dict()

        # Check the self.tracers_dict to fill in missing values parsed from the name
        self.check_extract_name_data()
        self.buffer_consistency_issues()

        # Now that all the tracer data has been collated and validated, load it
        self.load_tracers_dict()

    def build_tracers_dict(self):
        """Iterate over the row data in self.df to populate self.tracers_dict and self.valid_tracers

        Args:
            None

        Exceptions:
            Raises:
                Nothing
            Buffers:
                InfileError

        Returns:
            None
        """
        if not hasattr(self, "tracers_dict"):
            self.init_load()

        for _, row in self.df.iterrows():
            try:
                # missing required values update the skip_row_indexes before load_data is even called, and get_row_val
                # sets the current row index
                if self.is_skip_row(row.name):
                    self.errored(Tracer.__name__)
                    self.errored(TracerLabel.__name__)
                    continue

                (
                    tracer_number,
                    compound_name,
                    tracer_name,
                    element,
                    mass_number,
                    count,
                    positions,
                ) = self.get_row_data(row)

                if not self.valid_tracers[tracer_number]:
                    continue

                if tracer_number not in self.tracers_dict.keys():
                    # Initialize the tracer dict
                    self.tracers_dict[tracer_number] = {
                        "compound_name": compound_name,
                        "tracer_name": tracer_name,
                        "isotopes": [],
                        # Metadata for error reporting
                        "rownum": self.rownum,
                        "row_index": self.row_index,
                    }

                if (
                    element is not None
                    or mass_number is not None
                    or count is not None
                    or positions is not None
                ):
                    self.tracers_dict[tracer_number]["isotopes"].append(
                        {
                            "element": element,
                            "mass_number": mass_number,
                            "count": count,
                            "positions": positions,
                            # Metadata for error reporting
                            "rownum": self.rownum,
                            "row_index": self.row_index,
                        }
                    )

            except Exception as e:
                exc = InfileError(
                    str(e),
                    rownum=self.rownum,
                    sheet=self.sheet,
                    file=self.friendly_file,
                )
                self.add_skip_row_index(row.name)
                self.aggregated_errors_object.buffer_error(exc)
                if tracer_number is not None:
                    self.valid_tracers[tracer_number] = False

    @transaction.atomic
    @MaintainedModel.defer_autoupdates()
    def load_tracers_dict(self):
        """Iterate over the self.tracers_dict to get or create Tracer and TracerLabel database records

        Args:
            None

        Exceptions:
            None

        Returns:
            None
        """
        for tracer_number, tracer_dict in self.tracers_dict.items():
            # We are iterating a dict independent of the file rows, so set the row index manually
            self.set_row_index(tracer_dict["row_index"])

            if self.is_skip_row():
                # If the row the tracer name was first obtained from is a skip row, skip it
                continue

            num_labels = (
                len(tracer_dict["isotopes"]) if len(tracer_dict["isotopes"]) > 0 else 1
            )

            if not self.valid_tracers[tracer_number] or self.is_skip_row():
                # This happens if there was an error in the file processing, like missing required columns, unique
                # column constraint violations, or name parsing issues
                self.errored(Tracer.__name__)
                self.errored(TracerLabel.__name__, num=num_labels)
                continue

            try:
                # We want to roll back everything related to the current tracer record in here if there was an exception
                # We do that by catching outside the atomic block, below.  Note that this method has an atomic
                # transaction decorator that is just for good measure (because the automatically applied wrapper around
                # load_data will roll back everything if any exception occurs - but the decorator on this method is just
                # in case it's ever called from anywhere other than load_data)
                with transaction.atomic():
                    tracer_rec, tracer_created = self.get_or_create_tracer(tracer_dict)

                    # If the tracer rec is None, skip the synonyms
                    if tracer_rec is None:
                        # Assume the compound didn't exist and mark as skipped
                        self.skipped(Tracer.__name__)
                        self.skipped(TracerLabel.__name__, num=num_labels)
                        continue
                    elif not tracer_created:
                        # Note: tracer_rec has already been checked by check_tracer_name_consistent when it existed
                        self.existed(Tracer.__name__)
                        # Refresh the count with the actual existing records (i.e. in case isotope data wasn't provided)
                        num_labels = tracer_rec.labels.count()
                        self.existed(TracerLabel.__name__, num=num_labels)
                        continue

                    # Now, get or create the labels
                    for isotope_dict in self.tracers_dict[tracer_number]["isotopes"]:
                        # We are iterating a dict independent of the file rows, so set the row index manually
                        self.set_row_index(isotope_dict["row_index"])

                        if self.is_skip_row():
                            self.skipped(TracerLabel.__name__)
                            continue

                        self.get_or_create_tracer_label(isotope_dict, tracer_rec)

                    # Only mark as created after this final check (which raises an exception)
                    self.check_tracer_name_consistent(tracer_rec, tracer_dict)

                    # Refresh the count with the actual existing records (i.e. in case isotope data wasn't provided)
                    num_labels = tracer_rec.labels.count()

                    self.created(Tracer.__name__)
                    self.created(TracerLabel.__name__, num=num_labels)
            except Exception as e:
                if not self.aggregated_errors_object.exception_type_exists(type(e)):
                    self.aggregated_errors_object.buffer_error(e)
                # All exceptions are buffered in their respective functions, so just update the stats
                self.errored(Tracer.__name__)
                self.errored(TracerLabel.__name__, num=num_labels)

    def get_row_data(self, row):
        """Retrieve and validate the row data.

        Updates:
            self.tracer_name_to_number

        Args:
            row (pandas dataframe row)

        Exceptions:
            None

        Returns:
            tracer_number (integer)
            compound_name (string)
            tracer_name (string)
            element (string)
            mass_number (integer)
            count (integer)
            positions (list of integers)
        """
        tracer_number = self.get_row_val(row, self.headers.ID)
        compound_name = self.get_row_val(row, self.headers.COMPOUND)
        tracer_name = self.get_row_val(row, self.headers.NAME)
        element = self.get_row_val(row, self.headers.ELEMENT)
        mass_number = self.get_row_val(row, self.headers.MASSNUMBER)
        count = self.get_row_val(row, self.headers.LABELCOUNT)
        raw_positions = self.get_row_val(row, self.headers.LABELPOSITIONS)
        positions = self.parse_label_positions(raw_positions)

        if tracer_number is None:
            self.auto_tracer_number -= 1
            tracer_number = self.auto_tracer_number
            self.aggregated_errors_object.buffer_warning(
                InfileError(
                    (
                        f"{self.headers.ID} undefined.  Assuming a single-labeled-element tracer and assigning a "
                        f"negative number '{tracer_number}' (to avoid conflating this with another row that has a "
                        f"{self.headers.ID})."
                    ),
                    rownum=self.rownum,
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )
            self.errored(Tracer.__name__)
            self.errored(TracerLabel.__name__)

        retval = (
            tracer_number,
            compound_name,
            tracer_name,
            element,
            mass_number,
            count,
            positions,
        )

        if tracer_number not in self.tracers_dict.keys():
            # Check for tracer names that map to multiple different tracer numbers
            if tracer_name not in self.tracer_name_to_number.keys():
                # The normal case: 1 name, 1 number
                self.tracer_name_to_number[tracer_name][tracer_number] = self.rownum
            elif tracer_number not in self.tracer_name_to_number[tracer_name].keys():
                # An inconsistency: 1 name associated with a new number
                existing_num = list(self.tracer_name_to_number[tracer_name].keys())[0]
                self.inconsistent_numbers[tracer_name][existing_num].append(
                    self.tracer_name_to_number[tracer_name][existing_num]
                )
                self.inconsistent_numbers[tracer_name][tracer_number].append(
                    self.rownum
                )
            elif len(self.tracer_name_to_number[tracer_name].keys()) > 1:
                # This is already in an inconsistent state because there are multiple numbers, so append new rows
                self.inconsistent_numbers[tracer_name][tracer_number].append(
                    self.rownum
                )

            # If this is the first time we've seen this number, start out as valid
            if tracer_number not in self.valid_tracers.keys():
                self.valid_tracers[tracer_number] = True
        else:
            # Make sure the compound, tracer name, and number are consistent (even though we're not going to use
            # them on this row - we only use the first occurrence of a tracer name, but we should sanity check the rest)
            self.check_data_is_consistent(
                tracer_number,
                compound_name,
                tracer_name,
            )

        return retval

    def check_extract_name_data(self):
        """Fill in missing data in self.tracers_dict using data parsed from the tracer name, and check for
        inconsistencies.

        Args:
            None

        Exceptions:
            Raises:
                Nothing
            Buffers:
                InfileError

        Returns:
            Nothing
        """
        for tracer_number in self.tracers_dict.keys():
            table_tracer = self.tracers_dict[tracer_number]
            table_tracer_name = table_tracer["tracer_name"]

            if table_tracer_name is None:
                continue

            compound_name = table_tracer["compound_name"]
            parsed_tracer = parse_tracer_string(table_tracer_name)
            parsed_compound_name = parsed_tracer["compound_name"]

            if compound_name is None:
                table_tracer["compound_name"] = parsed_compound_name
            elif compound_name != parsed_compound_name:
                self.aggregated_errors_object.buffer_error(
                    InfileError(
                        (
                            f"{self.headers.COMPOUND}: [{compound_name}] does not match the {self.headers.COMPOUND} "
                            f"parsed from {self.headers.NAME} ({table_tracer_name}): [{parsed_compound_name}] on %s"
                        ),
                        file=self.friendly_file,
                        sheet=self.sheet,
                        rownum=table_tracer["rownum"],
                    )
                )

            fill_in = len(table_tracer["isotopes"]) == 0
            for table_isotope in table_tracer["isotopes"]:
                match = False
                for parsed_isotope in parsed_tracer["isotopes"]:
                    match = False
                    for key in ["element", "mass_number", "count", "positions"]:
                        if table_isotope[key] is None:
                            fill_in = True
                        elif parsed_isotope[key] == table_isotope[key]:
                            match = True
                        elif parsed_isotope[key] != table_isotope[key]:
                            match = False
                            break
                    if match is True:
                        break
                if match is False:
                    cols = ", ".join(
                        [
                            f"{self.headers.ELEMENT}: {table_isotope['element']}",
                            f"{self.headers.MASSNUMBER}: {table_isotope['mass_number']}",
                            f"{self.headers.LABELCOUNT}: {table_isotope['count']}",
                            f"{self.headers.LABELPOSITIONS}: {table_isotope['positions']}",
                        ]
                    )
                    irows = [ir["rownum"] for ir in table_tracer["isotopes"]]
                    self.aggregated_errors_object.buffer_error(
                        InfileError(
                            (
                                f"Isotope data from columns [{cols}] on row(s) {irows} does not match any of the "
                                f"isotopes parsed from the {self.headers.NAME} [{table_tracer_name}] on %s."
                            ),
                            file=self.friendly_file,
                            sheet=self.sheet,
                            rownum=table_tracer["rownum"],
                        )
                    )

            if len(table_tracer["isotopes"]) != len(parsed_tracer["isotopes"]):
                fill_in = True
                irows = [ir["rownum"] for ir in table_tracer["isotopes"]]
                self.aggregated_errors_object.buffer_warning(
                    InfileError(
                        (
                            f"There are [{len(table_tracer['isotopes'])}] rows {irows} of data defining isotopes for "
                            f"{self.headers.NAME} [{table_tracer_name}] in %s, but the number of labels parsed from "
                            f"the {self.headers.NAME} [{len(parsed_tracer['isotopes'])}] does not match the number of "
                            f"rows for {self.headers.ID} {tracer_number}.  Perhaps {self.headers.ID} {tracer_number} "
                            "is on the wrong number of rows?"
                        ),
                        file=self.friendly_file,
                        sheet=self.sheet,
                    )
                )
                self.warned(TracerLabel.__name__)

            # If anything was missing, we're going to just recreate the data from the names
            if fill_in is True:
                table_tracer["isotopes"] = []
                for parsed_isotope in parsed_tracer["isotopes"]:
                    table_tracer["isotopes"].append(
                        {
                            "element": parsed_isotope["element"],
                            "mass_number": parsed_isotope["mass_number"],
                            "count": parsed_isotope["count"],
                            "positions": parsed_isotope["positions"],
                            # The row info where the tracer name was obtained
                            "rownum": table_tracer["rownum"],
                            "row_index": table_tracer["row_index"],
                        }
                    )

    def get_or_create_tracer(self, tracer_dict):
        """Get or create a Tracer record.

        Args:
            tracer_dict (dict)

        Exceptions:
            None

        Returns:
            rec (Tracer)
            created (boolean)
        """
        created = False
        rec = None

        # See if we can retrieve an existing record
        rec = self.get_tracer(tracer_dict)

        if rec is not None:
            self.check_tracer_name_consistent(rec, tracer_dict)
            return rec, created

        # If we got here, we are creating, so first, try to retrieve the compound
        compound_rec = self.get_compound(tracer_dict["compound_name"])

        if compound_rec is None:
            return rec, created

        rec = self.create_tracer(compound_rec)
        created = True

        return rec, created

    def get_tracer(self, tracer_dict):
        """Get a Tracer record.

        Args:
            tracer_dict (dict)

        Exceptions:
            Raises:
                Nothing (explicitly)
            Buffers:
                InfileError (repackages other exceptions)

        Returns:
            rec (Optional[Tracer])
        """
        rec = None

        # See if we can retrieve an existing record
        try:
            # First, we will try to see if we can retrieve the precise tracer, using a TracerData object
            tracer_data = TracerData(
                unparsed_string=tracer_dict["tracer_name"],
                compound_name=tracer_dict["compound_name"],
                isotopes=[
                    IsotopeData(
                        element=ido["element"],
                        mass_number=ido["mass_number"],
                        count=ido["count"],
                        positions=ido["positions"],
                    )
                    for ido in tracer_dict["isotopes"]
                ],
            )

            rec = Tracer.objects.get_tracer(tracer_data)

        except Exception as e:
            exc = InfileError(
                str(e), rownum=self.rownum, sheet=self.sheet, file=self.friendly_file
            )
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.aggregated_errors_object.buffer_error(exc)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise e

        return rec

    def get_compound(self, compound_name):
        """Retrieves a Compound record whose name or synonym matches the supplied name.

        Args:
            compound_name (string)

        Exceptions:
            Raises:
                Nothing
            Buffers:
                CompoundDoesNotExist
                InfileError

        Returns:
            rec (Optional[Tracer])
        """
        rec = None

        # If we got here, we are creating, so first, try to retrieve the compound
        try:
            rec = Compound.compound_matching_name_or_synonym(compound_name)

            if rec is None:
                raise ProgrammingError(
                    "The assumption that compound_matching_name_or_synonym either returns a real record or else raises "
                    "an exception was incorrect."
                )

        except ObjectDoesNotExist:
            self.aggregated_errors_object.buffer_error(
                CompoundDoesNotExist(
                    name=compound_name,
                    rownum=self.rownum,
                    column=self.headers.COMPOUND,
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
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
    def create_tracer(self, compound_rec):
        """Creates a Tracer record.

        Args:
            compound_rec (Compound)

        Exceptions:
            Raises:
                Nothing (explicitly)
            Buffers:
                Nothing (explicitly)

        Returns:
            rec (Tracer)
        """
        rec = None
        rec_dict = {"compound": compound_rec}

        try:
            rec = Tracer.objects.create(**rec_dict)
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Tracer, rec_dict)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise e

        return rec

    def get_or_create_tracer_label(self, isotope_dict, tracer_rec):
        """Get or create a TracerLabel record.

        Args:
            isotope_dict (dict)
            tracer_rec (Tracer)

        Raises:
            Nothing (explicitly)

        Returns:
            rec (Optional[TracerLabel])
            created (boolean)
        """
        rec = None
        rec_dict = None
        created = False

        try:
            element = isotope_dict["element"]
            mass_number = isotope_dict["mass_number"]
            count = isotope_dict["count"]
            positions = isotope_dict["positions"]

            rec_dict = {
                "element": element,
                "mass_number": mass_number,
                "count": count,
                "positions": positions,
                "tracer": tracer_rec,
            }

            rec, created = TracerLabel.objects.get_or_create(**rec_dict)

            if created:
                rec.full_clean()

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, TracerLabel, rec_dict)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise e

        return rec, created

    def parse_label_positions(self, positions_str):
        """Create a list of integers from a delimited string of integers.

        Args:
            positions_str (string): delimited string of integers

        Raises:
            Nothing

        Returns:
            positions (Optional[list of integers])
        """
        positions = None
        if positions_str is None:
            return positions
        if positions_str:
            try:
                positions = [
                    int(pos.strip())
                    for pos in positions_str.split(self.positions_delimiter)
                    if pos.strip() != ""
                ]
            except Exception as e:
                self.add_skip_row_index()
                exc = InfileError(
                    str(e),
                    rownum=self.rownum,
                    sheet=self.sheet,
                    file=self.friendly_file,
                )
                # Package errors (like IntegrityError and ValidationError) with relevant details
                # This also updates the skip row indexes
                self.aggregated_errors_object.buffer_error(exc)

        return positions

    def check_data_is_consistent(
        self,
        tracer_number,
        compound_name,
        tracer_name,
    ):
        """Ensures that each tracer number is associated with the same compound and tracer name.

        Accesses:
            self.tracers_dict
            self.tracer_name_to_number

        Adds inconsistencies to:
            self.inconsistent_compounds
            self.inconsistent_names
            self.inconsistent_numbers

        Args:
            tracer_number (integer)
            compound_name (string)
            tracer_name (string)

        Exceptions:
            None

        Returns:
            None
        """
        # Make sure that each tracer number is always associated with the same compound
        if self.tracers_dict[tracer_number]["compound_name"] != compound_name:
            if tracer_number not in self.inconsistent_compounds.keys():
                self.inconsistent_compounds[tracer_number][
                    self.tracers_dict[tracer_number]["compound_name"]
                ] = [self.tracers_dict[tracer_number]["rownum"]]
            self.inconsistent_compounds[tracer_number][compound_name].append(
                self.rownum
            )

        # Make sure that each tracer number is always associated with the same tracer name
        if self.tracers_dict[tracer_number]["tracer_name"] != tracer_name:
            if tracer_number not in self.inconsistent_names.keys():
                self.inconsistent_names[tracer_number][
                    self.tracers_dict[tracer_number]["tracer_name"]
                ] = [self.tracers_dict[tracer_number]["rownum"]]
            self.inconsistent_names[tracer_number][tracer_name].append(self.rownum)

        if (
            tracer_name in self.tracer_name_to_number.keys()
            and tracer_number not in self.tracer_name_to_number[tracer_name].keys()
        ):
            self.inconsistent_numbers[tracer_name][tracer_number].append(self.rownum)

    def buffer_consistency_issues(self):
        """Buffers consistency errors.

        - When a tracer number is associated with multiple compounds
        - When a tracer number is associated with multiple tracer names
        - When a tracer name is associated with multiple tracer numbers

        Args:
            None

        Exceptions:
            Raises:
                None
            Buffers:
                InfileError

        Returns:
            None
        """
        # TODO: While all these errors are accurate, it would probably be better to organize them differently and reword
        #       them to be easier to follow.
        for tracer_number in self.inconsistent_compounds.keys():
            msg = (
                f"%s:\n\t{self.headers.ID} {tracer_number} is associated with multiple {self.headers.COMPOUND}s on the "
                f"indicated rows.  Only one {self.headers.COMPOUND} is allowed per {self.headers.ID}.\n\t\t"
            )
            msg += "\n\t\t".join(
                [
                    f"{c} (on rows: {self.inconsistent_compounds[tracer_number][c]})"
                    for c in self.inconsistent_compounds[tracer_number].keys()
                ]
            )
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    msg,
                    column=f"{self.headers.ID} and {self.headers.COMPOUND}",
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )
            for nk in self.inconsistent_compounds[tracer_number].keys():
                self.add_skip_row_index(
                    index_list=self.inconsistent_compounds[tracer_number][nk]
                )

        for tracer_number in self.inconsistent_names.keys():
            msg = (
                f"%s:\n\t{self.headers.ID} {tracer_number} is associated with multiple {self.headers.NAME}s on the "
                f"indicated rows.  Only one {self.headers.NAME} is allowed per {self.headers.ID}.\n\t\t"
            )
            msg += "\n\t\t".join(
                [
                    f"{n} (on rows: {self.inconsistent_names[tracer_number][n]})"
                    for n in self.inconsistent_names[tracer_number].keys()
                ]
            )
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    msg,
                    column=f"{self.headers.NAME} and {self.headers.ID}",
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )
            for nk in self.inconsistent_names[tracer_number].keys():
                self.add_skip_row_index(
                    index_list=self.inconsistent_names[tracer_number][nk]
                )

        for tracer_name in self.inconsistent_numbers.keys():
            msg = (
                f"%s:\n\t{self.headers.NAME} {tracer_name} is associated with multiple {self.headers.ID}s on the "
                f"indicated rows.  Only one {self.headers.ID} is allowed per {self.headers.NAME}.\n\t\t"
            )
            msg += "\n\t\t".join(
                [
                    f"{n} (on rows: {self.inconsistent_numbers[tracer_name][n]})"
                    for n in self.inconsistent_numbers[tracer_name].keys()
                ]
            )
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    msg,
                    column=f"{self.headers.ID} and {self.headers.NAME}",
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )
            for nk in self.inconsistent_numbers[tracer_name].keys():
                self.add_skip_row_index(
                    index_list=self.inconsistent_numbers[tracer_name][nk]
                )

    def check_tracer_name_consistent(self, rec, tracer_dict):
        """Checks for consistency between a dynamically generated tracer name and the one supplied in the file.

        Args:
            rec (Tracer): A Tracer model object
            tracer_dict (dict): Data parsed from potentially multiple rows relating to a single Tracer

        Exceptions:
            Raises:
                InfileError
            Buffers:
                InfileError

        Returns:
            None
        """
        supplied_name = tracer_dict["tracer_name"]

        if supplied_name is None:
            # Nothing to check
            return

        # We have to generate it instead of simply access it in the model object, because this is a maintained field,
        # and if the record was created, auto-update will not happen until the load is complete
        generated_name = rec.name_with_synonym(tracer_dict["compound_name"])

        # Ignoring differences in whitespace and case differences
        if (
            supplied_name.replace(" ", "").lower()
            != generated_name.replace(" ", "").lower()
        ):
            data_rownums = summarize_int_list(
                [rd["rownum"] for rd in tracer_dict["isotopes"]]
            )
            exc = InfileError(
                (
                    f"The supplied tracer name '{supplied_name}' from %s does not match the automatically generated "
                    f"name '{generated_name}' using the data on row(s) {data_rownums}."
                ),
                file=self.friendly_file,
                sheet=self.sheet,
                column=self.headers.NAME,
                rownum=tracer_dict["rownum"],
            )
            self.aggregated_errors_object.buffer_error(exc)
            raise exc
