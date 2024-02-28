from collections import defaultdict, namedtuple
from typing import Dict

from django.core.exceptions import ObjectDoesNotExist
from django.db import ProgrammingError, transaction

from DataRepo.loaders.table_loader import TableLoader
from DataRepo.models import Compound, MaintainedModel, Tracer, TracerLabel
from DataRepo.utils.exceptions import (
    CompoundDoesNotExist,
    InfileError,
    RequiredColumnValue,
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
    MASS_NUMBER_KEY = "MASSNUMBER"
    LABEL_COUNT_KEY = "LABELCOUNT"
    LABEL_POSITIONS_KEY = "LABELPOSITIONS"
    NAME_KEY = "NAME"

    POSITIONS_DELIMITER = ","

    DataSheetName = "Tissues"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "ID",
            "COMPOUND",
            "ELEMENT",
            "MASSNUMBER",
            "LABELCOUNT",
            "LABELPOSITIONS",
            "NAME",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        ID="Tracer Number",
        COMPOUND="Compound Name",
        ELEMENT="Element",
        MASSNUMBER="Mass Number",
        LABELCOUNT="Label Count",
        LABELPOSITIONS="Label Positions",
        NAME="Tracer Name",
    )

    # Whether each column is required to be present of not
    DataRequiredHeaders = DataTableHeaders(
        ID=True,
        COMPOUND=False,
        ELEMENT=False,
        MASSNUMBER=False,
        LABELCOUNT=False,
        LABELPOSITIONS=False,
        NAME=False,
    )

    # Whether a value for an row in a column is required or not (note that defined DataDefaultValues will satisfy this)
    DataRequiredValues = DataRequiredHeaders

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes: Dict[str, type] = {
        ID_KEY: int,
        COMPOUND_KEY: str,
        ELEMENT_KEY: str,
        MASS_NUMBER_KEY: int,
        LABEL_COUNT_KEY: str,
        LABEL_POSITIONS_KEY: str,
        NAME_KEY: str,
    }

    # No DataDefaultValues needed

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        [
            COMPOUND_KEY,
            ELEMENT_KEY,
            MASS_NUMBER_KEY,
            LABEL_COUNT_KEY,
            LABEL_POSITIONS_KEY,
        ],
    ]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        Tracer.__name__: {
            "name": NAME_KEY,
        },
        TracerLabel.__name__: {
            "element": ELEMENT_KEY,
            "mass_number": MASS_NUMBER_KEY,
            "count": LABEL_COUNT_KEY,
            # "positions": LABEL_POSITIONS_KEY,  # Not a direct mapping of column value to positions
            # "name": NAME_KEY,  # Derived
        },
    }

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Tracer, TracerLabel]

    def __init__(self, *args, **kwargs):
        """Constructor.

        Args:
            Superclass Args:
                df (pandas dataframe): Data, e.g. as parsed from a table-like file.
                headers (Optional[Tableheaders namedtuple]) [DataHeaders]: Header names by header key.
                defaults (Optional[Tableheaders namedtuple]) [DataDefaultValues]: Default values by header key.
                dry_run (Optional[boolean]) [False]: Dry run mode.
                defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT
                    MUST HANDLE THE ROLLBACK.
                sheet (Optional[str]) [None]: Sheet name (for error reporting).
                file (Optional[str]) [None]: File name (for error reporting).
            Derived (this) class Args:
                synonym_separator (Optional[str]) [;]: Synonym string delimiter.

        Raises:
            Nothing

        Returns:
            Nothing
        """
        self.positions_delimiter = kwargs.pop(
            "positions_delimiter", self.POSITIONS_DELIMITER
        )
        super().__init__(*args, **kwargs)

    def init_load(self):
        """Initializes load-related metadata.  Called before any load occurs (in load_data().

        Args:
            None

        Exceptions:
            None

        Returns:
            Nothing
        """
        self.tracer_dict = defaultdict(dict)
        self.tracer_name_to_number = defaultdict(defaultdict(list))
        self.valid_tracers = {}

        self.inconsistent_compounds = defaultdict(defaultdict(list))
        self.inconsistent_names = defaultdict(defaultdict(list))
        self.inconsistent_numbers = defaultdict(defaultdict(list))

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
        self.init_load()

        # Gather all the data needed for the tracers (a tracer can span multiple rows)
        for _, row in self.df.iterrows():
            try:
                # The tracer number is simply used to associate all rows belonging to a single tracer. It is not loaded.
                tracer_number = self.get_row_val(row, self.headers.ID)

                if tracer_number is None:
                    # Note, get_row_val buffers errors for missing required columns/values
                    self.skipped(Tracer.__name__)
                    self.skipped(TracerLabel.__name__)
                    continue

                (
                    compound_name,
                    tracer_name,
                    element,
                    mass_number,
                    count,
                    positions,
                ) = self.get_row_data(row, tracer_number)

                if not self.valid_tracers[tracer_number]:
                    continue

                if tracer_number not in self.tracer_dict.keys():
                    # Initialize the tracer dict
                    self.tracer_dict[tracer_number] = {
                        "compound_name": compound_name,
                        "tracer_name": tracer_name,
                        "isotopes": [],
                        # Metadata for error reporting
                        "rownum": self.rownum,
                        "row_index": self.row_index,
                    }

                self.tracer_dict[tracer_number]["isotopes"].append(
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
                    str(e), rownum=self.rownum, sheet=self.sheet, file=self.file
                )
                self.add_skip_row_index(row.name)
                self.aggregated_errors_object.buffer_error(exc)
                if tracer_number is not None:
                    self.valid_tracers[tracer_number] = False

        # Check the self.tracer_dict to fill in missing values parsed from the name
        self.check_extract_name_data()
        self.buffer_consistency_issues()

        # Now that all the tracer data has been collated and validated, load it
        for tracer_number, entry in self.tracer_dict.items():
            # We are iterating a dict independent of the file rows, so set the row index manually
            self.set_row_index(entry["row_index"])

            if not self.valid_tracers[tracer_number] or self.is_skip_row():
                self.errored(Tracer.__name__)
                num_labels = len(entry["isotopes"]) if len(entry["isotopes"]) > 0 else 1
                self.errored(TracerLabel.__name__, num=num_labels)
                continue

            try:
                tracer_rec, tracer_created = self.get_or_create_tracer(entry)
            except Exception:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

            # If the tracer rec is None or the row it was on was added to the skip list, skip the synonyms
            if tracer_rec is None or self.is_skip_row():
                self.skipped(TracerLabel.__name__, num=len(entry["isotopes"]))
                continue
            elif not tracer_created:
                self.existed(TracerLabel.__name__, num=len(entry["isotopes"]))
                continue

            for isotope_dict in self.tracer_dict[tracer_number]["isotopes"]:
                # We are iterating a dict independent of the file rows, so set the row index manually
                self.set_row_index(isotope_dict["row_index"])

                if self.is_skip_row():
                    continue

                self.get_or_create_tracer_label(isotope_dict, tracer_rec)

    def get_row_data(self, row, tracer_number):
        """Retrieve and validate the row data.

        Updates:
            self.tracer_name_to_number

        Args:
            row (pandas dataframe row)
            tracer_number (integer): Number used to associate multiple rows of label data to a distinct tracer.

        Exceptions:
            None

        Returns:
            compound_name (string)
            tracer_name (string)
            element (string)
            mass_number (integer)
            count (integer)
            positions (list of integers)
        """
        compound_name = (self.get_row_val(row, self.headers.COMPOUND),)
        tracer_name = (self.get_row_val(row, self.headers.NAME),)
        element = (self.get_row_val(row, self.headers.ELEMENT),)
        mass_number = (self.get_row_val(row, self.headers.MASSNUMBER),)
        count = (self.get_row_val(row, self.headers.LABELCOUNT),)
        raw_positions = self.get_row_val(row, self.headers.LABELPOSITIONS)
        positions = self.parse_label_positions(raw_positions)

        retval = (
            compound_name,
            tracer_name,
            element,
            mass_number,
            count,
            positions,
        )

        # Require either a tracer name or compound, element, mass number, and count
        if self.required_values_missing(
            tracer_name,
            compound_name,
            element,
            mass_number,
            count,
        ):
            self.valid_tracers[tracer_number] = False
            return retval

        if tracer_number not in self.tracer_dict.keys():
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
        """Fill in missing data using data parsed from the tracer name, and check for inconsistencies

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
        for table_tracer in self.tracer_dict.values():
            table_tracer_name = table_tracer["tracer_name"]

            if table_tracer_name is None:
                continue

            compound_name = table_tracer["compound_name"]
            parsed_tracer = parse_tracer_string(table_tracer_name)
            parsed_compound_name = parsed_tracer["compound_name"]

            if compound_name is None:
                compound_name = parsed_compound_name
            elif compound_name != parsed_compound_name:
                self.aggregated_errors_object.buffer_error(
                    InfileError(
                        (
                            f"Compound name from column [{compound_name}] does not match the name parsed from the "
                            f"tracer name ({table_tracer_name}): [{parsed_compound_name}]: %s"
                        ),
                        file=self.file,
                        sheet=self.sheet,
                        rownum=table_tracer["rownum"],
                        column=self.headers.COMPOUND,
                    )
                )

            fill_in = False
            for table_isotope in table_tracer["isotopes"]:
                match = True
                for parsed_isotope in parsed_tracer["isotopes"]:
                    for key in ["element", "mass_number", "count", "positions"]:
                        if table_isotope[key] is None:
                            fill_in = True
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
                    self.aggregated_errors_object.buffer_error(
                        InfileError(
                            (
                                f"Isotope from columns [{cols}] does not match any of the isotopes parsed from the "
                                f"tracer name [{table_tracer_name}]: %s"
                            ),
                            file=self.file,
                            sheet=self.sheet,
                            rownum=table_tracer["rownum"],
                        )
                    )

            if (
                len(table_tracer["isotopes"]) == 1
                and table_tracer["isotopes"][0]["element"] is None
                and table_tracer["isotopes"][0]["mass_number"] is None
                and table_tracer["isotopes"][0]["count"] is None
                and table_tracer["isotopes"][0]["positions"] is None
            ):
                fill_in = True
            elif len(table_tracer["isotopes"]) != len(parsed_tracer["isotopes"]):
                fill_in = True
                self.aggregated_errors_object.buffer_warning(
                    InfileError(
                        (
                            f"The number of isotopes from the rows of the table [{len(table_tracer['isotopes'])}] does "
                            f"not match the number parsed from the tracer name [{table_tracer_name}] on row "
                            f"[{table_tracer['rownum']}]: %s"
                        ),
                        file=self.file,
                        sheet=self.sheet,
                    )
                )

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

    @transaction.atomic
    def get_or_create_tracer(self, entry):
        """Get or create a Tracer record.

        Also counts skipped records (when the compound doesn't exist).

        Args:
            id (integer)
            entry (dict)

        Exceptions:
            None

        Returns:
            rec (Tracer)
            created (boolean)
        """
        created = False
        rec = None

        # See if we can retrieve an existing record
        rec = self.get_tracer(entry)

        if rec is not None:
            return rec, created

        # If we got here, we are creating, so first, try to retrieve the compound
        compound_rec = self.get_compound(entry["compound_name"])

        if compound_rec is None:
            self.skipped(Tracer.__name__)
            return rec, created

        rec = self.create_tracer(compound_rec)
        created = True

        return rec, created

    def get_tracer(self, entry):
        """Get a Tracer record.

        Also counts existing or errored records.

        Args:
            id (integer)
            entry (dict)

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
                unparsed_string=entry["tracer_name"],
                compound_name=entry["compound_name"],
                isotopes=[
                    IsotopeData(
                        element=ido["element"],
                        mass_number=ido["mass_number"],
                        count=ido["count"],
                        positions=ido["positions"],
                    )
                    for ido in entry["isotopes"]
                ],
            )

            rec = Tracer.objects.get_tracer(tracer_data)

            if rec is not None:
                self.existed(Tracer.__name__)

        except Exception as e:
            exc = InfileError(
                str(e), rownum=self.rownum, sheet=self.sheet, file=self.file
            )
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.aggregated_errors_object.buffer_error(exc)
            self.errored(Tracer.__name__)
            # Now that the exception has been handled, trigger a roolback of this record load attempt
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
                    file=self.file,
                    sheet=self.sheet,
                )
            )
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    str(e), rownum=self.rownum, sheet=self.sheet, file=self.file
                )
            )

        return rec

    @transaction.atomic
    def create_tracer(self, compound_rec):
        """Creates a Tracer record.

        Also counts created or errored records.

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
            self.created(Tracer.__name__)

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Tracer, rec_dict)
            self.errored(Tracer.__name__)
            # Now that the exception has been handled, trigger a roolback of this record load attempt
            raise e

        return rec

    @transaction.atomic
    def get_or_create_tracer_label(self, isotope_dict, tracer_rec):
        """Get or create a TracerLabel record.

        Also counts created, existed, or errored records.

        Args:
            row (pandas dataframe row)
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

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row():
                self.errored(TracerLabel.__name__)
                return rec, created

            rec, created = TracerLabel.objects.get_or_create(**rec_dict)

            if created:
                rec.full_clean()
                self.created(TracerLabel.__name__)
            else:
                self.existed(TracerLabel.__name__)

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, TracerLabel, rec_dict)
            self.errored(TracerLabel.__name__)
            # Now that the exception has been handled, trigger a roolback of this record load attempt
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
                    str(e), rownum=self.rownum, sheet=self.sheet, file=self.file
                )
                # Package errors (like IntegrityError and ValidationError) with relevant details
                # This also updates the skip row indexes
                self.aggregated_errors_object.buffer_error(exc)

        return positions

    def check_data_is_consistent(
        self,
        tracer_number,
        compound_str,
        tracer_str,
    ):
        """Ensures that each tracer number is associated with the same compound and tracer name.

        Accesses:
            self.tracer_dict
            self.tracer_name_to_number

        Adds inconsistencies to:
            self.inconsistent_compounds
            self.inconsistent_names
            self.inconsistent_numbers

        Args:
            tracer_number (integer)
            compound_str (string)
            tracer_str (string)

        Exceptions:
            None

        Returns:
            None
        """
        # Make sure that each tracer number is always associated with the same compound
        if self.tracer_dict[tracer_number]["compound_name"] != compound_str:
            if tracer_number not in self.inconsistent_compounds.keys():
                self.inconsistent_compounds[tracer_number][
                    self.tracer_dict[tracer_number]["compound_name"]
                ] = [self.tracer_dict[tracer_number]["rownum"]]
            self.inconsistent_compounds[tracer_number][compound_str].append(self.rownum)

        # Make sure that each tracer number is always associated with the same tracer name
        if self.tracer_dict[tracer_number]["tracer_name"] != tracer_str:
            if tracer_number not in self.inconsistent_names.keys():
                self.inconsistent_names[tracer_number][
                    self.tracer_dict[tracer_number]["tracer_name"]
                ] = [self.tracer_dict[tracer_number]["rownum"]]
            self.inconsistent_names[tracer_number][tracer_str].append(self.rownum)

        if (
            tracer_str in self.tracer_name_to_number.keys()
            and tracer_number not in self.tracer_name_to_number[tracer_str].keys()
        ):
            self.inconsistent_numbers[tracer_str][tracer_number].append(self.rownum)

    def buffer_consistency_issues(self):
        """Buffers consistency errors.

        - When a tracer number is associated with multiple compounds
        - When a tracer number is associated with multiple tracer names
        - When a tracer name is associated with multiple tracer numbers
        """
        for tracer_number in self.inconsistent_compounds.keys():
            msg = (
                f"%s:\n\tTracer number {tracer_number} is associated with multiple compounds on the indicated rows.  "
                "Only one compound is allowed per tracer number.\n\t\t"
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
                    file=self.file,
                    sheet=self.sheet,
                )
            )

        for tracer_number in self.inconsistent_names.keys():
            msg = (
                f"%s:\n\tTracer number {tracer_number} is associated with multiple tracer names on the indicated "
                "rows.  Only one tracer name is allowed per tracer number.\n\t\t"
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
                    column=f"{self.headers.ID} and {self.headers.NAME}",
                    file=self.file,
                    sheet=self.sheet,
                )
            )

        for tracer_name in self.inconsistent_numbers.keys():
            msg = (
                f"%s:\n\tTracer name {tracer_name} is associated with multiple tracer numbers on the indicated rows.  "
                "rows.  Only one tracer number is allowed per tracer name.\n\t\t"
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
                    file=self.file,
                    sheet=self.sheet,
                )
            )

    def required_values_missing(
        self,
        tracer_name,
        compound_name,
        element,
        mass_number,
        count,
    ):
        """Checks row data to ensure that either a tracer name or all of compound, element, mass_number, and count are
        defined.

        Also counts errored records and appends to skip row indexes.

        Args:
            tracer_name (string)
            compound_name (string)
            element (string)
            mass_number (integer)
            count (integer)

        Exceptions:
            Raises:
                Nothing
            Buffers:
                RequiredColumnValue

        Returns:
            missing (boolean)
        """
        missing = False
        if tracer_name is None and (
            compound_name is None
            or element is None
            or mass_number is None
            or count is None
        ):
            missing_cols = []
            if compound_name is None:
                missing_cols.append(self.headers.COMPOUND)
            if element is None:
                missing_cols.append(self.headers.ELEMENT)
            if mass_number is None:
                missing_cols.append(self.headers.MASSNUMBER)
            if count is None:
                missing_cols.append(self.headers.LABELCOUNT)

            self.aggregated_errors_object.buffer_error(
                RequiredColumnValue(
                    column=f"{self.headers.NAME} or [{', '.join(missing_cols)}]",
                    rownum=self.rownum,
                    sheet=self.sheet,
                    file=self.file,
                ),
            )

            self.add_skip_row_index()
            self.errored(Tracer.__name__)
            self.errored(TracerLabel.__name__)

            missing = True

        return missing
