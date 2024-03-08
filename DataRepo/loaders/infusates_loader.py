from collections import defaultdict, namedtuple
import math
from typing import Dict

from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction

from DataRepo.loaders.table_loader import TableLoader
from DataRepo.models import MaintainedModel, Infusate, InfusateTracer, Tracer
from DataRepo.utils.exceptions import (
    RecordDoesNotExist,
    InfileError,
    summarize_int_list,
)
from DataRepo.utils.infusate_name_parser import (
    InfusateData,
    InfusateTracerData,
    parse_infusate_name,
    parse_tracer_string,
)


class InfusatesLoader(TableLoader):
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    ID_KEY = "ID"
    GROUP_NAME_KEY = "TRACERGROUP"
    TRACER_NAME_KEY = "TRACERNAME"
    NAME_KEY = "NAME"
    CONC_KEY = "TRACERCONC"

    TRACER_DELIMETER = Infusate.TRACER_DELIMETER

    DataSheetName = "Infusates"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "ID",
            "TRACERGROUP",
            "TRACERNAME",
            "TRACERCONC",
            "NAME",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        ID="Infusate Number",
        TRACERGROUP="Tracer Group Name",
        TRACERNAME="Tracer Name",
        TRACERCONC="Tracer Concentration",
        NAME="Infusate Name",
    )

    # List of required header keys
    DataRequiredHeaders = [
        [
            # Either individual column data...
            [
                ID_KEY,
                GROUP_NAME_KEY,
                TRACER_NAME_KEY,
                CONC_KEY,
            ],
            # Or the ID, infusate name, and concentration (with concs in the same row order and their order in the name)
            [
                ID_KEY,
                NAME_KEY,
                CONC_KEY,
            ],
        ],
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes: Dict[str, type] = {
        ID_KEY: int,
        GROUP_NAME_KEY: str,
        TRACER_NAME_KEY: str,
        CONC_KEY: float,
        NAME_KEY: str,
    }

    # No DataDefaultValues needed

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        [
            ID_KEY,
            GROUP_NAME_KEY,
            TRACER_NAME_KEY,
            CONC_KEY,
        ],
        [
            NAME_KEY,
            GROUP_NAME_KEY,
            TRACER_NAME_KEY,
            CONC_KEY,
        ],
    ]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        InfusateTracer.__name__: {
            "infusate": NAME_KEY,
            "tracer": TRACER_NAME_KEY,
            "concentration": CONC_KEY,
        },
        Infusate.__name__: {
            "name": NAME_KEY,
            "tracer_group_name": GROUP_NAME_KEY,
        },
    }

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [InfusateTracer, Infusate]

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
        self.tracer_delimiter = kwargs.pop(
            "tracer_delimiter", self.TRACER_DELIMETER
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
        self.infusates_dict = defaultdict(dict)
        self.infusate_name_to_number = defaultdict(lambda: defaultdict(list))
        self.valid_infusates = {}

        self.inconsistent_group_names = defaultdict(lambda: defaultdict(list))
        self.inconsistent_names = defaultdict(lambda: defaultdict(list))
        self.inconsistent_numbers = defaultdict(lambda: defaultdict(list))

    @MaintainedModel.defer_autoupdates()
    def load_data(self):
        """Loads the Infusate and InfusateTracer tables from the dataframe (self.df).

        Args:
            None

        Exceptions:
            None (explicitly)

        Returns:
            Nothing
        """
        # Gather all the data needed for the infusates (an infusate can span multiple rows)
        self.build_infusates_dict()

        # Check the self.infusates_dict to fill in missing values parsed from the name
        self.check_extract_name_data()
        self.buffer_consistency_issues()

        # Now that all the infusate data has been collated and validated, load it
        self.load_infusates_dict()

    def build_infusates_dict(self):
        """Iterate over the row data in self.df to populate self.infusates_dict and self.valid_infusates

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
        if not hasattr(self, "infusates_dict"):
            self.init_load()

        for _, row in self.df.iterrows():
            try:
                # missing required values update the skip_row_indexes before load_data is even called, and get_row_val
                # sets the current row index
                if self.is_skip_row(row.name):
                    self.errored(Infusate.__name__)
                    self.errored(InfusateTracer.__name__)
                    continue

                (
                    infusate_number,
                    tracer_group_name,
                    infusate_name,
                    tracer_name,
                    tracer_concentration,
                ) = self.get_row_data(row)

                if infusate_number is None:
                    self.aggregated_errors_object.buffer_error(
                        InfileError(
                            f"{self.headers.ID} undefined.",
                            rownum=self.rownum,
                            file=self.file,
                            sheet=self.sheet,
                        )
                    )
                    self.errored(Infusate.__name__)
                    self.errored(InfusateTracer.__name__)
                    continue

                if not self.valid_infusates[infusate_number]:
                    continue

                if infusate_number not in self.infusates_dict.keys():
                    # Initialize the tracer dict
                    self.infusates_dict[infusate_number] = {
                        "tracer_group_name": tracer_group_name,
                        "infusate_name": infusate_name,
                        "tracers": [],
                        # Metadata for error reporting
                        "rownum": self.rownum,
                        "row_index": self.row_index,
                    }

                self.infusates_dict[infusate_number]["tracers"].append(
                    {
                        "tracer_name": tracer_name,
                        "tracer_concentration": tracer_concentration,
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
                if infusate_number is not None:
                    self.valid_infusates[infusate_number] = False

    @transaction.atomic
    @MaintainedModel.defer_autoupdates()
    def load_infusates_dict(self):
        """Iterate over the self.infusates_dict to get or create Infusate and InfusateTracer database records

        Args:
            None

        Exceptions:
            None

        Returns:
            None
        """
        for infusate_number, infusate_dict in self.infusates_dict.items():
            # We are iterating a dict independent of the file rows, so set the row index manually
            self.set_row_index(infusate_dict["row_index"])

            if self.is_skip_row():
                # If the row the tracer name was first obtained from is a skip row, skip it
                continue

            num_tracers = len(infusate_dict["tracers"]) if len(infusate_dict["tracers"]) > 0 else 1

            if not self.valid_infusates[infusate_number] or self.is_skip_row():
                # This happens if there was an error in the file processing, like missing required columns, unique
                # column constraint violations, or name parsing issues
                self.errored(Infusate.__name__)
                self.errored(InfusateTracer.__name__, num=num_tracers)
                continue

            try:
                # We want to roll back everything related to the current infusate record in here if there was an
                # exception.  We do that by catching outside the atomic block, below.  Note that this method has an
                # atomic transaction decorator that is just for good measure (because the automatically applied wrapper
                # around load_data will roll back everything if any exception occurs - but the decorator on this method
                # is just in case it's ever called from anywhere other than load_data)
                with transaction.atomic():
                    infusate_rec, infusate_created = self.get_or_create_infusate(infusate_dict)

                    # If the infusate rec is None, skip the synonyms
                    if infusate_rec is None:
                        # Assume the tracer didn't exist and mark as skipped
                        self.skipped(Infusate.__name__)
                        self.skipped(InfusateTracer.__name__, num=num_tracers)
                        continue
                    elif not infusate_created:
                        # Note: infusate_rec has already been checked by check_infusate_name_consistent when it existed
                        self.existed(Infusate.__name__)
                        # Refresh the count with the actual existing records (i.e. in case tracer data wasn't provided)
                        num_tracers = infusate_rec.tracers.count()
                        self.existed(InfusateTracer.__name__, num=num_tracers)
                        continue

                    # Now, get or create the labels
                    for infusate_tracer_dict in self.infusates_dict[infusate_number]["tracers"]:
                        # We are iterating a dict independent of the file rows, so set the row index manually
                        self.set_row_index(infusate_tracer_dict["row_index"])

                        if self.is_skip_row():
                            self.skipped(InfusateTracer.__name__)
                            continue

                        self.get_or_create_infusate_tracer(infusate_tracer_dict, infusate_rec)

                    # Only mark as created after this final check (which raises an exception)
                    self.check_infusate_name_consistent(infusate_rec, infusate_dict)

                    # Refresh the count with the actual existing records (i.e. in case tracer data wasn't provided)
                    num_tracers = infusate_rec.tracers.count()

                    self.created(Infusate.__name__)
                    self.created(InfusateTracer.__name__, num=num_tracers)
            except Exception as e:
                if not self.aggregated_errors_object.exception_type_exists(type(e)):
                    self.aggregated_errors_object.buffer_error(e)
                # All exceptions are buffered in their respective functions, so just update the stats
                self.errored(Infusate.__name__)
                self.errored(InfusateTracer.__name__, num=num_tracers)

    def get_row_data(self, row):
        """Retrieve and validate the row data.

        Updates:
            self.infusate_name_to_number

        Args:
            row (pandas dataframe row)

        Exceptions:
            None

        Returns:
            infusate_number (integer)
            tracer_group_name (string)
            infusate_name (string)
            tracer_name (string)
            tracer_concentration (float)
        """
        infusate_number = self.get_row_val(row, self.headers.ID)
        tracer_group_name = self.get_row_val(row, self.headers.TRACERGROUP)
        infusate_name = self.get_row_val(row, self.headers.NAME)
        tracer_name = self.get_row_val(row, self.headers.TRACERNAME)
        tracer_concentration = self.get_row_val(row, self.headers.TRACERCONC)

        retval = (
            infusate_number,
            tracer_group_name,
            infusate_name,
            tracer_name,
            tracer_concentration,
        )

        if infusate_number not in self.infusates_dict.keys():
            # Check for infusate names that map to multiple different infusate numbers
            if infusate_name not in self.infusate_name_to_number.keys():
                # The normal case: 1 name, 1 number
                self.infusate_name_to_number[tracer_name][infusate_number] = self.rownum
            elif infusate_number not in self.infusate_name_to_number[infusate_name].keys():
                # An inconsistency: 1 name associated with a new number
                existing_num = list(self.infusate_name_to_number[infusate_name].keys())[0]
                self.inconsistent_numbers[tracer_name][existing_num].append(
                    self.infusate_name_to_number[infusate_name][existing_num]
                )
                self.inconsistent_numbers[infusate_name][infusate_number].append(
                    self.rownum
                )
            elif len(self.infusate_name_to_number[infusate_name].keys()) > 1:
                # This is already in an inconsistent state because there are multiple numbers, so append new rows
                self.inconsistent_numbers[infusate_name][infusate_number].append(
                    self.rownum
                )

            # If this is the first time we've seen this number, start out as valid
            if infusate_number not in self.valid_infusates.keys():
                self.valid_infusates[infusate_number] = True
        else:
            # Make sure the infusate name and number are consistent (even though we're not going to use them on this row
            # - we only use the first occurrence of an infusate name, but we should sanity check the rest)
            self.check_data_is_consistent(
                infusate_number,
                tracer_group_name,
                infusate_name,
            )

        return retval

    def check_extract_name_data(self):
        """Fill in missing data in self.infusates_dict using data parsed from the infusate name, and check for
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
        for infusate_number in self.infusates_dict.keys():
            table_infusate = self.infusates_dict[infusate_number]
            table_infusate_name = table_infusate["infusate_name"]

            if table_infusate_name is None:
                continue

            table_concentrations = [tracer["tracer_concentration"] for tracer in table_infusate["tracers"]]

            tracer_group_name = table_infusate["tracer_group_name"]
            parsed_infusate = parse_infusate_name(table_infusate_name, table_concentrations)
            parsed_tracer_group_name = parsed_infusate["infusate_name"]

            if tracer_group_name is None:
                table_infusate["tracer_group_name"] = parsed_tracer_group_name
            elif tracer_group_name != parsed_tracer_group_name:
                self.aggregated_errors_object.buffer_error(
                    InfileError(
                        (
                            f"{self.headers.TRACERGROUP}: [{tracer_group_name}] does not match the "
                            f"{self.headers.TRACERGROUP} parsed from {self.headers.NAME} ({table_infusate_name}): "
                            f"[{parsed_tracer_group_name}] on %s"
                        ),
                        file=self.file,
                        sheet=self.sheet,
                        rownum=table_infusate["rownum"],
                    )
                )

            fill_in_tracer_data = len(table_infusate["tracers"]) == 0
            for table_tracer in table_infusate["tracers"]:
                match = False

                table_tracer_name = table_tracer["tracer_name"]
                if table_tracer_name is None:
                    fill_in_tracer_data = True

                table_tracer_conc = table_tracer["tracer_concentration"]
                if table_tracer_conc is None:
                    fill_in_tracer_data = True

                for parsed_infusate_tracer in parsed_infusate["tracers"]:
                    match = False

                    parsed_tracer_name = parsed_infusate_tracer["tracer"]["unparsed_string"]
                    if parsed_tracer_name == table_tracer_name:
                        match = True
                    elif parsed_tracer_name != table_tracer_name:
                        match = False
                        break

                    parsed_concentration = parsed_infusate_tracer["concentration"]
                    if parsed_concentration == table_tracer_conc:
                        match = True
                    elif parsed_concentration != table_tracer_conc:
                        match = False
                        break

                if match is False:
                    cols = ", ".join(
                        [
                            f"{self.headers.TRACERNAME}: {table_tracer['tracer_name']}",
                            f"{self.headers.TRACERCONC}: {table_tracer['tracer_concentration']}",
                        ]
                    )
                    irows = [tr["rownum"] for tr in table_infusate["tracers"]]
                    self.aggregated_errors_object.buffer_error(
                        InfileError(
                            (
                                f"Tracer data from columns [{cols}] on row(s) {irows} does not match any of the "
                                f"tracers parsed from the {self.headers.NAME} [{table_infusate_name}] on %s."
                            ),
                            file=self.file,
                            sheet=self.sheet,
                            rownum=table_infusate["rownum"],
                        )
                    )

            if len(table_infusate["tracers"]) != len(parsed_infusate["tracers"]):
                fill_in_tracer_data = True
                irows = [ir["rownum"] for ir in table_infusate["tracers"]]
                self.aggregated_errors_object.buffer_warning(
                    InfileError(
                        (
                            f"There are [{len(table_infusate['tracers'])}] rows {irows} of data defining tracers for "
                            f"{self.headers.NAME} [{table_infusate_name}] in %s, but the number of tracers parsed from "
                            f"the {self.headers.NAME} [{len(parsed_infusate['tracers'])}] does not match the number of "
                            f"rows for {self.headers.ID} {infusate_number}.  Perhaps {self.headers.ID} "
                            f"{infusate_number} is on the wrong number of rows?"
                        ),
                        file=self.file,
                        sheet=self.sheet,
                    )
                )

            # If anything was missing, we're going to just recreate the data from the names
            if fill_in_tracer_data is True:
                table_infusate["tracers"] = []
                for parsed_infusate in parsed_infusate["tracers"]:
                    table_infusate["tracers"].append(
                        {
                            "tracer_name": parsed_infusate["tracer"]["unparsed_string"],
                            "tracer_concentration": parsed_infusate["concentration"],
                            # The row info where the tracer name was obtained
                            "rownum": table_infusate["rownum"],
                            "row_index": table_infusate["row_index"],
                        }
                    )

    def get_or_create_infusate(self, infusate_dict):
        """Get or create an Infusate record.

        Args:
            infusate_dict (dict)

        Exceptions:
            None

        Returns:
            rec (Tracer)
            created (boolean)
        """
        created = False
        rec = None

        # See if we can retrieve an existing record
        rec = self.get_infusate(infusate_dict)

        if rec is not None:
            self.check_infusate_name_consistent(rec, infusate_dict)
            return rec, created

        rec = self.create_infusate(infusate_dict)
        created = True

        return rec, created

    def get_infusate(self, infusate_dict):
        """Get an Infusate record.

        Args:
            infusate_dict (dict)

        Exceptions:
            Raises:
                Nothing (explicitly)
            Buffers:
                InfileError (repackages other exceptions)

        Returns:
            rec (Optional[Infusate])
        """
        rec = None

        # See if we can retrieve an existing record
        try:
            # First, we will try to see if we can retrieve the precise tracer, using a TracerData object
            tracer_data = InfusateData(
                unparsed_string=infusate_dict["infusate_name"],
                infusate_name=infusate_dict["tracer_group_name"],
                tracers=[
                    InfusateTracerData(
                        tracer=parse_tracer_string(ido["tracer_name"]),
                        concentration=ido["tracer_concentration"],
                    )
                    for ido in infusate_dict["tracers"]
                ],
            )

            rec = Infusate.objects.get_infusate(tracer_data)

        except Exception as e:
            exc = InfileError(
                str(e), rownum=self.rownum, sheet=self.sheet, file=self.file
            )
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.aggregated_errors_object.buffer_error(exc)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise e

        return rec

    @transaction.atomic
    def create_infusate(self, infusate_dict):
        """Creates an Infusate record.

        Args:
            infusate_dict (dict)

        Exceptions:
            Raises:
                Nothing (explicitly)
            Buffers:
                Nothing (explicitly)

        Returns:
            rec (Tracer)
        """
        rec = None
        rec_dict = {"tracer_group_name": infusate_dict["tracer_group_name"]}

        try:
            rec = Infusate.objects.create(**rec_dict)
        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Infusate, rec_dict)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise e

        return rec

    def get_or_create_infusate_tracer(self, tracer_dict, infusate_rec):
        """Get or create an InfusateTracer record.

        Args:
            tracer_dict (dict)
            infusate_rec (Infusate)

        Raises:
            Nothing (explicitly)

        Returns:
            rec (Optional[InfusateTracer])
            created (boolean)
        """
        rec = None
        rec_dict = None
        created = False

        try:
            tracer_name = tracer_dict["tracer_name"]
            try:
                tracer_rec = Tracer.objects.get(name=tracer_name)
            except ObjectDoesNotExist:
                self.aggregated_errors_object.buffer_error(
                    RecordDoesNotExist(
                        model=Tracer,
                        name=tracer_name,
                        rownum=tracer_dict["rownum"],
                        column=self.headers.TRACERNAME,
                        file=self.file,
                        sheet=self.sheet,
                    )
                )
                return rec, created

            tracer_concentration = tracer_dict["tracer_concentration"]

            rec_dict = {
                "infusate": infusate_rec,
                "tracer": tracer_rec,
                "concentration": tracer_concentration,
            }

            rec, created = InfusateTracer.objects.get_or_create(**rec_dict)

            if created:
                rec.full_clean()

        except Exception as e:
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, InfusateTracer, rec_dict)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise e

        return rec, created

    def check_data_is_consistent(
        self,
        infusate_number,
        tracer_group_name,
        infusate_name,
    ):
        """Ensures that each infusate number is associated with the same tracer_group_name and infusate name.

        Accesses:
            self.infusates_dict
            self.infusate_name_to_number

        Adds inconsistencies to:
            self.inconsistent_group_names
            self.inconsistent_names
            self.inconsistent_numbers

        Args:
            infusate_number (integer)
            tracer_group_name (string)
            infusate_name (string)

        Exceptions:
            None

        Returns:
            None
        """
        # Make sure that each infusate number is always associated with the same tracer group name
        if self.infusates_dict[infusate_number]["tracer_group_name"] != tracer_group_name:
            if infusate_number not in self.inconsistent_group_names.keys():
                self.inconsistent_group_names[infusate_number][
                    self.infusates_dict[infusate_number]["tracer_group_name"]
                ] = [self.infusates_dict[infusate_number]["rownum"]]
            self.inconsistent_group_names[infusate_number][tracer_group_name].append(
                self.rownum
            )

        # Make sure that each infusate number is always associated with the same infusate name
        if self.infusates_dict[infusate_number]["infusate_name"] != infusate_name:
            if infusate_number not in self.inconsistent_names.keys():
                self.inconsistent_names[infusate_number][
                    self.infusates_dict[infusate_number]["infusate_name"]
                ] = [self.infusates_dict[infusate_number]["rownum"]]
            self.inconsistent_names[infusate_number][infusate_name].append(self.rownum)

        if (
            infusate_name in self.infusate_name_to_number.keys()
            and infusate_number not in self.infusate_name_to_number[infusate_name].keys()
        ):
            self.inconsistent_numbers[infusate_name][infusate_number].append(self.rownum)

    def buffer_consistency_issues(self):
        """Buffers consistency errors.

        - When an infusate number is associated with multiple tracer group names
        - When an infusate number is associated with multiple infusate names
        - When an infusate name is associated with multiple infusate numbers

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
        for infusate_number in self.inconsistent_group_names.keys():
            msg = (
                f"%s:\n\t'{self.headers.ID}' {infusate_number} is associated with multiple "
                f"'{self.headers.TRACERGROUP}'s on the indicated rows.  Only one '{self.headers.TRACERGROUP}' is "
                f"allowed per '{self.headers.ID}'.\n\t\t"
            )
            msg += "\n\t\t".join(
                [
                    f"{g} (on rows: {self.inconsistent_group_names[infusate_number][g]})"
                    for g in self.inconsistent_group_names[infusate_number].keys()
                ]
            )
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    msg,
                    column=f"{self.headers.ID} and {self.headers.TRACERGROUP}",
                    file=self.file,
                    sheet=self.sheet,
                )
            )
            for gk in self.inconsistent_group_names[infusate_number].keys():
                self.add_skip_row_index(
                    index_list=self.inconsistent_group_names[infusate_number][gk]
                )

        for infusate_number in self.inconsistent_names.keys():
            msg = (
                f"%s:\n\t'{self.headers.ID}' {infusate_number} is associated with multiple '{self.headers.NAME}'s on "
                f"the indicated rows.  Only one '{self.headers.NAME}' is allowed per '{self.headers.ID}'.\n\t\t"
            )
            msg += "\n\t\t".join(
                [
                    f"{n} (on rows: {self.inconsistent_names[infusate_number][n]})"
                    for n in self.inconsistent_names[infusate_number].keys()
                ]
            )
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    msg,
                    column=f"{self.headers.NAME} and {self.headers.ID}",
                    file=self.file,
                    sheet=self.sheet,
                )
            )
            for nk in self.inconsistent_names[infusate_number].keys():
                self.add_skip_row_index(
                    index_list=self.inconsistent_names[infusate_number][nk]
                )

        for infusate_name in self.inconsistent_numbers.keys():
            msg = (
                f"%s:\n\t'{self.headers.NAME}' {infusate_name} is associated with multiple '{self.headers.ID}'s on the "
                f"indicated rows.  Only one '{self.headers.ID}' is allowed per '{self.headers.NAME}'.\n\t\t"
            )
            msg += "\n\t\t".join(
                [
                    f"{n} (on rows: {self.inconsistent_numbers[infusate_name][n]})"
                    for n in self.inconsistent_numbers[infusate_name].keys()
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
            for nk in self.inconsistent_numbers[infusate_name].keys():
                self.add_skip_row_index(
                    index_list=self.inconsistent_numbers[infusate_name][nk]
                )

    def check_infusate_name_consistent(self, rec, infusate_dict):
        """Checks for consistency between a dynamically generated infusate name and the one supplied in the file.

        Args:
            rec (Infusate): An Infusate model object
            infusate_dict (dict): Data parsed from potentially multiple rows relating to a single Infusate

        Exceptions:
            Raises:
                InfileError
            Buffers:
                InfileError

        Returns:
            None
        """
        supplied_name = infusate_dict["infusate_name"]

        if supplied_name is None:
            # Nothing to check
            return

        # We have to generate it instead of simply access it in the model object, because this is a maintained field,
        # and if the record was created, auto-update will not happen until the load is complete
        generated_name = rec._name()

        if supplied_name != generated_name:
            data_rownums = summarize_int_list(
                [rd["rownum"] for rd in infusate_dict["tracers"]]
            )
            exc = InfileError(
                (
                    f"The supplied infusate name [{supplied_name}] from row %s does not match the automatically "
                    f"generated name [{generated_name}] using the data on rows {data_rownums}."
                ),
                file=self.file,
                sheet=self.sheet,
                column=self.headers.NAME,
                rownum=infusate_dict["rownum"],
            )
            self.aggregated_errors_object.buffer_error(exc)
            raise exc

        trownums = [te["rownum"] for te in infusate_dict["tracers"]]
        tnames = [te["tracer_name"] for te in infusate_dict["tracers"]]
        tconcs = [f"{te['tracer_name']}: {te['tracer_concentration']}" for te in infusate_dict["tracers"]]
        bad_tracer_names = []
        bad_concentrations = []
        err_msgs = []

        for it_rec in rec.tracers.all():
            db_conc = it_rec.concentration
            db_tracer_name = it_rec.tracer.name

            file_conc = None
            for te in infusate_dict["tracers"]:
                if te["tracer_name"] == db_tracer_name:
                    file_conc = te["tracer_concentration"]

            if file_conc is None:
                bad_tracer_names.append(db_tracer_name)
            elif not math.isclose(file_conc, db_conc):
                bad_concentrations.append(f"{db_tracer_name}: {db_conc}")

        if len(bad_tracer_names) > 0:
            err_msgs.append(
                f"Unable to find the created '{self.headers.TRACERNAME}'(s): [{bad_tracer_names}] among the tracer "
                f"names: {tnames} obtained from rows: {trownums}."
            )
        
        if len(bad_concentrations) > 0:
            err_msgs.append(
                f"Unable to match the created '{self.headers.TRACERCONC}'(s): [{bad_concentrations}] to the "
                f"tracer concentrations: {tconcs} obtained from rows: {trownums}."
            )

        if len(err_msgs) > 0:
            err_msg = "%s:\n\t" + "\n\t".join(err_msgs)
            exc = InfileError(
                err_msg,
                file=self.file,
                sheet=self.sheet,
            )
            self.aggregated_errors_object.buffer_error(exc)
            raise exc
