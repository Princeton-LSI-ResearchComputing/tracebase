import math
from collections import defaultdict, namedtuple
from typing import Dict

from django.db import transaction

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.tracers_loader import TracersLoader
from DataRepo.models import Infusate, InfusateTracer, MaintainedModel, Tracer
from DataRepo.utils.exceptions import (
    InfileError,
    TracerParsingError,
    summarize_int_list,
)
from DataRepo.utils.infusate_name_parser import (
    InfusateData,
    InfusateParsingError,
    InfusateTracerData,
    parse_infusate_name,
    parse_infusate_name_with_concs,
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
        ID="Infusate Row Group",
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
    DataRequiredValues = [
        [
            # Either individual column data...
            [
                ID_KEY,
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
            TRACER_NAME_KEY,
            CONC_KEY,
            NAME_KEY,
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

    # No FieldToDataValueConverter needed

    DataColumnMetadata = DataTableHeaders(
        ID=TableColumn.init_flat(
            name=DataHeaders.ID,
            help_text=(
                "Arbitrary number that identifies every row containing a tracer that belongs to a single infusate.  "
                f"Each row defines 1 tracer (at a particular {DataHeaders.TRACERCONC}) and this value links them "
                "together."
            ),
            guidance=(
                "The values in this column are not loaded into the database.  It is only used to populate the "
                f"{DataHeaders.NAME} column using an excel formula.  All rows having the same {DataHeaders.ID} are "
                f"used to build the {DataHeaders.NAME} column values."
            ),
            type=int,
        ),
        TRACERGROUP=TableColumn.init_flat(
            # TODO: Make the name (and the DataHeaders) automatically populated using a title-case version of a model
            # field's verbose_name.  Also make the type default map as well.
            name=DataHeaders.TRACERGROUP,
            field=Infusate.tracer_group_name,
            type=str,
            current_choices=True,
        ),
        TRACERNAME=TableColumn.init_flat(
            name=DataHeaders.TRACERNAME,
            field=InfusateTracer.tracer,
            # TODO: Add help text to the field in the Tracer model
            help_text=f"Name of a tracer in this infusate at a specific {DataHeaders.TRACERCONC}.",
            guidance=(
                f"Select a {DataHeaders.TRACERNAME} from the dropdowns in this column.  Those dropdowns are populated "
                f"by the {TracersLoader.DataHeaders.NAME} column in the {TracersLoader.DataSheetName} sheet.  "
                f"All of the {DataHeaders.TRACERNAME}s in an infusate with multiple {DataHeaders.TRACERNAME}s are "
                f"defined on separate rows and associated via the values in the {DataHeaders.ID} column."
            ),
            type=str,
            dynamic_choices=ColumnReference(
                loader_class=TracersLoader,
                loader_header_key=TracersLoader.NAME_KEY,
            ),
        ),
        TRACERCONC=TableColumn.init_flat(
            name=DataHeaders.TRACERCONC,
            field=InfusateTracer.concentration,
            type=float,
        ),
        NAME=TableColumn.init_flat(
            name=DataHeaders.NAME,
            field=Infusate.name,
            type=str,
            # TODO: Replace "Infusate" and "Animals" below with a reference to its loader's column and DataSheetName
            guidance=(
                "This column is automatically filled in using an excel formula and its values are used to populate "
                "Infusate choices in the Animals sheet."
            ),
            # TODO: Create the method that applies the formula to the NAME column on every row
            # Excel formula that creates the name using the spreadsheet columns on the rows containing the ID on the
            # current row.  The header keys will be replaced by the excel column letters.
            # Simplified example:
            # =CONCATENATE(
            #   IF(ISBLANK(B2),"",CONCATENATE(B2," ")),
            #   IF(ROWS(FILTER(A:A,A:A=A2,""))>1,"{",""),
            #   TEXTJOIN(";",TRUE,
            #     SORT(
            #       MAP(
            #         FILTER(C:C,A:A=A2,""),
            #         FILTER(D:D,A:A=A2,""),
            #         LAMBDA(a,b,CONCATENATE(a,"[",b,"]"))
            #       )
            #     )
            #   ),
            #   IF(ROWS(FILTER(A:A,A:A=A2,""))>1,"}","")
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
                # If there's any data on the row
                "=IF("
                # "OR" makes excel significantly more responsive
                "OR("
                f'NOT(ISBLANK(INDIRECT("{{{ID_KEY}}}" & ROW()))),'
                f'NOT(ISBLANK(INDIRECT("{{{GROUP_NAME_KEY}}}" & ROW()))),'
                f'NOT(ISBLANK(INDIRECT("{{{TRACER_NAME_KEY}}}" & ROW()))),'
                f'NOT(ISBLANK(INDIRECT("{{{CONC_KEY}}}" & ROW()))),'
                "),"
                # Build the infusate name
                "CONCATENATE("
                # If there is a tracer group name
                f'IF(ISBLANK(INDIRECT("{{{GROUP_NAME_KEY}}}" & ROW())),"",'
                # Insert the tracer group name
                f'CONCATENATE(INDIRECT("{{{GROUP_NAME_KEY}}}" & ROW())," ")),'
                # If there is more than 1 tracer with this row group ID, include an opening curly brace
                f"IF(ROWS(_xlfn._xlws.FILTER({{{ID_KEY}}}:{{{ID_KEY}}},{{{ID_KEY}}}:{{{ID_KEY}}}="
                f'INDIRECT("{{{ID_KEY}}}" & ROW()),""))>1,"{{{{",""),'
                # Join the sorted tracers (from multiple rows) using a ';' delimiter, (mapping the names and concs)
                '_xlfn.TEXTJOIN(";",TRUE,_xlfn._xlws.SORT(_xlfn.MAP('
                # Filter all tracer names to get ones whose tracer row group ID is the same as as this row's group ID
                f"_xlfn._xlws.FILTER({{{TRACER_NAME_KEY}}}:{{{TRACER_NAME_KEY}}},"
                f'{{{ID_KEY}}}:{{{ID_KEY}}}=INDIRECT("{{{ID_KEY}}}" & ROW()),""),'
                # Filter all concentrations to get ones whose tracer row group ID is the same as as this row's group ID
                f"_xlfn._xlws.FILTER({{{CONC_KEY}}}:{{{CONC_KEY}}},"
                f'{{{ID_KEY}}}:{{{ID_KEY}}}=INDIRECT("{{{ID_KEY}}}" & ROW()), ""),'
                # Apply this lambda to the tracer names and concentrations filtered above to concatenate the tracers and
                # their concentrations
                '_xlfn.LAMBDA(_xlpm.a,_xlpm.b,CONCATENATE(_xlpm.a,"[",_xlpm.b,"]"))))),'
                # If there is more than 1 tracer for this row group ID, include a closing curly brace
                f"IF(ROWS(_xlfn._xlws.FILTER({{{ID_KEY}}}:{{{ID_KEY}}},{{{ID_KEY}}}:{{{ID_KEY}}}="
                f'INDIRECT("{{{ID_KEY}}}" & ROW()),""))>1,"}}}}","")),'
                '"")'
            ),
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [InfusateTracer, Infusate]

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
                synonym_separator (Optional[str]) [;]: Synonym string delimiter.
        Exceptions:
            None
        Returns:
            None
        """
        self.tracer_delimiter = kwargs.pop("tracer_delimiter", self.TRACER_DELIMETER)
        super().__init__(*args, **kwargs)

    def init_load(self):
        """Initializes load-related metadata.  Called before any load occurs (in load_data()).

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        self.infusates_dict = defaultdict(dict)
        self.infusate_name_to_number = defaultdict(lambda: defaultdict(list))
        self.valid_infusates = {}

        self.inconsistent_tracer_groups = {
            # Tracer groups with multiple differing tracer group names
            "mult_names": defaultdict(lambda: defaultdict(list)),
            # Tracer groups with the same concentrations (duplicates, regardless of group name)
            "dupes": defaultdict(lambda: defaultdict(list)),
        }
        self.inconsistent_group_names = {
            # Infusate numbers with multiple differing tracer group names
            "mult_names": defaultdict(lambda: defaultdict(list)),
            # Tracer group names with multiple infusate numbers (containing a different assortment of tracers)
            "mult_nums": defaultdict(lambda: defaultdict(list)),
        }
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
            None
        """
        # Gather all the data needed for the infusates (an infusate can span multiple rows)
        self.build_infusates_dict()

        # Check the self.infusates_dict to fill in missing values parsed from the name
        self.check_extract_name_data()
        self.check_tracer_group_names()
        self.buffer_consistency_issues()

        # Now that all the infusate data has been collated and validated, load it
        self.load_infusates_dict()

    def build_infusates_dict(self):
        """Iterate over the row data in self.df to populate self.infusates_dict and self.valid_infusates

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
                            file=self.friendly_file,
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
                    str(e),
                    rownum=self.rownum,
                    sheet=self.sheet,
                    file=self.friendly_file,
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

            num_tracers = (
                len(infusate_dict["tracers"])
                if len(infusate_dict["tracers"]) > 0
                else 1
            )

            if not self.valid_infusates[infusate_number] or self.is_skip_row():
                # This happens if there was an error in the file processing, like missing required columns, unique
                # column constraint violations, or name parsing issues
                self.errored(Infusate.__name__)
                self.errored(InfusateTracer.__name__, num=num_tracers)
                continue

            validation_handled = False
            try:
                # We want to roll back everything related to the current infusate record in here if there was an
                # exception.  We do that by catching outside the atomic block, below.  Note that this method has an
                # atomic transaction decorator that is just for good measure (because the automatically applied wrapper
                # around load_data will roll back everything if any exception occurs - but the decorator on this method
                # is just in case it's ever called from anywhere other than load_data)
                with transaction.atomic():
                    infusate_rec, infusate_created = self.get_or_create_just_infusate(
                        infusate_dict
                    )

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
                    for infusate_tracer_dict in self.infusates_dict[infusate_number][
                        "tracers"
                    ]:
                        # We are iterating a dict independent of the file rows, so set the row index manually
                        self.set_row_index(infusate_tracer_dict["row_index"])

                        if self.is_skip_row():
                            self.skipped(InfusateTracer.__name__)
                            continue

                        self.get_or_create_infusate_tracer(
                            infusate_tracer_dict, infusate_rec
                        )

                    if infusate_created:
                        try:
                            # Clean after the InfusateTracer records have been added, so that the assortment of tracers
                            # can be validated
                            infusate_rec.full_clean()
                        except Exception as e:
                            # Package errors (like IntegrityError and ValidationError) with relevant details
                            # This also updates the skip row indexes
                            self.handle_load_db_errors(e, Infusate, infusate_dict)
                            validation_handled = True
                            # Now that the exception has been handled, trigger a rollback of this record load attempt
                            raise e

                    # Only mark as created after this final check (which raises an exception)
                    self.check_infusate_name_consistent(infusate_rec, infusate_dict)

                    # Refresh the count with the actual existing records (i.e. in case tracer data wasn't provided)
                    num_tracers = infusate_rec.tracers.count()

                    self.created(Infusate.__name__)
                    self.created(InfusateTracer.__name__, num=num_tracers)
            except Exception as e:
                if (
                    not validation_handled
                    and not self.aggregated_errors_object.exception_type_exists(type(e))
                ):
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
            elif (
                infusate_number
                not in self.infusate_name_to_number[infusate_name].keys()
            ):
                # An inconsistency: 1 name associated with a new number
                existing_num = list(self.infusate_name_to_number[infusate_name].keys())[
                    0
                ]
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
                None
            Buffers:
                InfileError
        Returns:
            None
        """
        for infusate_number in self.infusates_dict.keys():
            table_infusate = self.infusates_dict[infusate_number]
            table_infusate_name = table_infusate["infusate_name"]

            if table_infusate_name is None:
                continue

            table_concentrations = [
                tracer["tracer_concentration"] for tracer in table_infusate["tracers"]
            ]

            tracer_group_name = table_infusate["tracer_group_name"]

            try:
                parsed_infusate = parse_infusate_name(
                    table_infusate_name, table_concentrations
                )
            except TracerParsingError:
                parsed_infusate = parse_infusate_name_with_concs(table_infusate_name)
            except InfusateParsingError as ipe:
                rownums = [t["rownum"] for t in table_infusate["tracers"]]
                self.aggregated_errors_object.buffer_error(
                    InfileError(
                        (
                            f"'{type(ipe).__name__}' encountered while parsing {self.headers.NAME}: "
                            f"[{table_infusate_name}] on row {table_infusate['rownum']}, associated with "
                            f"{self.headers.TRACERCONC}(s): {table_concentrations} on row(s) {rownums} via "
                            f"{self.headers.ID} {infusate_number} in %s: {ipe}"
                        ),
                        file=self.friendly_file,
                        sheet=self.sheet,
                    )
                )
                self.add_skip_row_index(
                    index_list=[t["row_index"] for t in table_infusate["tracers"]]
                )
                continue

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
                        file=self.friendly_file,
                        sheet=self.sheet,
                        rownum=table_infusate["rownum"],
                    )
                )

            fill_in_tracer_data = len(table_infusate["tracers"]) == 0

            # If we're not already filling in data
            if fill_in_tracer_data is False:
                # If any Tracer name is missing
                all_match = True
                for table_tracer in table_infusate["tracers"]:
                    table_tracer_name = table_tracer["tracer_name"]
                    if table_tracer_name is None:
                        fill_in_tracer_data = True

                    table_tracer_conc = table_tracer["tracer_concentration"]

                    # Make sure that the concentrations associated with the tracers in the names have a match
                    match = False
                    for parsed_infusate_tracer in parsed_infusate["tracers"]:
                        parsed_tracer_name = parsed_infusate_tracer["tracer"][
                            "unparsed_string"
                        ]
                        parsed_concentration = parsed_infusate_tracer["concentration"]

                        if math.isclose(parsed_concentration, table_tracer_conc) and (
                            parsed_tracer_name == table_tracer_name
                            or table_tracer_name is None
                        ):
                            match = True
                            break

                    if match is False:
                        all_match = False
                        break

                # If at least 1 tracer did not match, report the first non-match at the point where the loop above was
                # broken
                if all_match is False:
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
                            file=self.friendly_file,
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
                        file=self.friendly_file,
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

            # After filling in tracer names

    def check_tracer_group_names(self):
        """Check the assortment of tracers for issues.

        - Identify any tracer groups with different tracer group names ("mult_names")
        - Identify any tracer groups (with concentrations) that are duplicate occurrences ("dupes")
        - Identify any tracer group names associated with different tracer groups

        Updates self.inconsistent_tracer_groups

        Agrs:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # NOTE: This method assumes that the isotopes are consistently ordered in the tracer names

        tracer_group_dict = defaultdict(lambda: defaultdict(list))
        tracer_group_conc_dict = defaultdict(list)
        group_name_dict = defaultdict(lambda: defaultdict(list))
        for infusate_number in self.infusates_dict.keys():
            # Tracer group name to use as dict key (str-caste to handle Nones)
            tgn = str(self.infusates_dict[infusate_number]["tracer_group_name"])
            # Tracer group string to use as dict key
            tracer_group_key = ";".join(
                sorted(
                    [
                        str(t["tracer_name"])
                        for t in self.infusates_dict[infusate_number]["tracers"]
                    ]
                )
            )
            # Tracer group string including concentrations to use as dict key
            tracer_group_conc_key = ";".join(
                sorted(
                    [
                        f"{t['tracer_name']}[{t['tracer_concentration']}]"
                        for t in self.infusates_dict[infusate_number]["tracers"]
                    ]
                )
            )

            # Identify groups of tracers with different tracer group names
            tracer_group_dict[tracer_group_key][tgn].append(infusate_number)
            if len(tracer_group_dict[tracer_group_key].keys()) > 1:
                self.inconsistent_tracer_groups["mult_names"][tracer_group_key] = (
                    tracer_group_dict[tracer_group_key]
                )

            # Identify duplicate occurrences of groups of tracers with the same concentrations
            tracer_group_conc_dict[tracer_group_conc_key].append(infusate_number)
            if len(tracer_group_conc_dict[tracer_group_conc_key]) > 1:
                self.inconsistent_tracer_groups["dupes"][tracer_group_conc_key] = (
                    tracer_group_conc_dict[tracer_group_conc_key]
                )

            # Identify tracer group names associated with different groups of tracers
            group_name_dict[tgn][tracer_group_key].append(infusate_number)
            if len(group_name_dict[tgn].keys()) > 1:
                self.inconsistent_group_names["mult_nums"][tgn] = group_name_dict[tgn]

    def get_or_create_just_infusate(self, infusate_dict):
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
                None
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
                f"{type(e).__name__}: {e}",
                rownum=self.rownum,
                sheet=self.sheet,
                file=self.friendly_file,
            )
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.aggregated_errors_object.buffer_error(exc)
            # Now that the exception has been handled, trigger a rollback of this record load attempt
            raise e

        return rec

    @transaction.atomic
    def create_infusate(self, infusate_dict):
        """Creates an Infusate record.  Note, it does not create associated InfusateTracer, Tracer, or TracerLabel
        records.

        Args:
            infusate_dict (dict)
        Exceptions:
            Raises:
                None
            Buffers:
                None
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

    # The overridden Infusate.objects.get_or_create has an atomic transaction decorator
    def get_or_create_infusate_tracer(self, tracer_dict, infusate_rec):
        """Get or create an InfusateTracer record.

        Args:
            tracer_dict (dict)
            infusate_rec (Infusate)
        Exceptions:
            None
        Returns:
            rec (Optional[InfusateTracer])
            created (boolean)
        """
        rec = None
        rec_dict = None
        created = False

        try:
            tracer_name = tracer_dict["tracer_name"]
            tracer_data = parse_tracer_string(tracer_name)
            tracer_rec = Tracer.objects.get_tracer(tracer_data)
        except Exception as e:
            # This is the "effective" query
            query_dict = {"name": tracer_name}
            # Package errors (like IntegrityError and ValidationError) with relevant details
            # This also updates the skip row indexes
            self.handle_load_db_errors(e, Tracer, query_dict)
            return rec, created

        try:
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
        if (
            self.infusates_dict[infusate_number]["tracer_group_name"]
            != tracer_group_name
        ):
            if (
                infusate_number
                not in self.inconsistent_group_names["mult_names"].keys()
            ):
                self.inconsistent_group_names["mult_names"][infusate_number][
                    self.infusates_dict[infusate_number]["tracer_group_name"]
                ] = [self.infusates_dict[infusate_number]["rownum"]]
            self.inconsistent_group_names["mult_names"][infusate_number][
                tracer_group_name
            ].append(self.rownum)

        # Make sure that each infusate number is always associated with the same infusate name
        if self.infusates_dict[infusate_number]["infusate_name"] != infusate_name:
            if infusate_number not in self.inconsistent_names.keys():
                self.inconsistent_names[infusate_number][
                    self.infusates_dict[infusate_number]["infusate_name"]
                ] = [self.infusates_dict[infusate_number]["rownum"]]
            self.inconsistent_names[infusate_number][infusate_name].append(self.rownum)

        if (
            infusate_name in self.infusate_name_to_number.keys()
            and infusate_number
            not in self.infusate_name_to_number[infusate_name].keys()
        ):
            self.inconsistent_numbers[infusate_name][infusate_number].append(
                self.rownum
            )

    def buffer_consistency_issues(self):
        """Buffers consistency errors.

        - When an infusate number is associated with multiple tracer group names
        - When a tracer group name is associated with multiple tracer nums
        - When an infusate number is associated with multiple infusate names
        - When an infusate name is associated with multiple infusate numbers
        - When a group of tracers is associated with multiple tracer group names

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
        for infusate_number in self.inconsistent_group_names["mult_names"].keys():
            msg = (
                f"%s:\n\t'{self.headers.ID}' {infusate_number} is associated with multiple "
                f"'{self.headers.TRACERGROUP}'s on the indicated rows.  Only one '{self.headers.TRACERGROUP}' is "
                f"allowed per '{self.headers.ID}'.\n\t\t"
            )
            msg += "\n\t\t".join(
                [
                    f"{g} (on rows: {self.inconsistent_group_names['mult_names'][infusate_number][g]})"
                    for g in self.inconsistent_group_names["mult_names"][
                        infusate_number
                    ].keys()
                ]
            )
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    msg,
                    column=f"{self.headers.ID} and {self.headers.TRACERGROUP}",
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )
            for gk in self.inconsistent_group_names["mult_names"][
                infusate_number
            ].keys():
                self.add_skip_row_index(
                    index_list=self.inconsistent_group_names["mult_names"][
                        infusate_number
                    ][gk]
                )

        for tracer_group_name in self.inconsistent_group_names["mult_nums"].keys():
            msg = (
                f"%s:\n\t{self.headers.TRACERGROUP}: '{tracer_group_name}' was assigned to infusates containing the "
                f"following different assortments of tracers, for the indicated '{self.headers.ID}'s:\n\t\t"
            )
            msg += "\n\t\t".join(
                [
                    (
                        f"{tgk} (on rows with '{self.headers.ID}'s: "
                        f"{self.inconsistent_group_names['mult_nums'][tracer_group_name][tgk]})"
                    )
                    for tgk in self.inconsistent_group_names["mult_nums"][
                        tracer_group_name
                    ].keys()
                ]
            )
            msg += (
                f"\n\tSuggested resolution: Use a different group name for each '{self.headers.ID}' containing "
                "different tracers."
            )
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    msg,
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )
            for tgk in self.inconsistent_group_names["mult_nums"][
                tracer_group_name
            ].keys():
                for infusate_number in self.inconsistent_group_names["mult_nums"][
                    tracer_group_name
                ][tgk]:
                    row_indexes = [
                        t["row_index"]
                        for t in self.infusates_dict[infusate_number]["tracers"]
                    ]
                    self.add_skip_row_index(index_list=row_indexes)

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
                    file=self.friendly_file,
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
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )
            for nk in self.inconsistent_numbers[infusate_name].keys():
                self.add_skip_row_index(
                    index_list=self.inconsistent_numbers[infusate_name][nk]
                )

        for tracer_group_key in self.inconsistent_tracer_groups["mult_names"].keys():
            msg = (
                f"%s:\n\tThe following differing '{self.headers.TRACERGROUP}'s are for infusates containing the same "
                f"assortment of tracers [{tracer_group_key}], with the indicated '{self.headers.ID}'s:\n\t"
            )
            msg += "\n\t".join(
                [
                    (
                        f"{tgn} (on rows with '{self.headers.ID}'s: "
                        f"{self.inconsistent_tracer_groups['mult_names'][tracer_group_key][tgn]})"
                    )
                    for tgn in self.inconsistent_tracer_groups["mult_names"][
                        tracer_group_key
                    ].keys()
                ]
            )
            msg += f"\nPlease use the same group name for each '{self.headers.ID}' containing the same tracers."
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    msg,
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )
            for tgn in self.inconsistent_tracer_groups["mult_names"][
                tracer_group_key
            ].keys():
                for infusate_number in self.inconsistent_tracer_groups["mult_names"][
                    tracer_group_key
                ][tgn]:
                    row_indexes = [
                        t["row_index"]
                        for t in self.infusates_dict[infusate_number]["tracers"]
                    ]
                    self.add_skip_row_index(index_list=row_indexes)

        for tracer_group_key in self.inconsistent_tracer_groups["dupes"].keys():
            msg = (
                "%s:\n\tThe following tracer group (with concentrations) represent duplicate infusates, indicated by "
                f"their list of '{self.headers.ID}'s:\n\t"
            )
            msg += "\n\t".join(
                [
                    (
                        f"{tracer_group_key} (on rows with '{self.headers.ID}'s: "
                        f"{self.inconsistent_tracer_groups['dupes'][tracer_group_key]})"
                    )
                ]
            )
            msg += "\nPlease remove the duplicates."
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    msg,
                    file=self.friendly_file,
                    sheet=self.sheet,
                )
            )

    def check_infusate_name_consistent(self, rec, infusate_dict):
        """Checks for consistency between a dynamically generated infusate name and the one supplied in the file.

        Note the following:

        - The database generated name includes concentrations, while the input name does not.
        - The database generated name orders the tracers and labels, but the input name is not required to be ordered.
        - The precision of the concentrations (saved as floats) are not reliably comparable.
        - The input name allows a variable number of spaces between the tracer group name, but the generated DB name
            always has 1 space.

        This method serves to overcome these obstacles to checking infusate name consistency.

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
        supplied_concentrations = [
            tracer["tracer_concentration"] for tracer in infusate_dict["tracers"]
        ]

        if supplied_name is None:
            # Nothing to check
            return

        rowidxs = [rd["row_index"] for rd in infusate_dict["tracers"]]
        rowidxs.append(infusate_dict["row_index"])

        if not rec.infusate_name_equal(supplied_name, supplied_concentrations):
            data_rownums = summarize_int_list(
                [rd["rownum"] for rd in infusate_dict["tracers"]]
            )
            exc = InfileError(
                (
                    f"The supplied {self.headers.NAME} [{supplied_name}] and tracer concentrations "
                    f"{supplied_concentrations} from %s do not match the automatically generated name (shown with "
                    f"concentrations) [{rec._name()}] using the data on rows {data_rownums}."
                ),
                file=self.friendly_file,
                sheet=self.sheet,
                rownum=infusate_dict["rownum"],
            )
            self.add_skip_row_index(index_list=rowidxs)
            self.aggregated_errors_object.buffer_error(exc)
            raise exc

        trownums = [te["rownum"] for te in infusate_dict["tracers"]]
        tnames = [te["tracer_name"] for te in infusate_dict["tracers"]]
        tconcs = [
            f"{te['tracer_name']}: {te['tracer_concentration']}"
            for te in infusate_dict["tracers"]
        ]
        bad_tracer_names = []
        bad_concentrations = []
        err_msgs = []

        for it_rec in rec.tracer_links.all():
            db_conc = it_rec.concentration
            db_tracer_name = it_rec.tracer._name()

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
                f"Unable to find the created '{self.headers.TRACERNAME}'(s): {bad_tracer_names} among the tracer "
                f"names: {tnames} obtained from rows: {trownums}."
            )
        if len(bad_concentrations) > 0:
            err_msgs.append(
                f"Unable to match the created '{self.headers.TRACERCONC}'(s): {bad_concentrations} to the "
                f"tracer concentrations: {tconcs} obtained from rows: {trownums}."
            )

        if len(err_msgs) > 0:
            err_msg = "%s:\n\t" + "\n\t".join(err_msgs)
            exc = InfileError(
                err_msg,
                file=self.friendly_file,
                sheet=self.sheet,
            )
            self.add_skip_row_index(index_list=rowidxs)
            self.aggregated_errors_object.buffer_error(exc)
            raise exc
