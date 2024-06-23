from __future__ import annotations

import os
import traceback
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional

from django.core.exceptions import (
    MultipleObjectsReturned,
    ObjectDoesNotExist,
    ValidationError,
)
from django.core.management import CommandError
from django.db.models import Q
from django.db.utils import ProgrammingError
from django.forms.models import model_to_dict

from DataRepo.models.animal import Animal
from DataRepo.models.researcher import get_researchers

if TYPE_CHECKING:
    from DataRepo.models.archive_file import ArchiveFile
    from DataRepo.models.msrun_sample import MSRunSample
    from DataRepo.models.sample import Sample


class SummarizableError(Exception, ABC):
    @property
    @abstractmethod
    def SummarizerExceptionClass(self):
        """An exception class that takes a list of Exceptions of derived exception classes of this class as the sole
        required positional argument to its constructor.  All keyword arguments are ignored (if they exist).

        Usage example:
            # Create a summarizer Exception class - code it however you want
            class MyExceptionSummarier(Exception):
                def __init__(self, exceptions: List[MyException])
                    ...

            # Create your exception class that inherits from SummarizableError - code it however you want, just define
            # SummarizerExceptionClass
            class MyException(SummarizableError):
                SummarizerExceptionClass = MyExceptionSummarier
                ...

            # Later, when you're handling multiple buffered exceptions (i.e. you have an AggregatedErrors object: aes),
            # you can check if it's summarizable, and replace them with the summarized version
            if issubclass(myexception, SummarizableError):
                aes.buffer_error(
                    myexception.SummarizerExceptionClass(
                        aes.remove_exception_type(type(myexception))
                    )
                )
        """
        pass


class InfileError(Exception):
    """An exception class to provide file location context to other exceptions (when used as a base class).

    It brings consistency between error types to make all exceptions that reference a position in a table-like file,
    conform to a single way of formatting file location information (file name, excel sheet name, row number, and column
    name (or number)).

    Often, the derived class may opt to list multiple columns or row numbers (but reference the same file or sheet), so
    none of the arguments are required.  If none are provided, the file will be generically referenced (showing that the
    erroneous data is located in an input file (as opposed to a database, for example)).

    Example usage:

    class MyException(InfileError):
        def __init__(self, erroneous_value, **kwargs):
            message = f"This is my error about {erroneous_value}, found here: %s. You chould change it to 'good value'."
            super().__init__(message, **kwargs)

    def some_load_method(dataframe, file, sheet):
        for rownum, row in dataframe.iterrows():
            value = row["my data column"]
            if value_is_bad(value):
                raise MyException(value, rownum=rownum, column="my data column", sheet=sheet, file=file)
            else:
                load_value(value)

    Args:
        message (string): An error message containing at least 1 `%s` placeholder for where to put the file location
            information.  If `%s` isn't in the supplied message, it will be appended.  Optionally, the message may
            contain up to 4 more occurrences of `%s`.  See the `order` arg for more information.
        rownum (integer or string): The row number (or name) where the erroneous data was encountered.
        column (integer or string): The column name (or number) where the erroneous data was encountered.
        sheet (integer or string): The sheet name (or index) of an excel file where the erroneous data was encountered.
        file (string): The name of the file where the erroneous data was encountered.
        order (list of strings) {"rownum", "sheet", "file", "column", "loc"}: By default, the message is assumed to have
            a single `%s` occurrence where the file location information ("loc"), but if `order` is populated, the
            number of `%s` occurrences is expected to be the same as the length of order.  However, "loc" must be
            included.  If it is not, it is appended (and if necessary, a `%s` is appended to the message as well).  The
            values of the corresponding arguments are inserted into the message at the locations of the `%s` occurrences
            in the given order.

    Raises:
        ValueError: If the length of the order list doesn't match the `%s` occurrences in the message.

    Returns:
        Instance
    """

    def __init__(
        self,
        message,
        file=None,
        sheet=None,
        column: Optional[object] = None,
        rownum: Optional[object] = None,
        order=None,
        suggestion=None,
    ):
        self.location_args = ["rownum", "column", "file", "sheet"]
        self.loc = generate_file_location_string(
            rownum=rownum, sheet=sheet, file=file, column=column
        )
        self.message = message
        self.orig_message = message
        self.set_formatted_message(
            rownum=rownum,
            sheet=sheet,
            file=file,
            column=column,
            order=order,
            suggestion=suggestion,
        )
        super().__init__(self.message)

    def set_formatted_message(
        self,
        rownum: Optional[object] = None,
        sheet=None,
        file=None,
        column=None,
        order=None,
        suggestion=None,
    ):
        """This method allows one to change the string that will be returned when the exception is in string context.

        Sets instance attributes:
            rownum
            sheet
            file
            column
            order
            message
            loc

        The purpose is so that a class independent of the file-processing code/script can raise an exception and be
        caught by the file loading script, and the debug info (location of the data in the file that caused the error)
        can be added to the exception.

        Args:
            rownum (Optional[int or str]): The row number in the file (where the header row is row 1) where the
                offending data is located.  A row "name" can alternatively be supplied.
            sheet (Optional[str]): If the file is an excel file, this is the sheet where the offending data is.
            file (Optional[str]): File name or path where the offending data is located.
            column (Optional[str]): Column name where the offending data is located.
            order (Optional(List[str])) {"loc", "file", "sheet", "column", "rownum"} ["loc"]: List of 1-5 strings
                corresponding the the '%s' placeholders in the message that was supplied to the constructor (i.e.
                self.orig_message).  The effective default is ["loc"], which is a dynamically built string of all the
                location information (rownum, column, sheet, and file).  The size of the list must equal the number of
                placeholders in the message.  Note that if '%s' is not in the message, the generated value for "loc" is
                appended to the message.
        Exceptions:
            Raises:
                ProgrammingError
            Buffers:
                None
        Returns:
            None
        """
        self.rownum = rownum
        self.sheet = sheet
        self.file = file
        self.column = column
        self.order = order
        self.loc = generate_file_location_string(
            rownum=rownum, sheet=sheet, file=file, column=column
        )
        message = self.orig_message

        if "%s" not in message:
            # The purpose of this (base) class is to provide file location context of erroneous data to exception
            # messages in a uniform way.  It inserts that information where `%s` occurs in the supplied message.
            # Instead of making `%s` (in the simplest case) required, and raise an exception, the `%s` is just appended.
            if not message.endswith("\n"):
                message += "  "
            message += "Location: %s."

        if order is not None:
            missing_loc_arg_placeholders = []
            for locarg in self.location_args:
                if getattr(self, locarg) is not None and locarg not in order:
                    missing_loc_arg_placeholders.append(locarg)
            if "loc" not in order and len(order) != len(self.location_args):
                order.append("loc")
                if message.count("%s") != len(order):
                    raise ProgrammingError(
                        f"You must either provide all location arguments in your order list: {self.location_args} or "
                        "provide an extra '%s' in your message for the leftover location information "
                        f"({missing_loc_arg_placeholders})."
                    )
            # Save the arguments in a dict
            vdict = {
                "file": file,
                "sheet": sheet,
                "column": column,
                "rownum": rownum,
            }
            # Set the argument value to None, so the ones included in order will not be included in loc
            if "file" in order:
                file = None
            if "sheet" in order:
                sheet = None
            if "column" in order:
                column = None
            if "rownum" in order:
                rownum = None
            self.loc = generate_file_location_string(
                rownum=rownum, sheet=sheet, file=file, column=column
            )
            vdict["loc"] = self.loc
            insertions = [vdict[k] for k in order]
            message = message % tuple(insertions)
        else:
            message = message % self.loc

        if suggestion is not None:
            if message.endswith("\n"):
                message += suggestion
            else:
                message += f"\n{suggestion}"

        self.message = message

    def __str__(self):
        return self.message


class HeaderError(Exception):
    pass


class RequiredValueErrors(Exception):
    """Summary of all missing required value errors

    Attributes:
        required_value_errors: A list of RequiredValueError exceptions
    """

    def __init__(
        self,
        required_value_errors: list[RequiredValueError],
    ):
        missing_dict: Dict[str, dict] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        for rve in required_value_errors:
            missing_dict[rve.loc][str(rve.column)]["rves"].append(rve)
            fld = f"{rve.model_name}.{rve.field_name}"
            if (
                "fld" in missing_dict[rve.loc][str(rve.column)].keys()
                and missing_dict[rve.loc][str(rve.column)]["fld"] != fld
            ):
                missing_dict[rve.loc][str(rve.column)]["fld"] += f" or {fld}"
            else:
                missing_dict[rve.loc][str(rve.column)]["fld"] = fld

        message = "Required values found missing during loading:\n"
        for filesheet in missing_dict.keys():
            message += f"\t{filesheet}:\n"
            for colname in missing_dict[filesheet].keys():
                deets = ", ".join(
                    summarize_int_list(
                        [r.rownum for r in missing_dict[filesheet][colname]["rves"]]
                    )
                )
                message += (
                    f"\t\tField: [{missing_dict[filesheet][colname]['fld']}] "
                    f"Column: [{colname}] "
                    f"on row(s): {deets}\n"
                )
        super().__init__(message)
        self.required_value_errors = required_value_errors


class RequiredValueError(InfileError, SummarizableError):
    SummarizerExceptionClass = RequiredValueErrors

    def __init__(
        self,
        column,
        rownum,
        model_name,
        field_name,
        rec_dict=None,
        message=None,
        **kwargs,
    ):
        if not message:
            message = "Value required on %s."
            if rec_dict is not None:
                message += f"  Record extracted from row: {str(rec_dict)}."
        super().__init__(message, **kwargs)
        self.column = column
        self.rownum = rownum
        self.model_name = model_name
        self.field_name = field_name
        self.rec_dict = rec_dict


class RequiredColumnValues(Exception):
    def __init__(self, required_column_values, init_message=None):
        if init_message is None:
            message = "Required column values missing on the indicated rows:\n"
        else:
            message = f"{init_message}:\n"

        rcv_dict = defaultdict(lambda: defaultdict(list))
        for rcv in required_column_values:
            loc = generate_file_location_string(sheet=rcv.sheet, file=rcv.file)
            col = rcv.column
            if rcv.rownum not in rcv_dict[loc][col]:
                if rcv.rownum is not None:
                    rcv_dict[loc][col].append(rcv.rownum)
        for loc in rcv_dict.keys():
            message += f"\t{loc}\n"
            for col in rcv_dict[loc].keys():
                rowstr = "No row numbers provided"
                if rcv_dict[loc][col] is not None and len(rcv_dict[loc][col]) > 0:
                    rowstr = summarize_int_list(rcv_dict[loc][col])
                message += f"\t\tColumn: [{col}] on rows: {rowstr}\n"
        super().__init__(message)
        self.required_column_values = required_column_values


class RequiredColumnValue(InfileError, SummarizableError):
    SummarizerExceptionClass = RequiredColumnValues

    def __init__(
        self,
        column,
        message=None,
        **kwargs,
    ):
        if not message:
            message = "Value required for column(s) [%s] in %s."
        super().__init__(message, column=column, order=["column", "loc"], **kwargs)


class RequiredColumnValueWhenNovel(RequiredColumnValue):
    def __init__(self, column, model_name, **kwargs):
        message = kwargs.pop("message", None)
        if message is None:
            # The 2 %s placeholders are filled in in the superclass
            message = f"Value required for column [%s] in %s when the [{model_name}] record does not exist."
        super().__init__(column, **kwargs, message=message)
        self.model_name = model_name


class RequiredColumnValuesWhenNovel(RequiredColumnValues):
    """Summarizes a list of RequiredColumnValueWhenNovel exceptions of the same model."""

    def __init__(self, required_column_values_when_novel, model_name):
        msg = f"Value required, when the [{model_name}] record does not exist, for columns on the indicated rows"
        super().__init__(required_column_values_when_novel, init_message=msg)
        self.model_name = model_name


class MissingColumnGroup(InfileError):
    def __init__(self, group_name, **kwargs):
        message = f"No {group_name} columns found in %s.  At least 1 column of this type is required."
        super().__init__(message, **kwargs)
        self.group_name = group_name


class UnequalColumnGroups(InfileError):
    def __init__(self, group_name: str, sheet_dict: Dict[str, list], **kwargs):
        """Constructor

        Args:
            group_name (str): The type of all the columns in a group of columns
            sheet_dict (Dict[str, list]): A dict of lists of column names keyed on sheet.
        """
        colcounts: Dict[str, dict] = defaultdict(lambda: defaultdict(int))
        all = []
        for sheet in sheet_dict.keys():
            for col in sheet_dict[sheet]:
                colcounts[col][sheet] += 1
                if col not in all:
                    all.append(col)
        missing = defaultdict(list)
        for col in colcounts.keys():
            if len(colcounts[col].keys()) < len(sheet_dict.keys()):
                for sheet in sheet_dict.keys():
                    if sheet not in colcounts[col].keys():
                        missing[sheet].append(col)

        nlt = "\n\t"
        nums_str = (
            "\n\t".join(
                [
                    (
                        f"The '{sheet}' sheet has {len(lst)} out of {len(all)} total unique {group_name} columns, and "
                        f"is missing:\n\t{nlt.join(missing[sheet])}\n"
                    )
                    for sheet, lst in sheet_dict.items()
                    if len(missing[sheet]) > 0
                ]
            )
            + f"All sheets compared: {list(sheet_dict.keys())} from %s."
        )

        message = f"{group_name} columns in the sheets {list(sheet_dict.keys())} differ.\n\t{nums_str}\n"
        super().__init__(message, **kwargs)
        self.colcounts = colcounts
        self.missing = missing
        self.group_name = group_name
        self.sheet_dict = sheet_dict


class RequiredHeadersError(InfileError, HeaderError):
    def __init__(self, missing, message=None, **kwargs):
        if not message:
            message = f"Required header(s) missing: {missing} in %s."
        super().__init__(message, **kwargs)
        self.missing = missing


class HeaderConfigError(HeaderError):
    def __init__(self, missing, message=None):
        if not message:
            message = (
                "No header string is configured for the following required column(s): "
                f"[{', '.join(missing)}]."
            )
        super().__init__(message)
        self.missing = missing


class RequiredValuesError(InfileError):
    def __init__(self, missing, message=None, **kwargs):
        if not message:
            nltab = "\n\t"
            deets = list(
                map(
                    lambda k: f"{str(k)} on row(s): {str(summarize_int_list(missing[k]))}",
                    missing.keys(),
                )
            )
            message = (
                f"Missing required values have been detected in %s in the following columns:\n\t"
                f"{nltab.join(deets)}\nIf you wish to skip this row, you can either remove the row entirely or enter "
                "dummy values to avoid this error."
            )
            # Row numbers are available, but not very useful given the sheet merge
        super().__init__(message, **kwargs)
        self.missing = missing


class RequiredSampleValuesError(Exception):
    """
    This is the same as RequiredValuesError except that it indicates the animal on the row where required values are
    missing.  This is explicitly for the sample table loader because the Animal ID is guaranteed to be there.  We know
    that the animal ID is present, because if it wasn't, the affected rows in this error would be in a SheetMergeError
    and those row wouldn't have been processed to be able to get into this error.  Adding the Animal can help speed up
    the search for missing data when the row numbers may be inaccurate (if there was also a sheet merge error).
    """

    def __init__(self, missing, animal_hdr="animal", message=None):
        if not message:
            nltab = "\n\t"
            deets = list(
                map(
                    lambda k: (
                        f"{str(k)} on row(s): {str(summarize_int_list(missing[k]['rows']))} "
                        f"for {animal_hdr}(s): {missing[k]['animals']}"
                    ),
                    missing.keys(),
                )
            )
            message = (
                "Missing required values have been detected in the following columns:\n\t"
                f"{nltab.join(deets)}\nIf you wish to skip this row, you can either remove the row entirely or enter "
                "dummy values to avoid this error.  (Note, row numbers could reflect a sheet merge and may be "
                f"inaccurate.)"
            )
            # Row numbers are available, but not very useful given the sheet merge
        super().__init__(message)
        self.missing = missing
        self.animal_hdr = animal_hdr


# TODO: Remove this class when the accucor loader is deleted.  A combination of factors, such as performing a
# get_or_create for PeakGroup, the new clean method that enforces no multiple representations, and using the primary
# compound name for the peakgroup name makes this obsolete/unnecessary.
class DuplicatePeakGroup(Exception):
    """Duplicate data for the same sample sequenced on the same day

    Records duplicate sample compound pairs for a given ms_run

    Attributes:
        adding_file: The peak annotation file in which the duplicate data was detected
        msrun_sample: The MSRunSample in which the peak groups were measured
        sample_name: The name of the sample the duplicated data blongs to
        peak_group_name (compounds): The name of duplicated peak group
        existing_peak_annotation_file: The peak annotation file that the previosly existing peak group blongs to
    """

    def __init__(
        self,
        adding_file: str,
        msrun_sample: MSRunSample,
        sample: Sample,
        peak_group_name: str,
        existing_peak_annotation_file: ArchiveFile,
    ):
        """Initializes a DuplicatePeakGroup exception"""

        message = (
            f"Duplicate peak group data found when loading file [{adding_file}]:\n"
            f"\tmsrun_sample: {msrun_sample}\n"
            f"\tsample: {sample}\n"
            f"\tpeak_group_name: {peak_group_name}\n"
            f"\texisting_peak_annotation_file: {existing_peak_annotation_file}\n"
            f"\tWas this file [{adding_file}] loaded previously?\n"
        )
        super().__init__(message)
        self.adding_file = adding_file
        self.msrun_sample = msrun_sample
        self.sample = sample
        self.peak_group_name = peak_group_name
        self.existing_peak_annotation_file = existing_peak_annotation_file


# TODO: Remove this class when the accucor loader is deleted.  A combination of factors, such as performing a
# get_or_create for PeakGroup, the new clean method that enforces no multiple representations, and using the primary
# compound name for the peakgroup name makes this obsolete/unnecessary.
class DuplicatePeakGroups(Exception):
    """Duplicate peak groups from a given peak annotation file

    Attributes:
        adding_file: The peak annotation file in which the duplicate data was detected
        duplicate_peak_groups: A list of DuplicatePeakGroup exceptions
    """

    def __init__(
        self,
        adding_file: str,
        duplicate_peak_groups: list[DuplicatePeakGroup],
    ):
        """Initializes a DuplicatePeakGroups exception"""

        message = (
            f"Duplicate peak groups data skipped when loading file [{adding_file}]:\n"
            "\tpeak_groups:\n"
        )
        for duplicate_peak_group in duplicate_peak_groups:
            message += (
                f"\t\tpeak_group_name: {duplicate_peak_group.peak_group_name} | "
                f"msrun_sample: {duplicate_peak_group.msrun_sample} | "
                f"existing_peak_annotation_file: {duplicate_peak_group.existing_peak_annotation_file.filename}\n"
            )
        super().__init__(message)
        self.adding_file = adding_file
        self.duplicate_peak_groups = duplicate_peak_groups


class UnknownHeaderError(InfileError, HeaderError):
    def __init__(self, unknown, known: Optional[list] = None, message=None, **kwargs):
        if not message:
            message = f"Unknown header encountered: [{unknown}] in %s."
            if known is not None:
                message += f"  Must be one of {known}."
        super().__init__(message, **kwargs)


# TODO: Once the sample table loader inherits from TableLoader, make this inherit from SummarizableError
class UnknownHeadersError(InfileError, HeaderError):
    def __init__(self, unknowns, message=None, **kwargs):
        if not message:
            message = f"Unknown header(s) encountered: [{', '.join(unknowns)}] in %s."
        super().__init__(message, **kwargs)
        self.unknowns = unknowns


class ResearcherNotNew(Exception):
    def __init__(self, new_researchers, new_flag, existing_researchers):
        nl = "\n"
        errstr = f"Researchers {new_researchers} exist."
        if isinstance(new_researchers, str) or len(new_researchers) == 1:
            errstr = f"Researcher {new_researchers} exists."
        message = (
            f"{errstr}  {new_flag} cannot be used for existing researchers.  Current researchers are:{nl}"
            f"{nl.join(sorted(existing_researchers))}"
        )
        super().__init__(message)
        self.new_researchers = new_researchers
        self.new_flag = new_flag
        self.existing_researchers = existing_researchers


class NewResearchers(Exception):
    """Summarization exception of NewResearcher exceptions.

    Example output:

    New researchers encountered.  Please check the existing researchers:
        George
        Frank
    to ensure that the following researchers (parsed from the indicated file locations) are not variants of existing
    names:
        Edith (in column [Operator] of sheet [Sequences] in study.xlsx, on rows: '1-10')
    """

    def __init__(self, new_researcher_exceptions: List[NewResearcher]):
        existing = "\n\t".join(get_researchers())
        nre_dict: Dict[str, dict] = defaultdict(lambda: defaultdict(list))
        for nre in new_researcher_exceptions:
            file_loc = generate_file_location_string(
                file=nre.file, sheet=nre.sheet, column=nre.column
            )
            if nre.rownum is not None:
                nre_dict[nre.researcher][file_loc].append(nre.rownum)
            elif "unreported rows" not in nre_dict[file_loc]:
                nre_dict[nre.researcher][file_loc].append("unreported rows")
        message = "New researchers encountered"
        if existing == "":
            message += ":"
        else:
            message += (
                f".  Please check the existing researchers:\n\t{existing}\nto ensure that the following researchers "
                "(parsed from the indicated file locations) are not variants of existing names:"
            )
        for nr in sorted(nre_dict.keys()):
            for loc in sorted(nre_dict[nr].keys()):
                message += f"\n\t{nr} (in {loc}, on rows: {summarize_int_list(nre_dict[nr][loc])})"
        super().__init__(message)
        self.new_researcher_exceptions = new_researcher_exceptions
        self.existing = existing


class NewResearcher(InfileError, SummarizableError):
    SummarizerExceptionClass = NewResearchers

    def __init__(self, researcher: str, message=None, **kwargs):
        existing = "\n\t".join(get_researchers())
        message = f"A new researcher [{researcher}] is being added (parsed from %s)."
        if existing != "":
            message += (
                "  Please check the existing researchers to ensure this researcher name isn't a variant of an existing "
                f"name:\n\t{existing}"
            )
        super().__init__(message, **kwargs)
        self.researcher = researcher
        self.existing = existing


class MissingRecords(InfileError):
    _one_source = True

    def __init__(
        self,
        exceptions: List[RecordDoesNotExist],
        message=None,
        suggestion=None,
        **kwargs,
    ):
        # Initialize the remaining kwargs
        msg = "" if message is None else message
        super().__init__(msg, **kwargs)

        tmp_message = ""
        exceptions_by_model_and_fields: Dict[str, dict] = defaultdict(
            lambda: defaultdict(list)
        )
        exceptions_by_model_and_query: Dict[str, dict] = defaultdict(
            lambda: defaultdict(list)
        )
        exceptions_by_model_query_and_loc: Dict[str, dict] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        for inst in exceptions:
            q_str = inst._get_query_stub()
            exceptions_by_model_and_fields[inst.model.__name__][q_str].append(inst)
            search_terms = inst._get_query_values_str()
            exceptions_by_model_and_query[inst.model.__name__][search_terms].append(
                inst
            )
            exceptions_by_model_query_and_loc[inst.model.__name__][search_terms][
                inst.loc
            ].append(inst)

        for mdl_name in exceptions_by_model_and_fields.keys():
            for categorized_exceptions in exceptions_by_model_and_fields[
                mdl_name
            ].values():
                loc_args, flds_str, vals_dict = (
                    RecordDoesNotExist.get_failed_searches_dict(
                        categorized_exceptions, _one_source=self._one_source
                    )
                )

                loc_str = generate_file_location_string(**loc_args)

                # Summarize the values and the rows on which they occurred
                nltab = "\n\t"
                search_terms_str = nltab.join(
                    list(
                        map(
                            lambda key: f"{key} from row(s): {summarize_int_list(vals_dict[key])}",
                            vals_dict.keys(),
                        )
                    )
                )
                tmp_message += (
                    f"{len(categorized_exceptions)} {mdl_name} records were not found (using search field(s): "
                    f"{flds_str}) with values found in {loc_str}:{nltab}{search_terms_str}\n"
                )

        if not message:
            message = tmp_message

        if suggestion is not None:
            if message.endswith("\n"):
                message += suggestion
            else:
                message += f"  {suggestion}"

        self.message = message
        self.exceptions = exceptions
        self.exceptions_by_model_and_query = exceptions_by_model_and_query
        self.exceptions_by_model_query_and_loc = exceptions_by_model_query_and_loc


class MissingModelRecords(MissingRecords, ABC):
    """Keeps tract of missing records for one model for one file"""

    _one_source = True

    @property
    @abstractmethod
    def ModelName(self):
        pass

    @property
    @abstractmethod
    def RecordName(self):
        pass

    def __init__(
        self,
        exceptions: List[RecordDoesNotExist],
        **kwargs,
    ):
        file = None
        sheet = None
        column = None
        for exc in exceptions:
            if exc.model.__name__ != self.ModelName:
                raise ProgrammingError(
                    f"The supplied exceptions must all be for model {self.ModelName}, but found "
                    f"{exc.model.__name__}."
                )
            if (
                (file is not None and exc.file is not None and file != exc.file)
                or (sheet is not None and exc.sheet is not None and sheet != exc.sheet)
                or (
                    column is not None
                    and exc.column is not None
                    and column != exc.column
                )
            ):
                raise ProgrammingError(
                    "The supplied exceptions must all be from the same file, sheet, and column."
                )
            file = exc.file
            sheet = exc.sheet
            column = exc.column

        kwargs["file"] = file
        kwargs["sheet"] = sheet
        kwargs["column"] = column

        # This sets self.loc, self.file, and self.sheet, which we need below. Then we'll set the message.
        super().__init__(exceptions, **kwargs)
        message = kwargs.pop("message", None)
        if message is None:
            nltab = "\n\t"
            summary = nltab.join(
                [
                    f"'{terms}' from row(s): ["
                    + ", ".join(summarize_int_list([exc.rownum for exc in excs]))
                    + "]"
                    for terms, excs in self.exceptions_by_model_and_query[
                        self.ModelName
                    ].items()
                ]
            )
            message = (
                f"{len(exceptions)} {self.ModelName}s matching the following values in %s were not found in the "
                f"database:{nltab}{summary}\n"
            )

        self.orig_message = message
        self.search_terms = list(
            self.exceptions_by_model_and_query[self.ModelName].keys()
        )
        self.exceptions_by_query = self.exceptions_by_model_and_query[self.ModelName]
        self.set_formatted_message(**kwargs)


class MissingModelRecordsByFile(MissingRecords, ABC):
    """Keeps tract of missing records for one model across multiple files"""

    _one_source = False

    @property
    @abstractmethod
    def ModelName(self):
        pass

    @property
    @abstractmethod
    def RecordName(self):
        pass

    def __init__(
        self,
        exceptions: List[RecordDoesNotExist],
        succinct=False,
        **kwargs,
    ):
        for exc in exceptions:
            if exc.model.__name__ != self.ModelName:
                raise ProgrammingError(
                    f"The supplied exceptions must all be for model {self.ModelName}, but found "
                    f"{exc.model.__name__}."
                )

        # This sets self.loc, self.file, and self.sheet, which we need below. Then we'll set the message.
        super().__init__(exceptions, **kwargs)
        message = kwargs.pop("message", None)
        if message is None:
            nltt = "\n\t\t"
            summary = ""
            for terms, loc_dict in self.exceptions_by_model_query_and_loc[
                self.ModelName
            ].items():
                summary += "\n\t"
                if succinct:
                    summary += "\n\t".join(loc_dict.keys())
                else:
                    summary += f"{terms}\n\t\t"
                    summary += nltt.join(
                        [
                            f"{loc}, row(s): ["
                            + ", ".join(
                                summarize_int_list([exc.rownum for exc in excs])
                            )
                            + "]"
                            for loc, excs in loc_dict.items()
                        ]
                    )
            if succinct:
                message = f"{len(exceptions)} {self.ModelName}s missing in the database:{summary}\nwhile processing %s."
            else:
                message = (
                    f"{len(exceptions)} {self.ModelName}s matching the following values were not found in the "
                    f"database:{summary}\nwhile processing %s."
                )

        self.orig_message = message
        self.search_terms = list(
            self.exceptions_by_model_and_query[self.ModelName].keys()
        )
        self.exceptions_by_query_and_loc = self.exceptions_by_model_query_and_loc[
            self.ModelName
        ]
        self.set_formatted_message(**kwargs)


class RecordDoesNotExist(InfileError, ObjectDoesNotExist, SummarizableError):
    SummarizerExceptionClass = MissingRecords

    def __init__(
        self, model, query_obj: dict | Q, message=None, suggestion=None, **kwargs
    ):
        """General use DoesNotExist exception constructor for errors retrieving Model records.

        Args:
            model: (Model)
            query_obj (dict or Q): A representation of the query parameters, to provide context for the user.
            message (Optional[str])
            suggestion (str): An addendum as to how to possibly fix this issue.
        Exceptions:
            None
        Returns:
            instance
        """
        if message is None:
            message = (
                f"{model.__name__} record matching {query_obj} from %s does not exist."
            )
        if suggestion is not None:
            message += f"  {suggestion}"
        super().__init__(message, **kwargs)
        self.query_obj = query_obj
        self.model = model
        self.suggestion = suggestion

    @classmethod
    def get_failed_searches_dict(
        cls, instances: List[RecordDoesNotExist], _one_source=True
    ):
        """Given a list of RecordDoesNotExist instances, a field description and dict like the following example is
        returned:

            {"George": [1, 2, 3, 4]}

        Where the first key is the search term, and the value is a list of row numbers from an input file where the term
        was found.

        Args:
            instances (List[RecordDoesNotExist]): RecordDoesNotExist exceptions
            _one_source (bool) [True]: When this is True, it ensures that all the exceptions come from the same file,
                sheet, and column.  This is necessary if you will be treating the list of exceptions as all having come
                from 1 such place.
        Exceptions:
            ProgrammingError
        Returns:
            loc_args (dict): file, sheet, and column values
            fields_stub (str): Search field/column and comparator
            search_valuecombos_rows_dict (defaultdict(list))
        """
        search_valuecombos_rows_dict = defaultdict(list)
        model = None
        fields_str = None
        def_loc_args: Dict[str, Optional[str]] = {
            "column": None,
            "file": None,
            "sheet": None,
        }
        loc_args = def_loc_args.copy()
        for inst in instances:
            if model is None:
                model = inst.model
            elif inst.model != model:
                raise ProgrammingError(
                    "instances must be a list of RecordDoesNotExist exceptions generated from queries of the same "
                    f"model.  {inst.model} != {model}"
                )

            if _one_source:
                cur_loc_args = {
                    "column": inst.column,
                    "file": inst.file,
                    "sheet": inst.sheet,
                }
                if loc_args == def_loc_args:
                    loc_args = cur_loc_args
                elif cur_loc_args != loc_args:
                    raise ProgrammingError(
                        "instances must be a list of RecordDoesNotExist exceptions generated from queries of the same "
                        f"file/column.  {cur_loc_args} != {loc_args}"
                    )

            query_fields_str = inst._get_query_stub()

            if fields_str is None:
                fields_str = query_fields_str
            elif fields_str != query_fields_str:
                raise ProgrammingError(
                    "instances must be a list of RecordDoesNotExist exceptions generated from queries using the same "
                    f"search fields (and comparators).  {fields_str} != {query_fields_str}"
                )

            query_values_str = inst._get_query_values_str()
            rownum = (
                inst.rownum if inst.rownum is not None else "no row number supplied"
            )
            search_valuecombos_rows_dict[query_values_str].append(rownum)

        return loc_args, fields_str, search_valuecombos_rows_dict

    def _get_query_stub(self, _query_obj: Optional[dict | Q] = None) -> str:
        """This takes an instance and returns a string describing the fields (and comparators) the query operates on.
        The values(/search terms) are replaced with numeric labels.  If there is only a single field/comparator, it will
        be retendered as a single string (instead of a string version of a list or Q object).

        The purpose of this method is to be able to generate strings that can be used as keys, so that a series of
        failed searches using the same fields but different values can be grouped together.

        Both arguments are private.  Do not supply manually.

        Args:
            _query_obj (Optional[dict|Q]): An object supplied to Model.objects.get/get_or_create/create/...
        Exceptions:
            None
        Returns:
            new_q (str|Q): The original call returns a string describing the search fields and comparators.  Recursive
                calls return Q objects.
        """
        if _query_obj is None:
            _query_obj = self.query_obj

        # Not recursive when given a dict
        if isinstance(_query_obj, dict):
            return ", ".join(_query_obj.keys())

        # Must be a Q instance
        search_fields_str = ""
        if _query_obj.negated:
            search_fields_str += "NOT "
        if len(_query_obj.children) > 1:
            search_fields_str += f"({_query_obj.connector}: "
        search_fields_str += ", ".join(
            [
                (
                    self._get_query_stub(_query_obj=sub_q)
                    if isinstance(sub_q, Q)
                    else str(sub_q[0])
                )
                for sub_q in _query_obj.children
            ]
        )
        if len(_query_obj.children) > 1:
            search_fields_str += ")"

        return search_fields_str

    def _get_query_values_str(
        self, _query_obj: Optional[dict | Q] = None, _uniq_vals: Optional[list] = None
    ) -> list | str:
        """This takes an instance and returns a string of comma-delimited search terms from the query_obj.

        The purpose of this method is to be able to generate strings that can be used as keys, so that a series of
        failed searches using the same values from multiple rownums can be grouped together.

        NOTE: The values(/search terms) will either be the values from every query field from the query_obj (unique or
        not) or will only be the unique values if self.column is defined.  A column value can be used in multiple field
        matches.  This is intended to reduce redundancy.  For example, if searching for a compound and looking in
        Compound.name or CompoundSynonym.name, the same value from 1 column is used.  We don't need to include it twice.

        Limitations:
            This is a simple heuristic.  If values are ever manipulated or static terms are added, the only result will
            be a different number of values compared to the listed column.

        Args:
            _query_obj (Optional[dict|Q]): An object supplied to Model.objects.get/get_or_create/create/...
            _label (int): 1 for first call, not 1 otherwise
        Exceptions:
            None
        Returns:
            _uniq_vals (list|str): The original call returns a string describing the search fields and comparators.
                Recursive calls return a list.
        """
        first_call = False
        if _query_obj is None:
            first_call = True
            _query_obj = self.query_obj

        if isinstance(_query_obj, dict):
            return ", ".join([str(val) for val in _query_obj.values()])

        if _uniq_vals is None:
            _uniq_vals = []

        for sub_q in _query_obj.children:
            if isinstance(sub_q, Q):
                _uniq_vals.extend(
                    self._get_query_values_str(_query_obj=sub_q, _uniq_vals=_uniq_vals)
                )
            else:
                val = str(sub_q[1])
                # We are only returning unique values if self.column is defined.  See doc string.
                if self.column is None or val not in _uniq_vals:
                    _uniq_vals.append(val)

        if first_call:
            return ", ".join(_uniq_vals)

        return _uniq_vals


class AllMissingSamplesError(Exception):
    """This is a summary of the MissingSamples and NoSamples classes."""

    def __init__(self, missing_samples_dict, message=None):
        """An exception for all missing samples errors (MissingSamples and NoSamples).

        Args:
            missing_samples_dict (dict): Example:
                {
                    "files_missing_all": {"accucor1.xlsx": ["s1", "s2"]},
                    "files_missing_some": {
                        "s3": ["accucor2.xlsx", "accucor3.xlsx"],
                        "s1": ["accucor4.xlsx"],
                    },
                    "all_missing_samples": {
                        "s1": ["accucor1.xlsx", "accucor4.xlsx"],
                        "s2": ["accucor1.xlsx"],
                        "s3": ["accucor2.xlsx", "accucor3.xlsx"],
                    },
                }

        Exceptions:
            None

        Returns:
            instance
        """
        if not message:
            nltab = "\n\t"
            smpls_str = ""
            if len(missing_samples_dict["files_missing_all"].keys()) > 0:
                smpls_str += (
                    f"All {len(missing_samples_dict['files_missing_all'].keys())} samples in file(s): "
                    + nltab.join(missing_samples_dict["files_missing_all"].keys())
                )
                if len(missing_samples_dict["files_missing_some"].keys()) > 0:
                    smpls_str += nltab
            if len(missing_samples_dict["files_missing_some"].keys()) > 0:
                smpls_str += nltab.join(
                    list(
                        map(
                            lambda smpl: f"{smpl} in file(s): {missing_samples_dict['files_missing_some'][smpl]}",
                            missing_samples_dict["files_missing_some"].keys(),
                        )
                    )
                )

            message = (
                f"{len(missing_samples_dict['all_missing_samples'].keys())} samples are missing in the database/"
                f"sample-table:{nltab}{smpls_str}\nSamples in the accucor/isocorr files must be present in the "
                "sample table file and loaded into the database before they can be loaded from the mass spec data "
                "files."
            )

        super().__init__(message)
        self.missing_samples_dict = missing_samples_dict


# TODO: Remove this class when the accucor loader is deleted
class MissingSamplesError(Exception):
    def __init__(
        self,
        missing_samples,
        suggestion="Samples must be loaded prior to loading mass spec data.",
        message=None,
        exceptions: Optional[List[RecordDoesNotExist]] = None,
    ):
        if missing_samples is None:
            missing_samples = []
        if not message:
            num_missing = len(missing_samples)
            nltab = "\n\t"
            message = (
                f"{num_missing} samples are missing in the database/sample-table-file:{nltab}"
                f"{nltab.join(missing_samples)}\n"
            )
        if suggestion is not None:
            message += suggestion
        super().__init__(message)
        self.missing_samples = missing_samples
        self.suggestion = suggestion
        self.exceptions = exceptions


class MissingSamples(MissingModelRecords):
    ModelName = "Sample"
    RecordName = ModelName


class AllMissingSamples(MissingModelRecordsByFile):
    ModelName = "Sample"
    RecordName = ModelName


class MissingCompounds(MissingModelRecords):
    ModelName = "Compound"
    RecordName = ModelName


class AllMissingCompounds(MissingModelRecordsByFile):
    ModelName = "Compound"
    RecordName = ModelName


class RequiredArgument(Exception):
    def __init__(self, argname, methodname=None, message=None):
        if message is None:
            if methodname is None:
                message = f"A non-None value for argument '{argname}' is required."
            else:
                message = (
                    f"{methodname} requires a non-None value for argument '{argname}'."
                )
        super().__init__(message)
        self.argname = argname
        self.methodname = methodname


# TODO: Remove this class when the accucor loader is deleted
class UnskippedBlanksError(MissingSamplesError):
    def __init__(self, sample_names, **kwargs):
        if sample_names is None or len(sample_names) == 0:
            raise RequiredArgument(
                "sample_names",
                type(self).__name__,
                message="A non-zero sized list is required.",
            )
        message = (
            f"{len(sample_names)} samples that appear to possibly be blanks are missing in the database: "
            f"[{', '.join(sample_names)}].  Blank samples should be skipped."
        )
        super().__init__(sample_names, message=message, **kwargs)


class UnskippedBlanks(MissingSamples):
    def __init__(
        self,
        exceptions: List[RecordDoesNotExist],
        **kwargs,
    ):
        super().__init__(exceptions, **kwargs)
        message = kwargs.pop("message", None)
        if message is None:
            message = (
                f"{len(exceptions)} samples that appear to possibly be blanks are missing in the database: "
                f"[{', '.join(self.search_terms)}]."
            )
        suggestion = kwargs.pop("suggestion", None)
        if suggestion is None:
            suggestion = (
                "Be sure to set the skip column in the PeakAnnotation Details sheet to 'true' for blank "
                "samples."
            )
        self.orig_message = message
        self.set_formatted_message(suggestion=suggestion, **kwargs)


# TODO: Remove this class when the accucor loader is deleted
class NoSamplesError(MissingSamplesError):
    def __init__(self, sample_names, **kwargs):
        """An error to abbreviate an error about all samples."""
        if sample_names is None or len(sample_names) == 0:
            raise RequiredArgument(
                "sample_names",
                type(self).__name__,
                message="A non-zero sized list is required.",
            )
        num_samples = len(sample_names)
        message = (
            f"None of the {num_samples} samples were found in the database/sample table file.  Samples "
            "in the peak annotation files must be present in the sample table file and loaded into the database "
            "before they can be loaded from the mass spec data files."
        )
        super().__init__(sample_names, message=message, **kwargs)


class NoSamples(MissingSamples):
    def __init__(
        self,
        exceptions: List[RecordDoesNotExist],
        **kwargs,
    ):
        num_samples = len(exceptions)
        message = kwargs.pop("message", None)
        if message is None:
            message = f"None of the {num_samples} samples were found in the database/sample table file."
        suggestion = kwargs.pop("suggestion", None)
        if suggestion is None:
            suggestion = (
                "Samples in the peak annotation files must be present in the sample table file and loaded into the "
                "database before they can be loaded from the mass spec data files."
            )
        super().__init__(exceptions, message=message, suggestion=suggestion, **kwargs)


class UnexpectedSamples(InfileError):
    def __init__(self, missing_samples, suggestion=None, **kwargs):
        if missing_samples is None or len(missing_samples) == 0:
            raise RequiredArgument(
                "missing_samples",
                type(self).__name__,
                message="A non-zero sized list is required.",
            )
        message = kwargs.pop("message", None)
        if message is None:
            message = (
                "The following sample data headers from the Peak Annotation Details sheet were not among the headers "
                f"in %s: {missing_samples}."
            )
        if suggestion is not None:
            message += f"  {suggestion}"
        super().__init__(message, **kwargs)
        self.missing_samples = missing_samples


class NoSampleHeaders(InfileError):
    def __init__(self, **kwargs):
        message = "No sample headers were found in %s."
        super().__init__(message, **kwargs)


class UnitsWrong(Exception):
    def __init__(self, units_dict, message=None):
        if not message:
            nltab = "\n\t"
            row_str = nltab.join(
                list(
                    (
                        f"{k} (example: [{units_dict[k]['example_val']}] does not match units: "
                        f"[{units_dict[k]['expected']}] on row(s): {units_dict[k]['rows']})"
                    )
                    for k in units_dict.keys()
                )
            )
            message = (
                f"Unexpected units were found in {len(units_dict.keys())} columns:{nltab}{row_str}\n"
                "Units are not allowed, but these also appear to be the wrong units."
            )
        super().__init__(message)
        self.units_dict = units_dict


# TODO: Delete when the accucor loader is deleted
class EmptyColumnsError(Exception):
    def __init__(self, sheet_name, col_names):
        message = (
            f"Sample columns missing headers found in the [{sheet_name}] data sheet. You have [{len(col_names)}] "
            "columns. Be sure to delete any unused columns."
        )
        super().__init__(message)
        self.sheet_name = sheet_name
        self.col_names = col_names


class EmptyColumns(InfileError):
    def __init__(
        self,
        group_name: str,
        expected: List[str],
        empty: List[str],
        all: List[str],
        addendum=None,
        **kwargs,
    ):
        group = list(set(all) - set(expected))
        message = (
            f"1 or more dynamically named [{group_name}] columns are expected, but some columns were parsed from %s "
            f"that appear to have been unnamed (and may be empty).  {len(expected)} expected constant columns were "
            f"present.  There are {len(all)} columns total, leaving {len(group)} potential {group_name} columns.  Of "
            f"those, {len(empty)} were unnamed."
        )
        if addendum is not None:
            message += f"  {addendum}"
        super().__init__(message, **kwargs)
        self.group_name = group_name
        self.expected = expected
        self.empty = empty
        self.all = all
        self.addendum = addendum


class SampleColumnInconsistency(Exception):
    def __init__(self, num_orig_cols, num_corr_cols, orig_only_cols, corr_only_cols):
        message = (
            "Samples in the original and corrected sheets differ."
            f"\nOriginal contains {num_orig_cols} samples | Corrected contains {num_corr_cols} samples"
            f"\nSamples in original sheet missing from corrected:\n{orig_only_cols}"
            f"\nSamples in corrected sheet missing from original:\n{corr_only_cols}"
        )
        super().__init__(message)
        self.num_orig_cols = num_orig_cols
        self.num_corr_cols = num_corr_cols
        self.orig_only_cols = orig_only_cols
        self.corr_only_cols = corr_only_cols


class MultipleAccucorTracerLabelColumnsError(Exception):
    def __init__(self, columns):
        message = (
            f"Multiple tracer label columns ({','.join(columns)}) in Accucor corrected data is not currently "
            "supported.  See --isocorr-format."
        )
        super().__init__(message)
        self.columns = columns


class DryRun(Exception):
    """
    Exception thrown during dry-run to ensure atomic transaction is not committed
    """

    # TODO: Figure out a way to suppress the trace (and optionally the exception string) when this is raised.
    # Could possibly use sys.excepthook:
    # stackoverflow.com/questions/20714644/python-sys-excepthook-and-logging-uncaught-exceptions-across-multiple-modules
    def __init__(self, message=None):
        if message is None:
            message = "Dry Run Complete."
        super().__init__(message)


class LoadingError(Exception):
    """
    Exception thrown if any errors encountered during loading
    """

    pass


class MultiLoadStatus(Exception):
    """
    This class holds the load status of multiple files and also can contain multiple file group statuses, e.g. a
    discrete list of missing compounds across all files.  It is defined as an Exception class so that it being raised
    from (for example) load_study will convey the load statuses to the validation interface.
    """

    def __init__(self, load_keys=None):
        self.statuses = {}
        # Initialize the load status of all load keys (e.g. file names).  Note, you can create arbitrary keys for group
        # statuses, e.g. for AllMissingCompounds errors that consolidate all missing compounds
        if load_keys:
            for load_key in load_keys:
                self.init_load(load_key)

    def init_load(self, load_key):
        self.statuses[load_key] = {
            "aggregated_errors": None,
            "state": "PASSED",
            "top": True,  # Passing files will appear first
        }

    def set_load_exception(
        self,
        exception,
        load_key,
        top=False,
        default_is_error=True,
        default_is_fatal=True,
    ):
        """Records the status of a load in a data member called statuses.  It tracks some overall stats and can set
        whether this load status should appear at the top of the reported statuses or not.

        Args:
            exception (Exception instance): An exception to add to the load_key
            load_key (string): A file name of category name describing a condition that must pass (i.e. must not be
                associated with any errors), e.g. "All samples exist in the database".
            top (boolean): Whether the file or category's pass/fail state should be among the first listed in the status
                report.
            default_is_error (boolean): If the added exception is an error (as opposed to a warning).  Ignored when
                exception is an AggregatedErrors object.  Otherwise only used if exception.is_error, is not present.
            default_is_fatal (boolean): If the added exception should be construed as having halted execution of the
                load associated with the load_key.  This is useful when summarizing a category of common exceptions from
                multiple load_keys.  Ignored when exception is an AggregatedErrors object.  Otherwise only used if
                exception.is_fatal, is not present.
        Exceptions:
            None
        Returns:
            None
        """

        if len(self.statuses.keys()) == 0:
            warnings.warn(
                f"Load keys such as [{load_key}] should be pre-defined when {type(self).__name__} is constructed or "
                "as they are encountered by calling [obj.init_load(load_key)].  A load key is by default the file "
                "name, but can be any key."
            )

        if load_key not in self.statuses.keys():
            self.init_load(load_key)

        if isinstance(exception, AggregatedErrors):
            new_aes = exception
        else:
            # Wrap the exception in an AggregatedErrors class
            new_aes = AggregatedErrors()
            is_error = (
                exception.is_error
                if hasattr(exception, "is_error")
                else default_is_error
            )
            is_fatal = (
                exception.is_fatal
                if hasattr(exception, "is_fatal")
                else default_is_fatal
            )
            # NOTE: This will cause the exception trace to be printed
            new_aes.buffer_exception(exception, is_error=is_error, is_fatal=is_fatal)

        if self.statuses[load_key]["aggregated_errors"] is not None:
            merged_aes = self.statuses[load_key]["aggregated_errors"]
            merged_aes.merge_aggregated_errors_object(new_aes)
            # Update the aes object and merge the top value
            merged_aes = self.statuses[load_key]["aggregated_errors"]
            top = self.statuses[load_key]["top"] or top
        else:
            merged_aes = new_aes
            self.statuses[load_key]["aggregated_errors"] = merged_aes

        # We have edited AggregatedErrors above, but we are explicitly not accounting for removed exceptions.
        # Those will be tallied later in handle_packaged_exceptions, because for example, we only want 1 missing
        # compounds error that accounts for all missing compounds among all the study files.
        self.statuses[load_key]["top"] = top

        if self.statuses[load_key]["aggregated_errors"].is_error:
            self.statuses[load_key]["state"] = "FAILED"
        else:
            self.statuses[load_key]["state"] = "WARNING"

    @property
    def num_errors(self):
        """Determined the formerly "num_errors" number, but doing it on the fly to simplify determination given the new
        features elsewhere of fixing and removing exceptions.

        Args:
            None

        Exceptions:
            None

        Returns:
            num_errors (boolean)
        """
        num_errors = 0
        for lk in self.statuses.keys():
            if (
                "aggregated_errors" in self.statuses[lk].keys()
                and self.statuses[lk]["aggregated_errors"] is not None
            ):
                num_errors += self.statuses[lk]["aggregated_errors"].num_errors
        return num_errors

    @property
    def num_warnings(self):
        """Determined the formerly "num_warnings" number, but doing it on the fly to simplify determination given the
        new features elsewhere of fixing and removing exceptions.

        Args:
            None

        Exceptions:
            None

        Returns:
            num_warnings (boolean)
        """
        num_warnings = 0
        for lk in self.statuses.keys():
            if (
                "aggregated_errors" in self.statuses[lk].keys()
                and self.statuses[lk]["aggregated_errors"] is not None
            ):
                num_warnings += self.statuses[lk]["aggregated_errors"].num_warnings
        return num_warnings

    @property
    def state(self):
        """Determined the formerly "state" status, but doing it on the fly to simplify determination given the
        new features elsewhere of fixing and removing exceptions.

        Args:
            None

        Exceptions:
            None

        Returns:
            state (string): "PASSED", "WARNING", or "FAILED"
        """
        state = "PASSED"
        for lk in self.statuses.keys():
            # Any exception (warning or error) results in an invalid load state (either FAILED or WARNING)
            if (
                "aggregated_errors" in self.statuses[lk].keys()
                and self.statuses[lk]["aggregated_errors"] is not None
            ):
                if self.statuses[lk]["aggregated_errors"].num_errors > 0:
                    return "FAILED"
                elif self.statuses[lk]["aggregated_errors"].num_warnings > 0:
                    state = "WARNING"
        return state

    @property
    def is_valid(self):
        """Determine the "is_valid" state, but do it on the fly to simplify determination given the new features
        elsewhere of fixing and removing exceptions.

        Args:
            None

        Exceptions:
            None

        Returns:
            is_valid (boolean)
        """
        for lk in self.statuses.keys():
            # Any exception (warning or error) results in an invalid load state (either FAILED or WARNING)
            if (
                "aggregated_errors" in self.statuses[lk].keys()
                and self.statuses[lk]["aggregated_errors"] is not None
                and len(self.statuses[lk]["aggregated_errors"].exceptions) > 0
            ):
                return False
        return True

    def remove_exception_type(self, load_key, *args, **kwargs):
        """An interface to AggregatedErrors.remove_exception_type that keeps the "state" of the load_key in the statuses
        member up-to-date.  Call this instead of calling
        obj.statuses[load_key]["aggregated_errors"].remove_exception_type().  If you do that, you must manually handle
        keeping obj.statuses[load_key]["state"] up to date.

        Args:
            load_key (string): Key into self.statuses dict where each aggregated errors object is stored.
            args (list): Positional arguments to the AggregatedErrors.remove_exception_type() method.
                exception_class (Exception): Required.  See AggregatedErrors.remove_exception_type().
            kwargs (dict): Keyword arguments to the AggregatedErrors.remove_exception_type() method.
                modify (boolean): See AggregatedErrors.remove_exception_type().

        Exceptions:
            ValueError

        Returns:
            removed_exceptions (list of Exception): The return of AggregatedErrors.remove_exception_type()
        """
        removed_exceptions = []
        if self.statuses[load_key]["aggregated_errors"] is not None:
            removed_exceptions = self.statuses[load_key][
                "aggregated_errors"
            ].remove_exception_type(*args, **kwargs)
            if len(self.statuses[load_key]["aggregated_errors"].exceptions) == 0:
                self.statuses[load_key]["aggregated_errors"] = None
        self.update_state(load_key)
        return removed_exceptions

    def update_state(self, load_key):
        """Set the state of an individual load key on the fly to simplify determination given the new features
        elsewhere of fixing and removing exceptions.

        Args:
            load_ley (string): Key to self.statuses

        Exceptions:
            None

        Returns:
            state (string): "PASSED", "WARNING", or "FAILED"
        """
        if self.statuses[load_key]["aggregated_errors"] is None:
            self.statuses[load_key]["state"] = "PASSED"
        elif self.statuses[load_key]["aggregated_errors"].num_errors > 0:
            self.statuses[load_key]["state"] = "FAILED"
        elif self.statuses[load_key]["aggregated_errors"].num_warnings > 0:
            self.statuses[load_key]["state"] = "WARNING"
        else:
            self.statuses[load_key]["state"] = "PASSED"

    def get_final_exception(self, message=None):
        # If success, return None
        if self.is_valid:
            return None

        aggregated_errors_dict = {}
        for load_key in self.statuses.keys():
            # Only include AggregatedErrors objects if they are defined.  If they are not defined, it means there were
            # no errors
            if self.statuses[load_key]["aggregated_errors"] is not None:
                aggregated_errors_dict[load_key] = self.statuses[load_key][
                    "aggregated_errors"
                ]

        # Sanity check
        if len(aggregated_errors_dict.keys()) == 0:
            raise ValueError(
                f"Success status is {self.is_valid} but there are no aggregated exceptions for any "
                "files."
            )

        return AggregatedErrorsSet(aggregated_errors_dict, message=message)

    def get_status_message(self):
        # Overall status message
        state = self.state
        message = f"Load {state}"
        if self.num_warnings > 0:
            message += f" {self.num_warnings} warnings"
        if self.num_errors > 0:
            message += f" {self.num_errors} errors"

        return message, state

    def get_status_messages(self):
        messages = []
        for load_key in self.get_ordered_status_keys(reverse=False):
            messages.append(
                {
                    "message": f"{load_key}: {self.statuses[load_key]['state']}",
                    "state": self.statuses[load_key]["state"],
                }
            )
            if self.statuses[load_key]["aggregated_errors"] is not None:
                state = (
                    "FAILED"
                    if self.statuses[load_key]["aggregated_errors"].num_errors > 0
                    else "WARNING"
                )
                messages.append(
                    {
                        "message": self.statuses[load_key][
                            "aggregated_errors"
                        ].get_summary_string(),
                        "state": state,
                    }
                )

        return messages

    def get_ordered_status_keys(self, reverse=False):
        return sorted(
            self.statuses.keys(),
            key=lambda k: self.statuses[k]["top"],
            reverse=not reverse,
        )

    def copy_constructor(self, instance):
        """Modifying contained AggregatedErrors exceptions makes the state of this object stale, so this copy
        constructor can be used to create a new object with accurate stats.  After making changes to the population of
        exceptions in a MultiLoadStatus instance, this method can be used to create a new instance with refreshed
        metadata.

        Args:
            instance (MultiLoadStatus): An existing MultiLoadStatus object
        Exceptions:
            None
        Returns:
            None
        """
        if len(self.statuses.keys()) == 0:
            for load_key in instance.statuses.keys():
                self.init_load(load_key)
        for load_key in instance.statuses.keys():
            if (
                instance.statuses[load_key]["aggregated_errors"] is not None
                and len(instance.statuses[load_key]["aggregated_errors"].exceptions) > 0
            ):
                self.set_load_exception(
                    instance.statuses[load_key]["aggregated_errors"],
                    load_key,
                )


class AggregatedErrorsSet(Exception):
    def __init__(self, aggregated_errors_dict, message=None):
        self.aggregated_errors_dict = aggregated_errors_dict
        self.num_warnings = 0
        self.num_errors = 0
        self.is_fatal = False
        self.is_error = False
        self.custom_message = message is not None
        self.message = None
        self.update(message=message)
        super().__init__(self.message)

    def __str__(self):
        return self.message

    def get_default_message(self):
        should_raise_message = (
            "  Use the return of self.should_raise() to determine if an exception should be raised before raising "
            "this exception."
        )
        if len(self.aggregated_errors_dict.keys()) > 0:
            message = (
                f"{len(self.aggregated_errors_dict.keys())} categories had exceptions, including {self.num_errors} in "
                f"an error state and {self.num_warnings} in a warning state."
            )
            if not self.is_fatal:
                message += f"  This exception should not have been raised.{should_raise_message}"
            message += (
                f"\n{self.get_summary_string()}\nScroll up to see tracebacks for each exception printed as it was "
                "encountered."
            )
        else:
            message = f"AggregatedErrors exception.  No exceptions have been buffered.{should_raise_message}"
        return message

    def should_raise(self):
        return self.is_fatal

    def get_summary_string(self):
        smry_str = ""
        for aes_key in self.aggregated_errors_dict.keys():
            smry_str += f"{aes_key}: {self.aggregated_errors_dict[aes_key].get_summary_string()}\n"
        return smry_str

    def update(self, message=None):
        """Refresh the instance attributes (e.g. if the contained AggregatedErrors objects changed)."""
        self.num_warnings = 0
        self.num_errors = 0
        self.is_fatal = False
        self.is_error = False
        if len(self.aggregated_errors_dict.keys()) > 0:
            for aes_key in self.aggregated_errors_dict.keys():
                if self.aggregated_errors_dict[aes_key].num_errors > 0:
                    self.num_errors += 1
                elif self.aggregated_errors_dict[aes_key].num_warnings > 0:
                    self.num_warnings += 1
                else:
                    # Remove AggregatedErrors objects that have been completely gutted
                    del self.aggregated_errors_dict[aes_key]
                if self.aggregated_errors_dict[aes_key].is_fatal:
                    self.is_fatal = True
                if self.aggregated_errors_dict[aes_key].is_error:
                    self.is_error = True
        self.custom_message = False
        if message:
            self.custom_message = True
            current_message = message
        else:
            current_message = self.get_default_message()
        self.message = current_message


class AggregatedErrors(Exception):
    """
    This is not a typical exception class.  You construct it before any errors have occurred and you use it to buffer
    exceptions using object.buffer_error(), object.buffer_warning(), and (where the error/warning can change based on a
    boolean) object.buffer_exception(is_error=boolean_variable).  You can also decide whether a warning should be
    raised as an exception or not using the is_fatal parameter.  This is intended to be able to report a warning to the
    validation interface (instead of just print it).  It know whether or not the AggregatedErrors should be raised as
    an exception or not, at the end of a script, call object.should_raise().

    A caught exception can be buffered, but you can also buffer an exception class that is constructed outside of a
    try/except block.

    Note, this class annotates the exceptions it buffers.  Each exception is available in the object.exceptions array
    and each exception contains these added data members:

        buffered_tb_str - a string version of a traceback (because a regular traceback will not exist if an
                          exception is not raised).  Note, exceptions with their traces will be printed on
                          standard out unless object.quiet is True.
        is_error        - a boolean indicating whether it is a warning or an exception.
        exc_type_str    - a string ("Warning" or "Error") that can be used in custom reporting.
    """

    # TODO: Figure out how to suppress exception prints during buffer_exception so that you don't see them during tests
    # TODO: Prune the simulated stack trace more and add output that makes it clear it's a simulated trace
    # TODO: Don't reset the current exception number when remove_* is used, because it's confusing to see repeated nums
    def __init__(
        self, message=None, exceptions=None, errors=None, warnings=None, quiet=False
    ):
        if not exceptions:
            exceptions = []
        if not errors:
            errors = []
        if not warnings:
            warnings = []

        self.exceptions = []

        self.num_errors = len(errors)
        self.num_warnings = len(warnings)

        self.is_fatal = False  # Default to not fatal. buffer_exception can change this.
        self.is_error = False  # Default to warning. buffer_exception can change this.

        # Providing exceptions, errors, and warnings is an optional alternative to using the methods: buffer_exception,
        # buffer_error, and buffer_warning.
        for exception in exceptions:
            is_error = True
            # It's possible this was called and supplied exceptions from another AggregatedErrors object
            if hasattr(exception, "is_error"):
                is_error = exception.is_error
            if hasattr(exception, "is_fatal"):
                is_fatal = exception.is_fatal
            else:
                is_fatal = is_error
            if is_fatal:
                self.is_fatal = is_fatal
            if is_error:
                self.is_error = is_error
            if is_error:
                self.num_errors += 1
            else:
                self.num_warnings += 1
            self.exceptions.append(exception)
            self.ammend_buffered_exception(
                exception,
                is_error=is_error,
                is_fatal=is_fatal,
                exc_no=len(self.exceptions),
            )

        for warning in warnings:
            self.exceptions.append(warning)
            self.ammend_buffered_exception(
                warning, len(self.exceptions), is_error=False
            )

        for error in errors:
            self.exceptions.append(error)
            self.ammend_buffered_exception(error, len(self.exceptions), is_error=True)
            self.is_fatal = True
            self.is_error = True

        self.custom_message = False
        if message:
            self.custom_message = True
            current_message = message
        else:
            current_message = self.get_default_message()
        super().__init__(current_message)

        self.buffered_tb_str = self.get_buffered_traceback_string()
        self.quiet = quiet

    def merge_aggregated_errors_object(self, aes_object: AggregatedErrors):
        """
        This is similar to a copy constructor, but instead of copying an existing oject, it merges the contents of the
        supplied object into self
        """
        self.num_errors += aes_object.num_errors
        self.num_warnings += aes_object.num_warnings
        if aes_object.is_fatal:
            self.is_fatal = aes_object.is_fatal
        if aes_object.is_error:
            self.is_error = aes_object.is_error
        self.exceptions += aes_object.exceptions
        if aes_object.custom_message:
            msg = str(self)
            if self.custom_message:
                msg += (
                    "\nAn additional AggregatedErrors object was merged with this one with an additional custom "
                    f"message:\n\n{aes_object.custom_message}"
                )
            else:
                self.custom_message = aes_object.custom_message
                msg = str(aes_object)
        else:
            msg = self.get_default_message()
        super().__init__(msg)
        self.buffered_tb_str += (
            "\nAn additional AggregatedErrors object was merged with this one.  The appended trace is:\n\n"
            + self.get_buffered_traceback_string()
        )
        if aes_object.quiet:
            self.quiet = aes_object.quiet

    def get_exception_type(self, exception_class, remove=False, modify=True):
        """
        This method is provided to retrieve exceptions (if they exist in the exceptions list) from this object and
        return them.

        Args:
            exception_class (Exception): The class of exceptions to remove
            remove (boolean): Whether to remove matching exceptions from this object
            modify (boolean): Whether the convert a removed exception to a warning
        """
        matched_exceptions = []
        unmatched_exceptions = []
        is_fatal = False
        is_error = False
        num_errors = 0
        num_warnings = 0

        # Look for exceptions to remove and recompute new object values
        for exception in self.exceptions:
            if self.exception_matches(exception, exception_class):
                if remove and modify:
                    # Change every removed exception to a non-fatal warning
                    exception.is_error = False
                    exception.is_fatal = False
                matched_exceptions.append(exception)
            else:
                if exception.is_error:
                    num_errors += 1
                else:
                    num_warnings += 1
                if exception.is_fatal:
                    is_fatal = True
                if exception.is_error:
                    is_error = True
                unmatched_exceptions.append(exception)

        if remove:
            self.num_errors = num_errors
            self.num_warnings = num_warnings
            self.is_fatal = is_fatal
            self.is_error = is_error

            # Reinitialize this object
            self.exceptions = unmatched_exceptions
            if not self.custom_message:
                super().__init__(self.get_default_message())

        # Return removed exceptions
        return matched_exceptions

    def modify_exception_type(
        self,
        exception_class,
        is_fatal=None,
        is_error=None,
        attr_name=None,
        attr_val=None,
    ):
        """
        To support consolidation of errors across files (like MissingCompounds, MissingSamples, etc), this method
        is provided to retrieve such exceptions (if they exist in the exceptions list) from this object and return them
        for consolidation.

        If is_fatal is not None, the exception's is_fatal is changed to the supplied boolean value.
        If is_error is not None, the exception's is_error is changed to the supplied boolean value.

        It is assumed that a separate exception will be created that is an error.
        """
        matched_exceptions = []
        num_errors = 0
        num_warnings = 0
        master_is_fatal = False
        master_is_error = False

        # Look for exceptions to remove and recompute new object values
        for exception in self.exceptions:
            if self.exception_matches(
                exception, exception_class, attr_name=attr_name, attr_val=attr_val
            ):
                if is_error is not None:
                    exception.is_error = is_error
                if is_fatal is not None:
                    exception.is_fatal = is_fatal
                matched_exceptions.append(exception)
            if exception.is_error:
                num_errors += 1
            else:
                num_warnings += 1
            if exception.is_fatal:
                master_is_fatal = True
            if exception.is_error:
                master_is_error = True

        self.num_errors = num_errors
        self.num_warnings = num_warnings
        self.is_fatal = master_is_fatal
        self.is_error = master_is_error

        # Reinitialize this object
        if not self.custom_message:
            super().__init__(self.get_default_message())

        # Return removed exceptions
        return matched_exceptions

    def remove_exception_type(self, exception_class, modify=True):
        """
        To support consolidation of errors across files (like MissingCompounds, MissingSamples, etc), this method
        is provided to remove such exceptions (if they exist in the exceptions list) from this object and return them
        for consolidation.

        Args:
            exception_class (Exception): The class of exceptions to remove
            modify (boolean): Whether to convert the removed exception to a warning
        """
        return self.get_exception_type(exception_class, remove=True, modify=modify)

    def get_default_message(self):
        should_raise_message = (
            "  Use the return of self.should_raise() to determine if an exception should be raised before raising "
            "this exception."
        )
        if len(self.exceptions) > 0:
            errtypes = []
            for errtype in [type(e).__name__ for e in self.exceptions]:
                if errtype not in errtypes:
                    errtypes.append(errtype)
            message = f"{len(self.exceptions)} exceptions occurred, including type(s): [{', '.join(errtypes)}]."
            if not self.is_fatal:
                message += f"  This exception should not have been raised.{should_raise_message}"
            message += (
                f"\n{self.get_summary_string()}\nScroll up to see tracebacks for these exceptions printed as they "
                "were encountered."
            )
        else:
            message = f"AggregatedErrors exception.  No exceptions have been buffered.{should_raise_message}"
        return message

    def print_summary(self):
        print(self.get_summary_string())

    def get_summary_string(self):
        sum_str = f"AggregatedErrors Summary ({self.num_errors} errors / {self.num_warnings} warnings):\n\t"
        sum_str += "\n\t".join(
            list(
                map(
                    lambda tpl: (
                        f"EXCEPTION{tpl[0]}({tpl[1].exc_type_str.upper()}): "
                        f"{self.get_exception_summary_string(tpl[1])}"
                    ),
                    enumerate(self.exceptions, start=1),
                )
            ),
        )
        return sum_str

    def get_exception_summary_string(self, buffered_exception):
        """
        Returns a string like:
        "ExceptionType: exception_string"
        """
        return f"{type(buffered_exception).__name__}: {buffered_exception}"

    @classmethod
    def ammend_buffered_exception(
        self, exception, exc_no, is_error=True, is_fatal=None, buffered_tb_str=None
    ):
        """
        This takes an exception that is going to be buffered and adds a few data memebers to it: buffered_tb_str (a
        traceback string), is_error (e.g. whether it's a warning or an exception), and a string that is used to
        classify it as a warning or an error.  The exception is returned for convenience.  The buffered_tb_str is not
        generated here because is can be called out of the context of the exception (see the constructor).
        """
        if buffered_tb_str is not None:
            exception.buffered_tb_str = buffered_tb_str
        elif not hasattr(exception, "buffered_tb_str"):
            exception.buffered_tb_str = None
        exception.is_error = is_error
        exception.exc_type_str = "Warning"
        if is_fatal is not None:
            exception.is_fatal = is_fatal
        else:
            exception.is_fatal = is_error
        if is_error:
            exception.exc_type_str = "Error"
        exception.exc_no = exc_no
        return exception

    def get_buffered_exception_summary_string(
        self, buffered_exception, exc_no=None, numbered=True, typed=True
    ):
        """
        Constructs the summary string using the info stored in the exception by ammend_buffered_exception()
        """
        exc_str = ""
        if numbered:
            if exc_no is None:
                exc_no = buffered_exception.exc_no
            exc_str += f"EXCEPTION{exc_no}({buffered_exception.exc_type_str.upper()}): "
        if typed:
            exc_str += f"{type(buffered_exception).__name__}: "
        exc_str += f"{buffered_exception}"
        return exc_str

    @classmethod
    def get_buffered_traceback_string(cls):
        """
        Creates a pseudo-traceback for debugging.  Tracebacks are only built as the raised exception travels the stack
        to where it's caught.  traceback.format_stack yields the entire stack, but that's overkill, so this loop
        filters out anything that contains "site-packages" so that we only see our own code's steps.  This should
        effectively show us only the bottom of the stack, though there's a chance that intermediate steps could be
        excluded.  I don't think that's likely to happen, but we should be aware that it's a possibility.

        The string is intended to only be used to debug a problem.  Print it inside an except block if you want to find
        the cause of any particular buffered exception.
        """
        return "".join(
            [
                str(step)
                for step in traceback.format_stack()
                if "site-packages" not in step
            ]
        )

    def buffer_exception(
        self, exception, is_error=True, is_fatal=True, orig_exception=None
    ):
        """
        Don't raise this exception. Save it to report later, after more errors have been accumulated and reported as a
        group.  The buffered_exception has a buffered_tb_str and a boolean named is_error added to it.  Returns self so
        that an AggregatedErrors exception can be instantiated, an exception can be added to it, and the return can be
        raised all on 1 line.

        is_fatal tells the AggregatedErrors object that after buffering is complete, the AggregatedErrors exception
        should be raised and execution should stop.  By default, errors will cause AggregatedErrors to be raised and
        warnings will not not, however, in validate mode, warnings are communicated to the validation interface by them
        being raised.
        """

        buffered_tb_str = self.get_buffered_traceback_string()
        buffered_exception = None
        if hasattr(exception, "__traceback__") and exception.__traceback__:
            added_exc_str = "".join(traceback.format_tb(exception.__traceback__))
            buffered_tb_str += f"\nThe above caught exception had a partial traceback:\n\n{added_exc_str}"
            buffered_exception = exception
        elif hasattr(orig_exception, "__traceback__") and orig_exception.__traceback__:
            setattr(exception, "__traceback__", orig_exception.__traceback__)
            added_exc_str = "".join(traceback.format_tb(orig_exception.__traceback__))
            buffered_tb_str += f"\nThe above caught exception had a partial traceback:\n\n{added_exc_str}"
            buffered_exception = exception
        else:
            try:
                raise exception
            except Exception as e:
                buffered_exception = e.with_traceback(e.__traceback__)

        self.exceptions.append(buffered_exception)
        self.ammend_buffered_exception(
            buffered_exception,
            len(self.exceptions),
            is_error=is_error,
            is_fatal=is_fatal,
            buffered_tb_str=buffered_tb_str,
        )

        if buffered_exception.is_error:
            self.num_errors += 1
        else:
            self.num_warnings += 1

        if is_fatal:
            self.is_fatal = True
        if is_error:
            self.is_error = True

        if not self.quiet:
            self.print_buffered_exception(buffered_exception)

        # Update the overview message
        if not self.custom_message:
            super().__init__(self.get_default_message())

        return self

    def buffer_error(self, exception, is_fatal=True, orig_exception=None):
        return self.buffer_exception(
            exception, is_error=True, is_fatal=is_fatal, orig_exception=orig_exception
        )

    def buffer_warning(self, exception, is_fatal=False, orig_exception=None):
        return self.buffer_exception(
            exception, is_error=False, is_fatal=is_fatal, orig_exception=orig_exception
        )

    def print_all_buffered_exceptions(self):
        for exc in self.exceptions:
            self.print_buffered_exception(exc)

    def print_buffered_exception(self, buffered_exception):
        print(self.get_buffered_exception_string(buffered_exception))

    def get_all_buffered_exceptions_string(self):
        return "\n".join(
            list(
                map(
                    lambda exc: self.get_buffered_exception_string(exc), self.exceptions
                )
            )
        )

    def get_buffered_exception_string(self, buffered_exception):
        exc_str = buffered_exception.buffered_tb_str.rstrip() + "\n"
        exc_str += f"{self.get_buffered_exception_summary_string(buffered_exception)}"
        return exc_str

    def should_raise(self):
        return self.is_fatal

    def get_num_errors(self):
        return self.num_errors

    def get_num_warnings(self):
        return self.num_warnings

    def get_exception_types(self):
        exc_dict = dict((type(exc).__name__, type(exc)) for exc in self.exceptions)
        return list(exc_dict.values())

    def exception_type_exists(self, exc_cls):
        return exc_cls in [type(exc) for exc in self.exceptions]

    def exception_matches(self, exception, cls, attr_name=None, attr_val=None):
        # Intentionally looks for exact type (not isinstance).  isinstance is not what we want here, because I cannot
        # separate MissingSamples exceptions from NoSamples exceptions (because NoSamples is a subclass of
        # MissingSamples))
        return type(exception).__name__ == cls.__name__ and (
            attr_name is None
            or (
                hasattr(exception, attr_name)
                and (
                    (
                        not type(attr_val).__name__ == "function"
                        and getattr(exception, attr_name) == attr_val
                    )
                    or (
                        type(attr_val).__name__ == "function"
                        and attr_val(getattr(exception, attr_name))
                    )
                )
            )
        )

    def exception_exists(self, cls, attr_name, attr_val):
        """Returns True if an exception of type cls, containing an attribute with the supplied value has been buffered.

        Args:
            cls (Exception): The Exception class to look for.
            attr_name (str): An attribute the buffered exception class has.
            attr_val (object): The value of the attribute the buffered exception class has.  If this is a function, it
                must take a single argument (the value of the attribute) and return a boolean.
        Exceptions:
            None
        Returns:
            bool
        """
        for exc in self.exceptions:
            if self.exception_matches(exc, cls, attr_name, attr_val):
                return True
        return False

    def remove_matching_exceptions(self, cls, attr_name, attr_val):
        """
        To support consolidation of errors across files (like MissingCompounds, MissingSamples, etc), this method
        is provided to remove such exceptions (if they exist in the exceptions list) from this object and return them
        for consolidation.

        Args:
            cls (Type): The class of exceptions to remove
            attr_name (str): An attribute the buffered exception class has.
            attr_val (object): The value of the attribute the buffered exception class has.  If this is a function, it
                must take a single argument (the value of the attribute) and return a boolean.
        Exceptions:
            None
        Returns (List[Exception]): A list of exceptions of the supplied type, and containing the supplied attribute with
            the supplied value (or with a value that yields true from the supplied value function).
        """
        matched_exceptions = []
        unmatched_exceptions = []
        is_fatal = False
        is_error = False
        num_errors = 0
        num_warnings = 0

        # Look for exceptions to remove and recompute new object values
        for exception in self.exceptions:
            if self.exception_matches(exception, cls, attr_name, attr_val):
                matched_exceptions.append(exception)
            else:
                if exception.is_error:
                    num_errors += 1
                else:
                    num_warnings += 1
                if exception.is_fatal:
                    is_fatal = True
                if exception.is_error:
                    is_error = True
                unmatched_exceptions.append(exception)

        self.num_errors = num_errors
        self.num_warnings = num_warnings
        self.is_fatal = is_fatal
        self.is_error = is_error

        # Reinitialize this object
        self.exceptions = unmatched_exceptions
        if not self.custom_message:
            super().__init__(self.get_default_message())

        # Return removed exceptions
        return matched_exceptions


class ConflictingValueErrors(Exception):
    """Conflicting values for a specific model object from a given file

    Attributes:
        model_name: The name of the model object type (Sample, PeakGroup, etc.)
        conflicting_value_errors: A list of ConflictingValueError exceptions
    """

    def __init__(
        self,
        conflicting_value_errors: list[ConflictingValueError],
    ):
        """Initializes a ConflictingValueErrors exception"""

        message = "Conflicting values encountered during loading:\n"
        conflict_data: Dict[str, dict] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        for cve in conflicting_value_errors:
            # Create a new location string that excludes the column
            cve_loc = generate_file_location_string(sheet=cve.sheet, file=cve.file)
            if cve.rec is None:
                conflict_data[cve_loc]["No record provided"][
                    "No file data provided"
                ].append(cve)
            else:
                mdl = type(cve.rec).__name__
                conflict_data[cve_loc][mdl][str(cve.rec_dict)].append(cve)

        for loc in sorted(conflict_data.keys()):
            message += f"\tDuring the processing of {loc}...\n"
            for mdl in conflict_data[cve_loc].keys():
                message += f"\tCreation of the following {mdl} record(s) encountered conflicts:\n"
                for file_rec_str in conflict_data[cve_loc][mdl].keys():
                    rowstr = ", ".join(
                        summarize_int_list(
                            [
                                cve.rownum
                                for cve in conflict_data[cve_loc][mdl][file_rec_str]
                                if cve is not None and cve.rownum is not None
                            ]
                        )
                    )
                    message += f"\t\tFile record:     {file_rec_str}"
                    if rowstr == "":
                        message += "\n"
                    else:
                        message += f" (on row(s): {rowstr})\n"
                    db_msgs = []
                    for cve in conflict_data[cve_loc][mdl][file_rec_str]:
                        recstr = "Database record not provided"
                        if cve.rec is not None:
                            recstr = str(model_to_dict(cve.rec, exclude=["id"]))
                        db_msg = f"\t\tDatabase record: {recstr}\n"
                        if cve.differences is None or len(cve.differences.keys()) == 0:
                            db_msg += "\t\t\tdifference data unavailable\n"
                        else:
                            for fld in cve.differences.keys():
                                db_msg += (
                                    f"\t\t\t[{fld}] values differ:\n"
                                    f"\t\t\t- database: [{str(cve.differences[fld]['orig'])}]\n"
                                    f"\t\t\t- file:     [{str(cve.differences[fld]['new'])}]\n"
                                )
                        if db_msg not in db_msgs:
                            db_msgs.append(db_msg)
                    message += "".join(db_msgs)
        super().__init__(message)
        self.conflicting_value_errors = conflicting_value_errors


class ConflictingValueError(InfileError, SummarizableError):
    SummarizerExceptionClass = ConflictingValueErrors

    def __init__(
        self,
        rec,
        differences,
        rec_dict=None,
        message=None,
        derived=False,
        **kwargs,
    ):
        """Constructor

        Args:
            rec (Optional[Model]): Matching existing database record that caused the unique constraint violation.
            differences (Optional[Dict(str)]): Dictionary keyed on field name and whose values are dicts whose keys are
                "orig" and "new", and the values are the value of the field in the database and file, respectively.
                Example:
                {
                    "description": {
                        "orig": "the database decription",
                        "new": "the file description",
                }
            rec_dict (dict obf objects): The dict that was (or would be) supplied to Model.get_or_create()
            derived (boolean): Whether the database value was a generated value or not.  Certain fields in the database
                are automatically maintained, and values in the loading file may not actually be loaded, thus
                differences with generated values should be designated as warnings only.
            message (str): The error message.
            kwargs:
                rownum (int): The row or line number with the data that caused the conflict.
                sheet (str): The name of the excel sheet where the conflict was encountered.
                file (str): The name/path of the file where the conflict was encoutnered.
        """
        if not message:
            mdl = "No record provided"
            recstr = "No record provided"
            if rec is not None:
                mdl = type(rec).__name__ if rec is not None else "No record provided"
                recstr = str(model_to_dict(rec, exclude=["id"]))
            message = (
                f"Conflicting field values encountered in %s in {mdl} record "
                f"[{recstr}]:\n"
            )
            if differences is not None:
                for fld in differences.keys():
                    message += (
                        f"\t{fld} in\n"
                        f"\t\tdatabase: [{differences[fld]['orig']}]\n"
                        f"\t\tfile: [{differences[fld]['new']}]\n"
                    )
            else:
                message += "\tDifferences not provided"
            if derived:
                message += (
                    "\nNote, the database field value(s) shown are automatically generated.  The database record may "
                    "nor may not exist.  The value in your file conflicts with the generated value."
                )
        super().__init__(message, **kwargs)
        self.rec = rec  # Model record that conflicts
        self.rec_dict = rec_dict  # Dict created from file
        self.differences = differences


class SaveError(Exception):
    def __init__(self, model_name, rec_name, e):
        message = f"Error saving {model_name} {rec_name}: {type(e).__name__}: {str(e)}"
        super().__init__(message)
        self.model_name = model_name
        self.rec_name = rec_name
        self.orig_err = e


class DupeCompoundIsotopeCombos(Exception):
    def __init__(self, dupe_dict):
        nltab = "\n\t"
        nltabtab = f"{nltab}\t"
        message = "The following duplicate compound/isotope combinations were found in the data:"
        for source in dupe_dict:
            message += f"{nltab}{source} sheet:{nltabtab}"
            message += nltabtab.join(
                list(
                    map(
                        lambda c: f"{c} on row(s): {summarize_int_list(dupe_dict[source][c])}",
                        dupe_dict[source].keys(),
                    )
                )
            )
        super().__init__(message)
        self.dupe_dict = dupe_dict


class DuplicateValueErrors(Exception):
    """
    Summary of DuplicateValues exceptions
    """

    def __init__(self, dupe_val_exceptions: list[DuplicateValues], message=None):
        if not message:
            dupe_dict: Dict[str, dict] = defaultdict(
                lambda: defaultdict(lambda: defaultdict(list))
            )
            for dve in dupe_val_exceptions:
                typ = f" ({dve.addendum})" if dve.addendum is not None else ""
                dupe_dict[dve.loc][str(dve.colnames)][typ].append(dve)
            message = (
                "The following unique column(s) (or column combination(s)) were found to have duplicate occurrences "
                "on the indicated rows:\n"
            )
            for loc in dupe_dict.keys():
                message += f"\t{loc}\n"
                for colstr in dupe_dict[loc].keys():
                    for typ in dupe_dict[loc][colstr].keys():
                        message += f"\t\tColumn(s) {colstr}{typ}"
                        for dve in dupe_dict[loc][colstr][typ]:
                            message += "\n\t\t\t"
                            message += "\n\t\t\t".join(dve.dupdeets)
                        message += "\n"
        super().__init__(message)
        self.dupe_val_exceptions = dupe_val_exceptions


class DuplicateValues(InfileError, SummarizableError):
    SummarizerExceptionClass = DuplicateValueErrors

    def __init__(self, dupe_dict, colnames, message=None, addendum=None, **kwargs):
        """
        Takes a dict whose keys are (composite, unique) strings and the values are lists of row indexes
        """
        if not message:
            # Each value is displayed as "Colname1: [value1], Colname2: [value2], ... (rows*: 1,2,3)" where 1,2,3 are
            # the rows where the combo values are found
            dupdeets = []
            for v, lst in dupe_dict.items():
                # dupe_dict contains row indexes. This converts to row numbers (adds 1 for starting from 1 instead of 0
                # and 1 for the header row)

                # TODO: This type check is a hack.  DuplicateValues is being called sometimes with different dupe_dict
                # structures (originating from either get_column_dupes or get_one_column_dupes).  A refactor made the
                # issue worse.  Before, it was called with a message arg, which avoided the issue.  Now it's not called
                # with a message.  This strategy needs to be consolidated.
                idxs = lst
                if isinstance(lst, dict):
                    idxs = lst["rowidxs"]
                dupdeets.append(
                    f"{str(v)} (rows*: {', '.join(summarize_int_list(list(map(lambda i: i + 2, idxs))))})"
                )
            if len(dupdeets) == 0:
                dupdeets.append("No duplicates data provided")
            nltab = "\n\t"
            message = (
                f"The following unique column (or column combination) {colnames} was found to have duplicate "
                f"occurrences in %s on the indicated rows:{nltab}{nltab.join(dupdeets)}"
            )
            if addendum is not None:
                message += f"\n{addendum}"
        super().__init__(message, **kwargs)
        self.dupe_dict = dupe_dict
        self.colnames = colnames
        self.addendum = addendum
        self.dupdeets = dupdeets


class DuplicateCompoundIsotopes(Exception):
    """
    Summary of DuplicateValues exceptions specific to the PeakAnnotationsLoader.  It removes the sample column, because
    all errors always affect all samples, given the pre-conversion pandas DataFrame, which has a column for each sample
    and compounds and isotopes defined on rows.
    """

    def __init__(
        self,
        dupe_val_exceptions: list[DuplicateValues],
        colnames: list[str],
        message=None,
    ):
        """Constructor.

        Args:
            dupe_val_exceptions (List[DuplicateValues])
            colnames (List[str]): A 2 element list containing the current compound and isotopeLabel column names
        Exceptions:
            Raises:
                ProgrammingError
            Buffers:
                None
        Returns:
            instance
        """
        if not message:
            dupe_dict: Dict[str, dict] = defaultdict(
                lambda: defaultdict(lambda: defaultdict(list))
            )
            for dve in dupe_val_exceptions:
                if not set(colnames) <= set(dve.colnames):
                    raise ProgrammingError(
                        f"The exceptions in dupe_val_exceptions must contain columns: {colnames}, but encountered: "
                        f"{dve.colnames}."
                    )
                typ = f" ({dve.addendum})" if dve.addendum is not None else ""
                dupe_dict[dve.loc][str(colnames)][typ].append(dve)
            message = (
                "The following unique column(s) (or column combination(s)) were found to have duplicate occurrences "
                "on the indicated rows:\n"
            )
            for loc in dupe_dict.keys():
                message += f"\t{loc}\n"
                for colstr in dupe_dict[loc].keys():
                    for typ in dupe_dict[loc][colstr].keys():
                        message += f"\t\tColumn(s) {colstr}{typ}"
                        for dve in dupe_dict[loc][colstr][typ]:
                            message += "\n\t\t\t"
                            message += "\n\t\t\t".join(dve.dupdeets)
                        message += "\n"
        super().__init__(message)
        self.dupe_val_exceptions = dupe_val_exceptions
        self.colnames = colnames


class SheetMergeError(Exception):
    def __init__(self, row_idxs, merge_col_name="Animal Name", message=None):
        if not message:
            message = (
                f"Rows which are missing an {merge_col_name} but have a value in some other column, cause confusing "
                f"errors because the {merge_col_name} is used to merge the data in separate files/excel-sheets.  To "
                "avoid these errors, a row must either be completely empty, or at least contain a value in the merge "
                f"column: [{merge_col_name}].  The following rows are affected, but the row numbers can be inaccurate "
                f"due to merges with empty rows: [{', '.join(summarize_int_list(row_idxs))}]."
            )
        super().__init__(message)
        self.row_idxs = row_idxs
        self.animal_col_name = merge_col_name


class NoTracerLabeledElements(InfileError):
    def __init__(
        self, compound: Optional[str] = None, elements: Optional[list] = None, **kwargs
    ):
        tcrstr = ""
        if elements is not None and len(elements) > 0:
            tcrstr = f" {elements}"
        if compound is not None:
            message = f"PeakGroup compound [{compound}] from %s contains no tracer_labeled_elements{tcrstr}."
        else:
            message = f"No tracer_labeled_elements{tcrstr}."
        super().__init__(message, **kwargs)


class NoTracers(InfileError):
    def __init__(self, animal: Optional[Animal] = None, message=None, **kwargs):
        if message is None:
            animal_str = f" [{animal}]"
            message = f"The Animal{animal_str} associated with %s, has no tracers."
        super().__init__(message, **kwargs)
        self.animal = animal


class IsotopeStringDupe(InfileError):
    """
    There are multiple isotope measurements that match the same parent tracer labeled element
    E.g. C13N15C13-label-2-1-1 would match C13 twice
    """

    def __init__(self, label, parent, **kwargs):
        message = (
            f"Cannot uniquely match tracer labeled element ({parent}) in the measured labeled element string: "
            f"[{label}]."
        )
        super().__init__(message, **kwargs)
        self.label = label
        self.parent = parent


class UnexpectedIsotopes(Exception):
    def __init__(self, detected_isotopes, labeled_isotopes, compounds):
        message = (
            f"Unexpected isotopes detected ({detected_isotopes}) that are not among the tracer labeled elements "
            f"({labeled_isotopes}) for compounds ({compounds}).  There could be contamination."
        )
        super().__init__(message)
        self.detected_isotopes = detected_isotopes
        self.labeled_isotopes = labeled_isotopes
        self.compounds = compounds


class AllMissingTissuesErrors(Exception):
    """
    Takes a list of MissingTissue exceptions and a list of existing tissue names.
    """

    def __init__(self, missing_tissue_errors, message=None):
        if not message:
            err_dict = defaultdict(lambda: defaultdict(list))
            for tissue_error in missing_tissue_errors:
                loc = generate_file_location_string(
                    file=tissue_error.file,
                    sheet=tissue_error.sheet,
                    column=tissue_error.column,
                )
                err_dict[loc][tissue_error.tissue_name].append(tissue_error.rownum)

            nltt = "\n\t\t"
            tissues_str = ""
            for loc in err_dict.keys():
                tissues_str += (
                    f"\t{loc}:\n\t\t"
                    + nltt.join(
                        [
                            f"{k} on row(s): {summarize_int_list(v)}"
                            for k, v in err_dict[loc].items()
                        ]
                    )
                    + "\n"
                )

            message = (
                f"The following tissues, obtained from the indicated file locations, were not found in the database:\n"
                f"{tissues_str}"
                "Please check these against the existing tissues.  If any tissue cannot be renamed to one of the "
                "existing tissues, it will have to be added to the database."
            )

        super().__init__(message)
        self.missing_tissue_errors = missing_tissue_errors


# TODO: Create an AllInfileErrors class using AllMissingTreatments and AllMissingTissues as a template
class AllMissingTreatmentsErrors(Exception):
    """
    Takes a list of MissingTreatment exceptions and a list of existing treatment names.
    """

    def __init__(self, missing_treatment_errors, message=None):
        if not message:
            err_dict = defaultdict(lambda: defaultdict(list))
            for treatment_error in missing_treatment_errors:
                loc = generate_file_location_string(
                    file=treatment_error.file,
                    sheet=treatment_error.sheet,
                    column=treatment_error.column,
                )
                err_dict[loc][treatment_error.treatment_name].append(
                    treatment_error.rownum
                )

            nltt = "\n\t\t"
            treatments_str = ""
            for loc in err_dict.keys():
                treatments_str += (
                    f"\t{loc}:\n\t\t"
                    + nltt.join(
                        [
                            f"{k} on row(s): {summarize_int_list(v)}"
                            for k, v in err_dict[loc].items()
                        ]
                    )
                    + "\n"
                )

            message = (
                f"The following treatments, obtained from the indicated file locations, were not found in the "
                "database:\n"
                f"{treatments_str}"
                "Please check these against the existing treatments.  If any treatment cannot be renamed to one of the "
                "existing treatments, it will have to be added to the database."
            )

        super().__init__(message)
        self.missing_treatment_errors = missing_treatment_errors


class AllMissingCompoundsErrors(Exception):
    """
    This is the same as the MissingCompounds class, but it takes a 3D dict that is used to report every file (and rows
    in that file) where each missing compound exists.
    """

    def __init__(self, compounds_dict, message=None):
        """
        Takes a dict whose keys are compound names and values are dicts containing key/value pairs of: formula/list-of-
        strings and indexes/list-of-ints.
        """
        if not message:
            nltab = "\n\t"
            nltt = f"{nltab}\t"
            cmdps_str = ""
            for compound in compounds_dict.keys():
                if cmdps_str != "":
                    cmdps_str += nltab
                cmdps_str += (
                    f"Compound: [{compound}], Formula: [{compounds_dict[compound]['formula']}], located in the "
                    f"following file(s):{nltt}"
                )
                cmdps_str += nltt.join(
                    list(
                        map(
                            lambda fl: f"{fl} on row(s): {summarize_int_list(compounds_dict[compound]['files'][fl])}",
                            compounds_dict[compound]["files"].keys(),
                        )
                    )
                )
            message = (
                f"{len(compounds_dict.keys())} compounds were not found in the database:{nltab}{cmdps_str}\n"
                "Compounds referenced in the accucor/isocorr files must be loaded into the database before "
                "the accucor/isocorr files can be loaded.  Please take note of the compounds, select a primary name, "
                "any synonyms, and find an HMDB ID associated with the compound to provide with your submission.  "
                "Note, a warning about the missing compounds is cross-referenced under the status of each affected "
                "individual load file containing 1 or more of these compounds."
            )
        super().__init__(message)
        self.compounds_dict = compounds_dict


class MissingCompoundsError(Exception):
    def __init__(self, compounds_dict, message=None):
        """
        Takes a dict whose keys are compound names and values are dicts containing key/value pairs of: formula/list-of-
        strings and indexes/list-of-ints.
        """
        if not message:
            nltab = "\n\t"
            cmdps_str = nltab.join(
                list(
                    map(
                        lambda c: (
                            f"{c} {compounds_dict[c]['formula']} on row(s): "
                            f"{summarize_int_list(compounds_dict[c]['rownums'])}"
                        ),
                        compounds_dict.keys(),
                    )
                )
            )
            message = (
                f"{len(compounds_dict.keys())} compounds were not found in the database:{nltab}{cmdps_str}\n"
                "Compounds referenced in the accucor/isocorr files must be loaded into the database before "
                "the accucor/isocorr files can be loaded.  Please take note of the compounds, select a primary name, "
                "any synonyms, and find an HMDB ID associated with the compound to provide with your submission."
            )
        super().__init__(message)
        self.compounds_dict = compounds_dict


# TODO: Delete when sample_table_loader is removed
class MissingTissue(InfileError):
    def __init__(self, tissue_name, message=None, **kwargs):
        if not message:
            message = f"Tissue '{tissue_name}' in %s was not found in the database"
        super().__init__(message, **kwargs)
        self.tissue_name = tissue_name


class MissingTissues(MissingModelRecords):
    ModelName = "Tissue"
    RecordName = ModelName


class AllMissingTissues(MissingModelRecordsByFile):
    ModelName = "Tissue"
    RecordName = ModelName


# TODO: Delete when sample_table_loader is removed
class MissingTreatment(InfileError):
    def __init__(self, treatment_name, message=None, **kwargs):
        if not message:
            message = (
                f"Treatment '{treatment_name}' in %s was not found in the database.\n"
            )
        super().__init__(message, **kwargs)
        self.treatment_name = treatment_name


class MissingTreatments(MissingModelRecords):
    ModelName = "Protocol"
    RecordName = "Treatment"


class AllMissingTreatments(MissingModelRecordsByFile):
    ModelName = "Protocol"
    RecordName = "Treatment"


class LCMethodFixturesMissing(Exception):
    def __init__(self, message=None, err=None):
        if message is None:
            message = (
                "The LCMethod fixtures defined in [DataRepo/fixtures/lc_methods.yaml] appear to have not been "
                "loaded."
            )
        if err is not None:
            message += f"  The triggering exception was: [{err}]."
        super().__init__(message)
        self.err = err


class ParsingError(Exception):
    pass


class InfusateParsingError(ParsingError):
    pass


class TracerParsingError(ParsingError):
    pass


class IsotopeParsingError(ParsingError):
    pass


class ObservedIsotopeParsingError(InfileError):
    pass


class ObservedIsotopeUnbalancedError(ObservedIsotopeParsingError):
    def __init__(self, elements, mass_numbers, counts, label, **kwargs):
        super().__init__(
            (
                f"Unable to parse the same number of elements ({len(elements)}), mass numbers "
                f"({len(mass_numbers)}), and counts ({len(counts)}) from isotope label: [{label}]."
            ),
            **kwargs,
        )
        self.elements = elements
        self.mass_numbers = mass_numbers
        self.counts = counts
        self.label = label


class MultipleMassNumbers(Exception):
    def __init__(self, labeled_element, mass_numbers):
        message = (
            f"Labeled element [{labeled_element}] exists among the tracer(s) with multiple mass numbers: "
            f"[{','.join(mass_numbers)}]."
        )
        super().__init__(message)
        self.labeled_element = labeled_element
        self.mass_numbers = mass_numbers


class MassNumberNotFound(Exception):
    def __init__(self, labeled_element, tracer_labeled_elements):
        message = (
            f"Labeled element [{labeled_element}] could not be found among the tracer(s) to retrieve its mass "
            "number.  Tracer labeled elements: "
            f"[{', '.join([x['element'] + x['mass_number'] for x in tracer_labeled_elements])}]."
        )
        super().__init__(message)
        self.labeled_element = labeled_element
        self.tracer_labeled_elements = tracer_labeled_elements


class TracerLabeledElementNotFound(Exception):
    pass


class UnexpectedLabels(InfileError):
    def __init__(self, unexpected, possible, **kwargs):
        message = (
            f"Observed peak label(s) {unexpected} were not among the expected labels {possible}.  There may be "
            "contamination."
        )
        super().__init__(message, **kwargs)
        self.possible = possible
        self.unexpected = unexpected


class SampleIndexNotFound(Exception):
    def __init__(self, sheet_name, num_cols, non_sample_colnames):
        message = (
            f"Sample columns could not be identified in the [{sheet_name}] sheet.  There were {num_cols} columns.  At "
            "least one column with one of the following names must immediately preceed the sample columns: "
            f"[{','.join(non_sample_colnames)}]."
        )
        super().__init__(message)
        self.sheet_name = sheet_name
        self.num_cols = num_cols


# TODO: Once the accucor loader is deleted, delete this class.
class CorrectedCompoundHeaderMissing(Exception):
    def __init__(self):
        message = (
            "Compound header [Compound] not found in the accucor corrected data.  This may be an isocorr file.  Try "
            "again and submit this file using the isocorr file upload form input (or add the --isocorr-format option "
            "on the command line)."
        )
        super().__init__(message)


class LCMSDefaultsRequired(Exception):
    def __init__(
        self,
        missing_defaults_list,
        affected_sample_headers_list=None,
    ):
        nlt = "\n\t"
        if (
            affected_sample_headers_list is None
            or len(affected_sample_headers_list) == 0
        ):
            message = (
                "Either an LCMS metadata dataframe or these missing defaults must be provided:\n\n\t"
                f"{nlt.join(missing_defaults_list)}"
            )
        else:
            message = (
                f"These missing defaults are required:\n\n\t"
                f"{nlt.join(missing_defaults_list)}\n\n"
                "because the following sample data headers are missing data in at least 1 of the corresponding "
                "columns:\n\n\t"
                f"{nlt.join(affected_sample_headers_list)}"
            )
        super().__init__(message)
        self.missing_defaults_list = missing_defaults_list
        self.affected_sample_headers_list = affected_sample_headers_list


# TODO: Delete this exception class when the accucor loader is removed.  It is obsolete.
class UnexpectedLCMSSampleDataHeaders(Exception):
    def __init__(self, unexpected, peak_annot_file):
        message = (
            "The following sample data headers in the LCMS metadata were not found among the peak annotation file "
            f"[{peak_annot_file}] headers: [{unexpected}].  Note that if this header is in a different peak annotation "
            "file, that file must be indicated in the peak annotation column (the default is the current file)."
        )
        super().__init__(message)
        self.unexpected = unexpected
        self.peak_annot_file = peak_annot_file


# TODO: Delete this exception class when the accucor loader is removed.  It is obsolete.
class MissingLCMSSampleDataHeaders(Exception):
    def __init__(self, missing, peak_annot_file, missing_defaults):
        using_defaults = len(missing_defaults) == 0
        message = (
            f"The following sample data headers in the peak annotation file [{peak_annot_file}], were not found in the "
            f"LCMS metadata supplied: {missing}.  "
        )
        if using_defaults:
            message += "Falling back to supplied defaults."
        else:
            message += (
                "Either add the sample data headers to the LCMS metadata or provide default values for: "
                f"{missing_defaults}."
            )
        super().__init__(message)
        self.missing = missing
        self.peak_annot_file = peak_annot_file
        self.missing_defaults = missing_defaults


class MissingMZXMLFiles(Exception):
    def __init__(self, mzxml_files):
        message = f"The following mzXML files listed in the LCMS metadata file were not supplied: {mzxml_files}."
        super().__init__(message)
        self.mzxml_files = mzxml_files


class NoMZXMLFiles(Exception):
    def __init__(self):
        message = "mzXML files are required for new uploads."
        super().__init__(message)


class PeakAnnotFileMismatches(Exception):
    def __init__(self, incorrect_pgs_files, peak_annotation_filename):
        bad_files_str = "\n\t".join(
            [
                k + f" [{incorrect_pgs_files[k]} != {peak_annotation_filename}]"
                for k in incorrect_pgs_files.keys()
            ]
        )
        message = (
            "The following sample headers' peak annotation files in the LCMS metadata file do not match the supplied "
            f"peak annotation file [{peak_annotation_filename}]:\n\t{bad_files_str}\n\nPlease ensure that the sample "
            "row in the LCMS metadata matches the supplied peak annotation file."
        )
        super().__init__(message)
        self.incorrect_pgs_files = incorrect_pgs_files
        self.peak_annotation_filename = peak_annotation_filename


class PeakAnnotationParseError(Exception):
    def __init__(
        self, message="Unknown problem attempting to parse peak annotation file"
    ):
        self.message = message
        super().__init__(self.message)


# TODO: Delete this exception class when the accucor loader is removed.  It is obsolete.
class MismatchedSampleHeaderMZXML(Exception):
    def __init__(self, mismatching_mzxmls):
        message = (
            "The following sample data headers do not match any mzXML file names.  No mzXML files will be loaded for "
            "these columns in the peak annotation file:\n\n"
            "\tSample Data Header\tmzXML File Name"
        )
        tab = "\t"
        for details in mismatching_mzxmls:
            message += f"\n\t{tab.join(str(li) for li in details)}"
        super().__init__(message)
        self.mismatching_mzxmls = mismatching_mzxmls


class MzxmlSampleHeaderMismatch(InfileError):
    def __init__(self, header, mzxml_file, **kwargs):
        mzxml_basename, _ = os.path.splitext(os.path.basename(mzxml_file))
        message = (
            f"The sample header does not match the base name of the mzXML file [{mzxml_file}], as listed in %s:\n"
            f"\tSample header:       [{header}]\n"
            f"\tmzXML Base Filename: [{mzxml_basename}]"
        )
        super().__init__(message, **kwargs)
        self.header = header
        self.mzxml_basename = mzxml_basename
        self.mzxml_file = mzxml_file


# TODO: Delete once the accucor and accompanying lcms code is deleted
class DuplicateSampleDataHeaders(Exception):
    def __init__(self, dupes, lcms_metadata, samples):
        cs = ", "
        dupes_str = "\n\t".join(
            [f"{k} rows: [{cs.join(dupes[k])}]" for k in dupes.keys()]
        )
        message = (
            "The following sample data headers were found to have duplicates on the LCMS metadata file on the "
            "indicated rows:\n\n"
            f"\t{dupes_str}"
        )
        super().__init__(message)
        self.dupes = dupes
        # used by code that catches this exception
        self.lcms_metadata = lcms_metadata
        self.samples = samples


class NonUniqueSampleDataHeaders(Exception):
    def __init__(self, nusdh_list: list[NonUniqueSampleDataHeader]):
        """Takes a dupes dict for duplicate sample data headers across 1 or more peak annotation files.

        Args:
            dupes = {
                <value>: {}
                    "<filename> (sheet <sheetname>)": count,
                }
            }
        """
        dupes_str = ""
        for nusdh in nusdh_list:
            dupes_str += f"\t{nusdh.header}\n"
            for file in nusdh.dupes.keys():
                dupes_str += f"\t\tOccurs {nusdh.dupes[file]} times in {file}\n"
        message = (
            "The following sample data headers are not unique across all supplied peak annotation files:\n"
            f"{dupes_str}"
        )
        super().__init__(message)
        self.nusdh_list = nusdh_list


class NonUniqueSampleDataHeader(SummarizableError):
    SummarizerExceptionClass = NonUniqueSampleDataHeaders

    def __init__(self, header, dupes):
        dupes_str = ""
        for file in dupes.keys():
            dupes_str += f"\n\tOccurs {dupes[file]} times in {file}"
        message = (
            f"Sample data header '{header}' is not unique across all supplied peak annotation files:"
            f"{dupes_str}"
        )
        super().__init__(message)
        self.dupes = dupes
        self.header = header


class InvalidHeaders(InfileError, ValidationError):
    def __init__(self, headers, expected_headers=None, fileformat=None, **kwargs):
        if expected_headers is None:
            expected_headers = expected_headers
        message = ""
        file = kwargs.get("file", None)
        if file is not None:
            if fileformat is not None:
                filedesc = f"{fileformat} file "
            else:
                filedesc = "File "
            kwargs["file"] = f"{filedesc} [{file}] "
            message += "%s "
        missing = [i for i in expected_headers if i not in headers]
        unexpected = [i for i in headers if i not in expected_headers]
        if len(missing) > 0:
            message += f"is missing headers {type(missing)}: {missing}"
        if len(missing) > 0 and len(unexpected) > 0:
            message += " and "
        if len(unexpected) > 0:
            message += f" has unexpected headers: {unexpected}"
        super().__init__(message, **kwargs)
        self.headers = headers
        self.expected_headers = expected_headers
        self.missing = missing
        self.unexpected = unexpected


class InvalidLCMSHeaders(InvalidHeaders):
    def __init__(self, headers, expected_headers=None, file=None):
        super().__init__(
            headers,
            expected_headers=expected_headers,
            file=file,
            fileformat="LCMS metadata",
        )


class DuplicateHeaders(ValidationError):
    def __init__(self, dupes, all):
        message = f"Duplicate column headers: {list(dupes.keys())}.  All: {all}"
        for k in dupes.keys():
            message += f"\n\t{k} occurs {dupes[k]} times"
        super().__init__(message)
        self.dupes = dupes
        self.all = all


class DuplicateFileHeaders(ValidationError):
    def __init__(self, filepath, nall, nuniqs, headers):
        message = (
            f"Column headers are not unique in {filepath}. There are {nall} columns and {nuniqs} unique values: "
            f"{headers}"
        )
        super().__init__(message)
        self.filepath = filepath
        self.nall = nall
        self.nuniqs = nuniqs
        self.headers = headers


class InvalidDtypeDict(InfileError):
    def __init__(
        self,
        dtype,
        columns=None,
        message=None,
        **kwargs,
    ):
        if message is None:
            message = (
                f"Invalid dtype dict supplied for parsing %s.  None of its keys {list(dtype.keys())} are present "
                f"in the dataframe, whose columns are {columns}."
            )
        super().__init__(message, **kwargs)
        self.dtype = dtype
        self.columns = columns


class InvalidDtypeKeys(InfileError):
    def __init__(
        self,
        missing,
        columns=None,
        message=None,
        **kwargs,
    ):
        if message is None:
            message = (
                f"Missing dtype dict keys supplied for parsing %s.  These keys {missing} are not present "
                f"in the resulting dataframe, whose available columns are {columns}."
            )
        super().__init__(message, **kwargs)
        self.missing = missing
        self.columns = columns


class DateParseError(InfileError):
    def __init__(self, string, ve_exc, format, **kwargs):
        format = format.replace("%", "%%")
        message = (
            f"The date string {string} obtained from the file did not match the pattern supplied {format}.  This is "
            "likely the result of excel converting a string to a date.  Try editing the data type of the column in "
            f"%s.\nOriginal error: {type(ve_exc).__name__}: {ve_exc}"
        )
        super().__init__(message, **kwargs)
        self.string = string
        self.ve_exc = ve_exc
        self.format = format


class MissingRequiredLCMSValues(Exception):
    def __init__(self, header_rownums_dict):
        head_rows_str = ""
        cs = ", "
        for header in header_rownums_dict.keys():
            head_rows_str += f"\n\t{header}: {cs.join([str(i) for i in header_rownums_dict[header]])}"
        message = f"The following required values are missing on the indicated rows:\n{head_rows_str}"
        super().__init__(message)
        self.header_rownums_dict = header_rownums_dict


class MissingPeakAnnotationFiles(Exception):
    def __init__(
        self, missing_peak_annot_files, unmatching_peak_annot_files=None, lcms_file=None
    ):
        nlt = "\n\t"
        message = (
            f"The following peak annotation files:\n\n"
            f"\t{nlt.join(missing_peak_annot_files)}\n\n"
        )
        if lcms_file is not None:
            message += f"from the LCMS metadata file:\n\n\t{lcms_file}\n\n"
        message += "were not supplied."
        if (
            unmatching_peak_annot_files is not None
            and len(unmatching_peak_annot_files) > 0
        ):
            message += (
                "  The following unaccounted-for peak annotation files in the LCMS metadata were also found:\n"
                f"\t\t{nlt.join(unmatching_peak_annot_files)}\n\nPerhaps there is a typo?"
            )
        super().__init__(message)
        self.missing_peak_annot_files = missing_peak_annot_files
        self.unmatching_peak_annot_files = unmatching_peak_annot_files
        self.lcms_file = lcms_file


class WrongExcelSheet(Exception):
    def __init__(self, file_type, sheet_name, expected_sheet_name, sheet_num):
        message = (
            f"Expected [{file_type}] Excel sheet [{sheet_num}] to be named [{expected_sheet_name}], but got "
            f"[{sheet_name}]."
        )
        super().__init__(message)
        self.file_type = file_type
        self.sheet_name = sheet_name
        self.expected_sheet_name = expected_sheet_name
        self.sheet_num = sheet_num


class ExcelSheetNotFound(InfileError):
    def __init__(self, sheet, file, all_sheets=None):
        avail_msg = "" if all_sheets is None else f"  Available sheets: {all_sheets}."
        message = f"Excel sheet [{sheet}] not found in %s.{avail_msg}"
        super().__init__(message, file=file)
        self.sheet = sheet


class ExcelSheetsNotFound(InfileError):
    def __init__(
        self,
        unknowns,
        all_sheets,
        source_file,
        source_column,
        source_sheet,
        message=None,
    ):
        if message is None:
            deets = "\n\t".join(
                [
                    f"[{k}] on rows: " + str(summarize_int_list(v))
                    for k, v in unknowns.items()
                ]
            )
            message = (
                f"The following excel sheet(s) parsed from %s on the indicated rows were not found.\n"
                f"\t{deets}\n"
                f"The available sheets are: [{all_sheets}]."
            )
        super().__init__(
            message, file=source_file, sheet=source_sheet, column=source_column
        )
        self.unknowns = unknowns
        self.all_sheets = all_sheets
        self.source_file = source_file
        self.source_column = source_column
        self.source_sheet = source_sheet


class InvalidHeaderCrossReferenceError(Exception):
    def __init__(
        self,
        source_file,
        source_sheet,
        column,
        unknown_headers,
        target_file,
        target_sheet,
        target_headers,
        message=None,
    ):
        if message is None:
            src_loc = generate_file_location_string(
                sheet=source_sheet, file=source_file, column=column
            )
            tgt_loc = generate_file_location_string(
                sheet=target_sheet, file=target_file
            )
            deets = "\n\t".join(
                [
                    f"[{k}] on row(s): " + str(summarize_int_list(v))
                    for k, v in unknown_headers.items()
                ]
            )
            message = (
                f"The following column-references parsed from {src_loc}:\n"
                f"\t{deets}\n"
                f"were not found in {tgt_loc}, which has the following columns:\n"
                f"\t{', '.join(target_headers)}."
            )
        super().__init__(message)
        self.source_file = source_file
        self.source_sheet = source_sheet
        self.column = column
        self.unknown_headers = unknown_headers
        self.target_file = target_file
        self.target_sheet = target_sheet
        self.target_headers = target_headers


class NoConcentrations(Exception):
    pass


class UnanticipatedError(Exception):
    def __init__(self, type, e):
        message = f"{type}: {str(e)}"
        super().__init__(message)


class SampleError(UnanticipatedError):
    pass


class TissueError(UnanticipatedError):
    pass


class TreatmentError(UnanticipatedError):
    pass


class LCMSDBSampleMissing(Exception):
    def __init__(self, lcms_samples_missing):
        nlt = "\n\t"
        message = (
            "The following sample names from the LCMS metadata are missing in the animal sample table:\n\t"
            f"{nlt.join(lcms_samples_missing)}"
        )
        super().__init__(message)
        self.lcms_samples_missing = lcms_samples_missing


class MixedPolarityErrors(Exception):
    def __init__(self, mixed_polarity_dict):
        deets = []
        for filename in mixed_polarity_dict.keys():
            deets.append(
                f"{filename}: {mixed_polarity_dict['filename']['first']} vs "
                f"{mixed_polarity_dict['filename']['different']} in scan {mixed_polarity_dict['filename']['scan']}"
            )
        nlt = "\n\t"
        message = (
            "The following mzXML files have multiple polarities, which is unsupported:\n\t"
            f"{nlt.join(deets)}"
        )
        super().__init__(message)
        self.mixed_polarity_dict = mixed_polarity_dict


class MzxmlConflictErrors(Exception):
    def __init__(self, mzxml_conflicts):
        deets = []
        for mzxml_file in mzxml_conflicts.keys():
            val = f"{mzxml_file}:\n"
            for var in mzxml_conflicts[mzxml_file].keys():
                val += (
                    f"\t\t{var}: {mzxml_conflicts[mzxml_file][var]['mzxml_value']} vs LCMS "
                    f"row [{mzxml_conflicts[mzxml_file][var]['sample_header']}]: "
                    f"{mzxml_conflicts[mzxml_file][var]['lcms_value']}\n"
                )
            deets.append(val)
        nlt = "\n\t"
        message = (
            "The following mzXML files have al least 1 value that differs from the value supplied in the LCMS metadata "
            f"file:\n\t{nlt.join(deets)}"
        )
        super().__init__(message)
        self.mzxml_conflicts = mzxml_conflicts


class InfileDatabaseError(InfileError):
    def __init__(self, exception, rec_dict, **kwargs):
        if rec_dict is not None:
            nltab = "\n\t"
            deets = [f"{k}: {v}" for k, v in rec_dict.items()]
        message = f"{type(exception).__name__} in %s"
        if rec_dict is not None:
            message += f", creating record:\n\t{nltab.join(deets)}"
        message += f"\n\t{type(exception).__name__}: {exception}"
        super().__init__(message, **kwargs)
        self.exception = exception
        self.rec_dict = rec_dict


class MzxmlParseError(Exception):
    pass


# TODO: Delete this exception class when the accucor loader is removed.  Errors in this situation are now presented as
# having only a single peak group representation of a compound per sequence/sample
class AmbiguousMSRun(InfileError):
    def __init__(self, pg_rec, peak_annot1, peak_annot2, **kwargs):
        message = (
            f"When processing the peak data located in %s for sample [{pg_rec.msrun_sample.sample}] and compound(s) "
            f"{pg_rec.name}, a duplicate peak group was found that was linked to MSRunSample: "
            f"{model_to_dict(pg_rec.msrun_sample)}, but the peak annotation file it was loaded from [{peak_annot1}] "
            f"was not the same as the current load file: [{peak_annot2}].  Either this is true duplicate peak data and "
            "should be removed from this file or this data is a different scan (polarity and/or scan range), in which "
            "case, both files should be loaded with a distinct polarity, mz_min, and mz_max.  If the mzXML file is "
            "unavailable, mz_min and mz_max can be approximated by using the medMz column from the accucor or isocorr "
            "data."
        )
        super().__init__(message, **kwargs)
        self.pg_rec = pg_rec
        self.peak_annot1 = peak_annot1
        self.peak_annot2 = peak_annot2


# TODO: Delete this exception class when the accucor loader is removed.  Errors in this situation are now presented as
# having only a single peak group representation of a compound per sequence/sample
class AmbiguousMSRuns(Exception):
    def __init__(self, ambig_dict, infile):
        deets = ""
        for orig_file in ambig_dict.keys():
            deets += f"\tAmbiguous MSRun details between current [{infile}] and original [{orig_file}] load files:\n"
            for amsre in ambig_dict[orig_file].values():
                deets += (
                    f"\t\tSample [{amsre.pg_rec.msrun_sample.sample}] "
                    f"PeakGroup [{amsre.pg_rec.name}] "
                    f"MSRun Polarity [{amsre.pg_rec.msrun_sample.polarity}] "
                    f"MSRun MZ Min [{amsre.pg_rec.msrun_sample.mz_min}] "
                    f"MSRun MZ Max [{amsre.pg_rec.msrun_sample.mz_max}]\n"
                )
        message = (
            f"When processing the peak data located in {infile}, duplicate peak groups were found that link to "
            "existing MSRunSample records, but the peak annotation file the original peak groups were loaded from were "
            "not the same as the current load file.  Either they are true duplicate peak groups and should be removed "
            "from this file or this data represents a different scan (polarity and/or scan range), in which case, both "
            "files should be loaded with a distinct polarity or mz_min and mz_max.  If the mzXML file is unavailable, "
            "mz_min and mz_max can be approximated by using the medMz column from the accucor or isocorr data.  The "
            "conflicting MSRunSample records below were encountered associated with the following table data:\n"
            f"{deets}"
            "Use --polarity, --mz-min, and --mz-max to set different MSRun characteristics for an entire peak "
            "annotations file or set per sample (header) values in the --lcms-file."
        )
        super().__init__(message)
        self.ambig_dict = ambig_dict
        self.infile = infile


class MultiplePeakGroupRepresentations(ValidationError):
    def __init__(self, new_rec, existing_recs):
        """MultiplePeakGroupRepresentations constructor.

        Args:
            new_rec (PeakGroup): An uncommitted record.
            existing_recs (PeakGroup.QuerySet)
        """
        leader = "Existing: "
        from_files = [r.peak_annotation_file.filename for r in existing_recs.all()]
        from_str = f"\n\t{leader}".join(from_files)
        message = (
            f"Multiple representations of this peak group were encountered:\n"
            f"\tcompound: {new_rec.name}\n"
            f"\tMSRunSequence: {new_rec.msrun_sample.msrun_sequence}\n"
            f"\tSample: {new_rec.msrun_sample.sample}\n"
            "Each peak group originated from:\n"
            f"\tProposed: {new_rec.peak_annotation_file.filename}\n"
            f"\t{leader}{from_str}\n"
            "Only 1 representation of a compound per sequence and sample is allowed.  "
        )
        super().__init__(message, code="MultiplePeakGroupRepresentations")
        self.new_rec = new_rec
        self.existing_recs = existing_recs


class CompoundSynonymExists(Exception):
    def __init__(self, syn):
        message = f"CompoundSynonym [{syn}] already exists."
        super().__init__(message)
        self.syn = syn


class CompoundExistsAsMismatchedSynonym(Exception):
    def __init__(self, name, compound_dict, conflicting_syn_rec):
        # Don't report the ID - it is arbitrary, so remove it from the record dicts
        excludes = ["id"]
        conflicting_syn_cpd_dict = {
            k: v
            for k, v in model_to_dict(conflicting_syn_rec.compound).items()
            if k not in excludes
        }

        nltt = "\n\t\t"
        message = (
            f"The compound name being loaded ({name}) already exists as a synonym, but the compound being loaded does "
            "not match the compound associated with the synonym in the database:\n"
            f"\tTo be loaded: {compound_dict}\n"
            f"\tExisting rec: {conflicting_syn_cpd_dict}\n"
            f"\t\twith existing synonyms:\n"
            f"\t\t{nltt.join(str(r) for r in conflicting_syn_rec.compound.synonyms.all())}\n"
            "Please make sure this synonym isn't being associated with different compounds.  Either fix the compound "
            "data in the load to match, or remove this synonym."
        )

        super().__init__(message)
        self.name = name
        self.compound_dict = compound_dict
        self.conflicting_cpd_rec = conflicting_syn_rec


class SynonymExistsAsMismatchedCompound(Exception):
    def __init__(self, name, compound, conflicting_cpd_rec):
        # Don't report the ID - it is arbitrary, so remove it from the record dicts
        excludes = ["id"]
        compound_dict = {
            k: v for k, v in model_to_dict(compound).items() if k not in excludes
        }
        conflicting_cpd_dict = {
            k: v
            for k, v in model_to_dict(conflicting_cpd_rec).items()
            if k not in excludes
        }

        message = (
            f"The compound synonym being loaded ({name}) already exists as a compound name, but that existing "
            "compound record does not match the compound associated with the synonym in the load data:\n"
            f"\tTo be loaded: {compound_dict}\n"
            f"\tExisting rec: {conflicting_cpd_dict}\n"
            "Please make sure this synonym isn't being associated with different compounds.  Either fix the compound "
            "data in the load to match, or remove this synonym."
        )

        super().__init__(message)
        self.name = name
        self.compound = compound
        self.conflicting_cpd_rec = conflicting_cpd_rec


class OptionsNotAvailable(ProgrammingError):
    """
    An exception class for methods that retrieve command line options, called too early.
    """

    def __init__(self):
        super().__init__(
            "Cannot get command line option values until handle() has been called."
        )


class MutuallyExclusiveOptions(CommandError):
    pass


class MutuallyExclusiveArgs(InfileError):
    pass


class RequiredOptions(CommandError):
    def __init__(self, missing, **kwargs):
        message = f"Missing required options: {missing}."
        super().__init__(message, **kwargs)
        self.missing = missing


class ConditionallyRequiredOptions(CommandError):
    pass


class ConditionallyRequiredArgs(InfileError):
    pass


class NoLoadData(Exception):
    pass


class NotATableLoader(TypeError):
    def __init__(self, command_inst):
        here = f"{type(command_inst).__module__}.{type(command_inst).__name__}"
        message = f"Invalid attribute [{here}.loader_class] TableLoader required, {type(command_inst).__name__} set"
        super().__init__(message)
        self.command_inst = command_inst


class CompoundDoesNotExist(InfileError, ObjectDoesNotExist):
    def __init__(self, name, **kwargs):
        message = f"Compound [{name}] from %s does not exist as either a primary compound name or synonym."
        super().__init__(message, **kwargs)
        self.name = name


class MultipleRecordsReturned(InfileError, MultipleObjectsReturned):
    def __init__(self, model, query_dict, message=None, **kwargs):
        if message is None:
            message = f"{model.__name__} record matching {query_dict} from %s returned multiple records."
        super().__init__(message, **kwargs)
        self.query_dict = query_dict
        self.model = model


class MissingDataAdded(InfileError):
    """Use this for warnings only, for when missing data exceptions were dealt with."""

    def __init__(self, addition_notes=None, **kwargs):
        message = "Missing data "
        if addition_notes is not None:
            message += f"{addition_notes} "
        message += "was added to %s."
        super().__init__(message, **kwargs)


class RollbackException(Exception):
    """This class only exists in order to be raised after specific exception handling has already occurred and an
    exception needs to be raised in order to trigger a rollback.  Often times, the exception handling is prefereable to
    co-locate with the code that attempts the database load, and it is done inside a method that is calledf from a loop
    on an input file so that exceptions can be safely handled and buffered (for later raising) in order to be able to
    proceed and report as many errors as possible to reduce time-consuming re-loads just to get the next error.
    """

    pass


class TracerGroupsInconsistent(ValidationError):
    def __init__(self, new_infusate, dupe_infusates, group_name_mismatches):
        message = (
            f"Validation error(s) adding new infusate: [{new_infusate}] with group name: "
            f"[{new_infusate.tracer_group_name}]:\n\t"
        )

        if len(dupe_infusates) == 1:
            message += (
                f"Infusate exists with the same tracers at the same concentrations already: "
                f"[{dupe_infusates[0]}]\n"
                "\tSuggested resolution: Use the existing infusate instead of creating a duplicate.\n"
            )
        elif len(dupe_infusates) > 1:
            message += "Multiple infusates exist with the same tracers at the same concentrations already:\n\t\t"
            message += "\n\t\t".join(dupe_infusates)
            message += "\n\tSuggested resolution: Duplicate records should be consolidated and re-used.\n"

        if len(group_name_mismatches) > 0:
            first_tgn = group_name_mismatches[0].tracer_group_name
            num_diff = len(
                [
                    inf
                    for inf in group_name_mismatches
                    if inf.tracer_group_name != first_tgn
                ]
            )
            if num_diff == 0:
                message += (
                    f"Infusate(s) (like [{group_name_mismatches[0]}]) exist with a different tracer group name: "
                    f"[{group_name_mismatches[0].tracer_group_name}]\n"
                    "\tSuggested resolution: Either edit your tracer group name to match the existing record, or if "
                    "you want to change the tracer group name for this assortment of tracers, edit the existing "
                    f"{len(group_name_mismatches)} records' tracer group name to match the new name (or lack there-of)."
                    "\n"
                )
            else:
                message += "Infusates exist with inconsistent tracer group names:\n"
                message += "\n\t".join(group_name_mismatches)
                message += (
                    "\n\tSuggested resolution: Existing infusate records with inconsistent tracer group names should "
                    "be edited to match and the tracer group name should be re-used.\n"
                )
                message += "\n\t\t".join(group_name_mismatches)
        super().__init__(message)
        self.new_infusate = new_infusate
        self.dupe_infusates = dupe_infusates
        self.group_name_mismatches = group_name_mismatches


def generate_file_location_string(column=None, rownum=None, sheet=None, file=None):
    loc_str = ""
    if column is not None:
        loc_str += f"column [{column}] "
    if loc_str != "" and rownum is not None:
        loc_str += "on "
    if rownum is not None:
        loc_str += f"row [{rownum}] "
    if loc_str != "" and sheet is not None:
        loc_str += "of "
    if sheet is not None:
        loc_str += f"sheet [{sheet}] "
    if loc_str != "":
        loc_str += "in "
    if file is not None:
        loc_str += file
    else:
        loc_str += "the load file data"
    return loc_str


def summarize_int_list(intlist):
    """
    This method was written to make long lists of row numbers more palatable to the user.
    Turns [1,2,3,5,6,9] into ['1-3','5-6','9']
    """
    sum_list = []
    last_num = None
    waiting_num = None
    for num in [n for n in sorted([i for i in intlist if i is not None])]:
        try:
            num = int(num)
        except ValueError:
            # Assume this is a "named" row
            if num not in sum_list:
                sum_list.append(num)
            continue
        if last_num is None:
            waiting_num = num
        else:
            if num > last_num + 1:
                if last_num == waiting_num:
                    sum_list.append(str(waiting_num))
                else:
                    sum_list.append(f"{str(waiting_num)}-{str(last_num)}")
                waiting_num = num
        last_num = num
    if waiting_num is not None:
        if last_num == waiting_num:
            sum_list.append(str(waiting_num))
        else:
            sum_list.append(f"{str(waiting_num)}-{str(last_num)}")
    return sum_list
