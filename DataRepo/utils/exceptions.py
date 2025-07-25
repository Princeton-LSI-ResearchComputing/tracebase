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

if TYPE_CHECKING:
    from DataRepo.models.animal import Animal
    from DataRepo.models.msrun_sequence import MSRunSequence
    from DataRepo.models.peak_group import PeakGroup
    from DataRepo.models.peak_group_label import PeakGroupLabel
    from DataRepo.models.sample import Sample


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
            self
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
            sup_phs = []
            for locarg in self.location_args:
                if getattr(self, locarg) is not None and locarg not in order:
                    missing_loc_arg_placeholders.append(locarg)
                elif getattr(self, locarg) is not None:
                    sup_phs.append(locarg)
            if "loc" not in order and len(order) != len(self.location_args):
                nphs = message.count("%s")
                order.append("loc")
                if message.count("%s") != len(order):
                    raise ProgrammingError(
                        f"The error message string does not contain the same number of placeholders ('%s': {nphs}) as "
                        f"the number of location values supplied {sup_phs}.  You must either provide all location "
                        f"arguments in your order list: {self.location_args} or provide an extra '%s' in your message "
                        f"for each part of the leftover location information ({missing_loc_arg_placeholders})."
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
            try:
                message = message % self.loc
            except TypeError:
                if not message.endswith("\n"):
                    message += "  "
                message += "Location: %s."

        if suggestion is not None:
            if message.endswith("\n"):
                message += suggestion
            else:
                message += f"\n{suggestion}"

        self.message = message
        self.suggestion = suggestion

        return self

    def __str__(self):
        return self.message


class SummarizableError(Exception, ABC):
    @property
    @abstractmethod
    def SummarizerExceptionClass(self):
        """An exception class that takes a list of Exceptions of derived exception classes of this class as the sole
        required positional argument to its constructor.  All keyword arguments are ignored (if they exist).

        Usage example:
            # Create a summarizer Exception class - code it however you want
            class MyExceptionSummarier(Exception):
                def __init__(self, exceptions: List[MyException]):
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


class SummarizedInfileError:
    """This class will break up a list of InfileError exceptions into a dict keyed on the generated file location
    (including the file, sheet, and column).  It is intended to be used in a derived class to sort exceptions into
    sections grouped by file.  Note, it does not call super.__init__, because it is intended to aid in the construction
    of the exception message, so you must multiply inherit to generate the exception message.  Example usage:

        class MyException(SummarizedInfileError, Exception):
            def __init__(
                self,
                exceptions: list[MySummarizableInfileErrorExceptions],
            ):
                SummarizedInfileError.__init__(self, exceptions)
                exc: InfileError
                for loc, exc_list in self.file_dict.items():
                    # Build message
                Exception.__init__(self, message)
    """

    def __init__(self, exceptions):
        """Constructor.

        Args:
            self (SummarizedInfileError)
            exceptions (List[InfileError])
        Exceptions:
            None
        Returns:
            instance (SummarizedInfileError)
        """
        file_dict = defaultdict(list)
        exc: InfileError
        for exc in exceptions:
            loc = generate_file_location_string(
                file=exc.file, sheet=exc.sheet, column=exc.column
            )
            file_dict[loc].append(exc)
        self.file_dict = file_dict


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
                message += f"\t\tColumn: [{colname}] on row(s): {deets}\n"

        # Append a suggestion
        message += (
            "Errors like this only happen when related data failed to load and is evidenced by the fact that the "
            "indicated column/rows have values.  Fixing errors above this will fix this error."
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
            message = f"Value required for '{field_name}' in %s."
            if rownum is not None:
                message += f"  Record extracted from row: {rownum}."

        if "suggestion" not in kwargs.keys():
            suggestion = (
                "This error only happens when related data failed to load.  Fixing errors above this one will fix this "
                "error."
            )
            kwargs["suggestion"] = suggestion

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


class FileFromInputNotFound(InfileError):
    """This is for reporting file names parsed from a file that could not be found."""

    def __init__(self, filepath: str, message=None, tmpfile=None, **kwargs):
        if message is None:
            msg = ""
            if tmpfile is not None and filepath != tmpfile:
                msg = f" (using temporary file path: '{tmpfile}')"
            message = f"File not found: '{filepath}'{msg}, as parsed from %s."
        super().__init__(message, **kwargs)
        self.filepath = filepath


class UnknownHeader(InfileError, HeaderError):
    def __init__(self, unknown, known: Optional[list] = None, message=None, **kwargs):
        if not message:
            message = f"Unknown header encountered: [{unknown}] in %s."
            if known is not None:
                message += f"  Must be one of {known}."
        super().__init__(message, **kwargs)


class UnknownHeaders(InfileError, HeaderError):
    def __init__(self, unknowns, message=None, **kwargs):
        if not message:
            message = f"Unknown header(s) encountered: [{', '.join(unknowns)}] in %s."
        super().__init__(message, **kwargs)
        self.unknowns = unknowns


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
        nre_dict: Dict[str, dict] = defaultdict(lambda: defaultdict(list))
        known: List[str] = []
        for nre in new_researcher_exceptions:
            if len(known) == 0:
                if nre.known is not None:
                    known = nre.known
            elif set(known) != set(nre.known):
                raise ProgrammingError(
                    "Different sets of known researchers encountered."
                )
            file_loc = generate_file_location_string(
                file=nre.file, sheet=nre.sheet, column=nre.column
            )
            if nre.rownum is not None:
                nre_dict[nre.researcher][file_loc].append(nre.rownum)
            elif "unreported rows" not in nre_dict[file_loc]:
                nre_dict[nre.researcher][file_loc].append("unreported rows")
        existing = "\n\t".join(known)
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

    def __init__(self, researcher: str, known=None, message=None, **kwargs):
        from DataRepo.models.researcher import Researcher

        if known is None:
            existing = "\n\t".join(Researcher.get_researchers())
        else:
            existing = "\n\t".join(known)
        message = f"A new researcher [{researcher}] is being added (parsed from %s)."
        if existing != "":
            message += (
                "  Please check the existing researchers to ensure this researcher name isn't a variant of an existing "
                f"name:\n\t{existing}"
            )
        super().__init__(message, **kwargs)
        self.researcher = researcher
        self.known = known


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

        tmp_message = (
            "The following records were missing either because they were not in the corresponding sheets or because "
            "errors were encountered during their load attempt that prevented their creation (in which case you will "
            "see those errors, likely above this one):\n"
        )
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
            # Group by file, sheet, and column - we will summarize the row numbers elsewhere
            inst_loc = generate_file_location_string(
                file=inst.file, sheet=inst.sheet, column=inst.column
            )
            exceptions_by_model_query_and_loc[inst.model.__name__][search_terms][
                inst_loc
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
                f"{len(self.exceptions_by_model_and_query[self.ModelName].keys())} {self.ModelName} records matching "
                f"the following values in %s were not found in the database:{nltab}{summary}\n"
            )

        self.orig_message = message
        self.search_terms = list(
            self.exceptions_by_model_and_query[self.ModelName].keys()
        )
        self.exceptions_by_query = self.exceptions_by_model_and_query[self.ModelName]
        self.set_formatted_message(**kwargs)


class MissingModelRecordsByFile(MissingRecords, ABC):
    """Keeps track of missing records for one model across multiple files"""

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
        num_examples = 3
        if message is None:
            nltt = "\n\t\t"
            summary = ""
            for i, terms_str in enumerate(
                sorted(
                    self.exceptions_by_model_query_and_loc[self.ModelName].keys(),
                    key=str.casefold,
                )
            ):
                loc_dict: dict = self.exceptions_by_model_query_and_loc[self.ModelName][
                    terms_str
                ]
                summary += "\n\t"
                summary += terms_str
                if not succinct:
                    summary += nltt
                    summary += nltt.join(
                        [
                            f"{loc}, row(s): ["
                            + ", ".join(
                                summarize_int_list(
                                    [exc.rownum for exc in loc_dict[loc]]
                                )
                            )
                            + "]"
                            for loc in sorted(loc_dict.keys(), key=str.casefold)
                        ]
                    )
                elif i >= num_examples - 1:
                    break
            if succinct:
                message = (
                    f"{len(exceptions)} {self.ModelName} records matching the following values were not found in the "
                    f"database while processing %s:{summary}"
                )
                if num_examples < len(
                    self.exceptions_by_model_query_and_loc[self.ModelName].keys()
                ):
                    message += "\n\t..."
                message += f"\nSee exceptions below for all missing {self.ModelName} record details."
            else:
                message = (
                    f"{len(exceptions)} {self.ModelName} records matching the following values were not found in the "
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
            model: (Type[Model])
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
                    "The instances argument must be a list of RecordDoesNotExist exceptions generated from queries of "
                    f"the same model.  {inst.model} != {model}"
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
                        "The instances argument must be a list of RecordDoesNotExist exceptions generated from queries "
                        f"of the same file/column.  {cur_loc_args} != {loc_args}"
                    )

            query_fields_str = inst._get_query_stub()

            if fields_str is None:
                fields_str = query_fields_str
            elif fields_str != query_fields_str:
                raise ProgrammingError(
                    "The instances argument must be a list of RecordDoesNotExist exceptions generated from queries "
                    f"using the same search fields (and comparators).  {fields_str} != {query_fields_str}"
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


class UnskippedBlanks(MissingSamples):
    def __init__(
        self,
        exceptions: List[RecordDoesNotExist],
        **kwargs,
    ):
        super().__init__(exceptions, **kwargs)
        message = kwargs.pop("message", None)
        nlt = "\n\t"
        if message is None:
            message = (
                f"{len(exceptions)} sample(s) from %s, that appear to possibly be blanks, are missing in the database: "
                f"{nlt}{nlt.join(self.search_terms)}"
            )
        suggestion = kwargs.pop("suggestion", None)
        if suggestion is None:
            suggestion = (
                "\nBe sure to set the skip column in the Peak Annotation Details sheet to 'true' for blank "
                "samples."
            )
        self.orig_message = message
        self.set_formatted_message(suggestion=suggestion, **kwargs)


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


class UnexpectedInput(InfileError):
    def __init__(
        self,
        value,
        message=None,
        **kwargs,
    ):
        if message is None:
            message = (
                f"Unexpected input supplied: '{str(value).replace('%', '%%')}' in %s."
            )
        super().__init__(message, **kwargs)
        self.value = value


class UnexpectedSamples(InfileError):
    def __init__(
        self,
        missing_samples,
        rel_file,
        rel_sheet,
        rel_column,
        suggestion=None,
        possible_blanks=False,
        **kwargs,
    ):
        if missing_samples is None or len(missing_samples) == 0:
            raise RequiredArgument(
                "missing_samples",
                type(self).__name__,
                message="A non-zero sized list is required.",
            )
        message = kwargs.pop("message", None)
        if message is None:
            rel_loc = generate_file_location_string(
                file=rel_file, sheet=rel_sheet, column=rel_column
            )
            blnkmsg = ", that appear to possibly be blanks," if possible_blanks else ""
            message = (
                f"According to the values in {rel_loc}, the following sample data headers{blnkmsg} should be in %s, "
                f"but they were not there: {missing_samples}."
            )
            if not possible_blanks:
                message += "  This can likely be fixed by changing the file associated with the headers."
        if suggestion is not None:
            message += f"  {suggestion}"
        super().__init__(message, **kwargs)
        self.missing_samples = missing_samples


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


class DryRun(Exception):
    """
    Exception thrown during dry-run to ensure atomic transaction is not committed
    """

    def __init__(self, message=None):
        if message is None:
            message = "Dry Run Complete."
        super().__init__(message)


class MultiLoadStatus(Exception):
    """
    This class holds the load status of multiple files and also can contain multiple file group statuses, e.g. a
    discrete list of missing compounds across all files.  It is defined as an Exception class so that it being raised
    from (for example) load_study will convey the load statuses to the validation interface.
    """

    def __init__(self, load_keys=None, debug=False):
        self.statuses = {}
        # Initialize the load status of all load keys (e.g. file names).  Note, you can create arbitrary keys for group
        # statuses, e.g. for AllMissingCompounds errors that consolidate all missing compounds
        if load_keys:
            for load_key in load_keys:
                self.init_load(load_key)
        self.debug = debug

    def clear_load(self):
        self.statuses = {}

    def init_load(self, load_key):
        if isinstance(load_key, str):
            self.statuses[load_key] = {
                "aggregated_errors": None,
                "state": "PASSED",
                "top": True,  # Passing files will appear first
            }
        elif isinstance(load_key, list):
            for lk in load_key:
                self.statuses[lk] = {
                    "aggregated_errors": None,
                    "state": "PASSED",
                    "top": True,  # Passing files will appear first
                }
        else:
            raise ProgrammingError(
                f"Invalid load_key type: {type(load_key).__name__}.  Only str & list are accepted."
            )

    def update_load(self, load_key):
        if isinstance(load_key, str):
            if load_key not in self.statuses.keys():
                self.statuses[load_key] = {
                    "aggregated_errors": None,
                    "state": "PASSED",
                    "top": True,  # Passing files will appear first
                }
        elif isinstance(load_key, list):
            for lk in load_key:
                if lk not in self.statuses.keys():
                    self.statuses[lk] = {
                        "aggregated_errors": None,
                        "state": "PASSED",
                        "top": True,  # Passing files will appear first
                    }
        else:
            raise ProgrammingError(
                f"Invalid load_key type: {type(load_key).__name__}.  Only str & list are accepted."
            )

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
        """Determine the "is_valid" state on the fly, to simplify the determination, given the possibility of fixing and
        removing of exceptions, which means that this can be relied on before having updated the entire object's state.

        Args:
            None
        Exceptions:
            None
        Returns:
            is_valid (boolean)
        """
        for lk in self.statuses.keys():
            # Any exception (warning or error) can result in an invalid load state (either FAILED or WARNING), depending
            # on whether the exception is marked as fatal - and that depends on the validate mode in the load classes.
            if (
                "aggregated_errors" in self.statuses[lk].keys()
                and self.statuses[lk]["aggregated_errors"] is not None
                and self.statuses[lk]["aggregated_errors"].should_raise()
            ):
                if self.debug:
                    print(
                        f"The AggregatedErrorsSet exception is fatal because the '{lk}' load key's AggregatedErrors "
                        "object contains the following fatal exceptions:"
                    )
                    for exc in self.statuses[lk]["aggregated_errors"].exceptions:
                        print(
                            f"\t{type(exc).__name__} is_error: {exc.is_error} is_fatal: {exc.is_fatal}"
                        )
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
            None
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

    def get_exception_type(self, load_key, *args, **kwargs):
        """An interface to AggregatedErrors.get_exception_type.  Call this instead of calling
        obj.statuses[load_key]["aggregated_errors"].get_exception_type().

        Args:
            load_key (string): Key into self.statuses dict where each aggregated errors object is stored.
            args (list): Positional arguments to the AggregatedErrors.get_exception_type() method.
                exception_class (Exception): Required.  See AggregatedErrors.get_exception_type().
            kwargs (dict): Keyword arguments to the AggregatedErrors.get_exception_type() method.
        Exceptions:
            None
        Returns:
            retrieved_exceptions (list of Exception): The return of AggregatedErrors.modify_exception_type()
        """
        retrieved_exceptions = []
        if self.statuses[load_key]["aggregated_errors"] is not None:
            retrieved_exceptions = self.statuses[load_key][
                "aggregated_errors"
            ].get_exception_type(*args, **kwargs)
        return retrieved_exceptions

    def modify_exception_type(self, load_key, *args, **kwargs):
        """An interface to AggregatedErrors.modify_exception_type that keeps the "state" of the load_key in the statuses
        member up-to-date.  Call this instead of calling
        obj.statuses[load_key]["aggregated_errors"].modify_exception_type().  If you do that, you must manually handle
        keeping obj.statuses[load_key]["state"] up to date.

        Args:
            load_key (string): Key into self.statuses dict where each aggregated errors object is stored.
            args (list): Positional arguments to the AggregatedErrors.modify_exception_type() method.
                exception_class (Type[Exception]): Required.  See AggregatedErrors.modify_exception_type().
            kwargs (dict): Keyword arguments to the AggregatedErrors.modify_exception_type() method.
                modify (boolean): See AggregatedErrors.modify_exception_type().
        Exceptions:
            None
        Returns:
            modified_exceptions (list of Exception): The return of AggregatedErrors.modify_exception_type()
        """
        modified_exceptions = []
        if self.statuses[load_key]["aggregated_errors"] is not None:
            modified_exceptions = self.statuses[load_key][
                "aggregated_errors"
            ].modify_exception_type(*args, **kwargs)
        self.update_state(load_key)
        return modified_exceptions

    def modify_exception(self, load_key, *args, **kwargs):
        """An interface to AggregatedErrors.modify_exception that keeps the "state" of the load_key in the statuses
        member up-to-date.  Call this instead of calling
        obj.statuses[load_key]["aggregated_errors"].modify_exception().  If you do that, you must manually handle
        keeping obj.statuses[load_key]["state"] up to date.

        Assumptions:
            1. self.statuses[load_key]["aggregated_errors"] is not None because the exception supplied is assumed to
            exist
        Args:
            load_key (string): Key into self.statuses dict where each aggregated errors object is stored.
            args (list): Positional arguments to the AggregatedErrors.modify_exception() method.
                exception (Exception): Required.  See AggregatedErrors.modify_exception().
            kwargs (dict): Keyword arguments to the AggregatedErrors.modify_exception() method.
                modify (boolean): See AggregatedErrors.modify_exception().
        Exceptions:
            None
        Returns:
            None
        """
        self.statuses[load_key]["aggregated_errors"].modify_exception(*args, **kwargs)
        self.update_state(load_key)

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
            message += f"\n{self.get_summary_string()}"
        else:
            message = f"AggregatedErrors exception.  No exceptions have been buffered.{should_raise_message}"
        return message

    def should_raise(self):
        return self.is_fatal

    def get_num_errors(self):
        return self.num_errors

    def get_num_warnings(self):
        return self.num_warnings

    def print_summary(self):
        print(self.get_summary_string())

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
            aes_keys = list(self.aggregated_errors_dict.keys())
            for aes_key in aes_keys:
                if self.aggregated_errors_dict[aes_key].is_fatal:
                    self.is_fatal = True
                if self.aggregated_errors_dict[aes_key].is_error:
                    self.is_error = True

                if self.aggregated_errors_dict[aes_key].num_errors > 0:
                    # The number of contained AggregatedErrors objects with at least 1 error
                    self.num_errors += 1
                elif self.aggregated_errors_dict[aes_key].num_warnings > 0:
                    # The number of contained AggregatedErrors objects with at least 1 warning
                    self.num_warnings += 1
                else:
                    # Remove AggregatedErrors objects that have been completely gutted.  An AggregatedErrors object
                    # should only exist if there was at least 1 exception.  Previous individual exceptions could have
                    # been removed from any such object, but here's where we blank out the AggregatedErrors object
                    # itself.
                    del self.aggregated_errors_dict[aes_key]

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

    # TODO: Prune the simulated stack trace more and add output that makes it clear it's a simulated trace
    # TODO: Don't reset the current exception number when remove_* is used, because it's confusing to see repeated nums
    def __init__(
        self,
        message=None,
        exceptions=None,
        errors=None,
        warnings=None,
        quiet=False,
        debug=False,
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
        self.debug = debug

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

    def get_exception_type(
        self,
        exception_class,
        remove=False,
        modify=True,
        attr_name=None,
        attr_val=None,
        is_error=None,
    ):
        """
        This method is provided to retrieve exceptions (if they exist in the exceptions list) from this object and
        return them.

        Args:
            exception_class (Exception): The class of exceptions to return (and optionally remove/modify)
            remove (boolean): Whether to remove matching exceptions from this object
            modify (boolean): Whether the convert a removed exception to a warning
            attr_name (str): An attribute required to be in the exception in order to match
            attr_val (object): The value of attr_name required to match in the exception
            is_error (Optional[bool]): The exceptions' is_error attribute must have this value (if not None)
        Exceptions:
            None
        Returns:
            matched_exceptions (List[Exception]): Exceptions from self.exception that matched the search criteria
        """
        matched_exceptions = []
        unmatched_exceptions = []
        final_is_fatal = False
        final_is_error = False
        num_errors = 0
        num_warnings = 0

        # Look for exceptions to remove and recompute new object values
        for exception in self.exceptions:
            if self.exception_matches(
                exception,
                exception_class,
                attr_name=attr_name,
                attr_val=attr_val,
                is_error=is_error,
            ):
                if remove and modify:
                    # Change every removed exception to a non-fatal warning
                    exception.is_error = False
                    exception.exc_type_str = "Warning"
                    exception.is_fatal = False
                matched_exceptions.append(exception)
            else:
                if exception.is_error:
                    num_errors += 1
                else:
                    num_warnings += 1
                if exception.is_fatal:
                    final_is_fatal = True
                if exception.is_error:
                    final_is_error = True
                unmatched_exceptions.append(exception)

        if remove:
            self.num_errors = num_errors
            self.num_warnings = num_warnings
            self.is_fatal = final_is_fatal
            self.is_error = final_is_error

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
        status_message=None,
        attr_name=None,
        attr_val=None,
    ):
        """
        To support consolidation of errors across files (like MissingCompounds, MissingSamples, etc), this method
        is provided to retrieve such exceptions (if they exist in the exceptions list) from this object and return them
        for consolidation.

        If is_fatal is not None, the exception's is_fatal is changed to the supplied boolean value.
        If is_error is not None, the exception's is_error is changed to the supplied boolean value.

        Assumptions:
            1. A separate exception will be created that is an error.
        Args:
            exception_class (Exception): The class of exceptions to modify
            is_fatal (bool): Change matching exceptions' is_fatal attribute to this value
            is_error (bool): Change matching exceptions' is_error attribute to this value (but only match the exception
                if it is NOT is_error [os is_error is None])
            status_message (str): Change matching exceptions' aes_status_message attribute to this value
            attr_name (str): An attribute required to be in the exception in order to match
            attr_val (object): The value of attr_name required to match in the exception
        Returns:
            matched_exceptions (List[Exception]): Exceptions from self.exception that matched the search criteria
        """
        matched_exceptions = []
        num_errors = 0
        num_warnings = 0
        master_is_fatal = False
        master_is_error = False

        # Look for exceptions to remove and recompute new object values
        for exception in self.exceptions:
            if self.exception_matches(
                exception,
                exception_class,
                attr_name=attr_name,
                attr_val=attr_val,
                is_error=(is_error if is_error is None else not is_error),
            ):
                if is_error is not None:
                    exception.is_error = is_error
                    exception.exc_type_str = "Error" if is_error else "Warning"
                if is_fatal is not None:
                    exception.is_fatal = is_fatal
                matched_exceptions.append(exception)
                if status_message is not None:
                    exception.aes_status_message = status_message
            if exception.is_error:
                num_errors += 1
            else:
                num_warnings += 1
            if exception.is_fatal:
                master_is_fatal = True
            if exception.is_error:
                master_is_error = True
            if not hasattr(exception, "aes_status_message"):
                # TODO: This was added as a safety precaution, to ensure it's present for the submission template.
                # This should be handled via buffer_exception and the constructor that takes a list of exceptions.
                exception.aes_status_message = None

        self.num_errors = num_errors
        self.num_warnings = num_warnings
        self.is_fatal = master_is_fatal
        self.is_error = master_is_error

        # Reinitialize this object
        if not self.custom_message:
            super().__init__(self.get_default_message())

        # Return removed exceptions
        return matched_exceptions

    def modify_exception(
        self,
        exception,
        is_fatal=None,
        is_error=None,
        status_message=None,
    ):
        """Modifies a supplied exception and updates the metadata.

        If is_fatal is not None, the exception's is_fatal is changed to the supplied boolean value.
        If is_error is not None, the exception's is_error is changed to the supplied boolean value.

        Assumptions:
            1. A separate exception will be created that is an error.
            2. Changing exception changes the occurrence of the exception in self.exceptions (because it's a reference).
        Args:
            exception_class (Exception): The class of exceptions to modify
            is_fatal (bool): Change matching exceptions' is_fatal attribute to this value
            is_error (bool): Change matching exceptions' is_error attribute to this value
            status_message (str): Change matching exceptions' aes_status_message attribute to this value
            attr_name (str): An attribute required to be in the exception in order to match
            attr_val (object): The value of attr_name required to match in the exception
        Returns:
            matched_exceptions (List[Exception]): Exceptions from self.exception that matched the search criteria
        """
        # Look for exceptions to modify and recompute new object values
        if exception not in self.exceptions:
            raise ProgrammingError(
                f"Exception not found in buffer: {type(exception).__name__}: {exception}"
            )

        if is_error is not None:
            if exception.is_error != is_error:
                exception.is_error = is_error
                exception.exc_type_str = "Error" if is_error else "Warning"
                if is_error:
                    self.num_errors += 1
                    self.num_warnings -= 1
                else:
                    self.num_errors -= 1
                    self.num_warnings += 1
                self.is_error = self.num_errors > 0

        if is_fatal is not None:
            if exception.is_fatal != is_fatal:
                exception.is_fatal = is_fatal
                self.is_fatal = len([e for e in self.exceptions if e.is_fatal]) > 0

        if status_message is not None:
            exception.aes_status_message = status_message
        elif not hasattr(exception, "aes_status_message"):
            # TODO: This was added as a safety precaution, to ensure it's present for the submission template.
            # This should be handled via buffer_exception and the constructor that takes a list of exceptions.
            exception.aes_status_message = None

        # Reinitialize this object
        if not self.custom_message:
            super().__init__(self.get_default_message())

    def remove_exception_type(self, exception_class, modify=True, is_error=None):
        """
        To support consolidation of errors across files (like MissingCompounds, MissingSamples, etc), this method
        is provided to remove such exceptions (if they exist in the exceptions list) from this object and return them
        for consolidation.

        Args:
            exception_class (Exception): The class of exceptions to remove
            modify (boolean): Whether to convert the removed exception to a warning
            is_error (Optional[bool]): Restricts the summarized exceptions to errors or warnings (or either when None)
        Exceptions:
            None
        Returns:
            (List[Exception])
        """
        return self.get_exception_type(
            exception_class, remove=True, modify=modify, is_error=is_error
        )

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
        self,
        exception,
        exc_no,
        is_error=True,
        is_fatal=None,
        buffered_tb_str=None,
        status_message=None,
    ):
        """This takes an exception that is or will be buffered and adds a few data memebers to it: buffered_tb_str (a
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
        exception.aes_status_message = status_message
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
    def get_trace(cls):
        """Alias for get_buffered_traceback_string (for convenient debugging)"""
        return cls.get_buffered_traceback_string()

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

        if (
            not self.quiet
            and not isinstance(buffered_exception, AggregatedErrors)
            and (not isinstance(buffered_exception, SummarizableError) or self.debug)
        ):
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

    def exception_matches(
        self, exception, cls, attr_name=None, attr_val=None, is_error=None
    ):
        """Returns whether the supplied exception matches the supplied class, has an attribute with a specific value and
        is an error or warning.

        Args:
            exception (Exception)
            cls (Type[Exception]): The exception's type must exactly match this class.
            attr_name (Optional[str]): The exception must have this attribute (if not None)
            attr_val (Optional[Any]): The exceptions attribute must have this value (if the attr_name is not None)
            is_error (Optional[bool]): The exception's is_error attribute must have this value (if not None)
        Exceptions:
            None
        Returns:
            (bool)
        """
        # Intentionally looks for exact type (not isinstance).  isinstance is not what we want here, because I cannot
        # separate MissingSamples exceptions from NoSamples exceptions (because NoSamples is a subclass of
        # MissingSamples))
        return (
            type(exception).__name__ == cls.__name__
            and (
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
            and (is_error is None or exception.is_error == is_error)
        )

    def exception_exists(
        self, cls, attr_name, attr_val, is_error: Optional[bool] = None
    ):
        """Returns True if an exception of type cls, containing an attribute with the supplied value has been buffered.

        Args:
            cls (Exception): The Exception class to look for.
            attr_name (str): An attribute the buffered exception class has.
            attr_val (object): The value of the attribute the buffered exception class has.  If this is a function, it
                must take a single argument (the value of the attribute) and return a boolean.
            is_error (Optional[bool])
        Exceptions:
            None
        Returns:
            bool
        """
        for exc in self.exceptions:
            if self.exception_matches(exc, cls, attr_name, attr_val) and (
                is_error is None or is_error == exc.is_error
            ):
                return True
        return False

    def remove_matching_exceptions(self, cls, attr_name, attr_val, is_error=None):
        """
        To support consolidation of errors across files (like MissingCompounds, MissingSamples, etc), this method
        is provided to remove such exceptions (if they exist in the exceptions list) from this object and return them
        for consolidation.

        Args:
            cls (Type): The class of exceptions to remove
            attr_name (str): An attribute the buffered exception class has.
            attr_val (object): The value of the attribute the buffered exception class has.  If this is a function, it
                must take a single argument (the value of the attribute) and return a boolean.
            is_error (Optional[bool]): Only match when the exception is or is not an error(/warning).
        Exceptions:
            None
        Returns (List[Exception]): A list of exceptions of the supplied type, and containing the supplied attribute with
            the supplied value (or with a value that yields true from the supplied value function).
        """
        matched_exceptions = []
        unmatched_exceptions = []
        final_is_fatal = False
        final_is_error = False
        num_errors = 0
        num_warnings = 0

        # Look for exceptions to remove and recompute new object values
        for exception in self.exceptions:
            if (
                is_error is None or exception.is_error == is_error
            ) and self.exception_matches(exception, cls, attr_name, attr_val):
                matched_exceptions.append(exception)
            else:
                if exception.is_error:
                    num_errors += 1
                else:
                    num_warnings += 1
                if exception.is_fatal:
                    final_is_fatal = True
                if exception.is_error:
                    final_is_error = True
                unmatched_exceptions.append(exception)

        self.num_errors = num_errors
        self.num_warnings = num_warnings
        self.is_fatal = final_is_fatal
        self.is_error = final_is_error

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


class DuplicateValueErrors(Exception):
    """
    Summary of DuplicateValues exceptions
    """

    def __init__(self, dupe_val_exceptions: list[DuplicateValues], message=None):
        suggestions = []
        if not message:
            dupe_dict: Dict[str, dict] = defaultdict(
                lambda: defaultdict(lambda: defaultdict(list))
            )
            for dve in dupe_val_exceptions:
                typ = f" ({dve.addendum})" if dve.addendum is not None else ""
                dupe_dict[dve.loc][str(dve.colnames)][typ].append(dve)
                if dve.suggestion is not None and dve.suggestion not in suggestions:
                    suggestions.append(dve.suggestion)
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

        if len(suggestions) > 0:
            # TODO: This suggestion attribute was added to parallel other exception classes derived from InfileError.
            # Create a new higher level exception class (e.g. ResolvableException) that InfileError,
            # MultiplePeakGroupRepresentation and this class should inherit from, which implements the suggestion
            # attribute and remove this custom suggestion attribute in this class.
            if message.endswith("\n"):
                message += "\n".join(suggestions)
            else:
                message += "\n" + "\n".join(suggestions)

        super().__init__(message)
        self.dupe_val_exceptions = dupe_val_exceptions
        self.suggestions = suggestions


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


class NoTracerLabeledElementsError(Exception):
    def __init__(self, ntle_errors: List[NoTracerLabeledElements]):
        ntle_dict: Dict[str, dict] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        for ntle in ntle_errors:
            loc = generate_file_location_string(
                file=ntle.file, sheet=ntle.sheet, column=ntle.column
            )
            compound = "unreported"
            if ntle.compound is not None:
                compound = ntle.compound
            elems = "unreported"
            if ntle.elements is not None and len(ntle.elements) > 0:
                elems = ", ".join(sorted(ntle.elements))
            ntle_dict[loc][elems][compound].append(ntle)
        message = (
            "The following files contain PeakGroup compounds that have none of the labeled elements in the "
            "tracers:\n"
        )
        for loc in ntle_dict.keys():
            message += f"\t{loc}\n"
            for elems in ntle_dict[loc].keys():
                message += f"\t\tTracer labeled elements [{elems}]:\n"
                message += (
                    "\t\t\t"
                    + "\n\t\t\t".join(sorted(ntle_dict[loc][elems].keys()))
                    + "\n"
                )
        message += "PeakGroups for these compounds will be skipped."
        super().__init__(message)
        self.ntle_errors = ntle_errors
        self.ntle_dict = ntle_dict


class NoTracerLabeledElements(InfileError, SummarizableError):
    SummarizerExceptionClass = NoTracerLabeledElementsError

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
        self.compound = compound
        self.elements = elements


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


class MissingC12ParentPeaks(SummarizedInfileError, Exception):
    """Summary of all MissingC12ParentPeak errors

    Attributes:
        exceptions: A list of MissingC12ParentPeak exceptions
    """

    def __init__(
        self,
        exceptions: list[MissingC12ParentPeak],
    ):
        SummarizedInfileError.__init__(self, exceptions)
        compounds_str = ""
        include_loc = len(self.file_dict.keys()) > 1
        exc: MissingC12ParentPeak
        for loc, exc_list in self.file_dict.items():
            if include_loc:
                compounds_str += f"\t{loc}\n"
            for exc in exc_list:
                if include_loc:
                    compounds_str += "\t"
                compounds_str += f"\t{exc.compound}\n"
        loc = ""
        if not include_loc:
            loc = " in " + list(self.file_dict.keys())[0]
        message = (
            f"The C12 PARENT peak row is missing for the following compounds{loc}:\n{compounds_str}"
            "Please make sure you didn't neglect to include the C12 PARENT peak for these compounds.  You may safely "
            "ignore this error if the peak is below the detection threshold."
        )
        Exception.__init__(self, message)


class MissingC12ParentPeak(InfileError, SummarizableError):
    SummarizerExceptionClass = MissingC12ParentPeaks

    def __init__(self, compound: str, **kwargs):
        message = (
            f"C12 PARENT peak row missing for compound '{compound}' in '%s'.\n"
            "Please make sure you didn't neglect to include the C12 PARENT peak for this compound.  You may safely "
            "ignore this error if the peak is below the detection threshold."
        )
        super().__init__(message, **kwargs)
        self.compound = compound


class MissingTissues(MissingModelRecords):
    ModelName = "Tissue"
    RecordName = ModelName


class AllMissingTissues(MissingModelRecordsByFile):
    ModelName = "Tissue"
    RecordName = ModelName


class MissingStudies(MissingModelRecords):
    ModelName = "Study"
    RecordName = ModelName


class AllMissingStudies(MissingModelRecordsByFile):
    ModelName = "Study"
    RecordName = ModelName


class MissingTreatments(MissingModelRecords):
    ModelName = "Protocol"
    RecordName = "Treatment"


class AllMissingTreatments(MissingModelRecordsByFile):
    ModelName = "Protocol"
    RecordName = "Treatment"


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


class AllUnexpectedLabels(Exception):
    def __init__(self, exceptions: List[UnexpectedLabel]):
        counts: Dict[str, dict] = defaultdict(lambda: {"files": [], "observations": 0})
        for exc in exceptions:
            for element in exc.unexpected:
                if exc.file not in counts[element]["files"]:
                    counts[element]["files"].append(exc.file)
                counts[element]["observations"] += 1
        message = "The following peak label observations were not among the label(s) in the tracer(s):\n"
        for element, stats in counts.items():
            message += (
                f"\t{element}: observed {stats['observations']} times across {len(stats['files'])} peak annotation "
                "files\n"
            )
        message += "There may be contamination."
        super().__init__(message)
        self.exceptions = exceptions
        self.counts = counts


class UnexpectedLabels(Exception):
    def __init__(self, exceptions: List[UnexpectedLabel]):
        counts: Dict[str, int] = defaultdict(int)
        for exc in exceptions:
            for element in exc.unexpected:
                counts[element] += 1
        message = "The following peak label observations were not among the label(s) in the tracer(s):\n"
        for element, count in counts.items():
            message += f"\t{element}: observed {count} times\n"
        message += "There may be contamination."
        super().__init__(message)
        self.exceptions = exceptions
        self.counts = counts


class UnexpectedLabel(InfileError, SummarizableError):
    SummarizerExceptionClass = UnexpectedLabels

    def __init__(self, unexpected, possible, **kwargs):
        message = (
            f"One or more observed peak labels were not among the label(s) in the tracer(s):\n"
            f"\tObserved: {unexpected}\n"
            f"\tExpected: {possible}\n"
            "There may be contamination.  (Note, the reported observed are only the unexpected labels.)"
        )
        super().__init__(message, **kwargs)
        self.possible = possible
        self.unexpected = unexpected


class NoCommonLabel(Exception):

    def __init__(self, peakgrouplabel: PeakGroupLabel):
        msg = (
            f"PeakGroupLabel '{peakgrouplabel.element}' for PeakGroup '{peakgrouplabel.peak_group.name}' (from "
            f"infusate '{peakgrouplabel.peak_group.msrun_sample.sample.animal.infusate}') not present in the peak "
            f"group's formula '{peakgrouplabel.peak_group.formula}'."
        )
        super().__init__(msg)
        self.peak_group_label = peakgrouplabel


class AllNoScans(Exception):
    """Takes a list of NoScans exceptions and summarizes them in a single exception."""

    def __init__(self, no_scans_excs, message=None):
        if not message:
            loc_msg_default = ", obtained from the indicated file locations,"
            loc_msg = ""
            err_dict = defaultdict(lambda: defaultdict(list))
            for exc in no_scans_excs:
                if exc.file or exc.sheet or exc.column:
                    loc_msg = loc_msg_default
                loc = generate_file_location_string(
                    file=exc.file,
                    sheet=exc.sheet,
                    column=exc.column,
                )
                if exc.rownum is None:
                    if (
                        loc not in err_dict.keys()
                        or exc.mzxml_file not in err_dict[loc].keys()
                    ):
                        err_dict[loc][exc.mzxml_file] = []
                else:
                    err_dict[loc][exc.mzxml_file].append(exc.rownum)
                    loc_msg = loc_msg_default

            nltt = "\n\t\t" if loc_msg != "" else "\n\t"
            mzxml_str = ""
            for loc in err_dict.keys():
                if loc_msg != "":
                    mzxml_str += f"\t{loc}:\n\t\t"
                else:
                    mzxml_str += "\t"
                mzxml_str += (
                    nltt.join(
                        [
                            f"{k} found on row(s): {summarize_int_list(v)}"
                            for k, v in err_dict[loc].items()
                        ]
                    )
                    + "\n"
                )

            message = (
                f"The following mzXML files{loc_msg} were not loaded because they contain no scan data:\n"
                f"{mzxml_str}"
            )

        super().__init__(message)
        self.no_scans_excs = no_scans_excs


class NoScans(InfileError, SummarizableError):
    SummarizerExceptionClass = AllNoScans

    def __init__(self, mzxml_file, **kwargs):
        from_str = (
            " (reference in %s)"
            if kwargs.get("file")
            or kwargs.get("sheet")
            or kwargs.get("rownum")
            or kwargs.get("column")
            else ""
        )
        message = f"mzXML File '{mzxml_file}'{from_str} contains no scans."
        super().__init__(message, **kwargs)
        self.mzxml_file = mzxml_file


class AllMzxmlSequenceUnknown(Exception):
    """Takes a list of MzxmlSequenceUnknown exceptions and summarizes them in a single exception."""

    def __init__(self, exceptions, message=None):
        if not message:
            loc_msg_default = ""
            loc_msg = ""
            err_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            exc: MzxmlSequenceUnknown
            for exc in exceptions:
                if exc.file or exc.sheet or exc.column:
                    loc_msg = loc_msg_default
                loc = generate_file_location_string(
                    file=exc.file,
                    sheet=exc.sheet,
                    column=exc.column,
                )
                if exc.rownum is None:
                    if (
                        loc not in err_dict.keys()
                        or exc.mzxml_basename not in err_dict[loc].keys()
                    ):
                        err_dict[loc][exc.mzxml_basename] = {
                            "rows": [],
                            "match_files": exc.match_files,
                        }
                else:
                    err_dict[loc][exc.mzxml_basename]["rows"].append(exc.rownum)
                    for mf in exc.match_files:
                        if mf not in err_dict[loc][exc.mzxml_basename]["match_files"]:
                            err_dict[loc][exc.mzxml_basename]["match_files"].append(mf)
                    loc_msg = loc_msg_default

            nltt = "\n\t\t" if loc_msg != "" else "\n\t"
            nlttt = "\n\t\t\t" if loc_msg != "" else "\n\t\t"
            mzxml_str = ""
            for loc in err_dict.keys():
                if loc_msg != "":
                    mzxml_str += f"\t{loc}:\n\t\t"
                else:
                    mzxml_str += "\t"
                mzxml_str += nltt.join(
                    [
                        k
                        + (
                            f" found on row(s): {summarize_int_list(v['rows'])}"
                            if len(v["rows"]) > 0
                            else ""
                        )
                        + nlttt
                        + (
                            nlttt.join(v["match_files"])
                            if len(v["match_files"]) > 1
                            else str(v["match_files"][0])
                        )
                        for k, v in err_dict[loc].items()
                    ]
                )

            message = (
                f"Multiple mzXML files with the same basename{loc_msg}.  Cannot determine the MSRunSequence without a "
                "row for each, so one will be attempted to be deduced.  If unsuccessful, an error requiring defaults "
                f"to be supplied will occur below:\n{mzxml_str}"
            )

        super().__init__(message)
        self.exceptions = exceptions


class MzxmlSequenceUnknown(InfileError, SummarizableError):
    SummarizerExceptionClass = AllMzxmlSequenceUnknown

    def __init__(self, mzxml_basename, match_files, **kwargs):
        match_files_str = "\n\t".join(match_files)
        message = (
            f"Multiple mzXML files with the same basename [{mzxml_basename}] found on %s:\n"
            f"\t{match_files_str}\n"
            "Cannot determine the MSRunSequence, so one will be attempted to be deduced.  If unsuccessful, an error "
            "requiring defaults to be supplied will occur below."
        )
        super().__init__(message, **kwargs)
        self.mzxml_basename = mzxml_basename
        self.match_files = match_files


class MzxmlNotColocatedWithAnnot(InfileError):
    def __init__(self, annot_dirs=None, **kwargs):
        if annot_dirs is None:
            annot_dirs = ["No peak annotation directories supplied."]
        message = (
            f"mzXML file '%s' does not share a common path with a peak annotation file ({annot_dirs}) from the peak "
            "annotation files sheet.\n"
            "Co-location of mzXML files with peak annotation files is what allows mzXML files to be linked to an "
            "MSRunSequence, based on the Default Sequence column in the Peak Annotation Files sheet."
        )
        super().__init__(message, **kwargs)


class MzxmlColocatedWithMultipleAnnot(InfileError):
    def __init__(self, msrun_sequence_names, **kwargs):
        nlt = "\n\t"
        message = (
            "mzXML file '%s' shares a common path with multiple peak annotation files (from the peak annotation files "
            f"sheet) that are associated with different sequences:\n\t{nlt.join(msrun_sequence_names)}\nCo-location of "
            "mzXML files with peak annotation files is what allows mzXML files to be linked to an MSRunSequence, based "
            "on the Default Sequence column in the Peak Annotation Files sheet."
        )
        super().__init__(message, **kwargs)


class DefaultSequenceNotFound(Exception):
    def __init__(self, operator, date, instrument, protocol):
        message = (
            "A search on the MSRunSample table given the following supplied arguments:\n"
            f"\toperator: {operator}\n"
            f"\tprotocol: {protocol}\n"
            f"\tinstrument: {instrument}\n"
            f"\tdate: {date}\n"
            f"produced no records.\n"
            "Please edit these arguments to produce a single result."
        )
        super().__init__(message)


class MultipleDefaultSequencesFound(Exception):
    def __init__(self, operator, date, instrument, protocol):
        message = (
            "A search on the MSRunSample table given the following supplied arguments:\n"
            f"\toperator: {operator}\n"
            f"\tprotocol: {protocol}\n"
            f"\tinstrument: {instrument}\n"
            f"\tdate: {date}\n"
            f"produced multiple records.\n"
            "Please ammend these arguments to produce a single result."
        )
        super().__init__(message)


class AllMzXMLSkipRowErrors(Exception):
    """Takes a list of MzxmlSequenceUnknown exceptions and summarizes them in a single exception."""

    def __init__(self, exceptions, message=None):
        if not message:
            # Build the dict of organized exceptions
            loc_msg_default = ", as obtained from the indicated file locations"
            loc_msg = ""
            err_dict = defaultdict(
                lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
            )
            exc: MzXMLSkipRowError
            for exc in exceptions:
                err_type = (
                    "diff_num" if len(exc.dirs_from_infile) == 0 else "diff_paths"
                )
                # The number of rows of the infile with mzxml_name is stored in the dict where the key is an empty
                # string when there were no paths provided.  This is only used if there was no matching path.
                num_header_rows = (
                    exc.skip_paths_dict[""] if "" in exc.skip_paths_dict.keys() else 0
                )
                if exc.file or exc.sheet or exc.column:
                    loc_msg = loc_msg_default
                loc = generate_file_location_string(
                    file=exc.file,
                    sheet=exc.sheet,
                    column=exc.column,
                )
                if exc.rownum is None:
                    if (
                        err_type not in err_dict.keys()
                        or loc not in err_dict[err_type].keys()
                        or exc.mzxml_name not in err_dict[err_type][loc].keys()
                    ):
                        err_dict[err_type][loc][exc.mzxml_name] = {
                            "rows": [],
                            "existing_files": exc.existing_files,
                            "dirs_from_infile": exc.dirs_from_infile,
                            "num_header_rows": [num_header_rows],
                        }
                else:
                    err_dict[err_type][loc][exc.mzxml_name]["rows"].append(exc.rownum)
                    err_dict[err_type][loc][exc.mzxml_name]["num_header_rows"] = [
                        num_header_rows
                    ]
                    for ef in exc.existing_files:
                        if (
                            ef
                            not in err_dict[err_type][loc][exc.mzxml_name][
                                "existing_files"
                            ]
                        ):
                            err_dict[err_type][loc][exc.mzxml_name][
                                "existing_files"
                            ].append(ef)
                    for dfi in exc.dirs_from_infile:
                        if dfi not in err_dict[loc][exc.mzxml_name]["dirs_from_infile"]:
                            err_dict[err_type][loc][exc.mzxml_name][
                                "dirs_from_infile"
                            ].append(dfi)
                    loc_msg = loc_msg_default

            nltt = "\n\t\t" if loc_msg != "" else "\n\t"
            nlttt = "\n\t\t\t" if loc_msg != "" else "\n\t\t"
            nltttt = "\n\t\t\t\t" if loc_msg != "" else "\n\t\t\t"
            message = ""
            if "diff_num" in err_dict.keys():
                diff_num_str = ""
                for loc in err_dict["diff_num"].keys():
                    if loc_msg != "":
                        diff_num_str += f"\t{loc}:\n\t\t"
                    else:
                        diff_num_str += "\t"
                    diff_num_str += nltt.join(
                        [
                            k
                            + (
                                f" found on row(s): {summarize_int_list(v['rows'])}"
                                if len(v["rows"]) > 0
                                else ""
                            )
                            + f" ({len(v['num_header_rows'][0])} sample header(s) from the infile [that is/are "
                            + f"skipped] and {len(v['existing_files'])} mzXML files)"
                            + nlttt
                            + (
                                nlttt.join(v["existing_files"])
                                if len(v["existing_files"]) > 1
                                else str(v["existing_files"][0])
                            )
                            for k, v in err_dict["diff_num"][loc].items()
                        ]
                    )

                message += (
                    "The number of supplied mzXML files with each of the names below match a different number of "
                    f"skipped sample headers{loc_msg}:\n{diff_num_str}\n"
                )
            if "diff_paths" in err_dict.keys():
                diff_paths_str = ""
                for loc in err_dict["diff_paths"].keys():
                    if loc_msg != "":
                        diff_paths_str += f"\t{loc}:\n\t\t"
                    else:
                        diff_paths_str += "\t"
                    diff_paths_str += nltt.join(
                        [
                            k
                            + (
                                f" found on row(s): {summarize_int_list(v['rows'])}"
                                if len(v["rows"]) > 0
                                else ""
                            )
                            + f"{nlttt}Supplied files:{nltttt}"
                            + (
                                nltttt.join(v["existing_files"])
                                if len(v["existing_files"]) > 1
                                else str(v["existing_files"][0])
                            )
                            + f"{nlttt}Supplied paths from input file:{nltttt}"
                            + (
                                nltttt.join(v["dirs_from_infile"])
                                if len(v["dirs_from_infile"]) > 1
                                else str(v["dirs_from_infile"][0])
                            )
                            for k, v in err_dict["diff_paths"][loc].items()
                        ]
                    )

                message += (
                    "The paths of the following mzXML files with the same indicated sample names do not match the "
                    f"supplied paths{loc_msg}:\n{diff_paths_str}"
                )

        super().__init__(message)
        self.exceptions = exceptions


class MzXMLSkipRowError(InfileError, SummarizableError):
    SummarizerExceptionClass = AllMzXMLSkipRowErrors

    def __init__(
        self,
        mzxml_name: str,
        existing_files: List[str],
        skip_paths_dict: Dict[str, int],
        **kwargs,
    ):
        """The situation here is that we may not be able to identify which files to skip and which to
        load.  dirs could contain files we should skip because the user didn't provide directory paths on the skipped
        rows from the infile.  We only know to skip them if the number of skips and the number of directories is the
        same - but they differ.

        Args:
            mzxml_name (str): The mzXML filename without the extension
            existing_files (List[str]): mzXML files all named mzxml_name
            skip_paths_dict (Dict[str, int]): Directories extracted from a Peak Annotation Details sheet that do not
                match existing_paths.  If a key is an empty string, it means no path was provided (or the path is the
                root directory).
        """
        dirs_from_infile = [d for d in skip_paths_dict.keys() if d != ""]
        nlt = "\n\t"
        # In this instance, we don't have any paths from the infile
        if len(dirs_from_infile) == 0:
            message = (
                f"The number of supplied mzXML files with the name '{mzxml_name}' ({len(existing_files)}) matches a "
                f"different number of skipped sample headers ({skip_paths_dict['']}) in %s:\n"
                f"\t{nlt.join(existing_files)}"
            )
        else:
            message = (
                f"The paths of the following mzXML files with the same sample name ({mzxml_name}):"
                f"\n\t{nlt.join(existing_files)}\n"
                "do not match the paths supplied in %s:\n"
                f"{nlt.join(dirs_from_infile)}"
            )
        super().__init__(message, **kwargs)
        self.mzxml_name = mzxml_name
        self.existing_files = existing_files
        self.skip_paths_dict = skip_paths_dict
        self.dirs_from_infile = dirs_from_infile


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


class AssumedMzxmlSampleMatches(Exception):
    """Takes a list of AssumedMzxmlSampleMatch exceptions and summarizes them in a single exception."""

    def __init__(self, exceptions: List[AssumedMzxmlSampleMatch], message=None):
        if message is None:
            message = (
                "Assuming the following imperfect (but unique) mzXML to sample name matches are due to peak annotation "
                "file header edits, and that they are correctly mapped:\n"
            )
            matches_by_annot_file = defaultdict(list)
            exc: AssumedMzxmlSampleMatch
            for exc in exceptions:
                loc = generate_file_location_string(
                    file=exc.file,
                    sheet=exc.sheet,
                )
                matches_by_annot_file[loc].append(exc)
            for loc, exc_list in sorted(
                matches_by_annot_file.items(), key=lambda tpl: tpl[0]
            ):
                message += f"\t{loc}\n"
                for exc in sorted(exc_list, key=lambda e: e.mzxml_name):
                    message += f"\t\t'{exc.sample_name}' <- '{exc.mzxml_name}' ('{exc.mzxml_file}')\n"
        super().__init__(message)
        self.exceptions = exceptions


class AssumedMzxmlSampleMatch(InfileError, SummarizableError):
    SummarizerExceptionClass = AssumedMzxmlSampleMatches

    def __init__(self, sample_name, mzxml_file, **kwargs):
        mzxml_name, _ = os.path.splitext(os.path.basename(mzxml_file))
        message = (
            "Sample uniquely but imprecisely matches the mzXML filename in %s:\n"
            f"\tSample: [{sample_name}]\n"
            f"\tmzXML:  [{mzxml_name}] (path: [{mzxml_file}])\n"
            "Assuming that the sample header was modified and that this is the intended sample."
        )
        super().__init__(message, **kwargs)
        self.sample_name = sample_name
        self.mzxml_name = mzxml_name
        self.mzxml_file = mzxml_file


class UnmatchedMzXMLs(Exception):
    """Takes a list of UnmatchedMzXML exceptions and summarizes them in a single exception."""

    def __init__(self, exceptions: List[UnmatchedMzXML]):
        # Assumes that the file/sheet/column are all the same and that there is at least 1 exception
        loc = generate_file_location_string(
            file=exceptions[0].file,
            sheet=exceptions[0].sheet,
            column=exceptions[0].column,
        )
        message = f"{len(exceptions)} mzXMLs could not be mapped to a {exceptions[0].sample_header_col}:\n"
        for exc in exceptions:
            message += f"\t{exc.mzxml_name} (file: '{exc.mzxml_file}')\n"
        message += (
            "Either these files were not included in any peak annotation analysis or the sample headers were modified "
            f"and could not be automatically matched to existing sample headers in {loc}.\n"
            "Either update rows to include these files or if any are unanalyzed mzXMLs, add a row and fill in the "
            f"'{exceptions[0].skip_col}' column."
        )
        super().__init__(message)
        self.exceptions = exceptions


class UnmatchedMzXML(InfileError, SummarizableError):
    SummarizerExceptionClass = UnmatchedMzXMLs

    def __init__(
        self, mzxml_file: str, sample_header_col: str, skip_col: str, **kwargs
    ):
        mzxml_name = os.path.splitext(os.path.basename(mzxml_file))[0]
        message = (
            f"mzXML name '{mzxml_name}' (from file '{mzxml_file}') could not be mapped to a {sample_header_col}.\n"
            "Either this file was not included in a peak annotation analysis or the sample header was modified and "
            "could not be automatically matched.\n"
            f"Either update a row that includes '{mzxml_file}' or if this is an unanalyzed mzXML, add a row and fill "
            f"in the '{skip_col}' column, including %s to resolve this."
        )
        super().__init__(message, **kwargs)
        self.skip_col = skip_col
        self.mzxml_name = mzxml_name
        self.mzxml_file = mzxml_file
        self.sample_header_col = sample_header_col


class UnmatchedBlankMzXMLs(Exception):
    """Takes a list of UnmatchedMzXML exceptions and summarizes them in a single exception."""

    def __init__(self, exceptions: List[UnmatchedBlankMzXML]):
        # Assumes that the file/sheet/column are all the same and that there is at least 1 exception
        loc = generate_file_location_string(
            file=exceptions[0].file,
            sheet=exceptions[0].sheet,
            column=exceptions[0].column,
        )
        message = f"{len(exceptions)} mzXMLs could not be mapped to a {exceptions[0].sample_header_col}:\n"
        for exc in exceptions:
            message += f"\t{exc.mzxml_name} (file: '{exc.mzxml_file}')\n"
        message += (
            "These mzXML files, that appear to be for blank samples, could not be automatically matched to existing "
            f"skipped blank sample headers in {loc}.\n"
            f"Either update or add rows and fill in the '{exceptions[0].column}' and '{exceptions[0].skip_col}' "
            "columns for each file."
        )
        super().__init__(message)
        self.exceptions = exceptions


class UnmatchedBlankMzXML(InfileError, SummarizableError):
    SummarizerExceptionClass = UnmatchedBlankMzXMLs

    def __init__(
        self, mzxml_file: str, sample_header_col: str, skip_col: str, **kwargs
    ):
        mzxml_name = os.path.splitext(os.path.basename(mzxml_file))[0]
        message = (
            f"mzXML name '{mzxml_name}' (from file '{mzxml_file}'), that appears to be for a blank sample, could not "
            f"be mapped to a {sample_header_col}.\n"
            "The file could not be automatically matched to an existing skipped blank sample header.\n"
            f"Either update a row that includes '{mzxml_name}' or add a row and fill in the '{skip_col}' column, "
            "including %s to resolve this."
        )
        super().__init__(message, **kwargs)
        self.skip_col = skip_col
        self.mzxml_name = mzxml_name
        self.mzxml_file = mzxml_file
        self.sample_header_col = sample_header_col


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
        # If the string has any number in it, suggest that the issue could be excel
        sugg = (
            (
                "  This may be the result of excel converting a string to a date.  If so, try editing the data type of "
                "the column in %s."
            )
            if any(char.isdigit() for char in str(string))
            else ""
        )
        message = f"The date string '{string}' found in %s did not match the pattern '{format}'.{sugg}"
        super().__init__(message, **kwargs)
        self.string = string
        self.ve_exc = ve_exc
        self.format = format


class DurationError(InfileError):
    def __init__(self, string, units, exc, **kwargs):
        message = f"The duration '{string}' found in %s must be a number of {units}.\n"
        super().__init__(message, **kwargs)
        self.string = string
        self.exc = exc
        self.units = units


class InvalidMSRunName(InfileError):
    pass


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


class InfileDatabaseError(InfileError):
    def __init__(self, exception, rec_dict, **kwargs):
        if rec_dict is not None:
            nltab = "\n\t"
            deets = [f"{k}: {str(v).replace('%', '%%')}" for k, v in rec_dict.items()]
        message = f"{type(exception).__name__} in %s"
        if rec_dict is not None:
            message += f", creating record:\n\t{nltab.join(deets)}"
        message += f"\n\t{type(exception).__name__}: {exception}"
        super().__init__(message, **kwargs)
        self.exception = exception
        self.rec_dict = rec_dict


class MzxmlParseError(Exception):
    pass


class AllMultiplePeakGroupRepresentations(Exception):
    """Summary of MultiplePeakGroupRepresentations errors across multiple loads.

    Attributes:
        exceptions: A list of MultiplePeakGroupRepresentation exceptions
    """

    def __init__(
        self,
        exceptions: list[MultiplePeakGroupRepresentation],
        succinct=False,
        suggestion=None,
    ):
        mpgr_dict: Dict[str, dict] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        mpgr: MultiplePeakGroupRepresentation
        for mpgr in exceptions:
            seq_key = str(mpgr.sequence)
            for file in mpgr.filenames:
                if file not in mpgr_dict[mpgr.compound][seq_key]["files"]:
                    mpgr_dict[mpgr.compound][seq_key]["files"].append(file)
            if mpgr.sample.name not in mpgr_dict[mpgr.compound][seq_key]["samples"]:
                mpgr_dict[mpgr.compound][seq_key]["samples"].append(mpgr.sample.name)
            mpgr_dict[mpgr.compound][seq_key]["exceptions"].append(mpgr)

        message = "The following compounds are present in multiple peak annotation files (for the same samples)."
        for compound in sorted(mpgr_dict.keys(), key=str.casefold):
            message += f"\n\t{compound}"
            if len(mpgr_dict[compound].keys()) > 1:
                for sequence in sorted(mpgr_dict[compound].keys(), key=str.casefold):
                    files = [
                        f if f is not None else ""
                        for f in mpgr_dict[compound][sequence]["files"]
                    ]
                    if succinct:
                        message += f"\n\t\t{sequence} ({len(mpgr_dict[compound][sequence]['samples'])} samples)\n\t\t\t"
                        message += "\n\t\t\t".join(sorted(files, key=str.casefold))
                    else:
                        message += f"\n\t\t{sequence}"
                        message += "\n\t\t\tSamples:\n\t\t\t\t"
                        message += "\n\t\t\t\t".join(
                            sorted(
                                mpgr_dict[compound][sequence]["samples"],
                                key=str.casefold,
                            )
                        )
                        message += "\n\t\t\tFiles:\n\t\t\t\t"
                        message += "\n\t\t\t\t".join(sorted(files, key=str.casefold))
            else:
                if succinct:
                    files = [
                        f if f is not None else ""
                        for f in list(mpgr_dict[compound].values())[0]["files"]
                    ]
                    message += "\n\t\t" + "\n\t\t".join(sorted(files, key=str.casefold))
                else:
                    files = [
                        f if f is not None else ""
                        for f in list(mpgr_dict[compound].values())[0]["files"]
                    ]
                    samples = list(mpgr_dict[compound].values())[0]["samples"]
                    message += "\n\t\tSamples:\n\t\t\t"
                    message += "\n\t\t\t".join(sorted(samples, key=str.casefold))
                    message += "\n\t\tFiles:\n\t\t\t\t"
                    message += "\n\t\t\t".join(sorted(files, key=str.casefold))
        message += "\nOnly 1 representation of a compound per sample is allowed."
        if suggestion is not None:
            # TODO: This suggestion attribute was added to parallel other exception classes derived from InfileError.
            # Create a new higher level exception class (e.g. ResolvableException) that InfileError,
            # MultiplePeakGroupRepresentation and this class should inherit from, which implements the suggestion
            # attribute and remove this custom suggestion attribute in this class.
            message += f"\n{suggestion}"
        super().__init__(message)
        self.exceptions = exceptions
        self.succinct = succinct
        self.suggestion = suggestion
        self.orig_message = message
        self.message = message

    def set_formatted_message(self, suggestion=None):
        message = self.orig_message
        if suggestion is not None:
            if message.endswith("\n"):
                message += suggestion
            else:
                message += f"\n{suggestion}"
        self.message = message
        return self

    def __str__(self):
        return self.message


class MultiplePeakGroupRepresentations(Exception):
    """Summary of all MultiplePeakGroupRepresentation errors

    Attributes:
        exceptions: A list of MultiplePeakGroupRepresentation exceptions
    """

    def __init__(
        self,
        exceptions: list[MultiplePeakGroupRepresentation],
        suggestion=None,
    ):
        mpgr_dict: Dict[str, dict] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        for mpgr in exceptions:
            files_str = ", ".join(sorted(mpgr.filenames))
            if files_str not in mpgr_dict[str(mpgr.sequence)].keys():
                mpgr_dict[str(mpgr.sequence)][files_str]["files"] = mpgr.filenames
            if (
                mpgr.compound
                not in mpgr_dict[str(mpgr.sequence)][files_str]["compounds"]
            ):
                mpgr_dict[str(mpgr.sequence)][files_str]["compounds"].append(
                    mpgr.compound
                )
            if (
                mpgr.sample.name
                not in mpgr_dict[str(mpgr.sequence)][files_str]["samples"]
            ):
                mpgr_dict[str(mpgr.sequence)][files_str]["samples"].append(
                    mpgr.sample.name
                )
            mpgr_dict[str(mpgr.sequence)][files_str]["exceptions"].append(mpgr)

        message = (
            "The following peak annotation files (containing common samples) each contain peak groups for the same "
            "compound.\n"
        )
        for sequence in mpgr_dict.keys():
            message += f"\tMS Run Sequence '{sequence}':"
            if len(mpgr_dict[sequence].keys()) > 1:
                # If there are multiple combinations of files, it means that there are difference complements of samples
                # in each file
                for files_set in mpgr_dict[sequence].keys():
                    samples = mpgr_dict[sequence][files_set]["samples"]
                    message += "\n\t\tSamples:\n\t\t\t" + "\n\t\t\t".join(
                        sorted(samples)
                    )
                    files = mpgr_dict[sequence][files_set]["files"]
                    message += "\n\t\tFiles:\n\t\t\t" + "\n\t\t\t".join(sorted(files))
                    compounds = mpgr_dict[sequence][files_set]["compounds"]
                    message += "\n\t\tCompounds:\n\t\t\t" + "\n\t\t\t".join(
                        sorted(compounds)
                    )
            else:
                files = list(mpgr_dict[sequence].values())[0]["files"]
                message += "\n\t\tFiles:\n\t\t\t" + "\n\t\t\t".join(sorted(files))
                compounds = list(mpgr_dict[sequence].values())[0]["compounds"]
                message += "\n\t\tCompounds:\n\t\t\t" + "\n\t\t\t".join(
                    sorted(compounds)
                )
        message += "\nOnly 1 representation of a compound per sample is allowed."
        if suggestion is not None:
            # TODO: This suggestion attribute was added to parallel other exception classes derived from InfileError.
            # Create a new higher level exception class (e.g. ResolvableException) that InfileError,
            # MultiplePeakGroupRepresentation and this class should inherit from, which implements the suggestion
            # attribute and remove this custom suggestion attribute in this class.
            message += f"\n{suggestion}"
        super().__init__(message)
        self.exceptions = exceptions
        self.suggestion = suggestion
        self.orig_message = message
        self.message = message

    def set_formatted_message(self, suggestion=None):
        message = self.orig_message
        if suggestion is not None:
            if message.endswith("\n"):
                message += suggestion
            else:
                message += f"\n{suggestion}"
        self.message = message
        return self

    def __str__(self):
        return self.message


class MultiplePeakGroupRepresentation(SummarizableError):
    SummarizerExceptionClass = MultiplePeakGroupRepresentations

    def __init__(
        self, new_rec: PeakGroup, existing_recs, message=None, suggestion=None
    ):
        """MultiplePeakGroupRepresentations constructor.

        Args:
            new_rec (PeakGroup): An uncommitted record.
            existing_recs (PeakGroup.QuerySet)
        """

        filenames = [new_rec.peak_annotation_file.filename]
        filenames.extend([r.peak_annotation_file.filename for r in existing_recs.all()])
        files_str = "\n\t".join(filenames)
        message = (
            "Multiple representations of this peak group compound were encountered:\n"
            f"\tCompound: {new_rec.name}\n"
            f"\tMSRunSequence: {new_rec.msrun_sample.msrun_sequence}\n"
            "Each peak group originated from:\n"
            f"\t{files_str}\n"
            "Only 1 representation of a compound per sample is allowed."
        )
        if suggestion is not None:
            # TODO: This suggestion attribute was added to parallel other exception classes derived from InfileError.
            # Create a new higher level exception class (e.g. ResolvableException) that InfileError,
            # AllMultiplePeakGroupRepresentations and this class should inherit from, which implements the suggestion
            # attribute and remove this custom suggestion attribute in this class.
            message += f"\n{suggestion}"
        super().__init__(message)
        self.new_rec = new_rec
        self.existing_recs = existing_recs
        self.filenames: List[str] = filenames
        self.compound: str = new_rec.name
        self.sequence: MSRunSequence = new_rec.msrun_sample.msrun_sequence
        self.sample: Sample = new_rec.msrun_sample.sample
        self.suggestion = suggestion
        self.orig_message = message
        self.message = message

    def set_formatted_message(self, suggestion=None):
        message = self.orig_message
        if suggestion is not None:
            if message.endswith("\n"):
                message += suggestion
            else:
                message += f"\n{suggestion}"
        self.message = message
        return self

    def __str__(self):
        return self.message


class PossibleDuplicateSamples(SummarizedInfileError, Exception):
    """Summary of all PossibleDuplicateSamples errors

    Attributes:
        exceptions: A list of PossibleDuplicateSamples exceptions
    """

    def __init__(
        self,
        exceptions: list[PossibleDuplicateSample],
        suggestion=None,
    ):
        SummarizedInfileError.__init__(self, exceptions)
        headers_str = ""
        include_loc = len(self.file_dict.keys()) > 1
        exc: PossibleDuplicateSample
        for loc, exc_list in self.file_dict.items():
            if include_loc:
                headers_str += f"\t{loc}\n"
            for exc in exc_list:
                if include_loc:
                    headers_str += "\t"
                if exc.rownum is not None and not isinstance(exc.rownum, list):
                    raise ProgrammingError("rownum is expected to be a list here.")
                rowlist = summarize_int_list(exc.rownum)
                rowstr = "" if len(rowlist) == 0 else f" on rows: {rowlist}"
                headers_str += f"\theader '{exc.sample_header}' maps to samples: {exc.sample_names}{rowstr}\n"
        loc = ""
        if not include_loc:
            loc = " in " + list(self.file_dict.keys())[0]
        message = (
            f"There are multiple sample headers{loc} from different peak annotation files with the same name that are "
            f"associated with different database samples:\n{headers_str}"
            "Are you sure these are different samples?  If they are not, they should all be associated with the same "
            "tracebase sample."
        )
        if suggestion is not None:
            message += f"\n{suggestion}"
        Exception.__init__(self, message)


class PossibleDuplicateSample(InfileError, SummarizableError):
    SummarizerExceptionClass = PossibleDuplicateSamples

    def __init__(self, sample_header: str, sample_names: List[str], **kwargs):
        nlt = "\n\t"
        message = (
            f"There are multiple sample headers from different peak annotation files with the name '{sample_header}' "
            f"that are associated with different database samples:\n\t{nlt.join(sample_names)}\nin %s.\n"
            "Are you sure these are different samples?  If they are not, they should all be associated with the same "
            "tracebase sample."
        )
        super().__init__(message, **kwargs)
        self.sample_header = sample_header
        self.sample_names = sample_names


class ReplacingPeakGroupRepresentation(InfileError):
    def __init__(self, delete_rec: PeakGroup, selected_file: str, **kwargs):
        message = (
            f"Replacing PeakGroup {delete_rec} (previously loaded from file "
            f"'{delete_rec.peak_annotation_file.filename}') with the version from file '{selected_file}', as specified "
            "in the Peak Group Conflict resolution selected on %s."
        )
        super().__init__(message, **kwargs)
        self.delete_rec = delete_rec
        self.selected_file = selected_file


class DuplicatePeakGroupResolutions(InfileError):
    def __init__(
        self, pgname: str, selected_files: List[str], conflicting=True, **kwargs
    ):
        dupetype = "conflicting" if conflicting else "equivalent"
        message = (
            f"Multiple {dupetype} resolutions for peak group '{pgname}' selecting file(s) {selected_files} in %s.\n"
            "Note, the peak group names may differ by case and/or compound order."
        )

        super().__init__(message, **kwargs)
        self.pgname = pgname
        self.selected_files = selected_files
        self.conflicting = conflicting


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


class StudyDocConversionException(InfileError):
    def __init__(self, from_version: str, to_version: str, message: str, **kwargs):
        message = (
            f"The conversion of the input study doc from version {from_version} to version {to_version} "
            "resulted in the following notable issue(s)...\n\n"
        ) + message
        super().__init__(message, **kwargs)
        self.from_version = from_version
        self.to_version = to_version


class PlaceholdersAdded(StudyDocConversionException):
    def __init__(
        self, from_version, to_version, pa_list: list[PlaceholderAdded], **kwargs
    ):
        pae_dict: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(object)))
        for pae in pa_list:
            filesheet = generate_file_location_string(file=pae.file, sheet=pae.sheet)
            row = pae.rownum if pae.rownum is not None else "all rows"
            column = pae.column if pae.column is not None else "all columns"
            pae_dict[filesheet][row][column] = pae
        sum_dict: dict = defaultdict(lambda: defaultdict(list))
        for filesheet in pae_dict.keys():
            for row in pae_dict[filesheet].keys():
                cols = ", ".join(sorted(pae_dict[filesheet][row].keys()))
                sum_dict[filesheet][cols].append(row)
        message = (
            "Placeholder values were added due to unconsolidated/missing data in the older version.\n"
            "Please edit the placeholder values in %s described below:\n"
        )
        for filesheet in sum_dict.keys():
            if len(sum_dict[filesheet].keys()) == 1:
                prepend_str = "\t"
                postpend_str = f" in {filesheet}"
            else:
                prepend_str = "\t\t"
                postpend_str = ""
                message += f"\t{filesheet}:\n"
            for cols in sum_dict[filesheet].keys():
                message += (
                    f"{prepend_str}columns [{cols}] on row(s) ["
                    + ", ".join(summarize_int_list(sum_dict[filesheet][cols]))
                    + f"]{postpend_str}\n"
                )
        super().__init__(from_version, to_version, message, **kwargs)
        self.pa_list = pa_list


class PlaceholderAdded(StudyDocConversionException):
    def __init__(self, from_version, to_version, message=None, **kwargs):
        if message is None:
            message = (
                f"The conversion of the input study doc from from version {from_version} to version {to_version} "
                "resulted in placeholder values being added due to unconsolidated/missing data in the older version.\n"
                "Please update the placeholder value(s) added to %s."
            )
        super().__init__(from_version, to_version, message, **kwargs)


class BlanksRemoved(StudyDocConversionException):
    def __init__(
        self,
        blanks_exceptions,
        new_loc,
        from_version,
        to_version,
        message=None,
        **kwargs,
    ):
        if message is None:
            message = (
                "The following sample's rows in %s were inferred to be blanks (due to the tissue column being empty) "
                "and removed:\n\t"
            )
            message += "\n\t".join([be.sample_name for be in blanks_exceptions])
            message += f"\nBlanks are now recorded in '{new_loc}'."
        super().__init__(from_version, to_version, message, **kwargs)
        self.blanks_exceptions = blanks_exceptions
        self.new_loc = new_loc


class BlankRemoved(StudyDocConversionException):
    def __init__(
        self, sample_name, new_loc, from_version, to_version, message=None, **kwargs
    ):
        if message is None:
            message = (
                f"Sample row for sample {sample_name} has been removed (the sample was inferred to be a blank by the "
                f"fact that the tissue column was empty) from %s.  Blanks are now recorded in '{new_loc}'."
            )
        super().__init__(from_version, to_version, message, **kwargs)
        self.sample_name = sample_name
        self.new_loc = new_loc


class PlaceholderDetected(InfileError):
    def __init__(self, message=None, suggestion=None, **kwargs):
        if message is None:
            message = "Placeholder values detected on %s.  Skipping load of the corresponding record(s)."
        if suggestion is not None:
            message += f"  {suggestion}"
        super().__init__(message, **kwargs)


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


class TracerCompoundNameInconsistent(InfileError):
    def __init__(
        self,
        tracer_name,
        primary_compound_name,
        compound_synonym,
        message=None,
        **kwargs,
    ):
        if message is None:
            message = (
                f"Tracer names '{tracer_name}' should use the primary compound name '{primary_compound_name}' (for "
                f"consistent tracer name search results), but a synonym '{compound_synonym}' was found on %s."
            )
        super().__init__(message, **kwargs)


class InvalidPeakAnnotationFileFormat(InfileError):
    def __init__(self, format_code, supported, annot_file, **kwargs):
        message = (
            f"Unrecognized format code: {format_code} specified for file '{annot_file}' from %s.  Supported "
            f"format codes are: {supported}."
        )
        super().__init__(message, **kwargs)
        self.format_code = format_code
        self.supported = supported
        self.annot_file = annot_file


# TODO: Create class 'PeakAnnotationFileFormatException' similar to StudyDocVersionException


class UnknownPeakAnnotationFileFormat(InfileError):
    # TODO: Implement this class similar to the corresponding StudyDoc exception classes below, once match_data is
    # generated by the PeakAnnotationsLoader.determine_matching_formats method
    def __init__(self, supported, **kwargs):
        message = (
            "No matching formats for peak annotation file: %s.  Must be one of these supported formats: "
            f"{supported}."
        )
        super().__init__(message, **kwargs)
        self.supported = supported


class MultiplePeakAnnotationFileFormats(InfileError):
    # TODO: Implement this class similar to the corresponding StudyDoc exception classes below, once match_data is
    # generated by the PeakAnnotationsLoader.determine_matching_formats method
    def __init__(self, matches, **kwargs):
        message = f"Multiple matching formats for peak annotation file '%s': {matches}."
        super().__init__(message, **kwargs)
        self.matches = matches


class DuplicatePeakAnnotationFileName(Exception):
    def __init__(self, filename):
        message = f"Peak annotation filenames must be unique.  Filename {filename} was encountered multiple times."
        super().__init__(message)
        self.filename = filename


class InvalidStudyDocVersion(Exception):
    pass


class StudyDocVersionException(Exception):
    def __init__(self, message, match_data, matches=None):
        """StudyDocVersionException constructor: appends match details to derived class exception messages.

        Args:
            message (str)
            match_data (dict): Data describing the extend of the match of the supplied data and the exoected data in
                each version.

                Example:

                {
                    "supplied": {sheet name: [supplied column names]},
                    "versions": {
                        version number: {
                            "match": bool,
                            "expected": {sheet: [required headers]},
                            "missing_sheets": []
                            "unknown_sheets": []
                            "matching": {sheet: {"matching": [headers], "missing": [headers], "unknown": [headers]}},
                        },
                    },
                }
            matches (Optional[List[str]]): List of version numbers that match, when there are multiuple matches.  This
                limits the report to only the details of the matching versions.
        Exceptions:
            None
        Returns:
            instance
        """
        guess, nheaders, nsheets = self.guess_version(match_data)
        if guess is not None:
            message += (
                f"\n\nSuggestion: It looks like, with {nheaders} missing required headers and {nsheets} matching "
                f"sheets overall, study doc version {guess} appears to be the likely match.  Look over the missing "
                "required headers for that version in the matching data below and edit the headers in your study doc "
                "to match."
            )
        message += "\n\nCompared to each supported version, the following differences prevented a version match:\n"
        for version in match_data["versions"].keys():
            if matches is not None and version in matches:
                # Limit the report to just the multiple matching versions
                continue
            message += f"\tVersion {version}\n"
            if len(match_data["versions"][version]["matching"].keys()) > 0:
                for sheet, mdict in match_data["versions"][version]["matching"].items():
                    message += f"\t\tSheet '{sheet}' has"
                    if len(mdict["missing"]) > 0 or len(mdict["unknown"]) > 0:
                        message += (
                            f"\n\t\t\t{len(mdict['matching'])} matching headers\n"
                        )
                        if len(mdict["missing"]) > 0:
                            message += (
                                f"\t\t\t**{len(mdict['missing'])} missing required headers: "
                                f"{mdict['missing']}**\n"
                            )
                        if len(mdict["unknown"]) > 0:
                            message += f"\t\t\t{len(mdict['unknown'])} ignored^ headers: {mdict['unknown']}\n"
                    else:
                        message += f" all {len(mdict['matching'])} matching headers.\n"
                if len(match_data["versions"][version]["unknown_sheets"]) > 0:
                    message += (
                        "\t\tThe following sheet names were not recognized: "
                        f"{match_data['versions'][version]['unknown_sheets']}.\n"
                    )
                    if len(match_data["versions"][version]["missing_sheets"]) > 0:
                        message += (
                            "\t\t\tIn addition, the following sheets were not found, and could potentially be a "
                            "match if the ignored^ sheet(s) were renamed: "
                            f"{match_data['versions'][version]['missing_sheets']}\n"
                        )
            elif len(match_data["versions"][version]["missing_sheets"]) > 0:
                message += (
                    f"\t\tNone of the expected sheet names: {match_data['versions'][version]['missing_sheets']}\n"
                    f"\t\twere found among the supplied sheets: {list(match_data['supplied'].keys())}\n"
                )

        message += (
            "\n^ 'ignored' headers may or may not be correct.  They are just not used in file format "
            "identification."
        )

        super().__init__(message)
        self.match_data = match_data

    @classmethod
    def guess_version(cls, match_data):
        """Returns the version with the fewest missing headers.  If there is a tie in missing header count, it returns
        the one with the most matching sheets.  Otherwise, it returns None.  Also returns the count of matching headers
        and sheet names."""
        counts = defaultdict(lambda: {"sheets": 0, "missing headers": 0})
        for version in match_data["versions"].keys():
            if len(match_data["versions"][version]["matching"].keys()) > 0:
                counts[version]["sheets"] += len(
                    match_data["versions"][version]["matching"].keys()
                )
                for mdict in match_data["versions"][version]["matching"].values():
                    counts[version]["missing headers"] += len(mdict["missing"])
        min_missing_headers = None
        max_sheets = 0
        best_versions = []
        for version, count_dict in counts.items():
            if (
                min_missing_headers is None
                or count_dict["missing headers"] < min_missing_headers
            ):
                min_missing_headers = count_dict["missing headers"]
                max_sheets = count_dict["sheets"]
                best_versions = [version]
            elif count_dict["missing headers"] == min_missing_headers:
                if count_dict["sheets"] > max_sheets:
                    min_missing_headers = count_dict["missing headers"]
                    max_sheets = count_dict["sheets"]
                    best_versions = [version]
                elif count_dict["sheets"] == max_sheets:
                    best_versions.append(version)
        if len(best_versions) == 1:
            return best_versions[0], min_missing_headers, max_sheets
        return None, None, None


class UnknownStudyDocVersion(StudyDocVersionException):
    def __init__(self, supported_versions: list, match_data, message=None):
        """UnknownStudyDocVersion constructor: use when there is no version match.

        Args:
            supported_versions (List[str]): List of supported study doc version numbers
            match_data (dict): See StudyDocVersionException.__init__()
            message (Optional[str])
        Exceptions:
            None
        Returns:
            instance
        """
        if message is None:
            message = (
                "Unable to determine study doc version.  Please supply one of the supported formats: "
                f"{supported_versions}."
            )
        super().__init__(message, match_data)
        self.supported_versions = supported_versions


class MultipleStudyDocVersions(StudyDocVersionException):
    def __init__(self, matching_version_numbers: list, match_data, message=None):
        """MultipleStudyDocVersions constructor: use when there are multiple matches.

        Args:
            supported_versions (List[str]): List of supported study doc version numbers
            match_data (dict): See StudyDocVersionException.__init__()
            message (Optional[str])
        Exceptions:
            None
        Returns:
            instance
        """
        if message is None:
            message = (
                "Unable to identify study doc version.  Please supply one of these multiple matching version numbers: "
                f"{matching_version_numbers}."
            )
        super().__init__(message, match_data, matching_version_numbers)
        self.matching_version_numbers = matching_version_numbers


class MissingFCircCalculationValues(Exception):
    def __init__(
        self,
        exceptions: List[MissingFCircCalculationValue],
    ):
        message = (
            "FCirc calculations on TraceBase are done using the tracer peak group(s) from the last serum sample, "
            "the infusion rate, and the animal weight.  The following values are missing:\n"
        )
        err_dict: dict = defaultdict(lambda: defaultdict(list))
        for exc in exceptions:
            loc = generate_file_location_string(file=exc.file, sheet=exc.sheet)
            err_dict[loc][exc.column].append(exc.rownum)

        for loc, data in sorted(err_dict.items(), key=lambda tpl: tpl[0]):
            message += f"\t{loc}\n"
            for col, rownums in sorted(data.items(), key=lambda tpl: tpl[0]):
                message += (
                    f"\t\t'{col}' on row(s): " + str(summarize_int_list(rownums)) + "\n"
                )

        super().__init__(message)


class MissingFCircCalculationValue(SummarizableError, InfileError):
    SummarizerExceptionClass = MissingFCircCalculationValues

    def __init__(self, message: Optional[str] = None, **kwargs):
        if (
            ("file" not in kwargs.keys() and "sheet" not in kwargs.keys())
            or "column" not in kwargs.keys()
            or "rownum" not in kwargs.keys()
        ):
            # Needed for the summary class.  Left off the outer single quotes on purpose to hack in multiple args.
            raise RequiredArgument(
                "rownum', 'column', and 'file' or 'sheet",
                methodname=MissingFCircCalculationValue.__name__,
            )
        if message is None:
            message = (
                "FCirc calculations on TraceBase are done using the tracer peak group(s) from the last serum sample, "
                "the infusion rate, and the animal weight.  This value is missing:\n\t%s"
            )
        if "suggestion" not in kwargs.keys() or kwargs["suggestion"] is None:
            kwargs["suggestion"] = (
                "You can load data into tracebase without these values, but the FCirc values will either be missing "
                "(when there is no animal weight or infusion rate) or potentially inaccurate (if the sample collection "
                "time is missing and the arbitrarily selected 'last' serum sample is not the actual last sample)."
            )
        super().__init__(message, **kwargs)


class ProhibitedCompoundNames(SummarizedInfileError, Exception):
    def __init__(
        self,
        exceptions: List[ProhibitedCompoundName],
    ):
        SummarizedInfileError.__init__(self, exceptions)
        exc: ProhibitedCompoundName
        data: dict = defaultdict(lambda: defaultdict(list))
        for loc, exc_list in self.file_dict.items():
            for exc in exc_list:
                for found in exc.found:
                    data[loc][found].append(exc.rownum)
        message = "Prohibited substrings encountered:\n"
        for loc in sorted(data.keys()):
            message += f"\t{loc}:\n"
            for substr in sorted(data[loc].keys()):
                rowlist = summarize_int_list(data[loc][substr])
                message += f"\t\t'{substr}' on row(s): {rowlist}\n"
        message += (
            "You may manually edit the compound names to address this issue with whatever replacement characters you "
            "wish, but be sure to do so in both the study doc's Compounds sheet AND in all peak annotation files."
        )
        Exception.__init__(self, message)
        self.exceptions = exceptions


class ProhibitedStringValue(Exception):
    def __init__(
        self,
        found: List[str],
        disallowed: Optional[List[str]] = None,
        value: Optional[str] = None,
        message: Optional[str] = None,
        **kwargs,
    ):
        if disallowed is None or len(disallowed) == 0:
            disallowed = found
        if message is None:
            valstr = f" (in '{value}')" if value is not None else ""
            message = (
                f"Prohibited character(s) {found} encountered{valstr}.\n"
                f"None of the following reserved substrings are allowed: {disallowed}."
            )
        super().__init__(message, **kwargs)
        self.found = found
        self.value = value
        self.disallowed = disallowed


class ProhibitedCompoundName(ProhibitedStringValue, InfileError, SummarizableError):
    SummarizerExceptionClass = ProhibitedCompoundNames

    def __init__(
        self,
        found: List[str],
        value: Optional[str] = None,
        fixed: Optional[str] = None,
        disallowed: Optional[List[str]] = None,
        message: Optional[str] = None,
        **kwargs,
    ):
        try:
            column = kwargs.pop("column")
        except KeyError:
            raise RequiredArgument("column", ProhibitedCompoundName.__name__)
        if disallowed is None or len(disallowed) == 0:
            disallowed = found
        if message is None:
            valstr = f" (in compound name '{value}')" if value is not None else ""
            message = (
                f"Prohibited compound name substring(s) {found} encountered{valstr} in %s.\n"
                f"Column '{column}' values may not have any of the following reserved substrings: {disallowed}."
            )
        suggestion = kwargs.get("suggestion")
        if fixed is not None:
            message += (
                f"\n\nThe compound name was automatically repaired to be '{fixed}'."
            )
        elif suggestion is None:
            kwargs["suggestion"] = "Please remove or replace the prohibited substrings."
        ProhibitedStringValue.__init__(
            self, found, disallowed=disallowed, value=value, message=message
        )
        InfileError.__init__(self, message, column=column, **kwargs)
        self.fixed = fixed


class AnimalsWithoutSamples(Exception):

    def __init__(
        self, exceptions: List[AnimalWithoutSamples], message: Optional[str] = None
    ):
        if message is None:
            nlt = "\n\t"
            message = (
                "The following animals do not have any samples:\n\t"
                f"{nlt.join(sorted([e.animal for e in exceptions]))}"
            )
        super().__init__(message)
        self.exceptions = exceptions
        self.animals = [e.animal for e in exceptions]


class AnimalWithoutSamples(InfileError, SummarizableError):
    SummarizerExceptionClass = AnimalsWithoutSamples

    def __init__(self, animal: str, message: Optional[str] = None, **kwargs):
        if message is None:
            message = f"Animal '{animal}' does not have any samples in %s."
        super().__init__(message, **kwargs)
        self.animal = animal


class AnimalsWithoutSerumSamples(Exception):

    def __init__(
        self, exceptions: List[AnimalWithoutSerumSamples], message: Optional[str] = None
    ):
        if message is None:
            nlt = "\n\t"
            message = (
                "The following animals do not have the necessary serum samples to perform FCirc calculations:\n"
                f"\t{nlt.join(sorted([e.animal for e in exceptions]))}\n"
            )
        message += (
            "FCirc calculations on TraceBase are done using the tracer peak group(s) from the last serum sample, the "
            "infusion rate, and the animal weight.  You can load data into TraceBase without serum samples, but the "
            "FCirc values will be missing."
        )
        super().__init__(message)
        self.exceptions = exceptions
        self.animals = [e.animal for e in exceptions]


class AnimalWithoutSerumSamples(InfileError, SummarizableError):
    SummarizerExceptionClass = AnimalsWithoutSerumSamples

    def __init__(self, animal: str, message: Optional[str] = None, **kwargs):
        if message is None:
            message = (
                f"Animal '{animal}' does not have the necessary serum samples to perform FCirc calculations in "
                "%s."
            )
        if "suggestion" not in kwargs.keys() or kwargs["suggestion"] is None:
            kwargs["suggestion"] = (
                "FCirc calculations on TraceBase are done using the tracer peak group(s) from the last serum sample, "
                "the infusion rate, and the animal weight.  You can load data into TraceBase without serum samples, "
                "but the FCirc values will be missing."
            )
        super().__init__(message, **kwargs)
        self.animal = animal


def generate_file_location_string(column=None, rownum=None, sheet=None, file=None):
    loc_str = ""
    if column is not None:
        loc_str += f"column [{column}] "
    if loc_str != "" and rownum is not None:
        loc_str += "on "
    if rownum is not None:
        if isinstance(rownum, list):
            loc_str += f"row(s) {summarize_int_list(rownum)} "
        else:
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
    if intlist is None:
        return []
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
