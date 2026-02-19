from __future__ import annotations

import os
import traceback
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from django.core.exceptions import (
    MultipleObjectsReturned,
    ObjectDoesNotExist,
    ValidationError,
)
from django.core.management import CommandError
from django.db.models import Model, Q, QuerySet
from django.db.utils import ProgrammingError
from django.forms.models import model_to_dict

from DataRepo.utils.text_utils import indent

if TYPE_CHECKING:
    from DataRepo.models.animal import Animal
    from DataRepo.models.archive_file import ArchiveFile
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
    ```
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
    ```
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
        """Constructor.

        Args:
            message (string): An error message containing at least 1 `%s` placeholder for where to put the file location
                information.  If `%s` isn't in the supplied message, it will be appended.  Optionally, the message may
                contain up to 4 more occurrences of `%s`.  See the `order` arg for more information.
            rownum (integer or string): The row number (or name) where the erroneous data was encountered.
            column (integer or string): The column name (or number) where the erroneous data was encountered.
            sheet (integer or string): The sheet name (or index) of an excel file where the erroneous data was
                encountered.
            file (string): The name of the file where the erroneous data was encountered.
            order (list of strings) {"rownum", "sheet", "file", "column", "loc"}: By default, the message is assumed to
                have a single `%s` occurrence where the file location information ("loc"), but if `order` is populated,
                the number of `%s` occurrences is expected to be the same as the length of order.  However, "loc" must
                be included.  If it is not, it is appended (and if necessary, a `%s` is appended to the message as
                well).  The values of the corresponding arguments are inserted into the message at the locations of the
                `%s` occurrences in the given order.
        Raises:
            ValueError: If the length of the order list doesn't match the `%s` occurrences in the message.
        Returns:
            None
        """
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
            ```
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
            ```
        """
        pass


class SummarizedInfileError:
    """This class will break up a list of InfileError exceptions into a dict keyed on the generated file location
    (including the file, sheet, and column).  It is intended to be used in a derived class to sort exceptions into
    sections grouped by file.  Note, it does not call super.__init__, because it is intended to aid in the construction
    of the exception message, so you must multiply inherit to generate the exception message.  Example usage:

    ```
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
    ```
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
    """Summary of every `RequiredValueError` exception.

    Instance Attributes:
        required_value_errors (List[RequiredValueError])
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
    """A value, required to exist in the database, was found to be missing.

    Each sheet in an excel file is loaded independently and the loads proceed in the order of those dependencies.

    Errors like this usually only happen when related dependent data failed to load (due to some other error) and is
    evidenced by the fact that the indicated columns/rows have values.  Fixing errors that appear above this will fix
    this error.

    For example, an Animal record must be loaded and exist in the database before a Sample record (which links to an
    Animal record) can be loaded.  If the loading of the Animal record encountered an error, anytime a Sample record
    that links to that animal is loaded, this error will occur.

    The loading code tries to avoid these "redundant" errors, but it also tries to gather as many errors as possible to
    reduce repeated validate/edit iterations.

    Summarized by `RequiredValueErrors`.

    Instance Attributes:
        column (Any)
        rownum (Any)
        model_name (str)
        field_name (str)
        rec_dict (dict)
    """

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
    """Summary of every RequiredColumnValue exception.

    DEV_SECTION - Everything above this delimiter is user-facing.  See TraceBaseDocs/README.md

    Args:
        exceptions (List[RequiredColumnValue])
        init_message (Optional[str]): The message preceding the summary of the affected file locations.
        suggestion (Optional[str])
    Attributes:
        Class:
            None
        Instance:
            exceptions (List[RequiredColumnValue])
    """

    def __init__(
        self,
        exceptions: List[RequiredColumnValue],
        init_message: Optional[str] = None,
        suggestion: Optional[str] = None,
    ):
        if init_message is None:
            message = "Required column values missing on the indicated rows:\n"
        else:
            message = f"{init_message}:\n"

        rcv_dict: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))
        for rcv in exceptions:
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
        if suggestion is not None:
            message += suggestion
        super().__init__(message)
        self.exceptions = exceptions
        self.init_message = init_message
        self.suggestion = suggestion


class RequiredColumnValue(InfileError, SummarizableError):
    """A value, required to exist in the input table, was not supplied.

    Summarized by `RequiredColumnValues`.
    """

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
    # NOTE: Very rarely user facing
    def __init__(self, group_name, **kwargs):
        message = f"No {group_name} columns found in %s.  At least 1 column of this type is required."
        super().__init__(message, **kwargs)
        self.group_name = group_name


class UnequalColumnGroups(InfileError):
    # NOTE: Very rarely user facing
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
    """Supplies a list of missing required column headers in the input file."""

    def __init__(self, missing, message=None, **kwargs):
        if not message:
            message = f"Required header(s) missing: {missing} in %s."
        super().__init__(message, **kwargs)
        self.missing = missing


class FileFromInputNotFound(InfileError):
    """A report of file names obtained from an input file that could not be found."""

    def __init__(self, filepath: str, message=None, tmpfile=None, **kwargs):
        if message is None:
            msg = ""
            if tmpfile is not None and filepath != tmpfile:
                msg = f" (using temporary file path: '{tmpfile}')"
            message = f"File not found: '{filepath}'{msg}, as parsed from %s."
        super().__init__(message, **kwargs)
        self.filepath = filepath


class UnknownHeader(InfileError, HeaderError):
    """A column header was encountered that is not a part of the file specification."""

    def __init__(self, unknown, known: Optional[list] = None, message=None, **kwargs):
        if not message:
            message = f"Unknown header encountered: [{unknown}] in %s."
            if known is not None:
                message += f"  Must be one of {known}."
        super().__init__(message, **kwargs)


class UnknownHeaders(InfileError, HeaderError):
    """A list of column headers encountered that are not a part of the file specification."""

    def __init__(self, unknowns, message=None, **kwargs):
        if not message:
            message = f"Unknown header(s) encountered: [{', '.join(unknowns)}] in %s."
        super().__init__(message, **kwargs)
        self.unknowns = unknowns


class NewResearchers(Exception):
    """Summary of all `NewResearcher` exceptions."""

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
    """When an as-yet unencountered researcher name is encountered, this exception is raised as a warning to ensure it
    is not a spelling variant of an existing researcher name.

    Summarized per file in `NewResearchers` and across files in `AllNewResearchers`.
    """

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
    # NOTE: Not directly user facing.

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
    """Superclass intended to keep track of missing records for one model for one file."""

    # NOTE: Not directly user facing.

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
    """Superclass intended to keep track of missing records for one model across multiple files."""

    # NOTE: Not directly user facing.

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
    """The expected record from the indicated database model was not found."""

    SummarizerExceptionClass = MissingRecords

    def __init__(
        self, model, query_obj: dict | Q, message=None, suggestion=None, **kwargs
    ):
        """General-use "DoesNotExist" exception constructor for errors retrieving Model records.

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
    """Summary of samples expected to exist in the database that were not found, while loading a single input file.

    Summarized across all files in `AllMissingSamples`.
    """

    ModelName = "Sample"
    RecordName = ModelName


class AllMissingSamples(MissingModelRecordsByFile):
    """Summary of samples expected to exist in the database that were not found, after having attempted to load all
    input files."""

    ModelName = "Sample"
    RecordName = ModelName


class AllUnskippedBlanks(MissingModelRecordsByFile):
    """Summary of samples likely not expected to exist in the database (because they contain "blank" in their name) that
    were not found."""

    ModelName = "Sample"
    RecordName = ModelName

    def __init__(self, *args, suggestion=None, **kwargs):
        if suggestion is None:
            suggestion = (
                "Note that the unskipped blank sample names can be the same in multiple files.  If this exception is "
                "accompanied by a NoPeakAnnotationDetails exception, the reported unskipped blanks are likelt "
                "associated with one of those peak annotation files.  Follow its suggestion and you can ignore this "
                "exception."
            )
        super().__init__(*args, suggestion=suggestion, **kwargs)


class MissingCompounds(MissingModelRecords):
    """Summary of compounds expected to exist in the database that were not found, while loading a single input file.

    Summarized across all files in `AllMissingCompounds`.
    """

    ModelName = "Compound"
    RecordName = ModelName


class AllMissingCompounds(MissingModelRecordsByFile):
    """Summary of compounds expected to exist in the database that were not found, after having attempted to load all
    input files."""

    ModelName = "Compound"
    RecordName = ModelName


class RequiredArgument(Exception):
    # NOTE: Not user facing
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
    """A sample, slated for loading, appears to be a blank.  Loading of blank samples should be skipped.

    Blank samples should be entirely excluded from the `Samples` sheet, but listed in the `Peak Annotation Details`
    sheet with a non-empty value in the `Skip` column.  This tells the peak annotations loader that loads the peak
    annotation file to ignore the sample column with this sample name.

    Blank samples are automatically skipped in the Upload **Start** page's Study Doc download, based on the sample name
    containing "blank" in its name.
    """

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
        self.exceptions = exceptions


class NoSamples(MissingSamples):
    """None of the samples in the indicated file, required to exist in the database, were found.

    Each sheet in an excel file is loaded independently and the loads proceed in the order of those dependencies.

    Errors like this usually only happen when related dependent data failed to load (due to some other error) and is
    evidenced by the fact that the indicated columns/rows have values.  Fixing errors that appear above this will fix
    this error.

    For example, an Animal record must be loaded and exist in the database before a Sample record (which links to an
    Animal record) can be loaded.  If the loading of the Animal record encountered an error, anytime a Sample record
    that links to that animal is loaded, this error will occur.

    The loading code tries to avoid these "redundant" errors, but it also tries to gather as many errors as possible to
    reduce repeated validate/edit iterations.
    """

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
    """The value in the indicated column is optional, but is required to be supplied **with** another neighboring
    column value, that was found to be absent.

    This exception can be resolved either by supplying the neighboring column's value or by removing this column's
    value.

    Example:
        If an infusion rate is supplied, but there was no infusate supplied, the infusion rate will cause an
        UnexpectedInput exception, because an infusion rate without an infusate makes no sense.
    """

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
    """Sample headers found in a peak annotations file were not in the Study Doc's `Peak Annotation Details` sheet.

    This could either be due to a sample header omission in the `Peak Annotation Details` sheet or due to the wrong peak
    annotation file being associated with one or more sample headers in the `Peak Annotation Details` sheet.
    """

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


class NoPeakAnnotationDetailsErrors(Exception):
    """Summarizes multiple NoPeakAnnotationDetails exceptions."""

    def __init__(self, exceptions: List[NoPeakAnnotationDetails]):
        summary_list = [
            (
                "No sample headers for the following peak annotation files were found in the Peak Annotation Details "
                "sheet:\n"
            )
        ]
        for exc in sorted(exceptions, key=lambda e: e.annot_file):
            line = f"\t{exc.annot_file}\n"
            if line not in summary_list:
                summary_list.append(line)
        summary_list.append(
            (
                "An attempt will be made to automatically associate the headers with Sample records, but you may see "
                "warnings about unskipped blanks.  If any samples cannot be found, you can associate them by "
                "populating the Peak Annotation Details sheet.  It is recommended that you use the submission start "
                "page to generate this data."
            )
        )
        super().__init__("\n".join(summary_list))


class NoPeakAnnotationDetails(InfileError, SummarizableError):
    """No sample headers were found in the Peak Annotation Details sheet of the Study doc for the indicated peak
    annotation file.

    This usually occurs if a user is adding data to an existing study doc and neglects to update the Peak Annotation
    Details sheet using the output of the submission start page."""

    SummarizerExceptionClass = NoPeakAnnotationDetailsErrors

    def __init__(self, annot_file: str, **kwargs):
        message = (
            f"No sample headers for peak annotation file '{annot_file}' were found in %s.  An attempt will be made to "
            "automatically associate the headers with Sample records, but you may see warnings about unskipped "
            "blanks.  If any samples cannot be found, you can associate them by populating the Peak Annotation Details "
            "sheet.  It is recommended that you use the submission start page to generate this data."
        )
        super().__init__(message, **kwargs)
        self.annot_file = annot_file


class EmptyColumns(InfileError):
    """The data import encountered empty columns that were expected to have data.

    If there are sample columns present and all expected samples are accounted for, this will be a warning.  If any of
    the expected sample columns are missing, this will be an error.

    In the warning case, this issue usually occurs when columns in Excel have been removed (or some unknown file
    manipulation has occurred).  Whatever the case may be, the excel reader package that the loading code uses treats
    these empty columns as populated and names them with an arbitrary column header that starts with 'Unnamed: '.

    In the error case, no sample headers were found.  The file either contains no sample data and should be either
    repaired or excluded from loading, meaning that it will need to be removed from the `Peak Annotation Details` and
    `Peak Annotation Files` sheets."""

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
    """Exception thrown during dry-run to ensure atomic transaction is not committed"""

    # NOTE: Not user facing

    def __init__(self, message=None):
        if message is None:
            message = "Dry Run Complete."
        super().__init__(message)


class MultiLoadStatus(Exception):
    """This class holds the load status of multiple files and also can contain multiple file group statuses, e.g. a
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
            new_aes = AggregatedErrors(debug=self.debug)
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
    """Contains multiple AggregatedErrors exceptions in a dict keyed on a string.  The string is arbitrary, but is
    treated usually as either a filename/path or as categorical/organizational, to summarize related/identical errors
    from multiple input files."""

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
    """This is not a typical exception class.  You construct it before any errors have occurred and you use it to buffer
    exceptions using object.buffer_error(), object.buffer_warning(), and (where the error/warning can change based on a
    boolean) object.buffer_exception(is_error=boolean_variable).  You can also decide whether a warning should be
    raised as an exception or not using the is_fatal parameter.  This is intended to be able to report a warning to the
    validation interface (instead of just print it).  It know whether or not the AggregatedErrors should be raised as
    an exception or not, at the end of a script, call object.should_raise().

    A caught exception can be buffered, but you can also buffer an exception class that is constructed outside of a
    try/except block.

    Note, this class annotates the exceptions it buffers.  Each exception is available in the object.exceptions array
    and each exception contains the following data members.

    Instance Attributes:
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
                # TODO: This should separate the search state and change state.  I.e. if you want to change things to a
                # warning, but want to retrieve both errors and warnings, that can't be done using this method.
                # Besides, the method name is inconsistent with the search behavior.
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
    """A summarization of `ConflictingValueError` exceptions.

    Instance Attributes:
        exceptions (List[ConflictingValueError])
    """

    def __init__(
        self,
        exceptions: list[ConflictingValueError],
        suggestion: Optional[str] = None,
    ):
        """Initializes a ConflictingValueErrors exception"""

        message = "Conflicting values encountered during loading:\n"
        conflict_data: Dict[str, dict] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        for cve in exceptions:
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
        if suggestion is not None:
            message += f"\n{suggestion}"
        super().__init__(message)
        self.exceptions = exceptions


class ConflictingValueError(InfileError, SummarizableError):
    """A conflicting value was encountered between previously loaded data and data being loaded from an input file.

    The loading code does not currently support database model record updates, but it does support **adding** data to an
    existing (and previously loaded) input file.  Some of those additions can **look** like updates.  Values on a
    previously loaded row in delimited columns like the `Synonyms` column in the `Compounds` sheet, can receive
    additional delimited values without error.

    But when values in a column (outside of columns containing delimited values) change in a file that has been
    previously loaded, you will get a `ConflictingValueError` exception.

    Note that formatted columns (e.g. an infusate name) may use delimiters, but are not treated as delimited columns.

    Summarized in `ConflictingValueErrors`.

    Instance Attributes:
        rec (Optional[Model]): Model record that conflicts.  Optional if the exception message was provided.
        rec_dict (Optional[dict]):

            Dict created from file representing what was attempted to be loaded.  Optional if the exception message was
            provided.

        differences (Optional[dict])
    """

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
            rec_dict (dict): The dict that was (or would be) supplied to Model.get_or_create()
            derived (boolean) [False]: Whether the database value was a generated value or not.  Certain fields in the
                database are automatically maintained, and values in the loading file may not actually be loaded, thus
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
                # TODO: Fix this logic issue. "No record provided" will never be assigned.
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
                    "or may not exist.  The value in your file conflicts with the generated value."
                )
        super().__init__(message, **kwargs)
        self.rec = rec  # Model record that conflicts
        self.rec_dict = rec_dict  # Dict created from file
        self.differences = differences


class DuplicateValueErrors(Exception):
    """Summary of DuplicateValues exceptions.

    This is set as the `SummarizerExceptionClass` class of `DuplicateValues`."""

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
    """A duplicate value (or value combination) was found in an input file column (or columns) that requires unique
    values (or a unique combination of values with 1 or more other columns).

    Fixing this issue typically involves either deleting a duplicate row or editing the duplicate to make it unique.

    Summarized in `DuplicateValueErrors`.
    """

    SummarizerExceptionClass = DuplicateValueErrors

    def __init__(self, dupe_dict, colnames, message=None, addendum=None, **kwargs):
        """Takes a dict whose keys are (composite, unique) strings and the values are lists of row indexes"""
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
    """Summary of DuplicateValues exceptions specific to the peak annotation files.  It does not report the affected
    samples because all such errors always affect all samples, as peak annotation files typically have a column for each
    sample and a row for each compound's isotopic state.

    This error occurs when a compound's unique isotopic makeup appears in multiple rows.
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


# TODO: Remove this SheetMergeError class and its associated tests.  It is no longer used.
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
    """A summarization of `NoTracerLabeledElements` exceptions."""

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
    """A compound in a peak annotation file was encountered that does not contain any of the labeled elements from any
    of the tracers.

    The purpose of a peak group (which the loading code populates) is to group a compound's peaks that result from
    various isotopic states (the incorporation of labeled elements from the tracer compounds).  If the formula of the
    measured compound does not contain any of the elements that are labeled in the tracers, this suggests a potential
    problem, such as the animal's infusate from the `Animals` sheet was incorrectly selected or omits a tracer with
    labels that are in this compound.

    Resolutions to this issue can involve either updating the associated animal's infusate/tracers to include a tracer
    with the labeled elements it shares with this compound or simply ignoring this warning noting that the compound will
    not be loaded as a peak group.^

    Summarized in `NoTracerLabeledElementsError`.

    ^ _TraceBase was not designed to support non-isotopic mass spectrometry data.  Adding support for non-isotopic_
    _data is a planned feature.  See GitHub issue_
    _[#1192](https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/1192)._
    """

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
    """An operation that requires an animal to have been infused with tracers encountered an animal that was not infused
    with tracers, such as FCirc calculations.

    This error occurs when an animal is associated with an infusate record, but that infusate is not linked to any
    tracers.  This is likely because an error occurred during infusate/tracer loading and arises when validating a serum
    sample."""

    def __init__(self, animal: Optional[Animal] = None, message=None, **kwargs):
        if message is None:
            animal_str = f" [{animal}]"
            message = f"The Animal{animal_str} associated with %s, has no tracers."
        super().__init__(message, **kwargs)
        self.animal = animal


class IsotopeStringDupe(InfileError):
    """The formatted isotope string matches the same labeled element more than once.

    Strings defining isotopes are formatted with multiple element symbols paired with mass numbers concatenated
    together, followed by their dash-delimited counts in the same relative order.  This error occurs when that isotope
    string matches the same element multiple times.

    Unfortunately, the only way to address this error would be to edit the peak annotation file to eliminate the
    duplicate.

    Example:
        `C13N15C13-label-2-1-1` would match `C13` twice, resulting in this error.
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
    """Summary of all `MissingC12ParentPeak` exceptions."""

    def __init__(
        self,
        exceptions: list[MissingC12ParentPeak],
    ):
        """Constructor.

        Args:
            exceptions (List[MissingC12ParentPeak]): A list of MissingC12ParentPeak exceptions
        """

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
    """No C12 PARENT row was found for this compound in the peak annotation file.

    This exception occurs (as a warning) in 2 cases:

    - The C12 PARENT peak exists, but was not picked in El-Maven.  In this case, the best solution is to redo the peak\
      annotation file starting from re-picked peaks from El-Maven that include the parent peak.  Alternatively, the\
      peak annotation file could be edited to remove all of that compound's peaks and a subsequent file could be loaded\
      using the complete peak group.
    - The C12 PARENT peak was below the detection threshold.  In this case, the warning can be ignored and a 0-count\
      will be assumed.

    Summarized in `MissingC12ParentPeaks`.
    """

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
    """Summary of tissues expected to exist in the database that were not found, while loading a single input file.

    Summarized across all files in `AllMissingTissues`.
    """

    ModelName = "Tissue"
    RecordName = ModelName


class AllMissingTissues(MissingModelRecordsByFile):
    """Summary of tissues expected to exist in the database that were not found, after having attempted to load all
    input files."""

    ModelName = "Tissue"
    RecordName = ModelName


class MissingStudies(MissingModelRecords):
    """Summary of studies expected to exist in the database that were not found, while loading a single input file.

    Summarized across all files in `AllMissingStudies`.
    """

    ModelName = "Study"
    RecordName = ModelName


class AllMissingStudies(MissingModelRecordsByFile):
    """Summary of studies expected to exist in the database that were not found, after having attempted to load all
    input files."""

    ModelName = "Study"
    RecordName = ModelName


class MissingTreatments(MissingModelRecords):
    """Summary of treatments expected to exist in the database that were not found, while loading a single input
    file.

    Summarized across all files in `AllMissingTreatments`.
    """

    ModelName = "Protocol"
    RecordName = "Treatment"


class AllMissingTreatments(MissingModelRecordsByFile):
    """Summary of treatments expected to exist in the database that were not found, after having attempted to load all
    input files."""

    ModelName = "Protocol"
    RecordName = "Treatment"


class ParsingError(Exception):
    """Superclass of infusate, tracer, and isotope parsing errors."""

    pass


class InfusateParsingError(ParsingError):
    """An error was encountered when reading (parsing) your Infusate.  The format or completeness of the infusate name
    must be manually corrected.  Consult formatting guidelines in the Study Doc Infusate column header's comment.
    """

    pass


class TracerParsingError(ParsingError):
    """A regular expression or other parsing error was encountered when parsing a Tracer string.  The formatting or
    completeness of the string must be manually fixed.  Consult formatting guidelines (check the file's header
    comment)."""

    pass


class IsotopeParsingError(ParsingError):
    """A regular expression or other parsing error was encountered when parsing an Isotope string.  The formatting or
    completeness of the string must be manually fixed.  Consult formatting guidelines (check the file's header
    comment)."""

    pass


class ObservedIsotopeParsingError(InfileError):
    """A regular expression or other parsing error was encountered when parsing an Isotope observation string.  The
    formatting or completeness of the string must be manually fixed.  Consult formatting guidelines (check the file's
    header comment)."""

    pass


class ObservedIsotopeUnbalancedError(ObservedIsotopeParsingError):
    """The number of elements, mass numbers, and counts parsed from the isotope string differ.  A single (fully labeled)
    isotope must include each value in the order of mass number, element symbol, and count.  E.g. `13C5` means that
    there are 5 heavy carbons of mass number 13 in a compound.

    Examples:
        - `13C` would cause this error because there is no count.
        - `C5` would cause this error because there is no mass number.
        - `135` would cause this error because there is no element and there's no way to tell where the count begins.
    """

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
    """Summary of `UnexpectedLabels` exceptions arising from multiple input files."""

    def __init__(self, exceptions: List[UnexpectedLabel], **kwargs):
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
        # We're going to ignore kwargs.  It's only there to consume args taken by all the other "All*" exceptions
        super().__init__(message)
        self.exceptions = exceptions
        self.counts = counts


class UnexpectedLabels(Exception):
    """Summary of `UnexpectedLabel` exceptions in a single input file.

    Summarized across all files in `AllUnexpectedLabels`.
    """

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
    """An isotope label, e.g. nitrogen (`N`) was detected in a measured compound, but that labeled element was not in
    any of the tracers.  This is reported as a warning to suggest that there could be contamination or the wrong
    infusate was selected for an animal, but this is often the result of naturally occurring isotopes and can be
    ignored.

    Summarized per file in `UnexpectedLabels` and across all files in `AllUnexpectedLabels`.
    """

    SummarizerExceptionClass = UnexpectedLabels

    def __init__(self, unexpected: List[str], possible: List[str], **kwargs):
        message = (
            f"One or more observed peak labels were not among the label(s) in the tracer(s):\n"
            f"\tObserved: {unexpected}\n"
            f"\tExpected: {possible}\n"
            "There may be contamination.  (Note, the reported observed are only the unexpected labels.)"
        )
        super().__init__(message, **kwargs)
        self.possible = possible
        self.unexpected = unexpected

        bad_unexpected = self.check_type(unexpected)
        if len(bad_unexpected) > 0:
            raise TypeError(
                f"unexpected argument must be a List[str].  These types were found: {bad_unexpected}"
            )
        bad_possible = self.check_type(possible)
        if len(bad_possible) > 0:
            raise TypeError(
                f"possible argument must be a List[str].  These types were found: {bad_possible}"
            )

    def check_type(self, lst: list) -> list:
        """Returns a unique list of non-str type names found in lst."""
        bad_type_names = []
        for elem in lst:
            if not isinstance(elem, str) and type(elem).__name__ not in bad_type_names:
                bad_type_names.append(type(elem).__name__)
        return bad_type_names


class NoCommonLabel(Exception):
    """A compound in a peak group was encountered (e.g. during an FCirc calculation) that does not contain any of the
    labeled elements from any of the tracers.

    The purpose of a peak group is to group a compound's peaks that result from various isotopic states (the
    incorporation of labeled elements from the tracer compounds).  If the formula of the peak group compound does not
    contain any of the elements that are labeled in the tracers, this suggests a potential problem, such as the animal's
    infusate was incorrectly selected or omits a tracer with labels that are in this compound.

    This exception is reported almost always as a warning and results in an FCirc calculation being recorded as `None`.^

    ^ _TraceBase was not designed to support non-isotopic mass spectrometry data.  Adding support for non-isotopic data_
    _is a planned feature.  See GitHub issue_
    _[#1192](https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/1192)._
    """

    # NOTE: Not user facing.  This occurs in the site log when in debug mode.  This is essentially the same exception as
    # NoTracerLabeledElements, but takes different arguments for convenience and is raised in a different context
    # (during FCirc calculations).

    def __init__(self, peakgrouplabel: PeakGroupLabel):
        msg = (
            f"PeakGroupLabel '{peakgrouplabel.element}' for PeakGroup '{peakgrouplabel.peak_group.name}' (from "
            f"infusate '{peakgrouplabel.peak_group.msrun_sample.sample.animal.infusate}') not present in the peak "
            f"group's formula '{peakgrouplabel.peak_group.formula}'."
        )
        super().__init__(msg)
        self.peak_group_label = peakgrouplabel


class AllNoScans(Exception):
    """Summary of NoScans exceptions."""

    def __init__(self, no_scans_excs, message=None, **kwargs):
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

        # We're going to ignore kwargs.  It's only there to consume args taken by all the other "All*" exceptions
        super().__init__(message)
        self.no_scans_excs = no_scans_excs


class NoScans(InfileError, SummarizableError):
    """An mzXML file was encountered that contains no scan data.

    This exception is raised as a warning and can be safely ignored.  Empty mzXML files are produced as a side-effect of
    the way they are produced.  Such files could be excluded from a study submission, but are hard to distinguish
    without looking inside the same-named files.  It is recommended that the files be left as-is.

    Summarized across all files in `AllNoScans`.
    """

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
    """Summary of `MzxmlSequenceUnknown` exceptions from multiple input files."""

    def __init__(self, exceptions, message=None, **kwargs):
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

        # We're going to ignore kwargs.  It's only there to consume args taken by all the other "All*" exceptions
        super().__init__(message)
        self.exceptions = exceptions


class MzxmlSequenceUnknown(InfileError, SummarizableError):
    """Unable to reliably match an `mzXML` file with an MSRunSequence.

    This exception is raised as a warning when the number of `mzXML` files with the same name are not all accounted for
    in the `Peak Annotation Details` sheet of the Study Doc.  I.e. there are more `mzXML` files than peak annotation
    files with sample headers of this name.

    There are a number of ways this can happen:
        - The extra files are empty (and are reported in `NoScans` warnings).
        - A peak annotation file for the extras has not been included in the load.
        - The sample was re-analyzed in a subsequence MS Run because there was a problem with the first run.
        - There are 2 different biological samples with the same name and one is not included in the current submission.

    The first case is handled automatically and can be safely ignored.  In fact, if it is any other case, an error would
    be raised after this warning, so in any case, this can be ignored, but if subsequent error does occur, this warning
    provides information that can help figure out the problem.

    In all of the other cases, there are 2 ways to resolve the warning:

    - Add rows to the `Peak Annotation Details` sheet that account for all the files (adding 'skip' to the Skip column\
      for any files that should be ignored).  This is the preferred solution.
    - Add the relative path from the study folder to the specific mzXML file in the existing 'mzXML File Name' column\
      (not including the study folder name).

    Despite the 'required columns' highlighted in blue indicating that `Sample Name` and `Sample Data Header` are
    required, when there is no associated peak annotation file, the `mzXML File Name`, `Sequence`, and `Skip` columns
    are all that's required.  This is a special case.

    If all of the files are for the same MS Run, nothing further is needed.  But if they are from different MS Runs, the
    `mzXML File Name` column must contain the relative path from the study folder to the `mzXML` file (not including the
    study folder name).

    Summarized across all files in `AllMzxmlSequenceUnknown`.
    """

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


class MzxmlNotColocatedWithAnnots(Exception):
    """mzXML files are not in a directory under an unambiguously associated peak annotation file in which they were
    used.

    This exception has to do with determining which MS Run `Sequence` produced the mzXML file, which is dynamically
    determined when there are multiple sequences containing the same sample names.

    mzXML files are assigned an MS Run Sequence based on either the value in the `Sequence` column in the
    `Peak Annotation Details` sheet or (if that's empty), the `Default Sequence` defined in the `Peak Annotation Files`
    sheet and the `Peak Annotation File Name` column in the `Peak Annotation Details` sheet.

    If the default is used, this exception is a warning.  However, if there is no default and no `Sequence` in the
    `Peak Annotation Details` sheet's `Sequence` column, the association is inferred by the directory structure.  By
    travelling up the path from the mzXML file to the study directory, the first peak annotation file encountered is the
    one that is associated with the mzXML file.  The simplest case is when the mzXML file is in the same directory as a
    single peak annotation file.

    The loading code only raises this as an error when the mzXML file name matches headers in multiple peak annotation
    files from different sequences and the specific one in which it was used was not explicitly assigned and it could
    not be inferred from the directory structure.

    The easiest fix is to put peak annotation files in a directory along with only the mzXML files that were used in its
    production.  The more laborious (but more versatile) solution is to add the file path of every mzXML reported in the
    error to the `Peak Annotation Details` sheet along with the `Sequence`.
    """

    def __init__(
        self,
        exceptions: List[MzxmlNotColocatedWithAnnot],
        message: Optional[str] = None,
        **kwargs,
    ):
        if not message:
            files = []
            exc: MzxmlNotColocatedWithAnnot
            for exc in exceptions:
                files.append(exc.file)
            nlt = "\n\t"
            message = (
                f"The following mzXML files do not have a peak annotation file existing along their paths.\n"
                "When a sequence is not provided in the 'Peak Annotation Details' sheet for an mzXML file, the "
                "association between an mzXML and the MSRunSequence it belongs to is inferred by its colocation with "
                "(or its location under a parent directory containing) a peak annotation file, based on the 'Default "
                "Sequence' column in the 'Peak Annotation Files' sheet.  These files do not have a peak annotation "
                "file associated with them:\n"
                f"\t{nlt.join(files)}\n"
                "Either provide values in the 'Sequence' column in the 'Peak Annotation Files' sheet or add the "
                "related peak annotation file to the directory containing the mzXML files that were used to generate "
                "it."
            )
        super().__init__(message, **kwargs)


class MzxmlNotColocatedWithAnnot(InfileError, SummarizableError):
    SummarizerExceptionClass = MzxmlNotColocatedWithAnnots

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
        self.annot_dirs = annot_dirs


class MzxmlColocatedWithMultipleAnnots(Exception):
    """`mzXML` files are in a directory that has multiple peak annotation files somewhere along its path.

    This exception has to do with determining which MS Run `Sequence` produced the `mzXML` file, which is dynamically
    determined when there are multiple sequences containing the same sample names.

    `mzXML` files are assigned an MS Run Sequence based on either the value in the `Sequence` column in the
    `Peak Annotation Details` sheet or (if that's empty), the `Default Sequence` defined in the `Peak Annotation Files`
    sheet and the `Peak Annotation File Name` column in the `Peak Annotation Details` sheet.

    If the default is used, this exception is a warning.  However, if there is no default and no `Sequence` in the
    `Peak Annotation Details` sheet's `Sequence` column, the association is inferred by the directory structure.  By
    travelling up the path from the `mzXML` file to the study directory, the first peak annotation file encountered is
    the one that is associated with the `mzXML` file.  The simplest case is when the `mzXML` file is in the same
    directory as a single peak annotation file.

    The loading code only raises this as an error when the `mzXML` file name matches headers in multiple peak annotation
    files from different sequences and the specific one in which it was used was not explicitly assigned and it could
    not be inferred from the directory structure.

    The easiest fix is to put peak annotation files in a directory along with only the `mzXML` files that were used in
    its production.  The more laborious (but more versatile) solution is to add the file path of every `mzXML` reported
    in the error to the `Peak Annotation Details` sheet along with the `Sequence`.
    """

    def __init__(
        self,
        exceptions: List[MzxmlColocatedWithMultipleAnnot],
        message: Optional[str] = None,
        **kwargs,
    ):
        if not message:
            dirs: Dict[str, Dict[str, List[str]]] = defaultdict(
                lambda: {"files": [], "seqs": []}
            )
            exc: MzxmlColocatedWithMultipleAnnot
            for exc in exceptions:
                # Append the mzXML file to the files list for that directory combo
                dirs[exc.matching_annot_dir]["files"].append(exc.file)
                # Append the differing sequences if not already encountered
                for seqn in exc.msrun_sequence_names:
                    if seqn not in dirs[exc.matching_annot_dir]["seqs"]:
                        dirs[exc.matching_annot_dir]["seqs"].append(seqn)
            # Summarize the nested lists in a string
            summary = ""
            for dir in dirs.keys():
                summary += (
                    f"\n\tDirectory '{dir}' contains multiple peak annotation files associated with sequences "
                    f"{dirs[dir]['seqs']}:"
                )
                for mzxml in dirs[dir]["files"]:
                    summary += f"\n\t\t{mzxml}"
            message = (
                f"The following directories have multiple peak annotation files (associated with different 'Default "
                "Sequence's, assigned in the 'Peak Annotation Files' sheet), meaning that the listed mzXML files "
                "cannot be unambiguously assigned an MSRunSequence record.\n"
                f"{summary}\n\n"
                "Explanation: When a sequence is not provided in the 'Peak Annotation Details' sheet for an mzXML "
                "file, the association between an mzXML and the MSRunSequence it belongs to is inferred by its "
                "colocation with (or its location under a parent directory containing) a peak annotation file, based "
                "on the 'Default Sequence' assigned in the 'Peak Annotation Files' sheet.\n\n"
                "Suggestion: Either provide values in the 'Sequence' column in the 'Peak Annotation Files' sheet or re-"
                "arrange the multiple colocated peak annotation files to ensure that they are in the directory "
                "containing the mzXML files that were used to generate them.  (If a peak annotation file was generated "
                "using a mix of mzXML files from different sequences, the 'Sequence' column in the 'Peak Annotation "
                "Details' sheet must be filled in and it is recommended that mzXML files are grouped into directories "
                "defined by the sequence that generated them.)"
            )
        super().__init__(message, **kwargs)


class MzxmlColocatedWithMultipleAnnot(InfileError, SummarizableError):
    SummarizerExceptionClass = MzxmlColocatedWithMultipleAnnots

    def __init__(
        self, msrun_sequence_names: List[str], matching_annot_dir: str, **kwargs
    ):
        nlt = "\n\t"
        message = (
            "mzXML file '%s' shares a common path with multiple peak annotation files (from the peak annotation files "
            f"sheet), located in directory '{matching_annot_dir}' that are associated with different "
            f"sequences:\n\t{nlt.join(msrun_sequence_names)}\nCo-location of mzXML files with peak annotation files is "
            "what allows mzXML files to be linked to an MSRunSequence, based on the Default Sequence column in the "
            "Peak Annotation Files sheet."
        )
        super().__init__(message, **kwargs)
        self.msrun_sequence_names = msrun_sequence_names
        self.matching_annot_dir = matching_annot_dir


class DefaultSequenceNotFound(Exception):
    """An MS Run Sequence record, expected to exist in the database, could not be found.

    Note that each sheet in the study doc is loaded independently, but the order in which they are loaded matters.  For
    example, the `Sequences` sheet must be loaded before the `Peak Annotation Files` sheet.  If there was an error when
    loading any rows on the `Sequences` sheet, this error would be encountered when attempting to find that sequence
    that was just loaded.

    Alternatively, this exception could have arisen because a `Sequences` sheet row was edited and values in the
    `Default Sequence` column in the `Peak Annotation Files` sheet (or other linking column) was not similarly updated
    and became unlinked.

    To resolve this exception, either the previous error must be fixed, or the `Default Sequence` column's value in the
    `Peak Annotation Files` sheet must be updated to match a row in the `Sequence Name` column in the `Sequences` sheet.
    """

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
    """Multiple MS Run Sequence record were found, but only one was expected.

    To resolve this exception, supply all 4 search criteria (operator, protocol, instrument, and date).
    """

    # NOTE: Not user facing.  This arises from the command line when not all default sequence options are used.
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
    """Summary of `MzXMLSkipRowErrors` exceptions from multiple input files."""

    def __init__(self, exceptions, message=None, **kwargs):
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

        # We're going to ignore kwargs.  It's only there to consume args taken by all the other "All*" exceptions
        super().__init__(message)
        self.exceptions = exceptions


class MzXMLSkipRowError(InfileError, SummarizableError):
    """Could not determine which mzXML file loads to skip.

    When the mzXML file paths are not supplied and the number of mzXML files of the same name exceed number of skipped
    sample data headers in the peak annotation files (i.e. some of the same-named files are to be loaded and others are
    to be skipped), it may be impossible to tell which ones are which.

    The loading code can infer which are which if the files are divided into directories with their peak annotation
    files in which they were used, but if that cannot be figured out, this error will be raised.

    This can be resolved either by accounting for all mzXML files in the `Peak Annotation Details` sheet with their
    paths or by organizing the mzXML files with the peak annotation files they were used to produce.

    Summarized per file in `AllMzXMLSkipRowErrors` and across all files in `AllMzXMLSkipRowErrors`.
    """

    SummarizerExceptionClass = AllMzXMLSkipRowErrors

    def __init__(
        self,
        mzxml_name: str,
        existing_files: List[str],
        skip_paths_dict: Dict[str, int],
        **kwargs,
    ):
        """The situation here is that we may not be able to identify which files to skip and which to load.  dirs could
        contain files we should skip because the user didn't provide directory paths on the skipped rows from the
        infile.  We only know to skip them if the number of skips and the number of directories is the same - but they
        differ.

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
    """The mzXML filename does not match the sample header in the peak annotation file.

    This situation can arise either if the filename has been (knowingly) manually modified or when the `mzXML File Name`
    entered into the `Peak Annotation Details` sheet was mistakenly associated with the wrong `Sample Data Header`.

    This exception is only ever raised as a warning and is not inspected by curators, so confirm the association and
    either make a correction or ignore, if the association is correct.
    """

    def __init__(self, header, mzxml_file, **kwargs):
        mzxml_basename, _ = os.path.splitext(os.path.basename(mzxml_file))
        message = (
            f"The sample header does not match the base name of the mzXML file [{mzxml_file}], as listed in %s:\n"
            f"\tSample header:       [{header}]\n"
            f"\tmzXML Base Filename: [{mzxml_basename}]\n"
            "Sample headers automatically adopt the mzXML file name.  The fact that they differ could suggest that the "
            "wrong mzXML has been associated with the wrong abundance correction analysis/peak annotation file.  If "
            "this mapping is correct, you may ignore this issue."
        )
        super().__init__(message, **kwargs)
        self.header = header
        self.mzxml_basename = mzxml_basename
        self.mzxml_file = mzxml_file


class AssumedMzxmlSampleMatches(Exception):
    """Summary of `AssumedMzxmlSampleMatch` exceptions."""

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
    """The sample name embedded in the mzXML filename uniquely but imperfectly matches.

    This exception is only ever raised as a warning.  Some peak abundance correction tools have certain character
    restrictions applied to sample headers.  Some such restrictions are:

    - Sample names cannot start with a number
    - No dashes (`-`) allowed
    - Length limits

    Those restritions are not the same as those of the mass spec instrument software or the tools that generate the
    mzXML files from RAW files.  To get around those restrictions when they are encountered, the sample headers are
    often modified, but the mzXML file names remain the original value.

    The loading code accommodates these peculiarities in order to be able to dynamically match the mzXML files with the
    corresponding peak annotation file sample header.  This warning serves to simply be transparent about the
    association being automatically made, in order to catch any potential authentic mismatches.

    In every known case, this warning can be safely ignored.

    Summarized in `AssumedMzxmlSampleMatches`.
    """

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


class AmbiguousMzxmlSampleMatches(Exception):
    """Summary of `AmbiguousMzxmlSampleMatch` exceptions.

    DEV_SECTION - Everything above this delimiter is user-facing.  See TraceBaseDocs/README.md

    Args:
        exceptions (List[AmbiguousMzxmlSampleMatch])
        message (Optional[str])
    Attributes:
        Class:
            None
        Instance:
            exceptions (List[AmbiguousMzxmlSampleMatch])
    """

    def __init__(self, exceptions: List[AmbiguousMzxmlSampleMatch], message=None):
        if message is None:
            message = (
                "The following mzXML files could not be mapped to a single sample.  Each mzXML must be associated with "
                "an MSRunSample, which links to a Sample record, so knowing which sample an mzXML is associated with "
                "is required.\n"
            )
            matches_by_annot_file: Dict[str, Dict[str, List[str]]] = defaultdict(
                lambda: defaultdict(list)
            )
            exc: AmbiguousMzxmlSampleMatch
            for exc in exceptions:
                loc = generate_file_location_string(
                    file=exc.file,
                    sheet=exc.sheet,
                )
                samples = ", ".join(sorted(exc.sample_names))
                matches_by_annot_file[loc][samples].extend(exc.mzxml_paths)
            for loc, samples_dict in sorted(
                matches_by_annot_file.items(), key=lambda tpl: tpl[0]
            ):
                message += f"{loc}\n"
                for samples_str, mzxml_list in sorted(
                    samples_dict.items(), key=lambda t: t[0]
                ):
                    message += f"\tmzXML(s) that map to samples: {samples_str}\n"
                    for mzxml in sorted(mzxml_list):
                        message += f"\t\t{mzxml}\n"
            message += (
                "To resolve this, either add every mzXML (with its path) to an existing row or new row, each "
                "associated with a single sample to the Peak Annotation Details sheet.  Paths should be relative to "
                "the study directory.  Set the row to be skipped if the mzXML was not used in an abundance correction "
                "analysis.  Every mzXML in the study directory must be accounted for in the Peak Annotation Details "
                "sheet."
            )
        super().__init__(message)
        self.exceptions = exceptions


class AmbiguousMzxmlSampleMatch(InfileError, SummarizableError):
    """An mzXML file could not be unambiguously mapped to a single sample record.

    Each mzXML must be associated with a single Sample record.  The link between the mzXML and the Sample is indirect.
    The link is via an MSRunSample record, which links to the Sample.  Knowing which sample an mzXML is associated with
    is thus required.

    Sometimes this is due to mzXML name collisions that the user explicitly mapped to different samples, but the
    association was not explicitly made for each mzXML file because there exist leftover, unaccounted-for mzXML files
    that were not involved in abundance correction.  For example, there could exist an unanalyzed positive scan or scan-
    range mzXML.  To ensure comprehensive saving of all mzXML files, the loading code looks at all files in the study
    directory and tries to automatically associate them with a sequence and a sample, and there are multiple routes that
    can result in an ambiguous match.

    To resolve this ambiguous match, the mzXML file with ambiguous name, including its path relative from the study
    directory, must be added to the Peak Annotation Details sheet.  The error will include all the potentially matching
    sample names.  Decide which one is the correct biological sample from which the mzXML was derived and do one of two
    things:

    1. If the mzXML was analyzed (i.e. included in an abundance correction analysis), there should exist a row already
       in the Peak Annotation Details sheet.  You must find that row and add the relative path from the study directory
       to the mzXML file to the "mzXML File Name" column on the row containing the corresponding Sample and the peak
       annotation file.
    2. If the mzXML was *not* analyzed (i.e. it was not used for peak picking and natural abundance correct), there will
       not exist a row in the Peak Annotation Details sheet.  (NOTE: Sometimes, this may be simply due to the fact that
       the mzXML contains no scan data.)  You must add a row to the Peak Annotation Details sheet that contains only~
       the Sample, mzXML File Name (including the relative path from the study directory), and "skip" in the Skip
       column.  Skip indicates only that there will be no peak group data and that an MSRunSample record does not need
       to be created to link it.  But note that this will not prevent later adding peak data to the study in the future.
       This should be done even for the mzXML files that do not contain scan data.  And it only needs to be done for
       those files affected by an ambiguous match.^

    ~ NOTE: The blue column headers in the Peak Annotation Details sheet indicate the required columns, but they are
      only for the common case (when there exists a peak annotation file and PeakGroup data).  The required columns are
      actually case-conditional.  In this case, where there is no peak annotation file, only the Sample and mzXML File
      Name columns are required.  In fact, the "skip" is in the Skip column, no columns are required.
    ^ There may be warnings associated with some files such as those associated with blank samples, no scans, or
      ambiguous sequence assignments.  Adding those to the Peak Annotation Details sheet will silence the warnings, but
      as long as the warnings don't indicate a data problem and that the automatic handling is accurate, the warnings
      are harmless.

    DEV_SECTION - Everything above this delimiter is user-facing.  See TraceBaseDocs/README.md

    Args:
        sample_names (List[str]): A list of 2 or more sample names that an mzXML file is ambiguously associated with.
        mzxml_paths (List[str]): An mzXML file name (with optional path) that ambiguously maps to multiple samples.
        kwargs (dict): See InfileError.
    Attributes:
        Class:
            SummarizerExceptionClass (Exception): The exception class that takes a list of objects from this class and
                summarizes their content.
        Instance:
            sample_names (List[str])
            mzxml_paths (List[str])
    """

    SummarizerExceptionClass = AmbiguousMzxmlSampleMatches

    def __init__(
        self,
        sample_names: List[str],
        mzxml_paths: List[str],
        inferred_match=False,
        **kwargs,
    ):
        if not isinstance(mzxml_paths, list):
            raise TypeError(
                f"mzxml_paths must be a list (of strings), not {type(mzxml_paths).__name__}."
            )
        inferred_message = (
            ""
            if not inferred_match
            else (
                " (The ambigous sample match for this mzXML file was inferred via mzXML files of the same name "
                "explicitly mapped to multiple samples.) "
            )
        )
        mzxmls_str = "\n\t".join(mzxml_paths)
        message = (
            f"mzXML file(s):\n\t{mzxmls_str}\ncould not be mapped to a single sample. {inferred_message} Each mzXML "
            "must be associated with an MSRunSample, which links to a Sample record, so knowing which sample an mzXML "
            "is associated with is required.  To resolve this, add a row for every mzXML file with this name, "
            "including its path relative from the study directory, to %s.  Potential sample matches include: "
            f"{sample_names}."
        )
        super().__init__(message, **kwargs)
        self.sample_names = sample_names
        self.mzxml_paths = mzxml_paths


class UnmatchedMzXMLs(Exception):
    """Summary of `UnmatchedMzXML` exceptions."""

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
    """An `mzXML` file was found under the study directory that does not match any sample names in the
    `Peak Annotation Details` sheet.

    This can occur if there are `mzXML` files from samples that have not been used to produce a peak annotation file
    yet, i.e. totally unanalyzed samples.  It can also occur if an existing peak annotation file was overlooked when
    generating the Study Doc on the Upload Start page.

    The resolution depends on what you want to do.  If you want to ignore these `mzXML` files that are unanalyzed, and
    put off their load until a subsequent submission or a later addendum to this submission, every unaccounted-for
    `mzXML File Name`, its `Sequence`, and a `Skip` value must be added to the `Peak Annotation Details` sheet (leaving
    the "required" columns blank - an alternative set of required columns).  Or you could create a separate study folder
    and move the extra `mzXML` files there.

    If you want to load these `mzXML` files, the best way to proceed is to generate a new template on the Upload
    **Start** page and copy your work from the old study doc to the new one, being careful to note differences such as
    newly inserted ordered sample rows in the `Samples` sheet.

    There are a few reasons for this strategy.  One of the big ones is that only the **Start** page checks for
    `Peak Group Conflicts`.  The other big one is that multiple linked sheets are consistently populated and all inter-
    sheet references are maintained, which is laborious and error prone when attempted manually.

    Summarized by `UnmatchedMzXMLs`.
    """

    # NOTE: Not user facing (currently).
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
    """Summary of `UnmatchedBlankMzXML` exceptions."""

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
    """This exception is the same as `UnmatchedMzXML`, but is a warning because the files have "blank" in their sample
    names and are assumed to have been intentionally excluded.

    Summarized by `UnmatchedBlankMzXMLs`.
    """

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
    """Unexpected headers encountered in the input file.

    No unexpected headers are allowed.
    """

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
    """Duplicate headers encountered in the input file.

    No duplicate headers are allowed.
    """

    def __init__(self, dupes, all):
        message = f"Duplicate column headers: {list(dupes.keys())}.  All: {all}"
        for k in dupes.keys():
            message += f"\n\t{k} occurs {dupes[k]} times"
        super().__init__(message)
        self.dupes = dupes
        self.all = all


class DuplicateFileHeaders(ValidationError):
    """Duplicate headers encountered in the input file.

    Alternative to `DuplicateHeaders`, taking different inputs, depending on the context.

    No duplicate headers are allowed.
    """

    # NOTE: Not user facing.

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
    # NOTE: Not user facing.
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
    # NOTE: Not user facing.
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
    """Unable to parse date string.  Date string not in the expected format.

    To resolve this exception, reformat the date using the format reported in the error.
    """

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
    """Invalid time duration value.  Must be a number.

    To resolve this exception, edit the value to only be a number (no units symbol)."""

    def __init__(self, string, units, exc, **kwargs):
        message = f"The duration '{string}' found in %s must be a number of {units}.\n"
        super().__init__(message, **kwargs)
        self.string = string
        self.exc = exc
        self.units = units


# TODO: Move the message construction into a constructor of this class.
class InvalidMSRunName(InfileError):
    """Unable to parse Sequence Name.  Must be 4 comma-delimited values of Operator, LC Protocol, Instrument, and
    Date."""

    pass


class ExcelSheetNotFound(InfileError):
    """Expected Excel file sheet not found.  Ensure the correct file was supplied."""

    def __init__(self, sheet, file, all_sheets=None):
        avail_msg = "" if all_sheets is None else f"  Available sheets: {all_sheets}."
        message = f"Excel sheet [{sheet}] not found in %s.{avail_msg}"
        super().__init__(message, file=file)
        self.sheet = sheet


class ExcelSheetsNotFound(InfileError):
    """1 or more expected Excel file sheet names parsed from another file were not found in that file.

    Ensure the correct file and sheet was specified in the referencing file.

    The exception is raised when the Study Doc's (currently unused) `Defaults` sheet incorrectly references another
    sheet.
    """

    # NOTE: Not user facing (currently in 3.1.5-beta or build 241e47d).

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
    """Invalid Excel sheet header reference.

    Ensure the correct file and sheet was specified in the referencing file.

    The exception is raised when the Study Doc's (currently unused) `Defaults` sheet incorrectly references a column in
    another sheet.
    """

    # NOTE: Not user facing (currently in 3.1.5-beta or build 241e47d).

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
    """A mix of positive and negative polarities were found in an mzXML file.

    TraceBase does not support mixed polarity mzXML files.
    """

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
    """An unexpected internal database error has been encountered when trying to load specific input from an input file.

    Exceptions like these are often hard to interpret, but the error was caught so that metadata about the related input
    could be provided, such as the file, sheet, row, and column values that were being loaded when the exception
    occurred.  However, the cause could be hard to determine if it is related to previously loaded data that did not
    report an error.

    If the cause of the error is not easily discernible, feel free to leave it for a curator to figure out.

    These exceptions, when they occur on a somewhat regular basis, are figured out and the work in figuring out the
    cause and likely solution is saved in a custom exception class to make them easier to fix when they crop up again.
    """

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
    """The structure of the mzXML file is not as expected.  An expected XML element or element attribute was not found.

    This could be due to an mzXML version change or a malformed or truncated file.

    TraceBase supports mzXML version 3.2.
    """

    pass


class AllMultiplePeakGroupRepresentations(Exception):
    """Summary of `MultiplePeakGroupRepresentations` exceptions across multiple input files.

    Instance Attributes:
        exceptions (List[MultiplePeakGroupRepresentation])
        succinct (bool) [False]
        suggestion (Optional[str])
        orig_message (str): The original exception message, used to apply a suggestion after having been caught, based
            on context.
        message (str)
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
    """Summary of `MultiplePeakGroupRepresentation` exceptions from a single input file.

    Summarized across all files in `AllMultiplePeakGroupRepresentations`.

    Instance Attributes:
        exceptions (List[MultiplePeakGroupRepresentation])
        suggestion (Optional[str])
        orig_message (str): The original exception message, used to apply a suggestion after having been caught, based
            on context.
        message (str)
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
    """A peak group for a measured compound was picked multiple times and abundance corrected for 1 or more samples.

    TraceBase requires that the single best representation of a peak group compound be loaded for any one sample.

    Certain compounds can show up in both positive and negative scans, or in abutting scan ranges of the same polarity.
    While neither representation may be perfect, this simple requirement prevents inaccuracies or mistakes when using
    the data from TraceBase.

    Be wary however that your compound names in your peak annotation files are consistently named, because different
    synonyms are not detected as multiple peak group representations.  This was a design decision to support succinct
    compound records while also supporting stereo-isomers.  (Note: This distinction may go away in a future version of
    TraceBase, where synonyms are treated as the same compound in the context of peak groups.)

    Multiple peak group representations are only detected and reported on the Upload **Start** page, when the peak
    annotation files have their compounds and samples extracted.  The multiple representations are recorded in an
    otherwise hidden sheet in the Study Doc named `Peak Group Conflicts`.

    For every row in the conflicts sheet, a peak annotation file drop-down is supplied to pick the file from which the
    peak group should be loaded.  The same peak group compound from the other file(s) will be skipped.

    If you forgot a peak annotation file when generating your study doc template, start over with a complete generated
    Study Doc in order to catch these issues.  But note, previously loaded Peak Group records are included in the
    detection of multiple representations, so alternatively, you may choose to add forgotten peak annotation files in a
    separate submission, keeping in mind that if you select the new peak annotation file as the peak group
    representation to load, the old previously loaded peak group will be deleted.

    Note that while manual editing of this sheet is discouraged, you can manually edit it as long as you preserve the
    hidden column.  There is a hidden sample column containing delimited sample names.  This column is required to
    accurately update all multiple representations.

    Summarized per file in `MultiplePeakGroupRepresentations` and across all files in
    `AllMultiplePeakGroupRepresentations`.
    """

    SummarizerExceptionClass = MultiplePeakGroupRepresentations

    def __init__(
        self, new_rec: PeakGroup, existing_recs: QuerySet, message=None, suggestion=None
    ):
        """MultiplePeakGroupRepresentations constructor.

        Args:
            new_rec (PeakGroup): An uncommitted record.
            existing_recs (PeakGroup.QuerySet)
            message (Optional[str])
            suggestion (Optional[str])
        """
        if message is None:
            # Build a dict to show the entire new record that is a multiple representation
            new_dict = {
                "name": new_rec.name,
                "formula": new_rec.formula,
                "msrun_sample": str(new_rec.msrun_sample),
                "peak_annotation_file": new_rec.peak_annotation_file.filename,
            }
            new_str = "\n\t\t".join([f"{k}: {v}" for k, v in new_dict.items()])

            # Build dicts out of the existing conflicting record(s)
            existing_dicts = [
                {
                    "name": e_rec.name,
                    "formula": e_rec.formula,
                    "msrun_sample": str(e_rec.msrun_sample),
                    "peak_annotation_file": e_rec.peak_annotation_file.filename,
                }
                for e_rec in existing_recs.all()
            ]
            if existing_recs.count() > 1:
                existing_str = "\n\t\t".join(
                    [
                        f"{i + 1}\n\t\t\t"
                        + "\n\t\t\t".join([f"{k}: {v}" for k, v in e_dict.items()])
                        for i, e_dict in enumerate(existing_dicts)
                    ]
                )
            else:
                # Assumes 1 record exists
                existing_str = "\n\t\t".join(
                    [f"{k}: {v}" for k, v in existing_dicts[0].items()]
                )

            filenames = [new_rec.peak_annotation_file.filename]
            filenames.extend(
                [r.peak_annotation_file.filename for r in existing_recs.all()]
            )
            files_str = "\n\t".join(filenames)

            message = (
                "Multiple representations of this peak group compound were encountered:\n"
                f"\tNew:\n\t\t{new_str}\n"
                f"\tExisting:\n\t\t{existing_str}\n"
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
        self.filenames: List[str] = [new_rec.peak_annotation_file.filename]
        self.filenames.extend(
            [r.peak_annotation_file.filename for r in existing_recs.all()]
        )
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


class DuplicatePeakGroups(Exception):
    """Summarizes multiple DuplicatePeakGroup exceptions."""

    def __init__(self, exceptions: List[DuplicatePeakGroup]):
        summary_dict: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        exc: DuplicatePeakGroup
        for exc in exceptions:
            summary_dict[exc.new_rec.peak_annotation_file.filename][
                exc.new_rec.name
            ] += exc.existing_recs.count()
        summary_list = [
            (
                "An attempt to create the following PeakGroup records from the indicated peak annotation files would "
                "result in duplicate PeakGroups:"
            )
        ]
        for annot_file in sorted(summary_dict.keys()):
            summary_list.append(f"\t{annot_file}")
            for pg in sorted(summary_dict[annot_file].keys()):
                summary_list.append(f"\t\t{pg}")

        summary_list.append(
            (
                "Researchers may ignore this exception.  This is a technical exception due to the fact that the "
                "duplicate PeakGroup records are linked to different MSRunSample records.  It is a side-effect of a "
                "change in business rules governing placeholder MSRunSample records between an initial load and a "
                "supplemental load.  This is not a serious issue.  A curator may choose to ignore it, but could "
                "reassign PeakGroup links to MSRunSample records based on the current business rules so that this "
                "exception no longer appears."
            )
        )

        message = "\n".join(summary_list)
        super().__init__(message)
        self.exceptions = exceptions


class DuplicatePeakGroup(InfileError, SummarizableError):
    """Duplicate PeakGroup record encountered.

    This is an internal technical issue.  It happens when the business rules that govern which MSRunSample record a
    PeakGroup record links to have changed between an initial and supplemental load of a Study Doc.  E.g. A researcher
    has added data (e.g. samples) to an existing Study doc and the entire load is re-run to fill in the missing data.

    When this happens, a get_or_create is used to retrieve previously created records if they exist, but if the linked-
    to MSRunSample record has changed from a caoncrete record to a placeholder record, the existing "duplicate"
    PeakGroup record is not "gotten" because it's not a perfect match.  So a new "duplicate" record is created.
    """

    SummarizerExceptionClass = DuplicatePeakGroups

    def __init__(
        self,
        new_rec: PeakGroup,
        existing_recs: QuerySet,
        message: Optional[str] = None,
        **kwargs,
    ):
        if message is None:
            message = (
                "Duplicate PeakGroup record created in %s:\n"
                f"\tCompound: {new_rec}\n"
                f"\tPeak Annotation File: {new_rec.peak_annotation_file.filename}\n"
                "Each is linked to these MSRunSamples:\n"
                f"\tNew: {new_rec.msrun_sample}\n"
                f"\tExisting: {[exstg.msrun_sample for exstg in existing_recs.all()]}\n"
                "Researchers may ignore this exception.  This is a technical exception due to the fact that the "
                "duplicate PeakGroup records are linked to different MSRunSample records.  It is a side-effect of a "
                "change in business rules governing placeholder MSRunSample records between an initial load and a "
                "supplemental load.  This is not a serious issue.  A curator may choose to ignore it, but could "
                "reassign PeakGroup links to MSRunSample records based on the current business rules so that this "
                "exception no longer appears."
            )
        super().__init__(message, **kwargs)
        self.new_rec: PeakGroup = new_rec
        self.existing_recs: QuerySet = existing_recs


class TechnicalPeakGroupDuplicates(Exception):
    """Summarizes multiple TechnicalPeakGroupDuplicate exceptions."""

    def __init__(self, exceptions: List[TechnicalPeakGroupDuplicate]):
        summary_dict: Dict[str, int] = defaultdict(int)
        exc: TechnicalPeakGroupDuplicate
        for exc in exceptions:
            summary_dict[exc.new_rec.peak_annotation_file.filename] += 1
        summary_list = [
            (
                "An attempt to create the following PeakGroup records from the indicated peak annotation files would "
                "result in duplicate PeakGroups because the peak annotation file appears to have been edited:"
            )
        ]
        for annot_file in sorted(summary_dict.keys()):
            summary_list.append(
                f"\t{annot_file} ({summary_dict[annot_file]} peak groups)"
            )

        summary_list.append(
            (
                "A curator must delete the previously loaded outdated peak annotation file along with all its peak "
                "groups, and must then rerun this load.  This will eliminate the stale peak annotation file so that "
                "all download links yield the same file."
            )
        )

        message = "\n".join(summary_list)
        super().__init__(message)
        self.exceptions = exceptions


class TechnicalPeakGroupDuplicate(InfileError, SummarizableError):
    """Duplicate PeakGroup record encountered due to an edited peak annotation file.

    This is an internal technical error.  It happens when the linked peak annotation file is the same file (as
    determined by name), but the file was edited.  In this case, that edit did not qualitatively change the peak group.
    It just didn't match the file.
    """

    SummarizerExceptionClass = TechnicalPeakGroupDuplicates

    def __init__(
        self,
        new_rec: PeakGroup,
        existing_recs,
        message: Optional[str] = None,
        **kwargs,
    ):
        if message is None:
            existing_files_str = ", ".join(
                [
                    f"{exstg.peak_annotation_file.filename} ({exstg.peak_annotation_file.checksum})"
                    for exstg in existing_recs.all()
                ]
            )
            message = (
                "Duplicate PeakGroup record created in %s due to an apparent edit of the peak annotation file:\n"
                f"\tCompound: {new_rec}\n"
                f"\tSample: {new_rec.msrun_sample.sample.name}\n"
                "Edited Peak Annotation Files:\n"
                f"\tNew: {new_rec.peak_annotation_file.filename} ({new_rec.peak_annotation_file.checksum})\n"
                f"\tExisting: {existing_files_str}\n"
                "A curator must delete the previously loaded outdated peak annotation file along with all its peak "
                "groups, and must then rerun this load.  This will eliminate the stale peak annotation file so that "
                "all download links yield the same file."
            )
        super().__init__(message, **kwargs)
        self.new_rec: PeakGroup = new_rec
        self.existing_recs = existing_recs


class ComplexPeakGroupDuplicates(ConflictingValueErrors):
    """Summarizes multiple ComplexPeakGroupDuplicate exceptions."""

    def __init__(self, exceptions: list, suggestion: Optional[str] = None):
        if suggestion is None:
            suggestion = ComplexPeakGroupDuplicate.suggestion
        super().__init__(exceptions, suggestion=suggestion)


class ComplexPeakGroupDuplicate(ConflictingValueError):
    """Complex Duplicate PeakGroup record encountered due to an edited peak annotation file.

    This happens when the PeakGroup was edited in the file, possibly also differing due to technical issues, like
    changed business rules regarding MSRunSample placeholder handling and/or a technically differing peak annotation
    file record.  In this case, that edit changed the PeakGroup.  A migration will be required to update the existing
    record to match.  Alternatively, the affected PeakGroup records can be deleted and reloaded.  Ideally, every Peak
    Annotation File and all its PeakGroup records should be deleted, so that files linked from every PeakGroup is
    consistent.
    """

    SummarizerExceptionClass = ComplexPeakGroupDuplicates
    suggestion = (
        "There are 3 likely cases causing this error:\n\n"
        "\t1. There are differences in this peak group (e.g. different formula) due to having edited in the "
        "peak annotation file between the initial load and a supplemental load.  All the user has to do here "
        "is confirm that the changes are correct.  See curator note below^.\n"
        "\t2. There are no apparent differences in this peak group, but the peak annotation file was edited "
        "between the initial load and a supplemental load.  In this case, the peak annotation file will be "
        "shown as different, but the files appear the same.  The user may ignore this error.  See curator note "
        "below^.\n"
        "\t3. The business rules that link a peak group to an MSRunSample record have changed between the "
        "initial load and a supplemental load.  This is a technical issue that the curator alone is "
        "responsible for.  The peak annotation file will not be presented as different.  The user may ignore "
        "this error.  A curator can likely ignore this error, but could reassign PeakGroup links to "
        "MSRunSample records based on the current business rules so that this error no longer appears.\n\n"
        "^ In cases 1 & 2, a curator should confirm differences shown are deemed correct by the user, and must "
        "delete the previously loaded outdated file along with all its peak groups, and must then rerun this "
        "load.  This will eliminate the stale peak annotation file so that all download links yield the same "
        "file."
    )

    def __init__(self, *args, suggestion: Optional[str] = None, **kwargs):
        if suggestion is None:
            suggestion = self.suggestion
        super().__init__(*args, suggestion=suggestion, **kwargs)


class PossibleDuplicateSamples(SummarizedInfileError, Exception):
    """Summary of `PossibleDuplicateSample` exceptions from a single input file."""

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
    """Multiple peak annotation files have an identical sample header, but are associated with distinctly different
    TraceBase biological Sample records.

    This exception is always raised as a warning as a check to ensure that the distinction is intentional, and not just
    a copy/paste error.

    If there do exist different biological samples that happen to have the exact same name, this warning can be safely
    ignored.  If they are the same biological sample, the `Sample Name` column in the `Peak Annotation Details` sheet
    must be updated.  You may also need to delete or update the associated row in the `Samples` sheet, if no other
    verified rows in the `Peak Annotation Details` sheet refers to it.

    Summarized by `PossibleDuplicateSamples`.
    """

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
    """A previously loaded peak group from a previous submission (for a measured compound was picked multiple times and
    abundance corrected for 1 or more samples) will be replaced with a new representation from a new peak annotation
    file that includes this compound for the same 1 or more samples.

    Refer to the documentation of the `MultiplePeakGroupRepresentation` exception for an explanation of multiple peak
    group representations and TraceBase's requirements related to them.

    This exception is always raised as a warning, to be transparent about the replacement of previously loaded Peak
    Group records.  By selecting the new peak annotation file as the peak group representation to load in the
    `Peak Group Conflicts` sheet, this warning informs you that an old previously loaded peak group will be deleted.

    This exception is expected when a selection has been made that supercedes a selection made in a previous load
    relating to the same samples and compound.
    """

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
    """A row in the `Peak Group Conflicts` sheet is duplicated, and may contain conflicting resolutions.

    A row in the `Peak Group Conflicts` sheet is a duplicate if it contains the same (case insensitive) compound synonym
    (or `/`-delimited synonyms in any order) and the same^ samples.

    Refer to the documentation of the `MultiplePeakGroupRepresentation` exception for an explanation of multiple peak
    group representations and the `Peak Group Conflicts` sheet's involvement in resolving them.

    This exception is a warning when the resolution is the same on each row, but an error if the resolution (i.e. the
    selected representation - the peak annotation file) is different on each row.

    ^ _The **same** samples means **all** samples.  There is assumed to be no partial overlap between sample sets for_
    _the same compounds because the automated construction of this file separates them programmatically, so be_
    _careful editing the `Peak Group Conflicts` sheet, to make sure you do not introduce partial sample overlap_
    _between rows._
    """

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
    """A compound row was added to the Compounds sheet whose name exists as a synonym of another compound.

    This exception can arise automatically in the downloaded study doc template all on its own.  TraceBase tries to add
    rows in the Compounds sheet for both compounds that already exist in TraceBase and novel compounds that do not yet
    exist in TraceBase.  However, the compound name and formula must both match.  When they do not match, a new row for
    a novel compound is added to the sheet, whether or not it creates a conflict.  Such a conflict can arise due to the
    formula (derived from the peak annotation file) representing the ionized state of the compound, e.g. with 1 less or
    1 more proton (H).

    A researcher can also cause this exception if they were to assign an HMDB ID that is already assigned to another
    compound existing in TraceBase.  This can often happen after fixing the issue described above caused by an ionized
    formula, because TraceBase did not pre-fill the existing compound due to the formula difference.

    Lastly, this issue can arise if the conflicting compound record simply has a synonym associated with a compound
    record that is just wrong.

    To resolve this issue, either merge the compound records (editing them to fix the formula) or remove the synonym
    from the differing compound record so that peak groups (and tracers) are associated with the other compound record.

    If the compound from the peak annotation file(s) differs from the existing TraceBase compound record (e.g. different
    formula or HMDB ID), and the new record represents a distinctly different compound, reach out to the curators.  The
    existing compound synonym may already be associated with a different compound in other studies, so either changes
    would need to be made to those other studies or the new study would need to be edited to distinguish the different
    compounds.  Either way, a curator will need to coordinate the fix to ensure database-wide consistency.
    """

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
    """A compound row was added to the Compounds sheet whose synonym exists as a primary name of another compound.

    This exception can arise automatically in the downloaded study doc template all on its own.  TraceBase tries to add
    rows in the Compounds sheet for both compounds that already exist in TraceBase and novel compounds that do not yet
    exist in TraceBase.  However, the compound name and formula must both match.  When they do not match, a new row for
    a novel compound is added to the sheet, whether or not it creates a conflict.  Such a conflict can arise due to the
    formula (derived from the peak annotation file) representing the ionized state of the compound, e.g. with 1 less or
    1 more proton (H).

    A researcher can also cause this exception if they were to assign an HMDB ID that is already assigned to another
    compound existing in TraceBase.  This can often happen after fixing the issue described above caused by an ionized
    formula, because TraceBase did not pre-fill the existing compound due to the formula difference.

    Lastly, this issue can arise if the conflicting compound record simply has a synonym associated with a compound
    record that is just wrong.

    To resolve this issue, either merge the compound records (editing them to fix the formula) or remove the synonym
    from the differing compound record so that peak groups (and tracers) are associated with the other compound record.

    If the compound from the peak annotation file(s) differs from the existing TraceBase compound record (e.g. different
    formula or HMDB ID), and the new record represents a distinctly different compound, reach out to the curators.  The
    existing compound name may already be associated with a different compound in other studies, so either changes would
    need to be made to those other studies or the new study would need to be edited to distinguish the different
    compounds.  Either way, a curator will need to coordinate the fix to ensure database-wide consistency.
    """

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
    """An exception class for methods that retrieve command line options, called too early."""

    # NOTE: Not user facing.

    def __init__(self):
        super().__init__(
            "Cannot get command line option values until handle() has been called."
        )


class MutuallyExclusiveOptions(CommandError):
    # NOTE: Not user facing.
    pass


class MutuallyExclusiveArgs(InfileError):
    # NOTE: Not user facing.
    pass


class MutuallyExclusiveMethodArgs(Exception):
    # NOTE: Not user facing.
    pass


class RequiredOptions(CommandError):
    # NOTE: Not user facing.
    def __init__(self, missing, **kwargs):
        message = f"Missing required options: {missing}."
        super().__init__(message, **kwargs)
        self.missing = missing


class ConditionallyRequiredOptions(CommandError):
    # NOTE: Not user facing.
    pass


class ConditionallyRequiredArgs(InfileError):
    # NOTE: Not user facing.
    pass


class NoLoadData(Exception):
    # NOTE: Not user facing.
    pass


class StudyDocConversionException(InfileError):
    # NOTE: Rarely user facing.  A user would have to submit an old version of the study doc to see these warnings.
    def __init__(self, from_version: str, to_version: str, message: str, **kwargs):
        message = (
            f"The conversion of the input study doc from version {from_version} to version {to_version} "
            "resulted in the following notable issue(s)...\n\n"
        ) + message
        super().__init__(message, **kwargs)
        self.from_version = from_version
        self.to_version = to_version


class PlaceholdersAdded(StudyDocConversionException):
    # NOTE: Rarely user facing.  A user would have to submit an old version of the study doc to see these warnings.
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
    # NOTE: Rarely user facing.  A user would have to submit an old version of the study doc to see these warnings.
    def __init__(self, from_version, to_version, message=None, **kwargs):
        if message is None:
            message = (
                f"The conversion of the input study doc from from version {from_version} to version {to_version} "
                "resulted in placeholder values being added due to unconsolidated/missing data in the older version.\n"
                "Please update the placeholder value(s) added to %s."
            )
        super().__init__(from_version, to_version, message, **kwargs)


class BlanksRemoved(StudyDocConversionException):
    # NOTE: Rarely user facing.  A user would have to submit an old version of the study doc to see these warnings.
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
    # NOTE: Rarely user facing.  A user would have to submit an old version of the study doc to see these warnings.
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
    # NOTE: Rarely user facing.  A user would have to submit an old version of the study doc to see these warnings.
    def __init__(self, message=None, suggestion=None, **kwargs):
        if message is None:
            message = "Placeholder values detected on %s.  Skipping load of the corresponding record(s)."
        if suggestion is not None:
            message += f"  {suggestion}"
        super().__init__(message, **kwargs)


class NotATableLoader(TypeError):
    # NOTE: Not user facing.
    def __init__(self, command_inst):
        here = f"{type(command_inst).__module__}.{type(command_inst).__name__}"
        message = f"Invalid attribute [{here}.loader_class] TableLoader required, {type(command_inst).__name__} set"
        super().__init__(message)
        self.command_inst = command_inst


class CompoundDoesNotExist(InfileError, ObjectDoesNotExist):
    """The compound from the input file does not exist as either a primary compound name or synonym.

    There are 2 possible resolutions to this exception.  Both involve updates to the `Compounds` sheet.

    - Add the name as a synonym to an existing matching compound record.
    - Add a new row to the `Compounds` sheet.

    In either case, if no matching compound exists in the `Compounds` sheet of the Study Doc, be sure to check
    TraceBase's Compounds page for a matching compound record (missing the current name as a synonym).  The Upload
    **Start** page which generates the Study Doc populates the sheet with existing compounds from TraceBase whose
    formulas exactly match the formula obtained from the peak annotation file(s).  But the formula derived from a peak
    annotation file may represent an ionized version of the compound record in TraceBase and thus, may not have been
    auto-added^, which is why the TraceBase site should be consulted.

    ^ _Note that pre-populating the `Compounds` sheet with ionization variants is a proposed feature._
    _See GitHub issue [#1195](https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/1195)._
    """

    def __init__(self, name, **kwargs):
        message = f"Compound [{name}] from %s does not exist as either a primary compound name or synonym."
        super().__init__(message, **kwargs)
        self.name = name


class MultipleRecordsReturned(InfileError, MultipleObjectsReturned):
    """The record search was expected to match exactly 1 record, but multiple records were found.

    This issue can arise for various reasons, but usually are related to conflicting sample naming conventions in the MS
    instrument that produces the RAW (and by implication, the `mzXML`) filenames, the abundance correction software that
    produces the peak annotation files (e.g. `AccuCor`), and TraceBase's unique biological sample naming constraints
    related to scan labels.

    TraceBase's attempts to map differing sample names in differing contexts to a single biological Sample record can
    yield this exception in one case when there exists a biological sample duplicate in the Study Doc's `Samples` sheet.
    This can happen due to the retention of scan labels in sample names when populating that sheet.  So one possible
    resolution may be to merge duplicate sample records in the `Samples` sheet that happen to be different scans of the
    same biological sample.

    Another possibility could be misidentified "scan labels" that for example do not refer to polarity that were
    manually fixed, but which cause issues when trying to map mzXML file names or peak annotation file sample headers to
    those Sample records.

    Each issue should be handled on a case-by-case basis."""

    def __init__(self, model, query_dict, message=None, **kwargs):
        if message is None:
            message = f"{model.__name__} record matching {query_dict} from %s returned multiple records."
        super().__init__(message, **kwargs)
        self.query_dict = query_dict
        self.model = model


class MissingDataAdded(InfileError):
    """Use this for warnings only, when missing data exceptions are caught, handled to autofill missing data in a
    related sheet, and repackaged as a warning to transparently let the user know when repairs have occurred.

    Examples:
        Novel animal treatment:
            A novel animal treatment is entered into the `Treatment` column of the `Animals` sheet in the Study Doc, but
            not into the `Treatments` sheet.  The TraceBase Upload **Validate** page will autofill the new treatment
            name in a new row added to the `Treatments` sheet of the Study Doc.

        Novel tissue:
            A novel tissue is entered into the `Tissue` column of the `Samples` sheet in the Study Doc, but not into the
            `Tissues` sheet.  The TraceBase Upload **Validate** page will autofill the new tissue name in a new row
            added to the `Tissues` sheet of the Study Doc.

    This warning is an indicator that there is new data to potentially fill in in the mentioned sheet.
    """

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

    # NOTE: Not user facing.
    pass


class TracerGroupsInconsistent(ValidationError):
    """An infusate is either a duplicate or exists with a conflicting tracer group name.

    A duplicate infusate can trigger this exception due to concentration value precision.  In other words, it's not
    technically a true duplicate, but is treated as such due to the fact that concentration values may exceed a
    precision threshold.  Excel and the underlying Postgres database have slightly different levels of precision.
    TraceBase saves what you enter, but when it is entered into the database, the precision may change and end up
    matching another record.  It's also important to note that while TraceBase saves the value you enter, it searches
    for infusates using significant figures, which can also lead to a duplicate exception.  See the tracer column
    headers in the `Infusates` sheet in the Study Doc for details of what significant figures are used.

    The resolution in the duplicate case is to use existing records whose concentration values insignificantly differ.

    The other reason this exception may be raised could be due to nomenclature control over the `Tracer Group Name`,
    which must be the same across all infusates that that include the same tracer compounds, regardless of concentration
    and isotopic inclusion.

    If the tracer group name differs, you must use the pre-existing group name already in TraceBase.  If the group name
    is problematic, reach out to a TraceBase curator to fix it.
    """

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
    """The compound name used in the tracer name is not the primary compound name.

    TraceBase requires that tracer names use the primary compound name so that searches yield complete and consistent
    results.  It automatically changes the tracer name to use the primary compound and raises this exception as a
    warning, to be transparent about the modification of the user-entered compound name.

    If the established primary compound name is problematic, reach out to a TraceBase curator to propose a change of a
    compound's primary name.  Note that such a change will affect all studies that use this tracer (if any).
    """

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
    """The peak annotation file format code is either unrecognized, or doesn't appear to match the auto-detected format
    of the supplied file.

    This exception is raised as an error on the Upload **Start** page only.

    To resolve this issue, select the format code using the dropdown menus in the `File Format` column of the
    `Peak Annotation Files` sheet in the Study Doc that corresponds to the reported file.

    Note that this error is more likely to occur when supplying CSV or TSV versions of peak annotation files.  Automatic
    format determination is based on the Excel sheet and/or column names, and there is a lot of overlap in the column
    names of the different formats.
    """

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
    """The peak annotation file format is unrecognized.

    This exception is raised as an error on the Upload **Start** page only.

    To resolve this issue, select the format code using the dropdown menus in the `File Format` column of the
    `Peak Annotation Files` sheet in the Study Doc that corresponds to the reported file.  If none of the supported
    formats in the dropdown match the file format, reach out to the TraceBase team to request adding support for the new
    format.  In the meantime, it is recommended that you use one of the supported natural abundance correction tools to
    regenerate the file in a TraceBase-compatible format.
    """

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
    """The peak annotation file format could not be uniquely determined.

    This exception is raised as an error on the Upload **Start** page only.

    To resolve this issue, select the format code using the dropdown menus in the `File Format` column of the
    `Peak Annotation Files` sheet in the Study Doc that corresponds to the reported file.

    Note that this error is more likely to occur when supplying CSV or TSV versions of peak annotation files.  Automatic
    format determination is based on the Excel sheet and/or column names, and there is a lot of overlap in the column
    names of the different formats.
    """

    # TODO: Implement this class similar to the corresponding StudyDoc exception classes below, once match_data is
    # generated by the PeakAnnotationsLoader.determine_matching_formats method
    def __init__(self, matches, **kwargs):
        message = f"Multiple matching formats for peak annotation file '%s': {matches}."
        super().__init__(message, **kwargs)
        self.matches = matches


class DuplicatePeakAnnotationFileName(Exception):
    """Multiple peak annotation files appear to have the same name.

    This exception is raised as an error on the Upload **Start** page only.

    To resolve this issue, either resubmit the files to exclude a truly duplicate file or rename one or both of the
    files to make their names unique.

    TraceBase requires that peak annotation filenames be globally unique to avoid ambiguities when sharing or
    referencing data files.
    """

    def __init__(self, filename):
        message = f"Peak annotation filenames must be unique.  Filename {filename} was encountered multiple times."
        super().__init__(message)
        self.filename = filename


class PeakAnnotationFileConflict(Exception):
    """The submitted peak annotation file was previously loaded, but the previously loaded version differs from the new
    version submitted.

    After an initial load, sometimes users submit new data to an existing study, and it's necessary to rerun the
    submission start page to create a new study doc template (from which data will be copied to update the existing/
    polished study doc) because the submission start interface has error checks to ensure the new data does not
    introduce errors relating to the previously loaded data.

    For example, newly picked peaks from an existing sample can introduce multiple representation errors.  The validate
    page cannot catch these errors, because it knows only sample and animal metadata, not the peak data.

    However, sometimes during the submission of new data, a researcher may find a mistake in old data and attempt to
    make a correction to the previously loaded peak annotation file.

    This is a problem because it will create a new ArchiveFile database record for the distinct version.  Edited/new
    peak groups will link to this new version while unchanged peak groups will still link to the old version.

    That means that which peak annotation file version you get depends on how you encounter it on the site.

    There's nothing a researcher can do to fix this, other than refraining from making unnecessary file edits.  If the
    edits are necessary, a curator will need to perform a database migration to delete and reload all data associated
    with the modified file.  The user is informed of this with this exception as a warning.
    """

    def __init__(
        self, peak_annot_filename: str, differing_annot_files: List[ArchiveFile]
    ):
        timestamps_str = "\n\t".join(
            [str(f.imported_timestamp) for f in differing_annot_files]
        )
        message = "".join(
            f"{len(differing_annot_files)} differing version of a peak annotation file with the same name, "
            f"'{peak_annot_filename}', was previously loaded on:\n\n\t"
            f"{timestamps_str}\n\n"
            "If the edits are intentional/necessary and some contained peak data, compound names, or sample names need "
            "to be changed, a curator must be notified so that they can make updates to the existing data, so that the "
            "old file version can be removed from the database.\n"
            "If the edits were unintentional or superficial (e.g. changing column widths in excel), notify the "
            "curation team that the original file must be used in the load of your supplemental data submission."
        )
        super().__init__(message)


class InvalidStudyDocVersion(Exception):
    """The study doc version that was automatically determined is not yet supported by the submission interface.

    TraceBase is backward compatible with older versions of the Study Doc and the Upload **Validate** page automatically
    detects the version based on sheet and column names.  The submission interface has not yet been updated to load this
    version.  Reach out to the TraceBase team if you ever see this exception.
    """

    # NOTE: Effectively not user facing.  The only way to get this is if the **Start** page generated a new version
    # number but was not updated to validate it.
    pass


class StudyDocVersionException(Exception):
    # NOTE: Rarely user facing.  A user would have to submit an old version of the study doc to see these warnings.
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
            None
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
    """The study doc version could not be automatically determined.

    This exception is accompanied by version determination metadata intended to highlight the supplied versus expected
    sheet and column names.  Note however, that the only column names that will be reported as missing are required
    columns.  Missing optional columns may be reported as "unknown".

    TraceBase is backward compatible with older versions of the Study Doc and the Upload **Validate** page automatically
    detects the version based on sheet and column names.

    This exception could arise if the sheet names and/or column names were modified.  Try generating a new study doc
    from the Upload **Start** page and compare the sheet and column names to ensure they were not inadvertently altered.
    If there are differences, fix them so that the version can be identified by the Upload **Validate** interface.
    """

    def __init__(self, supported_versions: list, match_data, message=None):
        """UnknownStudyDocVersion constructor: use when there is no version match.

        Args:
            supported_versions (List[str]): List of supported study doc version numbers
            match_data (dict): See StudyDocVersionException.__init__()
            message (Optional[str])
        Exceptions:
            None
        Returns:
            None
        """
        if message is None:
            message = (
                "Unable to determine study doc version.  Please supply one of the supported formats: "
                f"{supported_versions}."
            )
        super().__init__(message, match_data)
        self.supported_versions = supported_versions


class MultipleStudyDocVersions(StudyDocVersionException):
    """The study doc version could not be automatically narrowed down to a single matching version.

    This exception is accompanied by version determination metadata intended to highlight the supplied versus expected
    sheet and column names.  Note however, that the only column names that will be reported as missing are required
    columns.  Missing optional columns may be reported as "unknown".

    TraceBase is backward compatible with older versions of the Study Doc and the Upload **Validate** page automatically
    detects the version based on sheet and column names.

    This exception could arise if various sheets have been removed, leaving sheets whose required column names do not
    differ between versions.  There is currently no fix for this issue on the Upload **Validate** page and validation
    must happen on the command line where a version number can be supplied.  In this case, it is recommended that you
    skip validation and if you think the data is complete, move on to the **Submit** step.
    """

    def __init__(self, matching_version_numbers: list, match_data, message=None):
        """MultipleStudyDocVersions constructor: use when there are multiple matches.

        Args:
            supported_versions (List[str]): List of supported study doc version numbers
            match_data (dict): See StudyDocVersionException.__init__()
            message (Optional[str])
        Exceptions:
            None
        Returns:
            None
        """
        if message is None:
            message = (
                "Unable to identify study doc version.  Please supply one of these multiple matching version numbers: "
                f"{matching_version_numbers}."
            )
        super().__init__(message, match_data, matching_version_numbers)
        self.matching_version_numbers = matching_version_numbers


class MultipleConflictingValueMatchesSummary(Exception):
    """Summary of MultipleConflictingValueMatches exceptions."""

    def __init__(self, exceptions: List[MultipleConflictingValueMatches]):

        # Construct all the conflict data in a multi-dimensional dict
        conflict_data: Dict[str, dict] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        mcvm: MultipleConflictingValueMatches
        for mcvm in exceptions:
            # Create a new location string that excludes the column and includes the affected model
            file_loc = generate_file_location_string(sheet=mcvm.sheet, file=mcvm.file)
            mdl = mcvm.model.__name__
            mcvm_loc = f"Model {mdl} in {file_loc}"
            for rec, differences, _ in mcvm.recs_diffs_cves:
                conflict_data[mcvm_loc][str(mcvm.rec_dict)][str(rec.id)] = {
                    "rec": model_to_dict(rec, exclude=["id"]),
                    "diffs": differences,
                }

        preamble = (
            f"Data from {len(conflict_data.keys())} file sheets has conflicts with existing database records, but we "
            "were unable to determine which differences to report due to multiple unique constraints and query "
            "limitations, so all differences with each matching record are reported below, broken down by file sheet "
            "and model.  It is up to the user to decide which matching record is relevant and fix the differences "
            "derived from the file to match the correct existing database record.  The correct match will likely have "
            "more matching values to the record parsed from the file.\n\n"
            "Note that the file record may be incomplete due to either the order in which records are loaded or due to "
            "optional file values that were left empty.  Any values in the database that are not in the file are the "
            "likely correct match.\n\n"
            "The following are the differences/conflicts:\n\n"
        )
        summary = [preamble]

        # Compile the summary text, sorted
        for mcvm_loc in sorted(conflict_data.keys()):
            summary.append(mcvm_loc)
            for file_rec in sorted(conflict_data[mcvm_loc].keys()):
                summary.append(f"\tDifferences between file record: {file_rec} and:")
                for rec_id in conflict_data[mcvm_loc][file_rec].keys():
                    summary.append(
                        f"\t\tDatabase record {rec_id}: {conflict_data[mcvm_loc][file_rec][rec_id]['rec']}:"
                    )
                    for field, diff_dict in sorted(
                        conflict_data[mcvm_loc][file_rec][rec_id]["diffs"].items()
                    ):
                        summary.append(f"\t\t\t{field}")
                        summary.append(f"\t\t\t\tdatabase: [{diff_dict['orig']}]")
                        summary.append(f"\t\t\t\tfile: [{diff_dict['new']}]")

        # Append a note to developers
        summary.append(
            "Developers should see if either the conditions in the unique constraints of the models involved can be "
            "improved to be mutually exclusive or whether TableLoader.get_inconsistencies can be improved to identify "
            "the precise match."
        )

        message = "\n".join(summary)

        super().__init__(message)


class MultipleConflictingValueMatches(InfileError, SummarizableError):
    """The reports an error between file data and existing records in the database when there are multiple matching
    database records.

    Some file-created records can conflict with existing database records.  This usually happens when a researcher is
    adding supplemental study data to a study doc and has edited the old data that has already been loaded, but in doing
    so, they edited or filled in some of the missing values in the old study data.  TraceBase does not yet support the
    editing of previously loaded study data, even if that is to fill in previously upsupplied optional values.  Doing so
    creates conflicts with the previously loaded data.  If you have edited or added data to previously loaded records
    from your study doc, a curator will need to create a custom migration.

    The other possibility is that your data is a brand new study, but your conflicting records happen to collide with
    unrelated records from another study, in which case you would have to make your data unique.  Frequently, this will
    be unique fields like animal or sample name, for example.

    In this case however, the matching database record could not be narrowed down to a single offending database record.
    This can sometimes happen when a model has multiple unique constraints that apply individually to subsets of
    database records.  If a user has for example, deleted an optional value from an existing row of the file, there is
    no way to query in the deleted value to match it.  And that combined with multiple unique constraints can result in
    multiple matches.

    When this happens, the differences between the file-derived record and each of the matching database records is
    displayed.  The user must determine which database record is the matching one and either report this to a curator so
    that they can edit the existing database record(s) of you can reverse the file edit that caused the conflict.

    Tip: The correct matching and conflicting record will be the one that has more matching values.

    NOTE: This exception is analogous to the ConflictingValueError, but is specifically for the case when a single
    offending database record cannot be identified.

    DEV_SECTION - Everything above this delimiter is user-facing.  See TraceBaseDocs/README.md

    If this exception ever occurs, or occurs repeatedly, there are a couple options to avoid it and make a regular
    ConflictingValueError occur instead.  The inability to identify the exact offending record and report the precise
    conflicting values arises due to a couple of factors.  First, This occurs when there are multiple unique constraints
    and at least one uses a condition.  Second, the rec_dict from the file cannot be matched using the unique constraint
    condition in order to rule out a database match.

    One fix would be to improve TableLoader.get_inconsistencies so that it can rule out a unique constraint if its
    condition is violated by the file-derived rec_dict.  This would require a recursive method that takes a Q object and
    the rec_dict and determines in the rec_dict meets the unique constraint's condition.  If it does not, skip that
    unique constraint.

    The other option would be to modify the unique constraints to make the file record's values not match one of the
    unique constraints.
    """

    SummarizerExceptionClass = MultipleConflictingValueMatchesSummary

    def __init__(
        self,
        recs_diffs: List[Tuple[Model, dict]],
        rec_dict=None,
        message=None,
        derived=False,
        **kwargs,
    ):
        # This assumes all the records in recs_diffs are from the same model
        model = type(recs_diffs[0][0])

        # This builds contained ConflictingValueError exceptions to be able to include their difference descriptions in
        # this exception's verbiage.
        recs_diffs_cves = []
        for rec, differences in recs_diffs:
            cve = ConflictingValueError(
                rec,
                differences,
                rec_dict=rec_dict,
                derived=derived,
            )
            recs_diffs_cves.append((rec, differences, cve))

        if message is None:
            # The preamble describing the problem
            message = (
                f"Data from %s has conflicts with {len(recs_diffs)} existing database records, but we were unable to "
                "determine which differences to report due to multiple unique constraints and query limitations, so "
                "all differences with each matching record are reported below.  It is up to the user to decide which "
                "matching record is relevant and fix the differences derived from the file to match the correct "
                "existing database record.  The correct match will likely have more matching values to the "
                f"{type(recs_diffs[0][0]).__name__} record parsed from the file:\n\n"
                f"\t{rec_dict}\n\n"
                "Note that it may be incomplete due to either the order in which records are loaded or due to optional "
                "file values that were left empty.  Any values in the database that are not in the file are the likely "
                "correct match.\n\n"
                "The following are the differences between each of the matching records:\n\n"
            )

            # This includes the string version of a ConflictingValueError describing differences for each matching rec
            for rec, differences, cve in recs_diffs_cves:
                message += f"Differences with {type(rec).__name__} record {rec.id}:\n{indent(str(cve))}\n"

            # Append a note to developers to see if they can prevent this ambiguity from happening again.
            message += (
                "Developers should see if either the conditions in the unique constraints of model "
                f"{type(recs_diffs[0][0]).__name__} can be improved to be mutually exclusive or whether "
                "TableLoader.get_inconsistencies can be improved to identify the precise match."
            )

        super().__init__(message, **kwargs)

        self.recs_diffs_cves = recs_diffs_cves
        self.rec_dict = rec_dict
        self.derived = derived
        self.model = model


class DeveloperWarning(Warning):
    # NOTE: Not user facing.
    pass


class DBFieldVsFileColDeveloperWarnings(DeveloperWarning):
    """Summarization of multiple DBFieldVsFileColDeveloperWarning exceptions.

    This exception breaks down the type warnings between database fields and file columns by loader class/field, whether
    the string versions differed or not, and by file location (so that the column can be mapped).  See
    DBFieldVsFileColDeveloperWarning's docstring for details on how to address this exception.

    This warning is only issued when the types of database versus file values differ, and is only useful when it is
    followed by ConflictingValueErrors, in which case those exceptions can be resolved by explicitly setting the column
    type in the loader class.

    Args:
        exceptions (List[DBFieldVsFileColDeveloperWarning])
    Attributes:
        Class:
            None
        Instance:
            exceptions (List[DBFieldVsFileColDeveloperWarning])
            differences (Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]]): A dictionary summarizing differences
                between an existing database record and the values derived from the input file.
                See Differences.differences defined in the class attributes.
            has_mismatches (bool): Whether any values are effectively different, i.e. whether the values will "look"
                different to the developer (because everything that comes from the file is potentially a str).  This
                value is key as to how the issue described in the exception message should be resolved.
                See Differences.has_mismatches defined in the class attributes.
    """

    @dataclass
    class Differences:
        """A dataclass to hold information on differences between database records and rows in an input file.

        Args:
            differences (Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]]): See Attributes.
            has_mismatches (bool): See Attributes.

        Attributes:
            differences (Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]]): A dictionary summarizing differences
                between an existing database record and the values derived from the input file.
                See Differences.differences defined in the class attributes.
            has_mismatches (bool): Whether any values are effectively different, i.e. whether the values will "look"
                different to the developer (because everything that comes from the file is potentially a str).  This
                value is key as to how the issue described in the exception message should be resolved.
                See Differences.has_mismatches defined in the class attributes.
        """

        differences: Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]]
        has_mismatches: bool

    def __init__(self, exceptions: List[DBFieldVsFileColDeveloperWarning]):
        self.exceptions = exceptions

        result = self._build_differences()
        self.differences = result.differences
        self.has_mismatches = result.has_mismatches

        summary = self._summarize_differences()
        message = self._build_message(summary)

        super().__init__(message)

    def _build_differences(self) -> Differences:
        """Generates an organized dictionary of differences between existing database records & input file rows.

        The structure of the dict reflects how the exception's message will display the difference data.

        Requires self.exceptions to have been set.

        Args:
            None
        Exceptions:
            None
        Returns:
            (Differences)
        """
        differences_dict: Dict[str, Dict[str, Dict[str, List[Dict[str, str]]]]] = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        )
        has_mismatches = False
        exc: DBFieldVsFileColDeveloperWarning
        for exc in self.exceptions:
            loader_field = f"Loader: {exc.loader_name}, Field: {type(exc.rec).__name__}.{exc.field}"
            if exc.different:
                different_equal = "Differing string values"
                has_mismatches = True
            else:
                different_equal = "Equal string values"
            differences_dict[loader_field][different_equal][exc.loc].append(
                {
                    "database": exc.db_value,
                    "file": exc.file_value,
                }
            )

        return self.Differences(differences_dict, has_mismatches)

    def _summarize_differences(self) -> str:
        """Turns the self.differences dictionary into an indented list of differences.

        The differences are organized by the (loader name and) database field name, whether the string values differ,
        and the location in the file where the corresponding data with a unique constraint violation was encountered.

        Requires self.differences and self.has_mismatches to have been set.

        Example:
            # Example return value (string of an indented outline detailing differences)
            Loader: AnimalLoader, Field: Animal.age
                Differing string values:
                    column [Age] on row [5] of sheet [Animals] in [study.xlsx]
                        Database Value: '2' (type: int)
                        File Value: '5' (type: str)
        Args:
            None
        Exceptions:
            None
        Returns:
            (str)
        """
        summary = []
        for loader_field in self.differences.keys():
            summary.append(f"\n{loader_field}")

            for different_equal in self.differences[loader_field].keys():
                summary.append(f"\n\t{different_equal}:")

                for file_location in self.differences[loader_field][
                    different_equal
                ].keys():

                    for diff in self.differences[loader_field][different_equal][
                        file_location
                    ]:
                        dbval = diff["database"]
                        dbtype = type(dbval).__name__
                        fileval = diff["file"]
                        filetype = type(fileval).__name__

                        summary.append(
                            f"\n\t\t{file_location}:\n"
                            f"\t\t\tDatabase Value: '{dbval}' (type: {dbtype})\n"
                            f"\t\t\tFile Value: '{fileval}' (type: {filetype})"
                        )

        return "".join(summary)

    def _build_message(self, summary: str) -> str:
        message = (
            "Model field values from existing records in the database were compared to values from the input file, "
            "but the types differed.  Their values were cast to a string to make the comparison.  If the string "
            "comparison conclusions are correct, you can ignore this warning.  If any conclusions are wrong, the "
            "corresponding loader class must be updated so that the values can be correctly compared.  Consult the "
            "docstring of the DBFieldVsFileColDeveloperWarning class for details.  Summary of the type differences "
            f"encountered:\n{summary}"
        )

        if self.has_mismatches:
            message += (
                "\n\nNote that since in at least 1 case, the database and file string values differed, their "
                "exceptions will be followed by ConflictingValueError exceptions.  This warning is intended to help "
                "debug the case where any of those ConflictingValueError exceptions appear wrong (e.g. it says that "
                "'1' != 1)."
            )

        return message


class DBFieldVsFileColDeveloperWarning(
    InfileError, DeveloperWarning, SummarizableError
):
    """This warning helps developers find problems in the loader code that helps users to be able to fix unique
    constraint IntegrityErrors.

    Problems found relate to comparing file-derived values with database-derived values when their types are not
    recorded in the loader class.  The purpose is to catch automatic type conversion mistakes that Excel/pandas make so
    that that incorrect type can be explicitly fixed in the loader class (i.e. it can tell pandas what type to use).
    Only simple types are supported.

    Solving this problem means that whenever the user encounters a unique constraint violation, we will be able to tell
    the user what sheet, row, and column in their infile need to be corrected to resolve the conflict.  This warning
    helps us ensure that they are given valid difference information.

    The fix only applies to database fields that map 1:1 with a column in the input file.  If you encounter a type issue
    relating to more than 1 column, a parsed column, or a delimited column, the problem cannot be fixed in the method
    described below.  It means that your code is setting the incorrect type in one of the values in the dict supplied
    to get_or_create.  To determine if the advice below is relevant, you need to identify a column in the file/sheet,
    using the data provided in the exception, that corresponds exactly to the value from the database.  If the column
    contains multiple delimited values or if it is a parsed value, or if the DB field is constructed using values from
    multiple columns, the issue is in the loading code, and the advise below is not applicable.

    If there is a column that corresponds 1:1 with the DB field, there are 2 changes that need to be made to the loader
    code:

    1. The database field should be mapped to the model/column in the class attribute 'FieldToDataHeaderKey'
    2. The type of the values in the column should be added to the class attribute 'DataColumnTypes'.

    Example:
        If the exception is from the SamplesLoader, and the sample name column had the value '1', pandas will by default
        read in an integer, but the database expects a string.  We have to map the 'Sample.name' field to the column in
        the variable 'SAMPLE_KEY', and set its type to 'str'.  We do that by adding the values in the class attributes:

        DataColumnTypes: Dict[str, type] = {
            SAMPLE_KEY: str,
        }

        FieldToDataHeaderKey = {
            Sample.__name__: {
                "name": SAMPLE_KEY,
            },
        }

    """

    SummarizerExceptionClass = DBFieldVsFileColDeveloperWarnings

    def __init__(
        self,
        rec: Model,
        field: str,
        db_value: Any,
        file_value: Any,
        loader_name: str,
        **kwargs,
    ):
        different = str(db_value) != str(file_value)
        different_str = "different" if different else "equal"
        message = (
            f"A Model field '{type(rec).__name__}.{field}' value from an existing record in the database was compared "
            f"to a value from an unmapped column in %s, but the type of the value from the database ('{db_value}', a "
            f"'{type(db_value).__name__}') and the type of the value from the file ('{file_value}', a "
            f"'{type(file_value).__name__}') differs.  Both were cast to a string to compare and found to be "
            f"{different_str}.\n\n"
            f"If the string comparison conclusion ('{db_value}' vs '{file_value}' -> {different_str}) is correct, you "
            f"can ignore this warning.  If that conclusion is wrong, the loader ({loader_name}) must be updated so "
            "that the values can be correctly compared.  Consult the docstring of the "
            f"{__class__.__name__} class for details."  # type: ignore[name-defined]
        )
        if different:
            message += (
                "\n\nNote that since the database and file string values differed, this exception will be followed by "
                "a ConflictingValueError.  This warning is intended to help debug the case where that "
                "ConflictingValueError appears wrong (e.g. it says that 1 != 1)."
            )
        InfileError.__init__(self, message, **kwargs)
        self.rec = rec
        self.field = field
        self.db_value = db_value
        self.file_value = file_value
        self.loader_name = loader_name
        self.different = str(db_value) != str(file_value)
        self.different_str = different_str


class MissingFCircCalculationValues(Exception):
    """Summary of `MissingFCircCalculationValue` exceptions."""

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
    """A value, while not required, but necessary for (accurate) FCirc calculations, is missing.

    TraceBase does not require values for some database model fields because it supports animals that have not been
    infused with tracers, but when an animal does have a tracer infusion, certain values are necessary to accurately
    compute FCirc.  If any of those values have not been filled in, and the animal has an infusate, you will see this
    exception as a warning.

    While your data can be loaded without these values, it is highly recommended that all such values be supplied in
    order to show FCirc records with calculated values and without associated errors or warnings.

    Summarized in `MissingFCircCalculationValues`.
    """

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
    """Summary of `ProhibitedCompoundName` exceptions."""

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
    # NOTE: Not user facing.  This exception is always caught and repackaged to provide input file context.

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
    """The compound name or synonym contains disallowed characters that were replaced with similar allowed characters.

    This exception is always raised as a warning.

    Disallowed characters are either compound name/synonym delimiters or Peak Group name delimiters that are used during
    loading.

    While the offending characters are automatically replaced, you may elect to use an alternate character.  If you go
    with the automatic replacement, nothing further needs to be done, but if you edit the values in the Study Doc, but
    be sure to make the edit everywhere, including the `Compounds` sheet, the `Tracers`/`Infusates` sheets (and in the
    `Infusate` column in the `Animals` sheet), the `Peak Group Conflicts` sheet.  Also, all peak annotation files will
    need to be updated as well.

    Summarized by `ProhibitedCompoundNames`.
    """

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
    """Summary of `AnimalWithoutSamples` exceptions.

    Lists the names of animals (and the locations in the study doc in which they can be found) that have no samples and
    suggests how to resolve the issue.

    DEV_SECTION - Everything above this delimiter is user-facing.  See TraceBaseDocs/README.md

    Args:
        exceptions (List[AnimalWithoutSamples])

    Attributes:
        Class:
            None
        Instance:
            exceptions (List[AnimalWithoutSamples])
            animals (List[str]): List of animal names.
    """

    def __init__(self, exceptions: List[AnimalWithoutSamples]):
        # Assumes all exceptions are from the same 1 file's Animals sheet, and gets the file, sheet, and column from the
        # first exception
        first_exc: AnimalWithoutSamples = exceptions[0]
        loc = generate_file_location_string(
            file=first_exc.file, sheet=first_exc.sheet, column=first_exc.column
        )

        message = f"The following animals in {loc} do not have any samples in the Samples sheet:"

        for exc in sorted(exceptions, key=lambda e: e.animal):
            message += f"\n\t'{exc.animal}' on row {exc.rownum}"

        message += (
            "\n\nThe animals will be loaded if you do nothing.  You can ignore this for now and submit samples for "
            "these animals in the future either by resubmitting this amended study doc or by submitting a new study "
            "doc that supplements this existing study.  Or you can address the issue now by adding overlooked samples "
            "to the Samples sheet (or remove the animals from the Animals sheet)."
        )

        super().__init__(message)
        self.exceptions = exceptions
        self.animals = [e.animal for e in exceptions]


class AnimalWithoutSamples(InfileError, SummarizableError):
    """An animal was detected without any samples associated with it in the `Samples` sheet.

    If the animal has samples in the `Samples` sheet, it is likely that the load of every sample associated with the
    animal encountered a separate error.  Fixing those errors will resolve this warning.

    If however, there are no samples associated with the animal in the `Samples` sheet, it is likely that one or more
    peak annotation files associated with the animal was omitted when generating the Study Doc on the Upload **Start**
    page.  In this case, to address the issue, it is recommended that you generate a new Study Doc from **all** peak
    annotation files combined and copy over all of your work from the current file, being careful to account for new
    ordered samples rows and all auto-filled sheets, like `Peak Annotation File`/`Details` and `Compounds`.

    This is recommended for a number of reasons that are covered elsewhere in the TraceBase documentation, but to
    summarize, the Upload **Start** page performs checks that are not performed elsewhere to find conflicting issues
    between peak annotation files, and it fills in all inter-sheet references (including hidden sheets and columns and
    peak group conflicts) that are laborious and error prone to attempt manually.

    You may alternatively elect to add the forgotten peak annotation files in a separate submission after the current
    data has been loaded.  You may keep the animal records and ignore this warning.  The subsequent submission should
    include the complete animal record and associated study record.

    Summarized in `AnimalsWithoutSamples`.

    DEV_SECTION - Everything above this delimiter is user-facing.  See TraceBaseDocs/README.md

    Args:
        animal (str): Name of an animal without samples.
        message (Optional[str])

    Attributes:
        Class:
            SummarizerExceptionClass (Exception): Concrete class attribute of SummarizableError's abstract requirement.
                Exception classes derived from abstract base class `SummarizableError` are collected in
                `DataRepo.loaders.base.table_loader.TableLoader` and summarized by the class defined here.
        Instance:
            animal (str): Name of an animal without samples.
    """

    SummarizerExceptionClass = AnimalsWithoutSamples

    def __init__(self, animal: str, message: Optional[str] = None, **kwargs):
        if message is None:
            message = f"Animal '{animal}' does not have any samples in %s."

        if "suggestion" not in kwargs.keys() or kwargs["suggestion"] is None:
            message += (
                "\n\nThe animal will be loaded if you do nothing.  You can ignore this for now and submit samples for "
                "this animal in the future either by resubmitting this amended study doc or by submitting a new study "
                "doc that supplements this existing study.  Or you can address the issue now by adding overlooked "
                "samples to the Samples sheet (or remove the animal from the Animals sheet)."
            )

        super().__init__(message, **kwargs)
        self.animal = animal


class AnimalsWithoutSerumSamples(Exception):
    """Summary of `AnimalWithoutSerumSamples` exceptions.

    Lists the names of animals (and the locations in the study doc in which they can be found) that have no serum
    samples, explains why they're important, and suggests how to resolve the issue.

    DEV_SECTION - Everything above this delimiter is user-facing.  See TraceBaseDocs/README.md

    Args:
        exceptions (List[AnimalWithoutSerumSamples])

    Attributes:
        Class:
            None
        Instance:
            exceptions (List[AnimalWithoutSerumSamples])
            animals (List[str]): List of animal names.
    """

    def __init__(self, exceptions: List[AnimalWithoutSerumSamples]):
        # Assumes all exceptions are from the same 1 file's Animals sheet, and gets the file, sheet, and column from the
        # first exception
        first_exc: AnimalWithoutSerumSamples = exceptions[0]
        loc = generate_file_location_string(
            file=first_exc.file, sheet=first_exc.sheet, column=first_exc.column
        )

        message = (
            f"The following animals in {loc} do not have the necessary serum samples to perform FCirc "
            "calculations:"
        )

        for exc in sorted(exceptions, key=lambda e: e.animal):
            message += f"\n\t'{exc.animal}' on row {exc.rownum}"

        message += (
            "\nFCirc calculations on TraceBase are done using the tracer peak group(s) from the last serum sample, the "
            "infusion rate, and the animal weight.  You can load data into TraceBase without serum samples, but the "
            "FCirc values will be missing.\n\n"
            "Everything will be loaded if you do nothing.  You can ignore this for now and submit serum samples for "
            "these animals in the future either by resubmitting this amended study doc or by submitting a new study "
            "doc that supplements this existing study.  Or you can address the issue now by adding overlooked serum "
            "samples to the Samples sheet (or remove the animals from the Animals sheet)."
        )

        super().__init__(message)
        self.exceptions = exceptions
        self.animals = [e.animal for e in exceptions]


class AnimalWithoutSerumSamples(InfileError, SummarizableError):
    """An animal with a tracer infusion was detected without any serum samples associated with it in the `Samples`
    sheet.

    Serum samples are necessary in order for TraceBase to report FCirc calculations.

    If the animal has serum samples in the `Samples` sheet, it is possible that the load of every serum sample
    associated with the animal encountered a separate error.  Fixing those errors will resolve this warning.

    If however, there are no serum samples associated with the animal in the `Samples` sheet, it is possible that a
    peak annotation file associated with the animal was omitted when generating the Study Doc on the Upload **Start**
    page.  In this case, to address the issue, it is recommended that you generate a new Study Doc from **all** peak
    annotation files combined and copy over all of your work from the current file, being careful to account for new
    ordered samples rows and all auto-filled sheets, like `Peak Annotation File`/`Details` and `Compounds`.

    This is recommended for a number of reasons that are covered elsewhere in the TraceBase documentation, but to
    summarize, the Upload **Start** page performs checks that are not performed elsewhere to find conflicting issues
    between peak annotation files, and it fills in all inter-sheet references (including hidden sheets and columns and
    peak group conflicts) that are laborious and error prone to attempt manually.

    You may alternatively elect to add the forgotten peak annotation files in a separate submission after the current
    data has been loaded.  You may keep the animal records and ignore this warning.  The subsequent submission should
    include the complete animal record and associated study record.

    Summarized in `AnimalsWithoutSerumSamples`.

    DEV_SECTION - Everything above this delimiter is user-facing.  See TraceBaseDocs/README.md

    Args:
        animal (str): Name of an animal without serum samples.
        message (Optional[str])

    Attributes:
        Class:
            SummarizerExceptionClass (Exception): Concrete class attribute of SummarizableError's abstract requirement.
                Exception classes derived from abstract base class `SummarizableError` are collected in
                `DataRepo.loaders.base.table_loader.TableLoader` and summarized by the class defined here.
        Instance:
            animal (str): Name of an animal without samples.
    """

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
                "but the FCirc values will be missing.\n\n"
                "Everything will be loaded if you do nothing.  You can ignore this for now and submit serum samples "
                "for this animal in the future either by resubmitting this ammended study doc or by submitting a new "
                "study doc that supplements this existing study.  Or you can address the issue now by adding "
                "overlooked serum samples to the Samples sheet (or remove the animal from the Animals sheet)."
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


def trace(exc: Optional[Exception] = None):
    """
    Creates a pseudo-traceback for debugging.  Tracebacks are only built as the raised exception travels the stack to
    where it's caught.  traceback.format_stack yields the entire stack, but that's overkill, so this loop filters out
    anything that contains "site-packages" so that we only see our own code's steps.  This should effectively show us
    only the bottom of the stack, though there's a chance that intermediate steps could be excluded.  I don't think
    that's likely to happen, but we should be aware that it's a possibility.

    The string is intended to only be used to debug a problem.  Print it inside an except block if you want to find the
    cause of any particular buffered exception.

    Args:
        exc (Optional[Exception]): An optional caught exception to include a partial traceback with the returned trace.
    Exceptions:
        None
    Returns:
        trace (str): A string formatted stack trace (not including the optional exception's message)
    """
    trace = "".join(
        [str(step) for step in traceback.format_stack() if "site-packages" not in step]
    )
    if (
        isinstance(exc, Exception)
        and hasattr(exc, "__traceback__")
        and exc.__traceback__ is not None
    ):
        trace += "\nThe exception that triggered the above catch's trace has a partial traceback:\n\n"
        trace += "".join(traceback.format_tb(exc.__traceback__))
    return trace
