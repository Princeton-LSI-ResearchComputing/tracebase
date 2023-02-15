import traceback


class HeaderError(Exception):
    pass


class RequiredValueError(Exception):
    pass


class RequiredHeadersError(HeaderError):
    def __init__(self, missing, message=None):
        if not message:
            message = f"Required header(s) missing: [{', '.join(missing)}]."
        super().__init__(message)
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


class RequiredValuesError(Exception):
    def __init__(self, missing, message=None):
        if not message:
            nltab = "\n\t"
            deets = list(
                map(lambda k: f"{str(k)} on rows: {str(missing[k])}", missing.keys())
            )
            message = (
                "Missing required values have been detected in the following columns:\n\t"
                f"{nltab.join(deets)}\nNote, entirely empty rows are allowed, but having a single value on a "
                "row in one sheet can cause a duplication of empty rows, so be sure you don't have stray single "
                "values in a sheet."
            )
            # Row numbers are available, but not very useful given the sheet merge
        super().__init__(message)
        self.missing = missing


class ExistingMSRun(Exception):
    def __init__(self, date, researcher, protocol_name, file_samples_dict, adding_file):
        message = (
            "The following date, researcher, and protocol:\n"
            f"\tdate: {date}\n"
            f"\tresearcher: {researcher}\n"
            f"\tprotocol: {protocol_name}\n"
            f"for the load of the current accucor/isocorr file: [{adding_file}]\n"
            "contains samples that were also found to be associated with the following previously (or concurrently) "
            "loaded file(s).  The common/conflicting samples contained in each file are listed:\n"
        )
        for existing_file in file_samples_dict.keys():
            message += f"\t{existing_file}:\n\t\t"
            message += "\n\t\t".join(file_samples_dict[existing_file])
        message += (
            "\nThis indicates that the same samples were a part of multiple MSRuns.  The date, researcher, protocol, "
            "(and sample name) must be unique for each MSRun.  Try changing the date of the MSRun."
        )
        super().__init__(message)
        self.date = date
        self.researcher = researcher
        self.protocol_name = protocol_name
        self.file_samples_dict = file_samples_dict
        self.adding_file = adding_file


class UnknownHeadersError(HeaderError):
    def __init__(self, unknowns, message=None):
        if not message:
            message = f"Unknown header(s) encountered: [{', '.join(unknowns)}]."
        super().__init__(message)
        self.unknowns = unknowns


class ResearcherNotNew(Exception):
    def __init__(self, researcher, new_flag, researchers):
        nl = "\n"
        message = (
            f"Researcher [{researcher}] exists.  {new_flag} cannot be used for existing "
            f"researchers.  Current researchers are:{nl}{nl.join(sorted(researchers))}"
        )
        super().__init__(message)
        self.researcher = researcher
        self.new_flag = new_flag
        self.researchers = researchers


class MissingSamplesError(Exception):
    def __init__(self, samples, message=None):
        if not message:
            nltab = "\n\t"
            message = (
                f"{len(samples)} samples are missing in the database:{nltab}{nltab.join(samples)}\nSamples must be "
                "loaded prior to loading mass spec data."
            )
        super().__init__(message)
        self.sample_list = samples


class UnskippedBlanksError(MissingSamplesError):
    def __init__(self, samples):
        message = (
            f"{len(samples)} samples that appear to possibly be blanks are missing in the database: "
            f"[{', '.join(samples)}].  Blank samples should be skipped."
        )
        super().__init__(samples, message)


class NoSamplesError(Exception):
    def __init__(self, num_samples=0):
        if num_samples == 0:
            message = "No samples were supplied."
        else:
            message = (
                f"None of the {num_samples} samples were found in the database.  Samples must be loaded before mass "
                "spec data can be loaded."
            )
        super().__init__(message)


class EmptyColumnsError(Exception):
    def __init__(self, sheet_name, col_names):
        message = (
            f"Sample columns missing headers found in the [{sheet_name}] data sheet. You have [{len(col_names)}] "
            "columns. Be sure to delete any unused columns."
        )
        super().__init__(message)
        self.sheet_name = sheet_name
        self.col_names = col_names


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


# class AmbiguousCompoundDefinitionError(Exception):
#     pass


class ValidationDatabaseSetupError(Exception):
    message = "The validation database is not configured"


class DryRun(Exception):
    """
    Exception thrown during dry-run to ensure atomic transaction is not committed
    """

    def __init__(self, message=None):
        if message is None:
            message = "Dry-run successful."
        super().__init__(message)


class LoadingError(Exception):
    """
    Exception thrown if any errors encountered during loading
    """

    pass


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

    def __init__(
        self, message=None, exceptions=None, errors=None, warnings=None, quiet=False
    ):
        if not exceptions:
            exceptions = []
        if not errors:
            errors = []
        if not warnings:
            warnings = []

        self.num_errors = len(errors)
        self.num_warnings = len(warnings)

        self.is_fatal = False  # Default to not fatal. buffer_exception can change this.
        for exception in exceptions:
            if not hasattr(exception, "is_error"):
                self.ammend_buffered_exception(exception, is_error=True)
            if exception.is_error:
                self.num_errors += 1
                self.is_fatal = True
            else:
                self.num_warnings += 1

        self.exceptions = exceptions
        for warning in warnings:
            self.ammend_buffered_exception(warning, is_error=False)
            self.exceptions.append(warning)
        for error in errors:
            self.ammend_buffered_exception(error, is_error=True)
            self.exceptions.append(error)
            self.is_fatal = True

        if not message:
            message = self.get_default_message()
        super().__init__(message)

        self.buffered_tb_str = self.get_buffered_traceback_string()
        self.quiet = quiet

    def get_default_message(self, should_raise_called=False):
        if len(self.exceptions) > 0:
            errtypes = []
            for errtype in [type(e).__name__ for e in self.exceptions]:
                if errtype not in errtypes:
                    errtypes.append(errtype)
            message = f"{len(self.exceptions)} exceptions occurred, including type(s): [{', '.join(errtypes)}]."
            if not self.is_fatal:
                message += "  This exception should not have been raised."
        elif not should_raise_called:
            message = (
                "AggregatedErrors exception.  Use self.should_raise() to initialize the message and report an errors/"
                "warnings summary."
            )
        else:
            message = (
                "AggregatedErrors exception.  No exceptions have been buffered.  Use the return of "
                "self.should_raise() to determine if an exception should be raised before raising this exception."
            )
        return message

    def print_summary(self):
        print(self.get_summary_string())

    def get_summary_string(self):
        sum_str = f"AggregatedErrors Summary ({self.num_errors} errors / {self.num_warnings} warnings):\n\t"
        sum_str += "\n\t".join(
            list(
                map(
                    lambda tpl: f"EXCEPTION{tpl[0]}({tpl[1].exc_type_str.upper()}): {type(tpl[1]).__name__}: {tpl[1]}",
                    enumerate(self.exceptions, start=1),
                )
            ),
        )
        return sum_str

    @classmethod
    def ammend_buffered_exception(cls, exception, is_error=True, buffered_tb_str=None):
        """
        This takes an exception that is going to be buffered and adds a few data memebers to it: buffered_tb_str (a
        traceback string), is_error (e.g. whether it's a warning or an exception), and a string that is used to
        classify it as a warning or an error.  The exception is returned for convenience.  The buffered_tb_str is not
        generated here because is can be called out of the context of the exception (see the constructor).
        """
        exception.buffered_tb_str = buffered_tb_str
        exception.is_error = is_error
        exception.exc_type_str = "Warning"
        if is_error:
            exception.exc_type_str = "Error"
        return exception

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

    def buffer_exception(self, exception, is_error=True, is_fatal=True):
        """
        Don't raise this exception. Save it to report later, after more errors have been accumulated and reported as a
        group.  Returns the buffered_exception containing a buffered_tb_str and a boolean named is_error.

        is_fatal tells the AggregatedErrors object that after buffering is complete, the AggregatedErrors exception
        should be raised and execution should stop.  By default, errors will cause AggregatedErrors to be raised and
        warnings will not not, however, in validate mode, warnings are communicated to the validation interface by them
        being raised.
        """

        buffered_tb_str = self.get_buffered_traceback_string()
        buffered_exception = None
        if hasattr(exception, "__traceback__") and exception.__traceback__:
            added_exc_str = "".join(
                traceback.format_exception(
                    type(exception),
                    exception,
                    exception.__traceback__,
                )
            )
            buffered_tb_str += f"\n\nThe above caught exception had a partial traceback:\n{added_exc_str}"
            buffered_exception = exception
        else:
            try:
                raise exception
            except Exception as e:
                buffered_exception = e.with_traceback(e.__traceback__)

        self.ammend_buffered_exception(
            buffered_exception, is_error=is_error, buffered_tb_str=buffered_tb_str
        )
        self.exceptions.append(buffered_exception)

        if buffered_exception.is_error:
            self.num_errors += 1
        else:
            self.num_warnings += 1

        if is_fatal:
            self.is_fatal = True

        if not self.quiet:
            self.print_buffered_exception(buffered_exception)

        return buffered_exception

    def buffer_error(self, exception, is_fatal=True):
        self.buffer_exception(exception, is_error=True, is_fatal=is_fatal)

    def buffer_warning(self, exception, is_fatal=False):
        self.buffer_exception(exception, is_error=False, is_fatal=is_fatal)

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
        exc_str = f"Buffered {buffered_exception.exc_type_str}:\n"
        exc_str += buffered_exception.buffered_tb_str.rstrip() + "\n"
        exc_str += f"{type(buffered_exception).__name__}: {str(buffered_exception)}"
        return exc_str

    def should_raise(self, message=None):
        if not message:
            message = self.get_default_message(should_raise_called=True)
        super().__init__(message)
        return self.is_fatal

    def get_num_errors(self):
        return self.num_errors

    def get_num_warnings(self):
        return self.num_warnings


class ConflictingValueError(Exception):
    def __init__(
        self,
        rec,
        consistent_field,
        existing_value,
        differing_value,
        rownum=None,
        db=None,
        message=None,
    ):
        if not message:
            rowmsg = (
                f"on row {rownum} of the load file data " if rownum is not None else ""
            )
            dbmsg = f" in database [{db}]" if db is not None else ""
            message = (
                f"Conflicting values encountered {rowmsg}in {type(rec).__name__} record [{str(rec)}] for the "
                f"[{consistent_field}] field{dbmsg}:\n"
                f"\tdatabase {consistent_field} value: [{existing_value}]\n"
                f"\tfile {consistent_field} value: [{differing_value}]"
            )
        super().__init__(message)
        self.consistent_field = consistent_field
        self.existing_value = existing_value
        self.differing_value = differing_value
        self.rownum = rownum
        self.db = db


class SaveError(Exception):
    def __init__(self, model_name, rec_name, db, e):
        message = f"Error saving {model_name} {rec_name} to database {db}: {type(e).__name__}: {str(e)}"
        super().__init__(message)
        self.model_name = model_name
        self.rec_name = rec_name
        self.db = db
        self.orig_err = e


class DupeCompoundIsotopeCombos(Exception):
    def __init__(self, dupe_dict, source):
        nltab = "\n\t"
        message = (
            f"The following duplicate compound/isotope combinations were found in the {source} data:{nltab}"
            f"{nltab.join(list(map(lambda c: f'{c} on rows: {dupe_dict[c]}', dupe_dict.keys())))}"
        )
        super().__init__(message)
        self.dupe_dict = dupe_dict
        self.source = source


class DuplicateValues(Exception):
    def __init__(self, dupe_dict, colnames, message=None, addendum=None):
        """
        Takes a dict whose keys are (composite, unique) strings and the values are lists of row indexes
        """
        if not message:
            # Each value is displayed as "Colname1: [value1], Colname2: [value2], ... (rows*: 1,2,3)" where 1,2,3 are
            # the rows where the combo values are found
            dupdeets = []
            for v, l in dupe_dict.items():
                # dupe_dict contains row indexes. This converts to row numbers (adds 1 for starting from 1 instead of 0
                # and 1 for the header row)
                dupdeets.append(
                    f"{str(v)} (rows*: {', '.join(list(map(lambda i: str(i + 2), l)))})"
                )
            nltab = "\n\t"
            message = (
                f"{len(dupe_dict.keys())} values in unique column(s) {colnames} were found to have duplicate "
                "occurrences on the indicated rows (*note, row numbers could reflect a sheet merge and may be "
                f"inaccurate):{nltab}{nltab.join(dupdeets)}"
            )
            if addendum is not None:
                message += f"\n{addendum}"
        super().__init__(message)
        self.dupe_dict = dupe_dict
        self.colnames = colnames
        self.addendum = addendum


class EmptyAnimalNames(Exception):
    def __init__(self, row_idxs, animal_col_name="Animal Name", message=None):
        if not message:
            message = (
                f"Rows which are missing an {animal_col_name} but have a value in some other column cause meaningless "
                f"errors because the {animal_col_name} is used to merge the animal data with the sample data in "
                "separate files/excel-sheets.  To avoid these errors, a row must be completely empty, or at least "
                f"contain an {animal_col_name}.  The following rows are affected, but the row numbers can be "
                f"inaccurate due to merges with empty rows: [{', '.join(list(map(lambda v: str(v), row_idxs)))}]."
            )
        super().__init__(message)
        self.row_idxs = row_idxs
        self.animal_col_name = animal_col_name


class NoTracerLabeledElements(Exception):
    def __init__(self):
        message = "tracer_labeled_elements required to process PARENT entries."
        super().__init__(message)


class IsotopeStringDupe(Exception):
    """
    There are multiple isotope measurements that match the same parent tracer labeled element
    E.g. C13N15C13-label-2-1-1 would match C13 twice
    """

    def __init__(self, measurement_str, parent_str):
        message = (
            f"Cannot uniquely match tracer labeled element ({parent_str}) in the measured labeled element string: "
            f"[{measurement_str}]."
        )
        super().__init__(message)
        self.measurement_str = measurement_str
        self.parent_str = parent_str


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


class MissingCompounds(Exception):
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
                        lambda c: f"{c} {compounds_dict[c]['formula']} on row(s): {compounds_dict[c]['rownums']}",
                        compounds_dict.keys(),
                    )
                )
            )
            message = (
                f"{len(compounds_dict.keys())} compounds were not found in the database:{nltab}{cmdps_str}\n"
                "Compounds must be loaded prior to loading mass spec data."
            )
        super().__init__(message)
        self.compounds_dict = compounds_dict
