import traceback


class HeaderError(Exception):
    pass


class RequiredValueError(Exception):
    pass


class RequiredHeadersError(Exception):
    def __init__(self, missing, message=None):
        if not message:
            message = f"Required header(s) missing: [{', '.join(missing)}]."
        super().__init__(message)
        self.missing = missing


class HeaderConfigError(Exception):
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
            message = "Required values missing in the following columns/rows:\n"
            for col in missing.keys():
                message += f"\n{col}: {', '.join([str(r) for r in missing[col]])}\n"
        super().__init__(message)
        self.missing = missing


class UnknownHeadersError(Exception):
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
            message = (
                f"{len(samples)} samples are missing in the database: [{', '.join(samples)}].  Samples must be loaded "
                "prior to loading mass spec data."
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
    def __init__(self):
        message = (
            "No samples were either supplied or found in the database.  Samples must be loaded before mass spec data "
            "can be loaded."
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


class AmbiguousCompoundDefinitionError(Exception):
    pass


class ValidationDatabaseSetupError(Exception):
    message = "The validation database is not configured"


class DryRun(Exception):
    """
    Exception thrown during dry-run to ensure atomic transaction is not committed
    """

    def __init__(self, message="Dry-run successful"):
        super().__init__(message)


class LoadingError(Exception):
    """
    Exception thrown if any errors encountered during loading
    """

    pass


class AggregatedErrors(Exception):
    def __init__(
        self, message=None, exceptions=None, errors=None, warnings=None, quiet=True
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
        print("AggregatedErrors Summary:")
        for i, exception in enumerate(self.exceptions, start=1):
            print(
                f"\tEXCEPTION{i}({exception.exc_type_str.upper()}): {type(exception).__name__}: {exception}"
            )

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
            [step for step in traceback.format_stack() if "site-packages" not in step]
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

    def print_buffered_exception(self, buffered_exception):
        print(f"Buffered {buffered_exception.exc_type_str}:")
        print(buffered_exception.buffered_tb_str.rstrip())
        print(f"{type(buffered_exception).__name__}: {str(buffered_exception)}\n")

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
        message=None,
    ):
        if not message:
            rowmsg = ""
            if rownum:
                rowmsg = f"on row {rownum} "
            message = (
                f"Conflicting values encountered {rowmsg}in {type(rec).__name__} record [{str(rec)}] for the "
                f"[{consistent_field}] field:\n\tdatabase value: [{existing_value}]\n\tload data value: "
                f"[{differing_value}]."
            )
        super().__init__(message)
        self.consistent_field = consistent_field
        self.existing_value = existing_value
        self.differing_value = differing_value
        self.rownum = rownum


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
        message = (
            f"The following duplicate compound/isotope combinations were found in the {source} data: ["
            f"{'; '.join(list(map(lambda c: f'{c} on rows: {dupe_dict[c]}', dupe_dict.keys())))}]"
        )
        super().__init__(message)
        self.dupe_dict = dupe_dict
        self.source = source


class DuplicateValues(Exception):
    def __init__(self, dupe_dict, colname, message=None):
        if not message:
            # Each value is displayed as "value (1,2,3)" where "value" is the diplicate value and 1,2,3 are the rows
            # where it occurs
            dupdeets = []
            for v, l in dupe_dict.items():
                # dupe_dict contains row indexes. This converts to row numbers (adds 1 for starting from 1 instead of 0
                # and 1 for the header row)
                dupdeets.append(f"{v} ({','.join(list(map(lambda i: str(i + 2), l)))})")
            feed_indent = "\n\t"
            message = (
                f"{len(dupe_dict.keys())} values were found with duplicate occurrences in the [{colname}] column, "
                "whose values must be unique, on the indicated rows (note, row numbers reflect a merge of the Animal "
                f"and Sample sheet and may be inaccurate):\n\t{feed_indent.join(dupdeets)}"
            )
        super().__init__(message)
        self.dupe_dict = dupe_dict
        self.colname = colname


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
