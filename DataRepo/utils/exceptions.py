from __future__ import annotations

import traceback
import warnings
from typing import TYPE_CHECKING

from django.forms.models import model_to_dict

if TYPE_CHECKING:
    from DataRepo.models import MSRun, PeakGroupSet, Sample


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
                map(
                    lambda k: f"{str(k)} on row(s): {str(summarize_int_list(missing[k]))}",
                    missing.keys(),
                )
            )
            message = (
                "Missing required values have been detected in the following columns:\n\t"
                f"{nltab.join(deets)}\nIf you wish to skip this row, you can either remove the row entirely or enter "
                "dummy values to avoid this error."
            )
            # Row numbers are available, but not very useful given the sheet merge
        super().__init__(message)
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


class DuplicatePeakGroup(Exception):
    """Duplicate data for the same sample sequenced on the same day

    Records duplicate sample compound pairs for a given ms_run

    Attributes:
        adding_file: The peak annotation file in which the duplicate data was detected
        ms_run: The MSRun in which the peak groups were measured
        sample_name: The name of the sample the duplicated data blongs to
        peak_group_name (compounds): The name of duplicated peak group
        existing_peak_group_set: The peak group set that the previosly existing peak group blongs to
    """

    def __init__(
        self,
        adding_file: str,
        ms_run: MSRun,
        sample: Sample,
        peak_group_name: str,
        existing_peak_group_set: PeakGroupSet,
    ):
        """Initializes a DuplicatePeakGroup exception"""

        message = (
            f"Duplicate data found when loading file [{adding_file}]:\n"
            f"\tms_run: {ms_run}\n"
            f"\tsample: {sample}\n"
            f"\tpeak_group_name: {peak_group_name}\n"
            f"\texisting_peak_group_set: {existing_peak_group_set}\n"
            f"\tWas this file [{adding_file}] loaded previously?\n"
        )
        super().__init__(message)
        self.adding_file = adding_file
        self.ms_run = ms_run
        self.sample = sample
        self.peak_group_name = peak_group_name
        self.existing_peak_group_set = existing_peak_group_set


class DuplicatePeakGroups(Exception):
    """Duplicate peak groups from a given peak annotation file

    Attributes:
        adding_file: The peak annotation file in which the duplicate data was detected
        ms_run: The MSRun in which the peak groups were measured
        duplicate_peak_groups: A list of DuplicatePeakGroup exceiptions
    """

    def __init__(
        self,
        adding_file: str,
        duplicate_peak_groups: list[DuplicatePeakGroup],
    ):
        """Initializes a DuplicatePeakGroups exception"""

        message = (
            f"Duplicate data found when loading file [{adding_file}]:\n"
            "\tpeak_groups:\n"
        )
        for duplicate_peak_group in duplicate_peak_groups:
            message += (
                f"\t\tsample: {duplicate_peak_group.sample} | "
                f"peak_group_name: {duplicate_peak_group.peak_group_name} | "
                f"ms_run_date: {duplicate_peak_group.ms_run.date} | "
                f"ms_run_researcher: {duplicate_peak_group.ms_run.researcher} | "
                f"peak_group_set: {duplicate_peak_group.existing_peak_group_set.filename}\n"
            )
        super().__init__(message)
        self.adding_file = adding_file
        self.duplicate_peak_groups = duplicate_peak_groups


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


class AllMissingSamples(Exception):
    """
    This is the same as the MissingSamplesError class, but it takes a 2D dict that is used to report every file where
    each missing sample exists.
    """

    def __init__(self, samples, message=None):
        if not message:
            nltab = "\n\t"
            smpls_str = nltab.join(
                list(
                    map(
                        lambda smpl: f"{smpl} in file(s): {samples[smpl]}",
                        samples.keys(),
                    )
                )
            )
            message = (
                f"{len(samples)} samples are missing in the database/sample-table:{nltab}{smpls_str}\nSamples in the "
                "accucor/isocorr files must be present in the sample table file and loaded into the database before "
                "they can be loaded from the mass spec data files."
            )
        super().__init__(message)
        self.samples = samples


class MissingSamplesError(Exception):
    def __init__(self, samples, message=None):
        if not message:
            nltab = "\n\t"
            message = (
                f"{len(samples)} samples are missing in the database/sample-table-file:{nltab}{nltab.join(samples)}\n"
                "Samples must be loaded prior to loading mass spec data."
            )
        super().__init__(message)
        self.samples = samples


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
                f"None of the {num_samples} samples were found in the database/sample table file.  Samples in the "
                "accucor/isocorr files must be present in the sample table file and loaded into the database before "
                "they can be loaded from the mass spec data files."
            )
        super().__init__(message)


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


class DryRun(Exception):
    """
    Exception thrown during dry-run to ensure atomic transaction is not committed
    """

    def __init__(self, message=None):
        if message is None:
            message = "Dry Run Complete."
        super().__init__(message)


class LoadingError(Exception):
    """
    Exception thrown if any errors encountered during loading
    """

    pass


class LoadFileError(Exception):
    """
    This exception is a wrapper for other exceptions, which adds file-related context
    """

    def __init__(self, exception, line_num):
        message = f"{type(exception).__name__} on infile line {line_num}: {exception}"
        super().__init__(message)
        self.exception = exception
        self.line_num = line_num


class MultiLoadStatus(Exception):
    """
    This class holds the load status of multiple files and also can contain multiple file group statuses, e.g. a
    discrete list of missing compounds across all files.  It is defined as an Exception class so that it being raised
    from (for example) load_study will convey the load statuses to the validation interface.
    """

    def __init__(self, load_keys=None):
        self.state = "PASSED"
        self.is_valid = True
        self.num_errors = 0
        self.num_warnings = 0
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
            "num_errors": 0,
            "num_warnings": 0,
            "top": True,  # Passing files will appear first
        }

    def set_load_exception(self, exception, load_key, top=False):
        """
        This records the status of a load in a data member called statuses.  It tracks some overall stats and can set
        whether this load status should appear at the top of the reported statuses or not.
        """

        if len(self.statuses.keys()) == 0:
            warnings.warn(
                f"Load keys such as [{load_key}] should be pre-defined when {type(self).__name__} is constructed or "
                "as they are encountered by calling [obj.init_load(load_key)].  A load key is by default the file "
                "name, but can be any key can be explicitly added."
            )

        if load_key not in self.statuses.keys():
            self.init_load(load_key)

        if isinstance(exception, AggregatedErrors):
            new_aes = exception
        else:
            # All of the AggregatedErrors are printed to the console as they are encountered, but not other exceptions,
            # so...
            print(exception)

            # Wrap the exception in an AggregatedErrors class
            new_aes = AggregatedErrors(errors=[exception])

        new_num_errors = new_aes.num_errors
        new_num_warnings = new_aes.num_warnings
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
        self.num_errors += new_num_errors
        self.num_warnings += new_num_warnings
        self.statuses[load_key]["num_errors"] = merged_aes.num_errors
        self.statuses[load_key]["num_warnings"] = merged_aes.num_warnings
        self.statuses[load_key]["top"] = top

        # Any error or warning should make is_valid False and the user should decide whether they can ignore the
        # warnings or not.
        self.is_valid = False
        if self.statuses[load_key]["aggregated_errors"].is_error:
            self.statuses[load_key]["state"] = "FAILED"
            self.state = "FAILED"
        else:
            self.statuses[load_key]["state"] = "WARNING"
            self.state = "WARNING"

    def get_success_status(self):
        return self.is_valid

    def get_final_exception(self, message=None):
        # If success, return None
        if self.get_success_status():
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
                f"Success status is {self.get_success_status()} but there are no aggregated exceptions for any "
                "files."
            )

        return AggregatedErrorsSet(aggregated_errors_dict, message=message)

    def get_status_message(self):
        # Overall status message
        message = f"Load {self.state}"
        if self.num_warnings > 0:
            message += f" {self.num_warnings} warnings"
        if self.num_errors > 0:
            message += f" {self.num_errors} errors"

        return message, self.state

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


class AggregatedErrorsSet(Exception):
    def __init__(self, aggregated_errors_dict, message=None):
        self.aggregated_errors_dict = aggregated_errors_dict
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
        super().__init__(current_message)

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
            smry_str += (
                f"{aes_key}: "
                + self.aggregated_errors_dict[aes_key].get_summary_string()
            )
        return smry_str


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

    def get_exception_type(self, exception_class, remove=False):
        """
        To support consolidation of errors across files (like MissingCompounds, MissingSamplesError, etc), this method
        is provided to retrieve such exceptions (if they exist in the exceptions list) from this object and return them
        for consolidation.

        If remove is true, the exceptions are removed from this object.  If it's false, the exception is changed to a
        non-fatal warning (with the assumption that a separate exception will be created that is an error).
        """
        matched_exceptions = []
        unmatched_exceptions = []
        is_fatal = False
        is_error = False
        num_errors = 0
        num_warnings = 0

        # Look for exceptions to remove and recompute new object values
        for exception in self.exceptions:
            if type(exception) == exception_class:
                if not remove:
                    # Change every removed exception to a non-fatal warning
                    exception.is_error = False
                    exception.is_fatal = False
                    num_warnings += 1
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

        if remove:
            # Reinitialize this object
            self.exceptions = unmatched_exceptions
            if not self.custom_message:
                super().__init__(self.get_default_message())

        # Return removed exceptions
        return matched_exceptions

    def remove_exception_type(self, exception_class):
        """
        To support consolidation of errors across files (like MissingCompounds, MissingSamplesError, etc), this method
        is provided to remove such exceptions (if they exist in the exceptions list) from this object and return them
        for consolidation.
        """
        return self.get_exception_type(exception_class, remove=True)

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
            added_exc_str = "".join(traceback.format_tb(exception.__traceback__))
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
        exc_str = buffered_exception.buffered_tb_str.rstrip() + "\n"
        exc_str += f"{self.get_buffered_exception_summary_string(buffered_exception)}"
        return exc_str

    def should_raise(self):
        return self.is_fatal

    def get_num_errors(self):
        return self.num_errors

    def get_num_warnings(self):
        return self.num_warnings

    def exception_type_exists(self, exc_cls):
        return exc_cls in [type(exc) for exc in self.exceptions]


class ConflictingValueErrors(Exception):
    """Conflicting values for a specific model object from a given file

    Attributes:
        model_name: The name of the model object type (Sample, PeakGroup, etc.)
        conflicting_value_errors: A list of ConflictingValueError exceiptions
    """

    def __init__(
        self,
        model_name: str,
        conflicting_value_errors: list[ConflictingValueError],
    ):
        """Initializes a ConflictingValueErrors exception"""

        message = f"Conflicting values found when loading {model_name} records:\n"
        for conflicting_value_error in conflicting_value_errors:
            message += str(conflicting_value_error) + "\n"
        super().__init__(message)
        self.model_name = model_name
        self.conflicting_value_errors = conflicting_value_errors


class ConflictingValueError(Exception):
    def __init__(
        self,
        rec,
        consistent_field,
        existing_value,
        differing_value,
        rownum=None,
        sheet=None,
        message=None,
    ):
        if not message:
            rowmsg = (
                f"on row {rownum} of the load file data " if rownum is not None else ""
            )
            if sheet is not None:
                rowmsg += f"in sheet [{sheet}] "
            message = (
                f"Conflicting [{consistent_field}] field values encountered {rowmsg}in {type(rec).__name__} record "
                f"[{str(model_to_dict(rec))}]:\n"
                f"\tdatabase: [{existing_value}]\n"
                f"\tfile: [{differing_value}]"
            )
        super().__init__(message)
        self.consistent_field = consistent_field
        self.existing_value = existing_value
        self.differing_value = differing_value
        self.rownum = rownum
        self.sheet = sheet


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


class AllMissingTissues(Exception):
    """
    This is the same as the MissingTissues class, but it takes a 3D dict that is used to report every file (and rows
    in that file) where each missing tissue exists.
    """

    def __init__(self, tissues_dict, message=None):
        if not message:
            nltab = "\n\t"
            tissues_str = ""
            for tissue in tissues_dict["tissues"].keys():
                tissues_str += (
                    f"Tissue: [{tissue}], located in the following file(s):{nltab}"
                )
                tissues_str += nltab.join(
                    list(
                        map(
                            lambda fln: f"{fln} on row(s): {summarize_int_list(tissues_dict['tissues'][tissue][fln])}",
                            tissues_dict["tissues"][tissue].keys(),
                        )
                    )
                )
            message = (
                f"{len(tissues_dict['tissues'].keys())} tissues were not found in the database:{nltab}{tissues_str}"
                f"\nPlease check the tissue(s) against the existing tissues list:{nltab}"
                f"{nltab.join(tissues_dict['existing'])}\n"
                "If the tissue cannot be renamed to one of these existing tissues, a new tissue type will have to be "
                "added to the database."
            )
        super().__init__(message)
        self.tissues_dict = tissues_dict


class AllMissingCompounds(Exception):
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


class MissingTissues(Exception):
    def __init__(self, tissues_dict, existing, message=None):
        """
        Takes a dict whose keys are tissue names and values are lists of row numbers.
        """
        if not message:
            nltab = "\n\t"
            deets = list(
                map(
                    lambda k: f"{str(k)} on row(s): {str(summarize_int_list(tissues_dict[k]))}",
                    tissues_dict.keys(),
                )
            )
            message = (
                f"{len(tissues_dict.keys())} tissues were not found in the database:{nltab}{deets}\n"
                f"Please check the tissue against the existing tissues list:{nltab}{nltab.join(existing)}\nIf the "
                "tissue cannot be renamed to one of these existing tissues, a new tissue type will have to be added "
                "to the database."
            )
        super().__init__(message)
        self.tissues_dict = tissues_dict
        self.existing = existing


class LCMethodFixturesMissing(Exception):
    def __init__(self, message=None, err=None):
        if message is None:
            message = "The LCMethod fixtures defined in [DataRepo/fixtures/lc_methods.yaml] appear to have not been loaded."
        if err is not None:
            message += f"  The triggering exception was: [{err}]."
        super().__init__(message)
        self.err = err


def summarize_int_list(intlist):
    """
    This method was written to make long lists of row numbers more palatable to the user.
    Turns [1,2,3,5,6,9] into ['1-3','5-6','9']
    """
    sum_list = []
    last_num = None
    waiting_num = None
    for num in [int(n) for n in sorted(intlist)]:
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
