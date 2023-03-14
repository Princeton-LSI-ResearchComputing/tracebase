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
    def __init__(self, samples):
        message = (
            f"{len(samples)} samples are missing in the database: [{', '.join(samples)}].  Samples must be pre-"
            "loaded."
        )
        super().__init__(message)
        self.sample_list = samples


class NoSamplesError(Exception):
    def __init__(self):
        message = "No samples were supplied."
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
    def __init__(self, errors, message=None, verbosity=0):
        if not message:
            errtypes = []
            for errtype in [type(e).__name__ for e in errors]:
                if errtype not in errtypes:
                    errtypes.append(errtype)
            message = f"{len(errors)} exceptions occurred, including type(s): [{', '.join(errtypes)}]."
        super().__init__(message)
        if verbosity > 0:
            print("Aggregated error details:")
            for i, error in enumerate(errors, start=1):
                print(f"\tERROR{i}: {type(error).__name__}: {error}")
        self.errors = errors
        self.verbosity = verbosity
        self.warnings = []  # Populated by cull_warnings()

    def cull_warnings(self, validate):
        """
        This method divides the accumulated exceptions into fatal errors and warnings, based on whether we're in
        validate mode (which is inferred to connote that this is the validation web interface being run by a user).  A
        user cannot supply flags like "--new-researcher", so they should see a warning instead of an error, because
        they cannot prevent the error from happening (but a curator can).

        This assumes that the exception objects contain attributes "load_warning" and/or "validate_warning" IF the
        exception should be treated as a warning.

        Returns whether or not the AggregatedErrors exception should be raised.  Exceptions when load_warning is true
        and loading is happening should only print the warning and not raise an exception.  Exceptions when
        validate_warning is true and in validate mode *SHOULD* raise an exception so that the AggregatedErrors
        exception can be caught and the warnings and errors should all be presented to the user.
        """
        warnings = []
        errors = []
        for exception in self.errors:
            if (
                hasattr(exception, "load_warning")
                and exception.load_warning
                and not validate
            ):
                warnings.append(exception)
            elif (
                hasattr(exception, "validate_warning")
                and exception.validate_warning
                and validate
            ):
                warnings.append(exception)
            else:
                errors.append(exception)
        if self.verbosity and len(warnings) > 0:
            for i, warning in enumerate(warnings):
                print(f"WARNING{i}: {type(warning).__name__}: {str(warning)}")
        self.errors = errors
        self.warnings = warnings
        return len(self.errors) > 0 or (validate and len(self.warnings) > 0)


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
            f"{'; '.join(list(map(lambda c: c + ' on rows: ' + dupe_dict[c], dupe_dict.keys())))}]"
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

        # The following are used by the loading code to decide if this exception should be fatal or treated as a
        # warning, depending on the mode in which the loader is run.

        # This exception should be treated as a warning when validate is false.
        self.load_warning = True
        # This exception should be treated as a warning when validate is true.
        self.validate_warning = True
        # These 2 values can differ based on whether this is something the user can fix or not.  For example, the
        # validation interface does not enable the user to verify that the researcher is indeed a new researcher, so
        # they cannot quiet an unknown researcher exception.  A curator can, so when the curator goes to load, it
        # should be treated as an exception (curator_warning=False).
