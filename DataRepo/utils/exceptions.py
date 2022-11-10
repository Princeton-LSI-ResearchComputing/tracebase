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


class UnknownResearcherError(Exception):
    def __init__(self, unknown, new, known, source, addendum):
        nl = "\n"  # Put \n in a var to join in an f string
        message = (
            f"{len(unknown)} researchers from {source}: [{','.join(sorted(unknown))}] out of {len(new)} researchers "
            f"do not exist in the database.  Please ensure they are not variants of existing researchers:{nl}"
            f"{nl.join(sorted(known))}{nl}{addendum}"
        )
        super().__init__(message)
        self.unknown = unknown
        self.new = new
        self.known = known
        self.source = source
        self.addendum = addendum


class MissingSamplesError(Exception):
    def __init__(self, message, samples):
        super().__init__(message)
        self.sample_list = samples


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
    def __init__(self, errors, message=None):
        if not message:
            message = f"{len(errors)} errors occurred."
        super().__init__(message)
        self.errors = errors


class ConflictingValueError(Exception):
    def __init__(
        self,
        model_name,
        unique_field,
        unique_value,
        consistent_field,
        existing_value,
        differing_value,
        message=None,
    ):
        if not message:
            message = (
                f"Conflicting value encountered for {model_name} record.  The record for {unique_field}="
                f"'{unique_value}' should have the same {consistent_field} value, but the original value: "
                f"[{existing_value}] differs from the new value: [{differing_value}]."
            )
        super().__init__(message)
        self.model_name = model_name
        self.unique_field = unique_field
        self.unique_value = unique_value
        self.consistent_field = consistent_field
        self.existing_value = existing_value
        self.differing_value = differing_value


class SaveError(Exception):
    def __init__(self, model_name, rec_name, db, e):
        message = f"Error saving {model_name} {rec_name} to database {db}: {type(e).__name__}: {str(e)}"
        super().__init__(message)
