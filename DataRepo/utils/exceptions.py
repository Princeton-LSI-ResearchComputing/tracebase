class HeaderError(Exception):
    def __init__(self, message, headers):
        super().__init__(message)
        self.header_list = headers


class HeaderConfigError(Exception):
    pass


class RequiredValueError(Exception):
    pass


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
