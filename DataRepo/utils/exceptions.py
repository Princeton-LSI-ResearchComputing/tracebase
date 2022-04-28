class HeaderError(Exception):
    def __init__(self, message, headers):
        super().__init__(message)
        self.header_list = headers


class HeaderConfigError(Exception):
    pass


class RequiredValueError(Exception):
    pass


class ResearcherError(Exception):
    pass


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

    pass


class LoadingError(Exception):
    """
    Exception thrown if any errors encountered during loading
    """
