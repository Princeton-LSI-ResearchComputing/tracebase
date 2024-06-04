from django.conf import settings
from django.test.runner import DiscoverRunner


class TraceBaseTestSuiteRunner(DiscoverRunner):
    """Local test suite runner."""

    def setup_test_environment(self, *args, **kwargs):
        """Switch to in-memory storage."""
        super().setup_test_environment(*args, **kwargs)
        settings.STORAGES = settings.TEST_STORAGES

    def teardown_test_environment(self, *args, **kwargs):
        """Switch back to the original file storage."""
        super().teardown_test_environment(*args, **kwargs)
        settings.STORAGES = settings.DEFAULT_STORAGES
