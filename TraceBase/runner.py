from django.conf import settings
from django.test.runner import DiscoverRunner


class TraceBaseTestSuiteRunner(DiscoverRunner):
    """Local test suite runner."""

    def setup_test_environment(self, *args, **kwargs):
        """Switch to in-memory storage."""
        super().setup_test_environment(*args, **kwargs)
        settings._original_file_storage = settings.STORAGES["default"]
        settings.STORAGES["default"] = settings.STORAGES["testing"]

    def teardown_test_environment(self, *args, **kwargs):
        """Switch back to the original file storage."""
        super().teardown_test_environment(*args, **kwargs)
        settings.STORAGES["default"] = settings._original_file_storage
        del settings._original_file_storage
