from django.conf import settings
from django.test.runner import DiscoverRunner


class TempMediaMixin(object):
    """
    Mixin to use InMemoryStorage for ArchiveFile tests so that files stored in MEDIA_ROOT will not hit disk.
    Based originally on this article from 2013, but updated to use InMemoryStorage, which is new as of Django 4.2:
    https://www.caktusgroup.com/blog/2013/06/26/media-root-and-django-tests/
    """

    def setup_test_environment(self):
        """Create temp directory and update MEDIA_ROOT and default storage."""
        super(TempMediaMixin, self).setup_test_environment()
        settings._original_file_storage = settings.DEFAULT_FILE_STORAGE
        settings.DEFAULT_FILE_STORAGE = "django.core.files.storage.InMemoryStorage"

    def teardown_test_environment(self):
        """Delete temp storage."""
        super(TempMediaMixin, self).teardown_test_environment()
        settings.DEFAULT_FILE_STORAGE = settings._original_file_storage
        del settings._original_file_storage


class TraceBaseTestSuiteRunner(TempMediaMixin, DiscoverRunner):
    """Local test suite runner."""
