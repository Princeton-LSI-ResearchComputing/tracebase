from django.conf import settings
from django.test import TestCase


class TracebaseTestCase(TestCase):
    """
    This wrapper of TestCase makes the necessary/desirable settings for all test classes.
    """

    maxDiff = None
    databases = ["default", settings.VALIDATION_DB]

    class Meta:
        abstract = True
