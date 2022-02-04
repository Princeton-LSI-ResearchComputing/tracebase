from django.test import TestCase


class TracebaseTestCase(TestCase):
    """
    This wrapper of TestCase makes the necessary/desirable settings for all test classes.
    """

    maxDiff = None
    databases = "__all__"

    class Meta:
        abstract = True
