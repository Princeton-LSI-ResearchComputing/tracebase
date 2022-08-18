from django.test import TestCase, TransactionTestCase


class TracebaseTestCase(TestCase):
    """
    This wrapper of TestCase makes the necessary/desirable settings for all test classes.
    """

    maxDiff = None
    databases = "__all__"

    class Meta:
        abstract = True


class TracebaseTransactionTestCase(TransactionTestCase):
    """
    This wrapper of TestCase makes the necessary/desirable settings for all test classes.
    """

    maxDiff = None
    databases = "__all__"

    class Meta:
        abstract = True
