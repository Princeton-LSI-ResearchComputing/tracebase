import time

from django.test import TestCase, TransactionTestCase

LONG_TEST_THRESH_SECS = 20
LONG_TEST_ALERT_STR = f" [ALERT > {LONG_TEST_THRESH_SECS}]"


def test_case_class_factory(base_class):
    """
    Class creation factory where the base class is an argument.  Note, it must receive a TestCase-compatible class.
    """

    class TracebaseTestCaseTemplate(base_class):
        """
        This wrapper of both TestCase and TransactionTestCase makes the necessary/desirable settings for all test
        classes and implements running time reporting.
        """

        maxDiff = None
        databases = "__all__"
        classStartTime = time.time()

        def setUp(self):
            """
            This method in the superclass is intended to record the start time so that the test run time can be
            reported in tearDown.
            """
            self.testStartTime = time.time()

        def tearDown(self):
            """
            This method in the superclass is intended to provide run time information for each test.
            """
            reportRunTime(self.id(), self.testStartTime)

        @classmethod
        def setUpTestData(self):
            """
            This method in the superclass is intended to provide run time information for the setUpTestData method.
            """
            super().setUpTestData()
            reportRunTime(f"{self.__name__}.setUpTestData", self.classStartTime)

        class Meta:
            abstract = True

    return TracebaseTestCaseTemplate


def reportRunTime(id, startTime):
    """
    Print the runtime of a test given the test ID and start time.
    """

    t = time.time() - startTime
    heads_up = ""  # String to include for tests that run too long

    if t > LONG_TEST_THRESH_SECS:
        # Add a string that can be easily searched in the terminal to find long running tests.
        heads_up = LONG_TEST_ALERT_STR

    print("TEST TIME%s: %s: %.3f" % (heads_up, id, t))


# Classes created by the factory with different base classes:
TracebaseTestCase = test_case_class_factory(TestCase)
TracebaseTransactionTestCase = test_case_class_factory(TransactionTestCase)
