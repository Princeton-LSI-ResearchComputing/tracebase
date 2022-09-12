import time

from django.test import TestCase, TransactionTestCase

LONG_TEST_THRESH_SECS = 20
LONG_TEST_ALERT_STR = f" [ALERT > {LONG_TEST_THRESH_SECS}]"


class TracebaseTestCase(TestCase):
    """
    This wrapper of TestCase makes the necessary/desirable settings for all test classes.
    """

    maxDiff = None
    databases = "__all__"
    classStartTime = time.time()

    def setUp(self):
        """
        This method in the superclass is intended to record the start time so that the test run time can be reported in
        tearDown.
        """
        self.testStartTime = time.time()

    def tearDown(self):
        """
        This method in the superclass is intended to provide run time information for each test.
        """
        self.reportRunTime(self.id(), self.testStartTime)

    def setUpTestData(self):
        """
        This method in the superclass is intended to provide run time information for the setUpTestData method.
        """
        self.reportRunTime(f"{self.__class__.__name__}.setUpTestData", self.classStartTime)

    def reportRunTime(self, id, startTime):
        """
        Print the runtime of a test given the test ID and start time.
        """

        # TODO: When issue #480 is implemented, a test should be added here to ensure each test runs in under some
        #       reasonable threshold.  There might even be a way to ensure the data setup runs quickly as well.

        t = time.time() - startTime
        heads_up = ""  # String to include for tests that run too long

        if t > LONG_TEST_THRESH_SECS:
            # Add a string that can be easily searched in the terminal to find long running tests.
            heads_up = LONG_TEST_ALERT_STR

        print("TEST TIME%s: %s: %.3f" % (heads_up, id, t))

    class Meta:
        abstract = True


class TracebaseTestCase(TransactionTestCase):
    """
    This wrapper of TestCase makes the necessary/desirable settings for all test classes.
    """

    maxDiff = None
    databases = "__all__"
    classStartTime = time.time()

    def setUp(self):
        """
        This method in the superclass is intended to record the start time so that the test run time can be reported in
        tearDown.
        """
        self.testStartTime = time.time()

    def tearDown(self):
        """
        This method in the superclass is intended to provide run time information for each test.
        """
        self.reportRunTime(self.id(), self.testStartTime)

    def setUpTestData(self):
        """
        This method in the superclass is intended to provide run time information for the setUpTestData method.
        """
        self.reportRunTime(f"{self.__class__.__name__}.setUpTestData", self.classStartTime)

    def reportRunTime(self, id, startTime):
        """
        Print the runtime of a test given the test ID and start time.
        """

        # TODO: When issue #480 is implemented, a test should be added here to ensure each test runs in under some
        #       reasonable threshold.  There might even be a way to ensure the data setup runs quickly as well.

        t = time.time() - startTime
        heads_up = ""  # String to include for tests that run too long

        if t > LONG_TEST_THRESH_SECS:
            # Add a string that can be easily searched in the terminal to find long running tests.
            heads_up = LONG_TEST_ALERT_STR

        print("TEST TIME%s: %s: %.3f" % (heads_up, id, t))

    class Meta:
        abstract = True
