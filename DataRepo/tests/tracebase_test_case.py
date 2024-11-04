from __future__ import annotations

import os
import shutil
import time
from typing import Type

from django.test import TestCase, TransactionTestCase, override_settings

from DataRepo.models.utilities import get_all_models
from TraceBase import settings

LONG_TEST_THRESH_SECS = 20
LONG_TEST_ALERT_STR = f" [ALERT > {LONG_TEST_THRESH_SECS}]"


# See: https://stackoverflow.com/a/61345284/2057516
if "unittest.util" in __import__("sys").modules:
    # Show full diff in self.assertEqual.
    __import__("sys").modules["unittest.util"]._MAX_LENGTH = 999999999


def test_case_class_factory(base_class) -> Type[TestCase]:
    """
    Class creation factory where the base class is an argument.  Note, it must receive a TestCase-compatible class.
    """

    class TracebaseTestCaseTemplate(base_class):
        """
        This wrapper of both TestCase and TransactionTestCase makes the necessary/desirable settings for all test
        classes and implements running time reporting.
        """

        maxDiff = None

        def setUp(self):
            """
            This method in the superclass is intended to record the start time so that the test run time can be
            reported in tearDown.
            """
            self.testStartTime = time.time()
            super().setUp()
            print(
                "STARTING TEST: %s at %s"
                % (self.id(), time.strftime("%m/%d/%Y, %H:%M:%S"))
            )

        def tearDown(self):
            """
            This method in the superclass is intended to provide run time information for each test.
            """
            super().tearDown()
            reportRunTime(self.id(), self.testStartTime)

        @classmethod
        def setUpClass(cls):
            """
            This method in the superclass is intended to record the setUpTestData start time so that the setup run time
            can be reported in setUpTestData.
            """
            cls.setupStartTime = time.time()
            print(
                "SETTING UP TEST CLASS: %s.%s at %s"
                % (cls.__module__, cls.__name__, time.strftime("%m/%d/%Y, %H:%M:%S"))
            )
            super().setUpClass()

        @classmethod
        def setUpTestData(cls):
            """
            This method in the superclass is intended to provide run time information for the setUpTestData method.
            """
            super().setUpTestData()
            try:
                reportRunTime(
                    f"{cls.__module__}.{cls.__name__}.setUpTestData", cls.setupStartTime
                )
            except AttributeError:
                # This is an attribute error about cls not having an attribute named "setupStartTime"
                # If this method is called from outside the class (for code-re-use), we don't need to track its time
                pass

        @classmethod
        def get_record_counts(cls):
            """
            This can be used in any tests to check the number of records in every table.
            """
            record_counts = []
            for mdl in get_all_models():
                record_counts.append(mdl.objects.all().count())
            return record_counts

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
TracebaseTestCase: TestCase = test_case_class_factory(TestCase)
TracebaseTransactionTestCase: TestCase = test_case_class_factory(TransactionTestCase)


@override_settings(
    CACHES=settings.TEST_CACHES,
    STORAGES=settings.TEST_FILE_STORAGES,
    MEDIA_ROOT=settings.TEST_MEDIA_ROOT,
)
class TracebaseArchiveTestCase(TracebaseTransactionTestCase):
    ARCHIVE_DIR = settings.TEST_MEDIA_ROOT

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        shutil.rmtree(cls.ARCHIVE_DIR, ignore_errors=True)

    def setUp(self):
        super().setUp()
        shutil.rmtree(self.ARCHIVE_DIR, ignore_errors=True)
        os.mkdir(self.ARCHIVE_DIR)

    def tearDown(self):
        shutil.rmtree(self.ARCHIVE_DIR, ignore_errors=True)
        super().tearDown()
