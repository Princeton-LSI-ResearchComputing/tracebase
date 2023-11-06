import time

from django.conf import settings
from django.test import TestCase, TransactionTestCase

from DataRepo.models.utilities import get_all_models

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
            cls.set_postgres_autovac()
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
            reportRunTime(
                f"{cls.__module__}.{cls.__name__}.setUpTestData", cls.setupStartTime
            )

        @classmethod
        def get_record_counts(cls):
            """
            This can be used in any tests to check the number of records in every table.
            """
            record_counts = []
            for mdl in get_all_models():
                record_counts.append(mdl.objects.all().count())
            return record_counts

        @classmethod
        def set_postgres_autovac(cls):
            """
            The update to postgres 13 caused a doubling in time to run the test suite.  The following changes to the
            autovacuum settings brings the time time down to on-par with before the update.  See:

            https://forum.djangoproject.com/t/test-time-doubled-after-django-3-2-4-2-and-postgres-10-13-update/24843
            https://www.postgresql.org/message-id/18177-a282c2eaaf791f21%40postgresql.org
            """
            # The conditional keeps our code base somewhat database-independent
            if "postgresql" in settings.DATABASES["default"]["ENGINE"]:
                import psycopg2

                print(
                    f"Connecting to postgres DB ({settings.DATABASES['default']['NAME']}) to set autovacuum"
                )
                vacuumStartTime = time.time()
                conn = psycopg2.connect(
                    database=settings.DATABASES["default"]["NAME"],
                    host=settings.DATABASES["default"]["HOST"],
                    port=settings.DATABASES["default"]["PORT"],
                    user=settings.DATABASES["default"]["USER"],
                    password=settings.DATABASES["default"]["PASSWORD"],
                )
                conn.autocommit = True
                with conn.cursor() as cursor:
                    # cursor.execute("VACUUM FULL")
                    cursor.execute(
                        'ALTER TABLE "DataRepo_peakdata" SET (autovacuum_vacuum_scale_factor = 0.0);'
                    )
                    cursor.execute(
                        'ALTER TABLE "DataRepo_peakdata" SET (autovacuum_vacuum_threshold = 5000);'
                    )
                    cursor.execute(
                        'ALTER TABLE "DataRepo_peakdata" SET (autovacuum_analyze_scale_factor = 0.0);'
                    )
                    cursor.execute(
                        'ALTER TABLE "DataRepo_peakdata" SET (autovacuum_analyze_threshold = 5000);'
                    )
                conn.close()
                print("vacuum time: %.3f" % (time.time() - vacuumStartTime))

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
