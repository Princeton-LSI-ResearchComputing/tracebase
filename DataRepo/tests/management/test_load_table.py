import io
import sys
from collections import namedtuple

from django.db.models import AutoField, CharField, Model
from django.test.utils import isolate_apps

from DataRepo.loaders.table_loader import TableLoader
from DataRepo.management.commands.load_table import LoadTableCommand
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import AggregatedErrors, OptionsNotAvailable


# Class (Model) used for testing
class LoadTableTestModel(Model):
    id = AutoField(primary_key=True)
    name = CharField(unique=True)
    choice = CharField(choices=[("1", "1"), ("2", "2")])

    # Necessary for temporary models
    class Meta:
        app_label = "loader"


# Class used for testing
class TestLoader(TableLoader):
    DataSheetName = "Test"
    DataTableHeaders = namedtuple("DataTableHeaders", ["TEST"])
    DataHeaders = DataTableHeaders(TEST="Test")
    DataRequiredHeaders = DataTableHeaders(TEST=True)
    DataRequiredValues = DataRequiredHeaders
    DataColumnTypes = {"TEST": str}
    DataDefaultValues = DataTableHeaders(TEST="five")
    DataUniqueColumnConstraints = [["TEST"]]
    FieldToDataHeaderKey = {"LoadTableTestModel": {"name": "TEST"}}
    Models = [LoadTableTestModel]

    def load_data(self):
        # To ensure the wrapper returns the return of the wrapped function
        return 42


# Class used for testing
class TestCommand(LoadTableCommand):
    loader_class = TestLoader

    def handle(self, *args, **options):
        return self.load_data()


@isolate_apps("DataRepo.tests.apps.loader")
class LoadTableCommandTests(TracebaseTestCase):
    TEST_OPTIONS = {
        "infile": "DataRepo/data/tests/load_table/test.tsv",
        "headers": None,
        "defer_rollback": False,
        "dry_run": False,
        "data_sheet": "test sheet",
        "verbosity": 0,
        "defaults_file": None,
    }

    def test_abstract_attributes_enforced(self):
        """
        This tests that all required class attributes:

            loader_class

        are enforced.  We will do so by creating a class that does not have any of the required class attributes defined
        and catching the expected exception.
        """

        class MyCommand(LoadTableCommand):
            pass

        with self.assertRaises(TypeError) as ar:
            # Apparently pylint thinks a derived class without its required concrete methods is an abstract class and
            # complains about instantiating an abstract class
            # pylint: disable=abstract-class-instantiated
            MyCommand()
            # pylint: enable=abstract-class-instantiated
        self.assertIn(
            "Can't instantiate abstract class MyCommand with abstract method loader_class",
            str(ar.exception),
        )

    def test_apply_handle_wrapper(self):
        """
        This tests indirectly that apply_handle_wrapper works.  apply_handle_wrapper is called from the constructor and
        wraps the derived class's with a method called handle_wrapper.  When handle_wrapper executes, it adds an
        attribute named saved_aes, so to test that it was applied as a wrapper, we create an instance of MyCommand
        (which inherits from LoadTableCommand) and then we call handle().  We then ensure the saved_aes attribute
        was added, from which we infer that the handle_wrapper was applied.
        """
        mc = TestCommand()
        self.assertFalse(hasattr(mc, "saved_aes"))
        mc.handle(**self.TEST_OPTIONS)
        self.assertTrue(hasattr(mc, "saved_aes"))

    def test_check_class_attributes_pass(self):
        """
        Check that the types of the attributes are correctly checked.
        """
        tc = TestCommand()
        # No exception raised from the following means pass
        tc.check_class_attributes()

    def test_check_class_attributes_fail(self):
        """
        Check that the types of the attributes are correctly checked.
        """

        class NotATableLoaderClass:
            pass

        class TestTypeCommand(LoadTableCommand):
            loader_class = NotATableLoaderClass

            def handle(self, *args, **options):
                self.load_data()

        with self.assertRaises(AggregatedErrors) as ar:
            TestTypeCommand()
        aes = ar.exception
        self.assertEqual((1, 0), (aes.num_errors, aes.num_warnings))
        self.assertEqual(TypeError, type(aes.exceptions[0]))
        self.assertIn("loader_class", str(aes.exceptions[0]))

    def test_load_data(self):
        """
        Test that load_data does what it's supposed to (call loader_class.load_data)
        """
        tc = TestCommand()
        # handle sets the options needed to call load_data (handle also calls load_data, but we'll ignore that for now,
        # in deference to a "more" direct test call) and set the options manually
        tc.options = self.TEST_OPTIONS
        # TestCommand's load_data returns the return of TestLoader's load_data method, which above returns 42
        tv = tc.load_data()
        # Ensure that TestLoader's load_data method is called (indirectly, by testing its returned value)
        self.assertEqual(42, tv)

    def test_get_infile(self):
        """
        Tests the return of get_infile is what was set in the options by the handle method
        """
        tc = TestCommand()
        tc.handle(**self.TEST_OPTIONS)
        self.assertEqual("DataRepo/data/tests/load_table/test.tsv", tc.get_infile())

    def test_get_defaults(self):
        """
        Tests the return of get_defaults is what was set in TestLoader.DataDefaultValues
        """
        tc = TestCommand()
        tc.handle(**self.TEST_OPTIONS)
        self.assertEqual(TestLoader.DataDefaultValues, tc.get_defaults())

    def test_get_headers(self):
        """
        Tests the return of get_defaults is what was set in TestLoader.DataHeaders
        """
        tc = TestCommand()
        tc.handle(**self.TEST_OPTIONS)
        self.assertEqual(TestLoader.DataHeaders, tc.get_headers())

    def test_get_dataframe(self):
        """
        Tests the return of get_defaults is what was set in TestLoader.DataHeaders
        """
        tc = TestCommand()
        tc.handle(**self.TEST_OPTIONS)
        expected = {"Test": {0: "1"}}
        self.assertEqual(expected, tc.get_dataframe().to_dict())

    def test_init_loader(self):
        tc = TestCommand()
        # Assert that the initially set headers and defaults from the initial loader instance are as expected
        self.assertEqual(tc.loader.headers.TEST, "Test")
        self.assertEqual(tc.loader.defaults.TEST, "five")
        # Change the default headers and defaults in the initial version of the loader self.instance that is created by
        # __init__ for this purpose
        tc.set_headers({"TEST": "Test2"})
        tc.set_defaults({"TEST": "one"})
        tc.init_loader(
            df=None,
            dry_run=True,  # Diff from default False
            defer_rollback=True,  # Diff from default False
            file="DataRepo/data/tests/load_table/test.xlsx",  # Diff from default None
            data_sheet="Test",  # Diff from default None
            defaults_df=None,
            defaults_sheet=None,
            defaults_file=None,
            user_headers=None,
        )
        self.assertTrue(hasattr(tc, "loader"))

        # Assert that the loader object has its basic instance attributes
        self.assertTrue(hasattr(tc.loader, "df"))
        self.assertTrue(hasattr(tc.loader, "dry_run"))
        self.assertTrue(hasattr(tc.loader, "defer_rollback"))
        self.assertTrue(hasattr(tc.loader, "file"))
        self.assertTrue(hasattr(tc.loader, "sheet"))
        self.assertTrue(hasattr(tc.loader, "defaults_df"))
        self.assertTrue(hasattr(tc.loader, "defaults_sheet"))
        self.assertTrue(hasattr(tc.loader, "defaults_file"))
        self.assertTrue(hasattr(tc.loader, "user_headers"))

        # Assert that the loader has the custom values we set
        self.assertTrue(tc.loader.dry_run)
        self.assertTrue(tc.loader.defer_rollback)
        self.assertEqual("DataRepo/data/tests/load_table/test.xlsx", tc.loader.file)
        self.assertEqual("Test", tc.loader.sheet)

        # Assert that the set headers and defaults from the initial loader instance carried over
        self.assertEqual("Test2", tc.loader.headers.TEST)
        self.assertEqual("one", tc.loader.defaults.TEST)

    def test_report_status(self):
        # Capture STDOUT
        capture_stdout = io.StringIO()
        sys.stdout = capture_stdout

        try:
            tc = TestCommand()

            opts = {
                "infile": "DataRepo/data/tests/load_table/test.tsv",
                "defaults_sheet": None,
                "data_sheet": "Test",
                "defaults_file": None,
                "headers": None,
                "dry_run": False,
                "defer_rollback": False,
                "verbosity": 1,
            }

            tc.init_loader()

            # Required attributes normally set when handle() is called (which we're skipping)
            tc.saved_aes = None
            tc.options = opts

            # Initialize the stats
            tc.loader.created()
            tc.loader.existed(num=2)
            tc.loader.skipped(num=3)
            tc.loader.errored(num=4)

            # Report the stats result to the console
            tc.report_status()
        except Exception as e:
            sys.stdout = sys.__stdout__
            raise e
        finally:
            # Reset STDOUT
            sys.stdout = sys.__stdout__

        # Now test the output is correct
        self.assertEqual(
            "Done.\nLoadTableTestModel records created: [1], existed: [2], skipped [3], and errored: [4].\n",
            capture_stdout.getvalue(),
        )

    def test_report_status_dryrun(self):
        # Capture STDOUT
        capture_stdout = io.StringIO()
        sys.stdout = capture_stdout

        try:
            tc = TestCommand()

            opts = {
                "infile": "DataRepo/data/tests/load_table/test.tsv",
                "defaults_sheet": None,
                "data_sheet": "Test",
                "defaults_file": None,
                "headers": None,
                "dry_run": True,
                "defer_rollback": False,
                "verbosity": 1,
            }

            tc.init_loader()

            # Required attributes normally set when handle() is called (which we're skipping)
            tc.saved_aes = None
            tc.options = opts

            # Initialize the stats
            tc.loader.created()
            tc.loader.existed(None, 2)
            tc.loader.skipped(None, 3)
            tc.loader.errored(None, 4)

            # Report the stats result to the console
            tc.report_status()
        except Exception as e:
            sys.stdout = sys.__stdout__
            raise e
        finally:
            # Reset STDOUT
            sys.stdout = sys.__stdout__

        # Now test the output is correct
        self.assertEqual(
            (
                "Dry-run complete.  The following would occur during a real load:\n"
                "LoadTableTestModel records created: [1], existed: [2], skipped [3], and errored: [4].\n"
            ),
            capture_stdout.getvalue(),
        )

    def test_get_defaults_sheet(self):
        tc = TestCommand()

        # Options not available
        with self.assertRaises(OptionsNotAvailable):
            tc.get_defaults_sheet()

        tc.set_headers({"TEST": "Test2"})  # The file's header is "Test2"

        # Defined valid sheet
        opts = {
            "infile": "DataRepo/data/tests/load_table/test.xlsx",
            "defaults_sheet": "MyDefaults",
            "data_sheet": "Test",  # This is normally defaulted by argparse, but not here, manually
            "defaults_file": None,
            "headers": None,
            "dry_run": False,
            "defer_rollback": False,
            "verbosity": 0,
        }
        tc.handle(**opts)
        self.assertEqual("MyDefaults", tc.get_defaults_sheet())

        # When not excel
        tc = TestCommand()
        opts = {
            "infile": "DataRepo/data/tests/load_table/test.tsv",
            "defaults_sheet": "MyDefaults",
            "data_sheet": "Test",  # This is normally defaulted by argparse, but not here, manuallyNone,
            "defaults_file": None,
            "headers": None,
            "dry_run": False,
            "defer_rollback": False,
            "verbosity": 0,
        }
        tc.handle(**opts)
        self.assertIsNone(tc.get_defaults_sheet())

    def test_get_user_headers(self):
        tc = TestCommand()
        opts = {
            "infile": "DataRepo/data/tests/load_table/test.xlsx",
            "defaults_sheet": None,
            "data_sheet": "Test",  # This is normally defaulted by argparse, but not here, manually
            "defaults_file": None,
            "headers": "DataRepo/data/tests/load_table/test_headers.yaml",
            "dry_run": False,
            "defer_rollback": False,
            "verbosity": 0,
        }
        tc.handle(**opts)
        self.assertEqual({"TEST": "Test2"}, tc.get_user_headers())

    def test_set_headers(self):
        tc = TestCommand()
        tc.set_headers({"TEST": "Test2"})
        expected = TestLoader.DataTableHeaders(TEST="Test2")
        self.assertEqual(expected, tc.loader.headers)

    def test_set_defaults(self):
        tc = TestCommand()
        tc.set_defaults({"TEST": "six"})
        expected = TestLoader.DataTableHeaders(TEST="six")
        self.assertEqual(expected, tc.loader.defaults)

    def test_get_user_defaults_excel(self):
        tc = TestCommand()
        tc.set_headers({"TEST": "Test2"})  # The file's header is "Test2"
        opts = {
            "infile": "DataRepo/data/tests/load_table/test.xlsx",
            "defaults_sheet": "MyDefaults",
            "data_sheet": "Test",  # This is normally defaulted by argparse, but not here, manually
            "defaults_file": None,
            "headers": None,
            "dry_run": False,
            "defer_rollback": False,
            "verbosity": 0,
        }
        tc.handle(**opts)
        ud_df_as_dict = tc.get_user_defaults().to_dict("records")
        expected = [
            {
                "Sheet Name": "Test",
                "Column Header": "Test2",
                "Default Value": "three",
            },
        ]
        self.assertEqual(expected, ud_df_as_dict)

    def test_get_user_defaults_tsv(self):
        tc = TestCommand()
        opts = {
            "infile": "DataRepo/data/tests/load_table/test.tsv",
            "defaults_sheet": None,
            "data_sheet": "Test",  # This is normally defaulted by argparse, but not here, manually
            "defaults_file": "DataRepo/data/tests/load_table/defaults.tsv",
            "headers": None,
            "dry_run": False,
            "defer_rollback": False,
            "verbosity": 0,
        }
        tc.handle(**opts)
        ud_df_as_dict = tc.get_user_defaults().to_dict("records")
        expected = [
            {
                "Sheet Name": "Test",
                "Column Header": "Test",
                "Default Value": "three",
            },
        ]
        self.assertEqual(expected, ud_df_as_dict)
