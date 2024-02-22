from collections import namedtuple

from DataRepo.management.commands.load_table import LoadFromTableCommand
from DataRepo.models import Tissue
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import AggregatedErrors
from DataRepo.utils.loader import TraceBaseLoader


# Class used for testing
class TestLoader(TraceBaseLoader):
    TableHeaders = namedtuple("TableHeaders", ["TEST"])
    DefaultHeaders = TableHeaders(TEST="Test")
    RequiredHeaders = TableHeaders(TEST=True)
    RequiredValues = RequiredHeaders
    ColumnTypes = {"TEST": str}
    DefaultValues = TableHeaders(TEST=5)
    UniqueColumnConstraints = [["TEST"]]
    FieldToHeaderKey = {"Tissue": {"name": "TEST"}}
    # TODO: Create a TestModel to use instead of Tissue (and change/rename TraceBaseLoader to not depend on TraceBase)
    Models = [Tissue]

    def load_data(self):
        # To ensure the wrapper returns the return of the wrapped function
        return 42


# Class used for testing
class TestCommand(LoadFromTableCommand):
    loader_class = TestLoader
    data_sheet_default = "test"

    def handle(self, *args, **options):
        return self.load_data()


class LoadFromTableCommandSuperclassUnitTests(TracebaseTestCase):
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
            data_sheet_default

        are enforced.  We will do so by creating a class that does not have any of the required class attributes defined
        and catching the expected exception.
        """

        class MyCommand(LoadFromTableCommand):
            pass

        with self.assertRaises(TypeError) as ar:
            # Apparently pylint thinks a derived class without its required concrete methods is an abstract class and
            # complains about instantiating an abstract class
            # pylint: disable=abstract-class-instantiated
            MyCommand()
            # pylint: enable=abstract-class-instantiated
        self.assertIn(
            "Can't instantiate abstract class MyCommand with abstract methods data_sheet_default, loader_class",
            str(ar.exception),
        )

    def test_apply_handle_wrapper(self):
        """
        This tests indirectly that apply_handle_wrapper works.  apply_handle_wrapper is called from the constructor and
        wraps the derived class's with a method called handle_wrapper.  When handle_wrapper executes, it adds an
        attribute named saved_aes, so to test that it was applied as a wrapper, we create an instance of MyCommand
        (which inherits from LoadFromTableCommand) and then we call handle().  We then ensure the saved_aes attribute
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

        class NotATraceBaseLoaderClass:
            pass

        class TestTypeCommand(LoadFromTableCommand):
            loader_class = NotATraceBaseLoaderClass
            data_sheet_default = 1

            def handle(self, *args, **options):
                self.load_data()

        with self.assertRaises(AggregatedErrors) as ar:
            TestTypeCommand()
        aes = ar.exception
        self.assertEqual((1, 0), (aes.num_errors, aes.num_warnings))
        self.assertEqual(TypeError, type(aes.exceptions[0]))
        self.assertIn("loader_class", str(aes.exceptions[0]))
        self.assertIn("data_sheet_default", str(aes.exceptions[0]))

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
        Tests the return of get_defaults is what was set in TestLoader.DefaultValues
        """
        tc = TestCommand()
        tc.handle(**self.TEST_OPTIONS)
        self.assertEqual(TestLoader.DefaultValues, tc.get_defaults())

    def test_get_headers(self):
        """
        Tests the return of get_defaults is what was set in TestLoader.DefaultHeaders
        """
        tc = TestCommand()
        tc.handle(**self.TEST_OPTIONS)
        self.assertEqual(TestLoader.DefaultHeaders, tc.get_headers())

    def test_get_dataframe(self):
        """
        Tests the return of get_defaults is what was set in TestLoader.DefaultHeaders
        """
        tc = TestCommand()
        tc.handle(**self.TEST_OPTIONS)
        expected = {"Test": {0: "1"}}
        self.assertEqual(expected, tc.get_dataframe().to_dict())
