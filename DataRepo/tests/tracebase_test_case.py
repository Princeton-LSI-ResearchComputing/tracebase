from __future__ import annotations

import os
import shutil
import time
from collections import defaultdict
from typing import Dict, Type, TypeVar

from django.apps import apps
from django.conf import settings
from django.db import ProgrammingError
from django.db.models import AutoField, Field, Model
from django.test import TestCase, TransactionTestCase, override_settings

from DataRepo.models.utilities import get_all_models
from DataRepo.utils.exceptions import trace

try:
    import importlib
    import inspect
    import pathlib
    import pkgutil

    IMPORT_ERROR = None
except ImportError:
    IMPORT_ERROR = ImportError(
        "pkgutil, importlib, and inspect modules are all required to generate tests"
    )

TEST_GENERATION_IGNORE_MODULES = [
    "DataRepo.apps",
    "DataRepo.context_processors",
    "DataRepo.multiforms",
    "DataRepo.urls",
    "DataRepo.tests",
    "DataRepo.migrations",
    "DataRepo.formats.search_group",  # covered by dataformat_group
]
TEST_MODULE_REPLACEMENT = ("DataRepo", "DataRepo.tests")
LONG_TEST_THRESH_SECS = 20
LONG_TEST_ALERT_STR = f" [ALERT > {LONG_TEST_THRESH_SECS}]"

T = TypeVar("T", TestCase, TransactionTestCase)

# See: https://stackoverflow.com/a/61345284/2057516
if "unittest.util" in __import__("sys").modules:
    # Show full diff in self.assertEqual.
    __import__("sys").modules["unittest.util"]._MAX_LENGTH = 999999999


def test_case_class_factory(base_class: Type[T]) -> Type[T]:
    """
    Class creation factory where the base class is an argument.  Note, it must receive a TestCase-compatible class.
    """

    # The default django test client will cause a 301 error for every get/post when SECURE_SSL_REDIRECT=True
    # https://stackoverflow.com/questions/49626899/
    @override_settings(SECURE_SSL_REDIRECT=False)
    class TracebaseTestCaseTemplate(base_class):  # type: ignore
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

        def assertDictEquivalent(
            self, d1: dict, d2: dict, max_depth=10, _path=None, **kwargs
        ):
            """Checks whether dicts are equal when their values can be objects that technically differ, but are
            essentially the same when you evaluate them using their __dict__ attribute (and check their types).
            """
            _path = "" if _path is None else _path
            if len(_path.split(",")) >= max_depth:
                return
            ignores = [
                "creation_counter",
                "identity",
                "_django_version",
                "fields_cache",
                "_lookup_joins",
                "used_aliases",
            ]
            self.assertEqual(
                set([k for k in d1.keys() if k not in ignores]),
                set([k for k in d2.keys() if k not in ignores]),
                msg=(
                    f"Object path: {_path} difference: keys differ\n"
                    f"In dict1 only: {list(set(d1.keys()) - set(d2.keys()))}\n"
                    f"In dict2 only: {list(set(d2.keys()) - set(d1.keys()))}"
                ),
            )
            for key, v1 in d1.items():
                if key in ignores:
                    continue
                self.assertEquivalent(
                    v1, d2[key], _path=_path + key + ",", max_depth=max_depth, **kwargs
                )

        def assertEquivalent(
            self, o1: object, o2: object, max_depth=10, _path=None, **kwargs
        ):
            """Checks whether values are equal.  If the values are objects, it essentially checks that their types and
            __dict__ attributes are equal."""
            _path = "" if _path is None else _path
            if len(_path.split(",")) >= max_depth:
                return
            primitives = (bool, str, int, float, type(None))
            if isinstance(o1, primitives):
                self.assertEqual(o1, o2, **kwargs, msg=f"Object path: {_path}")
            elif type(o1).__name__ == "function":
                self.assertEqual(o1, o2, **kwargs, msg=f"Object path: {_path}")
            else:
                self.assertIsInstance(
                    o2,
                    type(o1),
                    **kwargs,
                    msg=f"Type: '{type(o2).__name__}'. Object path: {_path}",
                )
                if isinstance(o1, (list, tuple)) and isinstance(o2, (list, tuple)):
                    self.assertEqual(
                        len(o1), len(o2), **kwargs, msg=f"Object path: {_path}"
                    )
                    for i in range(len(o1)):
                        self.assertEquivalent(
                            o1[i],
                            o2[i],
                            _path=_path + f"{i},",
                            max_depth=max_depth,
                            **kwargs,
                        )
                elif (
                    isinstance(o1, dict)
                    and all(isinstance(k, str) for k in o1.keys())
                    and isinstance(o2, dict)
                    and all(isinstance(k, str) for k in o2.keys())
                ):
                    self.assertDictEquivalent(
                        o1, o2, _path=_path + "dict,", max_depth=max_depth, **kwargs
                    )
                elif hasattr(o1, "__dict__"):
                    self.assertDictEquivalent(
                        o1.__dict__,
                        o2.__dict__,
                        _path=_path + "__dict__,",
                        max_depth=max_depth,
                        **kwargs,
                    )
                else:
                    try:
                        self.assertEqual(o1, o2, **kwargs)
                    except AssertionError as ae:
                        if _path is None:
                            raise ae
                        raise AssertionError(
                            f"Object path: {_path} difference: {ae}"
                        ).with_traceback(ae.__traceback__)

        @staticmethod
        def assertNotWarns(unexpected_warning=Warning):
            """This is a decorator.  Apply it to tests that should not raise a warning.

            Usage:
                @MyTestCase.assertNotWarns()
                def test_no_warnings(self):
                    do_something_that_should_not_warn()

                @MyTestCase.assertNotWarns(SpecificWarning)
                def test_no_SpecificWarning(self):
                    do_something_that_does_not_raise_specific_warning()
            """

            def decorator(fn):
                # This is to be able to be able to include the lines in the test that caused the assertion error about
                # an unexpected warning
                traceback = trace().split("\n")
                tb = ""
                include = False
                for tbl in traceback:
                    if include:
                        if "tracebase_test_case.py" in tbl:
                            # Stop including this portion of the trace when it gets back to here
                            include = False
                            continue
                        tb += "\n" + tbl
                        continue
                    if "File " in tbl and "/test_" in tbl:
                        # Start including the portion of the trace when it gets into a test file
                        tb += "\n" + tbl
                        include = True

                def wrapper(testcase_obj, *args, **kwargs):
                    aw = None
                    other_exception = None
                    try:
                        with testcase_obj.assertRaises(AssertionError):
                            with testcase_obj.assertWarns(unexpected_warning) as aw:
                                try:
                                    return fn(testcase_obj, *args, **kwargs)
                                except Exception as e:
                                    other_exception = e
                    except AssertionError as ae:
                        # The test may raise AssertionErrors unrelated to the above AssertRaises.  We need to allow them
                        # to be raised.
                        if aw is None or len(aw.warnings) == 0:
                            raise ae

                    if other_exception is not None:
                        raise other_exception.with_traceback(
                            other_exception.__traceback__
                        )

                    if len(aw.warnings) > 0:
                        uws = "\n\t".join([str(w.message) for w in aw.warnings])
                        raise AssertionError(
                            f"{tb}\n{len(aw.warnings)} unexpected {unexpected_warning.__name__} triggered:\n\t{uws}"
                        )

                return wrapper

            return decorator

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
TracebaseTestCase: Type[TestCase] = test_case_class_factory(TestCase)
TracebaseTransactionTestCase: Type[TransactionTestCase] = test_case_class_factory(
    TransactionTestCase
)


def create_test_model(
    model_name: str, fields: Dict[str, Field], attrs: dict = {}
) -> Type[Model]:
    """Dynamically create a Django model for testing purposes.

    Example:
        TestModel = create_test_model("TestModel", {
            "name": models.CharField(max_length=255),
            "value": models.IntegerField(),
        })
    Args:
        model_name (str): The name of the model.
        fields (dict): A dictionary of model fields, where keys are field names and values are field instances
            (e.g., models.CharField(max_length=255)).
    Exceptions:
        None
    Returns:
        A dynamically created Django model class.
    """
    app_label = "loader"
    model_names = [mdl.__name__ for mdl in apps.get_app_config(app_label).get_models()]
    if model_name in model_names:
        raise ProgrammingError(f"A model named '{model_name}' already exists.")

    if not any(f.primary_key for f in fields.values()):
        if not any(n == "id" for n in fields.keys()):
            fields["id"] = AutoField(primary_key=True)
        else:
            raise ValueError(f"Primary key for test model {model_name} required.")

    model_attrs = {
        "__module__": __name__,
        **fields,
        "Meta": type(
            "Meta",
            (),
            # TODO: Change "loader" to something disassociated with the loader classes
            {"app_label": app_label},
        ),
    }
    model_attrs.update(attrs)

    model: Type[Model] = type(model_name, (Model,), model_attrs)

    return model


@override_settings(
    CACHES=settings.TEST_CACHES,
    STORAGES=settings.TEST_FILE_STORAGES,
    MEDIA_ROOT=settings.TEST_MEDIA_ROOT,
    # The default django test client will cause a 301 error for every get/post when SECURE_SSL_REDIRECT=True
    # https://stackoverflow.com/questions/49626899/
    SECURE_SSL_REDIRECT=False,
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


def _generate_test_stubs(package, verbose=0):
    """Takes a package (e.g. the name output of pkgutil.iter_modules) and prints test classes with stub test methods for
    every method (including class constructors) hard-coded into that package (i.e. not methods from a superclass defined
    in a separate file).

    Test stub construction is done in 2 steps:

    1. Populate a test_classes dict, structured like this:
           {test_class_name: {"python_path": str, "class_name": str, "methods": [method_names]}}
       using a default class name for methods to test in __main__: "MainTests".  All other classes are named
       "<class name>Tests".
       Lastly, if the only test in a test class is the class's constructor, the test is moved to the "MainTests" class
       (and the empty test class removed).

    2. The test_classes dict is traversed and the test method stubs are printed.  The "MainTests" class is given a
       custom name based on the package name.  A search is conducted for existing test methods.  If an existing test's
       name starts with the automatically constructed name, the test stub for that method is not printed.  NOTE: If a
       method's name is contained in another method being tested, the existing test method name must exactly match to
       not be printed.

    Limitations:
        1. Methods starting with "__" (except __init__) will not get test stubs generated for them.
    Assumptions:
        1. package is not a tests package.
    Args:
        package (str): a python path to a package (file), e.g. DataRepo.loaders
        verbose (int)
    Exceptions:
        None
    Returns:
        None
    """
    if IMPORT_ERROR is not None:
        raise IMPORT_ERROR

    # A dict to contain the test classes and their methods
    # Example:
    # test_classes = {test_class_name: {"python_path": str, "class_name": str, "methods": [method_names]}}
    test_classes = defaultdict(dict)

    # Use a class for all tests of methods in __main__.  This generates a titlecase version from the package name.
    # Package names are typically underscore-delimited lower-cased words.  This package_name will then be used to name
    # the class for testing methods in __main__.
    # E.g. DataRepo.context_processors -> ContextProcessors
    package_name = to_title_case(package.__name__.split(".")[-1])
    # In the meantime, we will use a static string in order to check how many methods it has without having to worry
    # about a dynamically determined name
    test_mainclass_name = "MainTests"
    test_classes[test_mainclass_name] = {
        "python_path": package.__name__,
        "class_name": "__main__",
        "methods": [],
    }

    test_package_name = "test_" + package.__name__.split(".")[-1]
    test_package = str(package.__name__).replace(*TEST_MODULE_REPLACEMENT)
    test_package = ".".join(test_package.split(".")[:-1]) + "." + test_package_name
    try:
        test_module = importlib.import_module(test_package)
        if verbose > 1:
            print(f"Test package {test_package} exists.")
    except ImportError as ie:
        if verbose > 1:
            print(f"Test package {test_package} does not exist.  {ie}")
        test_module = None

    # This loop populates the test_classes dict.  In this first phase, we populate that dict so that we can avoid having
    # many test classes with only 1 test method (the constructor).
    for name, obj in inspect.getmembers(package):
        # If this is a class that is defined in this module
        if (
            inspect.isclass(obj)
            and hasattr(obj, "__module__")
            and package.__name__ == obj.__module__
        ):
            test_class_name = f"{name}Tests"
            test_classes[test_class_name] = {
                "python_path": obj.__module__,
                "class_name": name,
                "methods": [],
            }
            for method_name, method_obj in inspect.getmembers(obj):
                if (
                    inspect.isfunction(method_obj)
                    and method_obj.__module__ == package.__name__
                    and not method_name.startswith("__")
                ):
                    if verbose > 2:
                        print(f"Adding class method {test_class_name} {method_name}")
                    test_classes[test_class_name]["methods"].append(method_name)
                elif verbose > 2 and inspect.isfunction(method_obj):
                    print(
                        f"Not a target method because not in module?: {method_obj.__module__} == {package.__name__} or "
                        f"has dunderscore: {method_name}"
                    )
                elif verbose > 2:
                    print(f"Not a target method because not a method: {method_name}")

            # Add a test for the constructor
            if len(test_classes[test_class_name]["methods"]) == 0:
                # ...to MainTests if there are no other methods to test
                test_classes[test_mainclass_name]["methods"].append(name)
                del test_classes[test_class_name]
                if verbose > 2:
                    print(
                        f"Putting constructor for {name} in __main__ tests ({test_mainclass_name} and removing from "
                        f"{test_class_name}): {test_classes[test_mainclass_name]}."
                    )
            else:
                if verbose > 2:
                    print(f"Putting constructor for {name} in {test_class_name} tests.")
                test_classes[test_class_name]["methods"].insert(0, name)

        elif (
            inspect.isfunction(obj)
            and obj.__module__ == package.__name__
            and not name.startswith("__")
        ):
            if verbose > 2:
                print(
                    f"Adding method from __main__: {name} module.__name__ == obj.__module__: {package.__name__} == "
                    f"{obj.__module__}"
                )
            test_classes[test_mainclass_name]["methods"].append(name)
        elif verbose > 2:
            print(
                f"Not a class in this module or not a method: {name} {type(obj).__name__}"
            )

    # Remove any test classes that have no methods, e.g. an exception class the relies solely on the class name and has
    # no methods of its own
    if len(test_classes[test_mainclass_name]["methods"]) == 0:
        del test_classes[test_mainclass_name]

    for test_class_name in test_classes.keys():
        if test_class_name == test_mainclass_name:
            # Customize the name for the test class if it is for __main__
            test_class_pretty_name = f"{package_name}MainTests"
        else:
            test_class_pretty_name = test_class_name
        test_class_def = (
            f"class {test_class_pretty_name}(TracebaseTestCase):\n"
            f'    """Test class for {test_classes[test_class_name]["python_path"]}.'
            f'{test_classes[test_class_name]["class_name"]}"""\n'
        )

        existing_test_method_names = []
        if test_module is not None:
            try:
                for test_method_name, _ in inspect.getmembers(
                    getattr(test_module, test_class_pretty_name)
                ):
                    existing_test_method_names.append(test_method_name)
            except AttributeError:
                pass

        created = 0
        existed = 0
        for method_name in test_classes[test_class_name]["methods"]:
            test_method_name = f"test_{method_name}"
            # If a test matching this method does not already exist
            if (
                # Any existing test method starts with the test name (but not because the name of the method being
                # tested is the start of another method being tested)
                not any(
                    item != method_name and item.startswith(method_name)
                    for item in test_classes[test_class_name]["methods"]
                )
                and any(
                    item.startswith(test_method_name)
                    for item in existing_test_method_names
                )
            ) or (
                # Any existing test method matches an existing test name
                # NOTE: We could filter the existing test method names for ones matching other methods, but that's a
                # bit more complex than I want to be RN
                test_method_name
                in existing_test_method_names
            ):
                if verbose > 1:
                    print(
                        f"Test method {test_classes[test_class_name]['class_name']}.{test_method_name} already exists"
                    )
                existed += 1
            else:
                if test_class_def is not None:
                    print(test_class_def)
                    test_class_def = None
                print(
                    f"    def {test_method_name}(self):\n"
                    f'        """Test {test_classes[test_class_name]["class_name"]}.{method_name}"""\n'
                    f"        # TODO: Implement test\n"
                    "        pass\n"
                )
                created += 1

        return created, existed


def generate_test_stubs(module_name: str = "DataRepo", verbose=0):
    """Takes a module name in the form of a python path and recursively traverses all submodules to call
    _generate_test_stubs on every package to generate test stubs for methods that don't already have test stubs.

    Modules whose names are in TEST_GENERATION_IGNORE_MODULES are skipped.

    Assumptions:
        1. Existing tests follow the same directory/file structure.
        2. Existing tests are located in DataRepo.tests.
        3. Directories under DataRepo/tests exactly match the directory names of the submodules being tested.
        4. Test file names have "test_" prepended compared to the package's file names.
        5. "DataRepo/tests" is the path of the tests module and that the CWD contains "DataRepo".
    Args:
        module_name (str) ["DataRepo"]: Python path
        verbose (int)
    Exceptions:
        None
    Returns:
        created (int): Count of the number of test stubs printed.
        existed (int): Count of the number of pre-existing tests.
    """

    if IMPORT_ERROR is not None:
        raise IMPORT_ERROR

    total_created = 0
    total_existed = 0

    for filepath, member_name, ispkg in pkgutil.iter_modules(
        path=[os.path.join(os.getcwd(), *module_name.split("."))]
    ):
        if f"{module_name}.{member_name}" in TEST_GENERATION_IGNORE_MODULES:
            if verbose > 0:
                print(f"Skipping {filepath} {module_name}.{member_name}")
            continue

        if ispkg:
            if verbose > 0:
                dir = os.path.join(filepath.path, member_name)  # type: ignore
                print(f"Recursing into {dir} {module_name}.{member_name}")
            created, existed = generate_test_stubs(
                module_name=f"{module_name}.{member_name}", verbose=verbose
            )
        else:
            submodule = importlib.import_module(f"{module_name}.{member_name}")
            module_file_path = os.path.relpath(str(submodule.__file__), os.getcwd())
            po = pathlib.Path(module_file_path)
            test_module_file_path = os.path.join(
                "DataRepo", "tests", *po.parts[1:-1], "_".join(["test", po.parts[-1]])
            )
            print(f"{test_module_file_path} ({module_name}.{member_name})")
            created, existed = _generate_test_stubs(submodule, verbose=verbose)

        total_created += created
        total_existed += existed

    return total_created, total_existed


def generate_tracebase_test_stubs(verbose=0):
    created, existed = generate_test_stubs(verbose=verbose)
    print(f"Done. Test stubs created: {created} existed: {existed}")


def to_title_case(text):
    """Converts an underscore delimited string like title_case to TitleCase."""
    return "".join(word.title() for word in text.split("_"))
