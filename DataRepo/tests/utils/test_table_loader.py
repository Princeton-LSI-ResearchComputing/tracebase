from collections import namedtuple

import pandas as pd
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db.models import AutoField, CharField, Model, UniqueConstraint
from django.test.utils import isolate_apps

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    ConflictingValueErrors,
    DryRun,
    DuplicateHeaders,
    DuplicateValueErrors,
    DuplicateValues,
    InfileDatabaseError,
    NoLoadData,
    RequiredColumnValue,
    RequiredColumnValues,
    RequiredHeadersError,
    RequiredValueError,
    RequiredValueErrors,
    UnknownHeadersError,
)
from DataRepo.utils.table_loader import TableLoader


@isolate_apps("DataRepo.tests.apps.loader")
class TableLoaderTests(TracebaseTestCase):
    @classmethod
    def generate_test_model(cls):
        # Model used for testing
        class TestModel(Model):
            id = AutoField(primary_key=True)
            name = CharField(unique=True)
            choice = CharField(choices=[("1", "1"), ("2", "2")])

            class Meta:
                app_label = "loader"

        return TestModel

    @classmethod
    def generate_test_model_with_unique_constraints(cls):
        # Model used for testing
        class TestUCModel(Model):
            id = AutoField(primary_key=True)
            name = CharField(unique=True)
            uf1 = CharField()
            uf2 = CharField()

            class Meta:
                app_label = "loader"
                constraints = [
                    UniqueConstraint(
                        fields=["uf1", "uf2"],
                        name="testuc",
                    ),
                ]

        return TestUCModel

    @classmethod
    def generate_test_loader(cls, mdl):
        class TestLoader(TableLoader):
            DataSheetName = "test"
            DataTableHeaders = namedtuple("DataTableHeaders", ["NAME", "CHOICE"])
            DataHeaders = DataTableHeaders(NAME="Name", CHOICE="Choice")
            DataRequiredHeaders = DataTableHeaders(NAME=True, CHOICE=False)
            DataRequiredValues = DataRequiredHeaders
            DataUniqueColumnConstraints = [["NAME"]]
            FieldToDataHeaderKey = {mdl.__name__: {"name": "NAME", "choice": "CHOICE"}}
            Models = [mdl]

            def load_data(self):
                return None

        return TestLoader

    @classmethod
    def generate_uc_test_loader(cls, mdl):
        class TestUCLoader(TableLoader):
            DataSheetName = "test"
            DataTableHeaders = namedtuple(
                "DataTableHeaders", ["NAME", "UFONE", "UFTWO"]
            )
            DataHeaders = DataTableHeaders(NAME="Name", UFONE="uf1", UFTWO="uf2")
            DataRequiredHeaders = DataTableHeaders(NAME=True, UFONE=False, UFTWO=False)
            DataRequiredValues = DataRequiredHeaders
            DataUniqueColumnConstraints = [["NAME"], ["UFONE", "UFTWO"]]
            DataColumnTypes = {"NAME": str, "UFONE": str, "UFTWO": str}
            FieldToDataHeaderKey = {
                "TestUCModel": {"name": "NAME", "uf1": "UFONE", "uf2": "UFTWO"}
            }
            Models = [mdl]

            def load_data(self):
                return None

        return TestUCLoader

    def __init__(self, *args, **kwargs):
        # The test model and loader must be created for the entire class or instance.  I chose "instance" so that I
        # didn't need to put the generators in a separate class or at __main__ level.  If you try and generate them in
        # each test, the model will get destroyed after the test and generating it again silently fails.
        self.TestModel = self.generate_test_model()
        self.TestLoader = self.generate_test_loader(self.TestModel)
        self.TestUCModel = self.generate_test_model_with_unique_constraints()
        self.TestUCLoader = self.generate_uc_test_loader(self.TestUCModel)
        super().__init__(*args, **kwargs)

    # handle_load_db_errors Tests
    def test_handle_load_db_errors_ve_choice(self):
        """Ensures handle_load_db_errors packages ValidationError about invalid choices"""
        tl = self.TestLoader()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        tl.handle_load_db_errors(
            ValidationError("3 is not a valid choice"),
            self.TestModel,
            {"name": "test", "choice": "3"},
        )
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            (
                "ValidationError in row [2] in the load file data, creating record:\n"
                "\tname: test\n"
                "\tchoice: 3\n"
                "\tValidationError: ['3 is not a valid choice']"
            ),
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertEqual(
            InfileDatabaseError, type(tl.aggregated_errors_object.exceptions[0])
        )

    def test_handle_load_db_errors_ie_unique(self):
        """Ensures handle_load_db_errors packages ValidationError about invalid choices"""
        tl = self.TestLoader()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = {"name": "test2", "choice": "2"}
        self.TestModel.objects.create(**recdict)
        # An integrity error requires a conflict, so:
        recdict["choice"] = "1"
        tl.handle_load_db_errors(
            IntegrityError("duplicate key value violates unique constraint"),
            self.TestModel,
            recdict,
        )
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            ConflictingValueError, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(
            (
                "Conflicting field values encountered in row [2] in the load file data in TestModel record [{'name': "
                "'test2', 'choice': '2'}]:\n"
                "\tchoice in\n"
                "\t\tdatabase: [2]\n"
                "\t\tfile: [1]\n"
            ),
            str(tl.aggregated_errors_object.exceptions[0]),
        )

    def test_handle_load_db_errors_catches_ie_with_not_null_constraint(self):
        """Ensures that handle_load_db_errors packages IntegrityErrors as RequiredValueError"""
        tl = self.TestLoader()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = {"name": None, "choice": "2"}
        # An integrity error requires a conflict, so:
        tl.handle_load_db_errors(
            IntegrityError('null value in column "name" violates not-null constraint'),
            self.TestModel,
            recdict,
        )
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            RequiredValueError, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(
            "Value required on the load file data.  Record extracted from row: {'name': None, 'choice': '2'}.",
            str(tl.aggregated_errors_object.exceptions[0]),
        )

    def test_handle_load_db_errors_catches_RequiredColumnValue(self):
        """Ensures that handle_load_db_errors catches RequiredColumnValue errors."""
        tl = self.TestLoader()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = {"name": "test", "choice": "2"}
        # An integrity error requires a conflict, so:
        tl.handle_load_db_errors(RequiredColumnValue("NAME"), self.TestModel, recdict)
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            RequiredColumnValue, type(tl.aggregated_errors_object.exceptions[0])
        )

    def test_handle_load_db_errors_raises_1_ve_with_same_dict(self):
        """Ensures that multiple ValidationErrors about the same dict are only buffered by handle_load_db_errors once"""
        tl = self.TestLoader()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        tl.handle_load_db_errors(
            ValidationError("3 is not a valid choice"),
            self.TestModel,
            {"name": "test", "choice": "3"},
        )
        tl.set_row_index(1)
        tl.handle_load_db_errors(
            ValidationError("3 is not a valid choice"),
            self.TestModel,
            {"name": "test", "choice": "3"},
        )
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            InfileDatabaseError, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(
            (
                "ValidationError in row [2] in the load file data, creating record:\n"
                "\tname: test\n"
                "\tchoice: 3\n"
                "\tValidationError: ['3 is not a valid choice']"
            ),
            str(tl.aggregated_errors_object.exceptions[0]),
        )

    def test_handle_load_db_errors_leaves_unpackaged_if_rec_dict_None(self):
        """Ensures that the exception is not packaged by handle_load_db_errors inside an InfileDatabaseError if rec_dict
        is None
        """
        tl = self.TestLoader()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = None
        # An integrity error requires a conflict, so:
        tl.handle_load_db_errors(AttributeError("Error"), self.TestModel, recdict)
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            AttributeError, type(tl.aggregated_errors_object.exceptions[0])
        )

    def test_check_for_inconsistencies(self):
        """Ensures that check_for_inconsistencies correctly packages conflicts"""
        tl = self.TestLoader()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = {"name": "test2", "choice": "2"}
        self.TestModel.objects.create(**recdict)
        rec = self.TestModel.objects.get(**recdict)
        # An integrity error requires a conflict, so:
        recdict["choice"] = "1"
        errfound = tl.check_for_inconsistencies(rec, recdict)
        self.assertTrue(errfound)
        self.assertEqual(
            ConflictingValueError, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(rec, tl.aggregated_errors_object.exceptions[0].rec)
        self.assertIn(
            "choice", tl.aggregated_errors_object.exceptions[0].differences.keys()
        )
        self.assertEqual(
            "1", tl.aggregated_errors_object.exceptions[0].differences["choice"]["new"]
        )
        self.assertEqual(
            "2", tl.aggregated_errors_object.exceptions[0].differences["choice"]["orig"]
        )

    # Method tests
    def test_get_load_stats(self):
        tl = self.TestLoader()
        ls = tl.get_load_stats()
        self.assertEqual(
            {
                self.TestModel.__name__: {
                    "created": 0,
                    "existed": 0,
                    "skipped": 0,
                    "errored": 0,
                }
            },
            ls,
        )

    def test_get_models(self):
        tl = self.TestLoader()
        self.assertEqual([self.TestModel], tl.get_models())

    def test_created(self):
        tl = self.TestLoader()
        tl.created()
        self.assertEqual(
            {
                self.TestModel.__name__: {
                    "created": 1,
                    "existed": 0,
                    "skipped": 0,
                    "errored": 0,
                }
            },
            tl.record_counts,
        )

    def test_existed(self):
        tl = self.TestLoader()
        tl.existed()
        self.assertEqual(
            {
                self.TestModel.__name__: {
                    "created": 0,
                    "existed": 1,
                    "skipped": 0,
                    "errored": 0,
                }
            },
            tl.record_counts,
        )

    def test_skipped(self):
        tl = self.TestLoader()
        tl.skipped()
        self.assertEqual(
            {
                self.TestModel.__name__: {
                    "created": 0,
                    "existed": 0,
                    "skipped": 1,
                    "errored": 0,
                }
            },
            tl.record_counts,
        )

    def test_errored(self):
        tl = self.TestLoader()
        tl.errored()
        self.assertEqual(
            {
                self.TestModel.__name__: {
                    "created": 0,
                    "existed": 0,
                    "skipped": 0,
                    "errored": 1,
                }
            },
            tl.record_counts,
        )

    def test__get_model_name(self):
        tl = self.TestLoader()
        self.assertEqual(self.TestModel.__name__, tl._get_model_name())

        # Check exception when models > 1
        class TestMultiModelLoader(self.TestLoader):
            # Note, this does not define all columns. Just need any values to prevent errors.
            Models = [self.TestModel, self.TestUCModel]

            def load_data(self):
                return None

        tlmms = TestMultiModelLoader()
        with self.assertRaises(AggregatedErrors) as ar:
            tlmms._get_model_name()
        aes = ar.exception
        self.assertEqual(ValueError, type(aes.exceptions[0]))

    def test_get_defaults_dict_by_header_name(self):
        class TestDefaultsLoader(TableLoader):
            DataSheetName = "test"
            DataTableHeaders = namedtuple("DataTableHeaders", ["NAME", "CHOICE"])
            DataHeaders = DataTableHeaders(NAME="Name", CHOICE="Choice")
            DataRequiredHeaders = DataTableHeaders(NAME=True, CHOICE=False)
            DataRequiredValues = DataRequiredHeaders
            DataDefaultValues = DataTableHeaders(NAME="test", CHOICE="1")
            DataUniqueColumnConstraints = [["NAME"]]
            FieldToDataHeaderKey = {"TestModel": {"name": "NAME", "choice": "CHOICE"}}
            Models = [self.TestModel]

            def load_data(self):
                return None

        tdl = TestDefaultsLoader()
        self.assertEqual(
            {"Name": "test", "Choice": "1"}, tdl.get_defaults_dict_by_header_name()
        )

    def test_get_row_val(self):
        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tl = self.TestLoader(pddata)
        n = None
        c = None
        for _, row in tl.df.iterrows():
            n = tl.get_row_val(row, "Name")
            c = tl.get_row_val(row, "Choice")
            break
        # Increments the current row
        self.assertEqual(0, tl.row_index)
        self.assertEqual("A", n)
        self.assertEqual("1", c)

    def test_get_skip_row_indexes(self):
        tl = self.TestLoader()
        tl.skip_row_indexes = [1, 9, 22]
        self.assertEqual([1, 9, 22], tl.get_skip_row_indexes())

    def test_add_skip_row_index(self):
        tl = self.TestLoader()
        # one
        tl.add_skip_row_index(index=1)
        self.assertEqual([1], tl.skip_row_indexes)
        # Multiple
        tl.add_skip_row_index(index_list=[9, 22])
        self.assertEqual([1, 9, 22], tl.skip_row_indexes)
        # Redundant
        tl.add_skip_row_index(index=1)
        self.assertEqual([1, 9, 22], tl.skip_row_indexes)

    def test_check_header_names(self):
        class TestDoubleHeaderLoader(TableLoader):
            DataSheetName = "test"
            DataTableHeaders = namedtuple("DataTableHeaders", ["NAME", "CHOICE"])
            DataHeaders = DataTableHeaders(NAME="Choice", CHOICE="Choice")
            DataRequiredHeaders = DataTableHeaders(NAME=True, CHOICE=False)
            DataRequiredValues = DataRequiredHeaders
            DataUniqueColumnConstraints = [["NAME"]]
            FieldToDataHeaderKey = {"TestModel": {"name": "NAME", "choice": "CHOICE"}}
            Models = [self.TestModel]

            def load_data(self):
                return None

        tdhl = TestDoubleHeaderLoader()
        excs = "\n\t".join(
            [
                f"{type(e).__name__}: {e}"
                for e in tdhl.aggregated_errors_object.exceptions
            ]
        )
        self.assertEqual(
            1,
            len(tdhl.aggregated_errors_object.exceptions),
            msg=(
                f"Expected 1 DuplicateHeaders exception.  Got {len(tdhl.aggregated_errors_object.exceptions)}:\n"
                f"\t{excs}"
            ),
        )
        self.assertEqual(
            DuplicateHeaders, type(tdhl.aggregated_errors_object.exceptions[0])
        )

    def test_check_unique_constraints(self):
        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "A", "B", "C"],
                "uf1": ["x", "x", "y", "y"],
                "uf2": ["1", "2", "3", "3"],
            },
        )
        tucl = self.TestUCLoader(pddata)
        tucl.check_unique_constraints()
        self.assertEqual(2, len(tucl.aggregated_errors_object.exceptions))
        self.assertEqual(
            DuplicateValues, type(tucl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(
            (
                "The following unique column (or column combination) ['Name'] was found to have duplicate occurrences "
                "in the load file data on the indicated rows:\n\tA (rows*: 2-3)"
            ),
            str(tucl.aggregated_errors_object.exceptions[0]),
        )
        self.assertEqual(
            DuplicateValues, type(tucl.aggregated_errors_object.exceptions[1])
        )
        self.assertEqual(
            (
                "The following unique column (or column combination) ['uf1', 'uf2'] was found to have duplicate "
                "occurrences in the load file data on the indicated rows:\n\tuf1: [y], uf2: [3] (rows*: 4-5)"
            ),
            str(tucl.aggregated_errors_object.exceptions[1]),
        )

    def test_header_key_to_name(self):
        tucl = self.TestUCLoader()
        kd = {
            "NAME": 1,
            "UFONE": 2,
            "UFTWO": 3,
        }
        nd = tucl.header_key_to_name(kd)
        expected = {
            "Name": 1,
            "uf1": 2,
            "uf2": 3,
        }
        self.assertEqual(expected, nd)

    def test_get_column_types(self):
        tucl = self.TestUCLoader()
        td = tucl.get_column_types()
        expected = {
            "Name": str,
            "uf1": str,
            "uf2": str,
        }
        self.assertEqual(expected, td)

    def test_isnamedtuple(self):
        nt = self.TestLoader.DataTableHeaders(
            NAME=True,
            CHOICE=True,
        )
        self.assertTrue(self.TestLoader.isnamedtuple(nt))

    def test_initialize_metadata(self):
        tl = self.TestLoader()
        # initialize_metadata is called in the constructor

        # Initialized via initialize_metadata, directly
        # - record_counts (dict of dicts of ints): Created, existed, and errored counts by model.
        # - defaults_current_type (str): Set the self.sheet (before sheet is potentially set to None).
        # - sheet (str): Name of the data sheet in an excel file (changes to None if not excel).
        # - defaults_sheet (str): Name of the defaults sheet in an excel file (changes to None if not excel).
        # - record_counts (dict): Record created, existed, and errored counts by model name.  All set to 0.

        # Initialized via set_headers
        # - headers (DataTableHeaders namedtuple of strings): Customized header names by header key.
        # - all_headers (list of strings): Customized header names.
        # - reqd_headers (DataTableHeaders namedtuple of booleans): Required header booleans by header name.
        # - FieldToHeader (dict of dicts of strings): Header names by model and field.
        # - unique_constraints (list of lists of strings): Header name combos whose columns must be unique.
        # - reqd_values (DataTableHeaders namedtuple of booleans): Required value booleans by header name.
        # - defaults_by_header (dict): Default values by header name.

        # Initialized by get_defaults (called from initialize_metadata)
        # - defaults (DataTableHeaders namedtuple of objects): Customized default values by header key.
        # - defaults_by_header (dict): Default values by header name.

        self.assertTrue(hasattr(tl, "headers"))
        self.assertTrue(hasattr(tl, "defaults"))
        self.assertTrue(hasattr(tl, "all_headers"))
        self.assertTrue(hasattr(tl, "reqd_headers"))
        self.assertTrue(hasattr(tl, "FieldToHeader"))
        self.assertTrue(hasattr(tl, "unique_constraints"))
        self.assertTrue(hasattr(tl, "record_counts"))
        self.assertTrue(hasattr(tl, "defaults_current_type"))
        self.assertTrue(hasattr(tl, "sheet"))
        self.assertTrue(hasattr(tl, "defaults_sheet"))
        self.assertTrue(hasattr(tl, "reqd_values"))
        self.assertTrue(hasattr(tl, "defaults_by_header"))

    def test_check_class_attributes(self):
        class TestInvalidLoader(TableLoader):
            DataSheetName = "test"
            DataTableHeaders = namedtuple("DataTableHeaders", ["NAME", "CHOICE"])
            DataHeaders = None
            DataRequiredHeaders = None
            DataRequiredValues = None
            DataDefaultValues = None
            DataUniqueColumnConstraints = None
            FieldToDataHeaderKey = None
            Models = None

            def load_data(self):
                return None

        with self.assertRaises(AggregatedErrors) as ar:
            TestInvalidLoader()
        # check_class_attributes is called in the constructor
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(TypeError, type(aes.exceptions[0]))
        self.assertEqual(
            (
                "Invalid attributes:\n"
                "\tattribute [TestInvalidLoader.DataHeaders] namedtuple required, <class 'NoneType'> set\n"
                "\tattribute [TestInvalidLoader.DataRequiredHeaders] namedtuple required, <class 'NoneType'> set\n"
                "\tattribute [TestInvalidLoader.DataRequiredValues] namedtuple required, <class 'NoneType'> set\n"
                "\tattribute [TestInvalidLoader.DataUniqueColumnConstraints] list required, <class 'NoneType'> set\n"
                "\tattribute [TestInvalidLoader.FieldToDataHeaderKey] dict required, <class 'NoneType'> set\n"
                "\tModels is required to have at least 1 Model class"
            ),
            str(aes.exceptions[0]),
        )

    def test_check_class_attributes_type(self):
        class TestModel(Model):
            id = AutoField(primary_key=True)
            name = CharField(unique=True)
            choice = CharField(choices=[("1", "1"), ("2", "2")])

            # Necessary for temporary models
            class Meta:
                app_label = "loader"

        class TestLoader(TableLoader):
            DataSheetName = "test"
            DataTableHeaders = namedtuple("DataTableHeaders", ["TEST"])
            DataHeaders = DataTableHeaders(TEST="Test")
            DataRequiredHeaders = DataTableHeaders(TEST=True)
            DataRequiredValues = DataRequiredHeaders
            DataColumnTypes = {"TEST": str}
            DataDefaultValues = DataTableHeaders(TEST=5)
            DataUniqueColumnConstraints = [["TEST"]]
            FieldToDataHeaderKey = {"TestModel": {"name": "TEST"}}
            Models = [TestModel]

            def load_data(self):
                pass

        with self.assertRaises(AggregatedErrors) as ar:
            TestLoader()
        # check_class_attributes is called in the constructor
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(TypeError, type(aes.exceptions[0]))
        self.assertEqual(
            (
                "Invalid attributes:\n"
                "\tattribute [TestLoader.DataDefaultValues.TEST] str required (according to "
                "TestLoader.DataColumnTypes['TEST']), but int set"
            ),
            str(aes.exceptions[0]),
        )

    def test_get_defaults(self):
        tl = self.TestLoader()
        # initialize_metadata is called in the constructor
        self.assertEqual(tl.DataDefaultValues, tl.get_defaults())

    def test_get_header_keys(self):
        tl = self.TestLoader()
        self.assertEqual(list(tl.DataHeaders._asdict().keys()), tl.get_header_keys())

    def test_get_pretty_headers(self):
        tl = self.TestLoader()
        self.assertEqual("[Name*, Choice] (* = Required)", tl.get_pretty_headers())

    def test_get_headers(self):
        tl = self.TestLoader()
        self.assertEqual(tl.DataHeaders, tl.get_headers())

    def test_set_row_index(self):
        tl = self.TestLoader()
        tl.set_row_index(3)
        self.assertEqual(3, tl.row_index)
        self.assertEqual(5, tl.rownum)

    def test_get_one_column_dupes(self):
        """Test that get_one_column_dupes identifies dupes in col2, row indexes 0 and 1 only (2 is ignored)."""
        pddata = pd.DataFrame.from_dict(
            {
                "col1": ["A", "B", "C"],
                "col2": ["x", "x", "x"],
                "col3": ["1", "2", "3"],
            },
        )
        outdict, outlist = self.TestLoader.get_one_column_dupes(
            pddata, "col2", ignore_row_idxs=[2]
        )
        self.assertEqual({"x": [0, 1]}, outdict)
        self.assertEqual([0, 1], outlist)

    def test_get_unique_constraint_fields(self):
        unique_field_sets = self.TestLoader.get_unique_constraint_fields(
            self.TestUCModel
        )
        self.assertEqual(1, len(unique_field_sets))
        self.assertEqual(
            (
                "uf1",
                "uf2",
            ),
            unique_field_sets[0],
        )

    def test_get_non_auto_model_fields(self):
        expected = [
            "name",
            "uf1",
            "uf2",
        ]
        field_names = [
            f.name if hasattr(f, "name") else f.field_name
            for f in self.TestLoader.get_non_auto_model_fields(self.TestUCModel)
        ]
        self.assertEqual(expected, field_names)

    def test_get_enumerated_fields(self):
        field_names = self.TestLoader.get_enumerated_fields(self.TestModel)
        self.assertEqual(["choice"], field_names)

    def test_get_unique_fields(self):
        field_names = self.TestLoader.get_unique_fields(self.TestUCModel)
        self.assertEqual(["name"], field_names)

    # apply_loader_wrapper tests
    def test_abstract_attributes_required(self):
        class TestEmptyLoader(TableLoader):
            pass

        with self.assertRaises(TypeError) as ar:
            # In order to test this behavior, we need to make pylint not error about it
            # pylint: disable=abstract-class-instantiated
            TestEmptyLoader()
            # pylint: enable=abstract-class-instantiated
        self.assertEqual(
            (
                "Can't instantiate abstract class TestEmptyLoader with abstract methods DataHeaders, "
                "DataRequiredHeaders, DataRequiredValues, DataSheetName, DataTableHeaders, "
                "DataUniqueColumnConstraints, FieldToDataHeaderKey, Models, load_data"
            ),
            str(ar.exception),
        )

    def test_is_skip_row(self):
        tl = self.TestLoader()
        tl.skip_row_indexes = [3]
        self.assertTrue(tl.is_skip_row(3))
        self.assertFalse(tl.is_skip_row(0))

    def test_tableheaders_to_dict_by_header_name(self):
        nt = self.TestLoader.DataTableHeaders(
            NAME=True,
            CHOICE=True,
        )
        tl = self.TestLoader()
        self.assertTrue(
            {
                "Name": True,
                "Choice": True,
            },
            tl.tableheaders_to_dict_by_header_name(nt),
        )

    # Test that load_wrapper
    def test_load_wrapper_does_not_nest_AggregatedErrors(self):
        class TestNestedAesLoader(self.TestLoader):
            def load_data(self):
                # Buffer an exception correctly
                self.aggregated_errors_object.buffer_warning(ValueError("WARNING"))
                # Raise a new AggregatedErrors exception (can happen in classmethods called from an instance)
                raise AggregatedErrors().buffer_error(ValueError("Nested"))

        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tnal = TestNestedAesLoader(pddata)

        with self.assertRaises(AggregatedErrors) as ar:
            tnal.load_data()
        aes = ar.exception
        self.assertEqual(2, len(aes.exceptions))
        self.assertEqual(ValueError, type(aes.exceptions[0]))
        self.assertEqual("WARNING", str(aes.exceptions[0]))
        self.assertEqual(ValueError, type(aes.exceptions[1]))
        self.assertEqual("Nested", str(aes.exceptions[1]))

    def test_load_wrapper_summarizes_ConflictingValueErrors(self):
        class TestMultiCVELoader(self.TestLoader):
            def load_data(self):
                # Buffer 2 ConflictingValueError exceptions
                self.aggregated_errors_object.buffer_error(
                    ConflictingValueError(rec=None, differences=None)
                )
                self.aggregated_errors_object.buffer_error(
                    ConflictingValueError(rec=None, differences=None)
                )

        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tmcl = TestMultiCVELoader(pddata)

        with self.assertRaises(AggregatedErrors) as ar:
            tmcl.load_data()
        aes = ar.exception
        # The 2 exceptions should be summarized as 1
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(ConflictingValueErrors, type(aes.exceptions[0]))
        self.assertEqual(
            (
                "Conflicting values encountered during loading:\n\tDuring the processing of the load file data...\n"
                "\tCreation of the following No record provided record(s) encountered conflicts:\n"
                "\t\tFile record:     No file data provided\n"
                "\t\tDatabase record: Database record not provided\n"
                "\t\t\tdifference data unavailable\n"
                "\t\tDatabase record: Database record not provided\n"
                "\t\t\tdifference data unavailable\n"
            ),
            str(aes.exceptions[0]),
        )

    def test_load_wrapper_summarizes_RequiredValueErrors(self):
        class TestMultiRVELoader(self.TestLoader):
            def load_data(self):
                # Buffer 2 RequiredValueError exceptions
                self.aggregated_errors_object.buffer_error(
                    RequiredValueError(
                        column="Name",
                        rownum=2,
                        model_name="TestModel",
                        field_name="name",
                    )
                )
                self.aggregated_errors_object.buffer_error(
                    RequiredValueError(
                        column="Choice",
                        rownum=3,
                        model_name="TestModel",
                        field_name="choice",
                    )
                )

        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tmrl = TestMultiRVELoader(pddata)

        with self.assertRaises(AggregatedErrors) as ar:
            tmrl.load_data()
        aes = ar.exception
        # The 2 exceptions should be summarized as 1
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(RequiredValueErrors, type(aes.exceptions[0]))
        self.assertEqual(
            (
                "Required values found missing during loading:\n"
                "\tthe load file data:\n"
                "\t\tField: [TestModel.name] Column: [Name] on row(s): 2\n"
                "\t\tField: [TestModel.choice] Column: [Choice] on row(s): 3\n"
            ),
            str(aes.exceptions[0]),
        )

    def test_load_wrapper_summarizes_DuplicateValueErrors(self):
        class TestMultiDVELoader(self.TestLoader):
            def load_data(self):
                # Buffer 2 RequiredValueError exceptions
                self.aggregated_errors_object.buffer_error(
                    DuplicateValues(dupe_dict={}, colnames=["Name"])
                )
                self.aggregated_errors_object.buffer_error(
                    DuplicateValues(dupe_dict={}, colnames=["Name"])
                )

        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tmdl = TestMultiDVELoader(pddata)

        with self.assertRaises(AggregatedErrors) as ar:
            tmdl.load_data()
        aes = ar.exception
        # The 2 exceptions should be summarized as 1
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(DuplicateValueErrors, type(aes.exceptions[0]))
        self.assertEqual(
            (
                "The following unique column(s) (or column combination(s)) were found to have duplicate occurrences on "
                "the indicated rows:\n"
                "\tthe load file data\n"
                "\t\tColumn(s) ['Name']\n"
                "\t\t\tNo duplicates data provided\n"
                "\t\t\tNo duplicates data provided\n"
            ),
            str(aes.exceptions[0]),
        )

    def test_load_wrapper_summarizes_RequiredColumnValues(self):
        class TestMultiRCVLoader(self.TestLoader):
            def load_data(self):
                # Buffer 2 RequiredValueError exceptions
                self.aggregated_errors_object.buffer_error(
                    RequiredColumnValue(column="Name")
                )
                self.aggregated_errors_object.buffer_error(
                    RequiredColumnValue(column="Name")
                )

        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tmrl = TestMultiRCVLoader(pddata)

        with self.assertRaises(AggregatedErrors) as ar:
            tmrl.load_data()
        aes = ar.exception
        # The 2 exceptions should be summarized as 1
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(RequiredColumnValues, type(aes.exceptions[0]))
        self.assertEqual(
            (
                "Required column values missing on the indicated rows:\n"
                "\tthe load file data\n"
                "\t\tColumn: [Name] on rows: No row numbers provided\n"
            ),
            str(aes.exceptions[0]),
        )

    def test_load_wrapper_handles_defer_rollback(self):
        class TestDeferedLoader(self.TestLoader):
            def load_data(self):
                self.Models[0].objects.create(name="A", choice=1)
                self.aggregated_errors_object.buffer_error(ValueError("Test"))

        # Calling load_data checks the dataframe, so we need a real one (though we're not going to use it for this test,
        # to isolate what we're testing)
        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tdl = TestDeferedLoader(pddata, defer_rollback=True)

        # There should be no record found initially
        self.assertEqual(0, self.TestModel.objects.filter(name="A", choice=1).count())

        # Expect an AggregatedErrors exception
        with self.assertRaises(AggregatedErrors) as ar:
            tdl.load_data()

        # To avoid overlooking another error, check the exception
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(ValueError, type(aes.exceptions[0]))
        self.assertEqual("Test", str(aes.exceptions[0]))

        # This get should not cause an exception because the record should have been created and kept
        self.TestModel.objects.get(name="A", choice=1)

    def test_load_wrapper_handles_DryRun(self):
        class TestDryRunLoader(self.TestLoader):
            def load_data(self):
                self.Models[0].objects.create(name="A", choice=1)

        # Calling load_data checks the dataframe, so we need a real one (though we're not going to use it for this test,
        # to isolate what we're testing)
        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tdl = TestDryRunLoader(pddata, dry_run=True)

        # There should be no record found initially
        self.assertEqual(0, self.TestModel.objects.filter(name="A", choice=1).count())

        # Expect a DryRun exception
        with self.assertRaises(DryRun):
            tdl.load_data()

        # Nothing should load in a dry run
        self.assertEqual(0, self.TestModel.objects.filter(name="A", choice=1).count())

    def test_load_wrapper_preserves_return(self):
        class TestStatsLoader(self.TestLoader):
            def load_data(self):
                return 42

        # Calling load_data checks the dataframe, so we need a real one (though we're not going to use it for this test,
        # to isolate what we're testing)
        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tsl = TestStatsLoader(pddata)

        retval = tsl.load_data()

        self.assertEqual(42, retval)

    def test_set_headers(self):
        tl = self.TestLoader()
        tl.set_headers({"NAME": "X", "CHOICE": "Selection"})
        expected = self.TestLoader.DataTableHeaders(NAME="X", CHOICE="Selection")
        self.assertEqual(expected, tl.headers)

    def test__merge_headers(self):
        # User-supplied headers (i.e. from the command line) trump derived class custom headers
        tl = self.TestLoader(
            user_headers={"NAME": "UsersDumbNameHeader", "CHOICE": "UsersChoice"}
        )
        tl._merge_headers({"NAME": "X", "CHOICE": "Selection"})
        expected = self.TestLoader.DataTableHeaders(
            NAME="UsersDumbNameHeader", CHOICE="UsersChoice"
        )
        self.assertEqual(expected, tl.headers)

    def test_set_defaults(self):
        tl = self.TestLoader()
        tl.set_defaults({"NAME": "one", "CHOICE": "two"})
        expected = self.TestLoader.DataTableHeaders(NAME="one", CHOICE="two")
        self.assertEqual(expected, tl.defaults)

    def test__merge_defaults(self):
        df = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        # User-supplied headers (i.e. from the command line) trump derived class custom headers
        def_df = pd.DataFrame.from_dict(
            {
                "Sheet Name": ["Test"],
                "Column Header": ["Name"],
                "Default Value": ["G"],
            },
        )
        tl = self.TestLoader(df=df, data_sheet="Test", defaults_df=def_df)
        tl._merge_defaults({"NAME": "D"})
        expected = self.TestLoader.DataTableHeaders(NAME="G", CHOICE=None)
        self.assertEqual(expected, tl.defaults)

    def test_isnamedtupletype(self):
        self.assertTrue(
            self.TestLoader.isnamedtupletype(self.TestLoader.DataTableHeaders)
        )
        self.assertFalse(self.TestLoader.isnamedtupletype(self.TestLoader.DataHeaders))
        self.assertTrue(
            self.TestLoader.isnamedtupletype(self.TestLoader.DefaultsTableHeaders)
        )
        self.assertFalse(self.TestLoader.isnamedtupletype(1))

    def test_check_dataframe_headers(self):
        df = pd.DataFrame.from_dict(
            {
                "Wrong": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tl = self.TestLoader(df=df)
        with self.assertRaises(AggregatedErrors) as ar:
            tl.check_dataframe_headers()
        aes = ar.exception
        self.assertEqual(2, len(aes.exceptions))
        self.assertEqual(RequiredHeadersError, type(aes.exceptions[0]))
        self.assertIn("Name", str(aes.exceptions[0]))
        self.assertEqual(UnknownHeadersError, type(aes.exceptions[1]))
        self.assertIn("Wrong", str(aes.exceptions[1]))

        tl2 = self.TestLoader()
        tl2.check_dataframe_headers()
        aes2 = tl2.aggregated_errors_object
        self.assertEqual(1, len(aes2.exceptions))
        self.assertEqual(NoLoadData, type(aes2.exceptions[0]))

    def test_get_user_defaults(self):
        df = pd.DataFrame.from_dict(
            {
                "Name": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        # User-supplied headers (i.e. from the command line) trump derived class custom headers
        def_df = pd.DataFrame.from_dict(
            {
                "Sheet Name": ["Test"],
                "Column Header": ["Name"],
                "Default Value": ["G"],
            },
        )
        tl = self.TestLoader(df=df, data_sheet="Test", defaults_df=def_df)
        # Derived class defaults (just to ensure these aren't included)
        tl.set_defaults({"NAME": "one", "CHOICE": "two"})
        expected = {"Name": "G"}
        self.assertEqual(expected, tl.get_user_defaults())

    def test_header_name_to_key(self):
        tl = self.TestLoader()
        self.assertEqual({"NAME": 2}, tl.header_name_to_key({"Name": 2}))
