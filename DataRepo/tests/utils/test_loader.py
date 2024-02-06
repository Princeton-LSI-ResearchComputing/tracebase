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
    DuplicateValues,
    InfileDatabaseError,
    RequiredColumnValue,
    RequiredValueError,
)
from DataRepo.utils.loader import TraceBaseLoader


@isolate_apps("DataRepo.tests.apps.loader")
class TraceBaseLoaderTests(TracebaseTestCase):
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
        class TestLoader(TraceBaseLoader):
            TableHeaders = namedtuple("TableHeaders", ["NAME", "CHOICE"])
            RequiredHeaders = TableHeaders(NAME=True, CHOICE=False)
            DefaultHeaders = TableHeaders(NAME="Name", CHOICE="Choice")
            RequiredValues = RequiredHeaders
            FieldToHeaderKey = {"TestModel": {"name": "NAME", "choice": "CHOICE"}}
            UniqueColumnConstraints = [["NAME"]]
            Models = [mdl]

            def load_data(self):
                return None

        return TestLoader

    @classmethod
    def generate_uc_test_loader(cls, mdl):
        class TestLoader(TraceBaseLoader):
            TableHeaders = namedtuple("TableHeaders", ["NAME", "UFONE", "UFTWO"])
            DefaultHeaders = TableHeaders(NAME="Name", UFONE="uf1", UFTWO="uf2")
            RequiredHeaders = TableHeaders(NAME=True, UFONE=False, UFTWO=False)
            RequiredValues = RequiredHeaders
            UniqueColumnConstraints = [["NAME"], ["UFONE", "UFTWO"]]
            ColumnTypes = {"NAME": str, "UFONE": str, "UFTWO": str}
            FieldToHeaderKey = {
                "TestUCModel": {"name": "NAME", "uf1": "UFONE", "uf2": "UFTWO"}
            }
            Models = [mdl]

            def load_data(self):
                return None

        return TestLoader

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
        tl = self.TestLoader(None)
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
        tl = self.TestLoader(None)
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
                "\t\tfile: [1]"
            ),
            str(tl.aggregated_errors_object.exceptions[0]),
        )

    def test_handle_load_db_errors_catches_ie_with_not_null_constraint(self):
        """Ensures that handle_load_db_errors packages IntegrityErrors as RequiredValueError"""
        tl = self.TestLoader(None)
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
        tl = self.TestLoader(None)
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
        tl = self.TestLoader(None)
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
        tl = self.TestLoader(None)
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
        tl = self.TestLoader(None)
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
        tl = self.TestLoader(None)
        ls = tl.get_load_stats()
        self.assertEqual(
            {self.TestModel.__name__: {"created": 0, "existed": 0, "errored": 0}}, ls
        )

    def test_get_models(self):
        tl = self.TestLoader(None)
        self.assertEqual([self.TestModel], tl.get_models())

    def test_created(self):
        tl = self.TestLoader(None)
        tl.created()
        self.assertEqual(
            {self.TestModel.__name__: {"created": 1, "existed": 0, "errored": 0}},
            tl.record_counts,
        )

    def test_existed(self):
        tl = self.TestLoader(None)
        tl.existed()
        self.assertEqual(
            {self.TestModel.__name__: {"created": 0, "existed": 1, "errored": 0}},
            tl.record_counts,
        )

    def test_errored(self):
        tl = self.TestLoader(None)
        tl.errored()
        self.assertEqual(
            {self.TestModel.__name__: {"created": 0, "existed": 0, "errored": 1}},
            tl.record_counts,
        )

    def test__get_model_name(self):
        tl = self.TestLoader(None)
        self.assertEqual(self.TestModel.__name__, tl._get_model_name())

        # Check exception when models > 1
        class TestMultiModelLoader(TraceBaseLoader):
            TableHeaders = namedtuple("TableHeaders", ["NAME", "CHOICE"])
            DefaultHeaders = TableHeaders(NAME="Name", CHOICE="Choice")
            RequiredHeaders = TableHeaders(NAME=True, CHOICE=False)
            RequiredValues = RequiredHeaders
            UniqueColumnConstraints = [["NAME"]]
            FieldToHeaderKey = {"TestModel": {"name": "NAME", "choice": "CHOICE"}}
            Models = [self.TestModel, self.TestUCModel]

            def load_data(self):
                return None

        tlmms = TestMultiModelLoader(None)
        with self.assertRaises(AggregatedErrors) as ar:
            tlmms._get_model_name()
        aes = ar.exception
        self.assertEqual(ValueError, type(aes.exceptions[0]))

    def test_get_defaults_dict_by_header_name(self):
        class TestDefaultsLoader(TraceBaseLoader):
            TableHeaders = namedtuple("TableHeaders", ["NAME", "CHOICE"])
            DefaultHeaders = TableHeaders(NAME="Name", CHOICE="Choice")
            RequiredHeaders = TableHeaders(NAME=True, CHOICE=False)
            RequiredValues = RequiredHeaders
            DefaultValues = TableHeaders(NAME="test", CHOICE="1")
            UniqueColumnConstraints = [["NAME"]]
            FieldToHeaderKey = {"TestModel": {"name": "NAME", "choice": "CHOICE"}}
            Models = [self.TestModel]

            def load_data(self):
                return None

        tdl = TestDefaultsLoader(None)
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
        tl = self.TestLoader(None)
        tl.skip_row_indexes = [1, 9, 22]
        self.assertEqual([1, 9, 22], tl.get_skip_row_indexes())

    def test_add_skip_row_index(self):
        tl = self.TestLoader(None)
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
        class TestDoubleHeaderLoader(TraceBaseLoader):
            TableHeaders = namedtuple("TableHeaders", ["NAME", "CHOICE"])
            DefaultHeaders = TableHeaders(NAME="Choice", CHOICE="Choice")
            RequiredHeaders = TableHeaders(NAME=True, CHOICE=False)
            RequiredValues = RequiredHeaders
            UniqueColumnConstraints = [["NAME"]]
            FieldToHeaderKey = {"TestModel": {"name": "NAME", "choice": "CHOICE"}}
            Models = [self.TestModel]

            def load_data(self):
                return None

        tdhl = TestDoubleHeaderLoader(None)
        tdhl.check_header_names()
        self.assertEqual(1, len(tdhl.aggregated_errors_object.exceptions))
        self.assertEqual(ValueError, type(tdhl.aggregated_errors_object.exceptions[0]))
        self.assertIn(
            "Duplicate Header names encountered",
            str(tdhl.aggregated_errors_object.exceptions[0]),
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
        tucl = self.TestUCLoader(None)
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
        tucl = self.TestUCLoader(None)
        td = tucl.get_column_types()
        expected = {
            "Name": str,
            "uf1": str,
            "uf2": str,
        }
        self.assertEqual(expected, td)

    def test_isnamedtuple(self):
        nt = self.TestLoader.TableHeaders(
            NAME=True,
            CHOICE=True,
        )
        self.assertTrue(self.TestLoader.isnamedtuple(nt))

    def test_initialize_metadata(self):
        tl = self.TestLoader(None)
        # initialize_metadata is called in the constructor
        self.assertTrue(hasattr(tl, "headers"))
        self.assertTrue(hasattr(tl, "defaults"))
        self.assertTrue(hasattr(tl, "all_headers"))
        self.assertTrue(hasattr(tl, "reqd_headers"))
        self.assertTrue(hasattr(tl, "FieldToHeader"))
        self.assertTrue(hasattr(tl, "unique_constraints"))
        self.assertTrue(hasattr(tl, "record_counts"))

    def test_check_class_attributes(self):
        class TestInvalidLoader(TraceBaseLoader):
            TableHeaders = namedtuple("TableHeaders", ["NAME", "CHOICE"])
            DefaultHeaders = None
            RequiredHeaders = None
            RequiredValues = None
            DefaultValues = None
            UniqueColumnConstraints = None
            FieldToHeaderKey = None
            Models = None

            def load_data(self):
                return None

        with self.assertRaises(AggregatedErrors) as ar:
            TestInvalidLoader(None)
        # check_class_attributes is called in the constructor
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(TypeError, type(aes.exceptions[0]))
        self.assertEqual(
            (
                "Invalid attributes:\n"
                "\tattribute [TestInvalidLoader.DefaultHeaders] namedtuple required, <class 'NoneType'> set\n"
                "\tattribute [TestInvalidLoader.RequiredHeaders] namedtuple required, <class 'NoneType'> set\n"
                "\tattribute [TestInvalidLoader.RequiredValues] namedtuple required, <class 'NoneType'> set\n"
                "\tattribute [TestInvalidLoader.UniqueColumnConstraints] list required, <class 'NoneType'> set\n"
                "\tattribute [TestInvalidLoader.FieldToHeaderKey] dict required, <class 'NoneType'> set"
            ),
            str(aes.exceptions[0]),
        )

    def test_get_defaults(self):
        tl = self.TestLoader(None)
        # initialize_metadata is called in the constructor
        self.assertEqual(tl.DefaultValues, tl.get_defaults())

    def test_get_header_keys(self):
        tl = self.TestLoader(None)
        self.assertEqual(list(tl.DefaultHeaders._asdict().keys()), tl.get_header_keys())

    def test_get_pretty_default_headers(self):
        tl = self.TestLoader(None)
        self.assertEqual(
            (["Name*", "Choice"], "(* = Required)"), tl.get_pretty_default_headers()
        )

    def test_get_headers(self):
        tl = self.TestLoader(None)
        self.assertEqual(tl.DefaultHeaders, tl.get_headers())

    def test_set_row_index(self):
        tl = self.TestLoader(None)
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
        class TestEmptyLoader(TraceBaseLoader):
            pass

        with self.assertRaises(TypeError) as ar:
            TestEmptyLoader()
        self.assertEqual(
            (
                "Can't instantiate abstract class TestEmptyLoader with abstract methods DefaultHeaders, "
                "FieldToHeaderKey, Models, RequiredHeaders, RequiredValues, TableHeaders, UniqueColumnConstraints, "
                "load_data"
            ),
            str(ar.exception),
        )

    def test_is_skip_row(self):
        tl = self.TestLoader(None)
        tl.skip_row_indexes = [3]
        self.assertTrue(tl.is_skip_row(3))
        self.assertFalse(tl.is_skip_row(0))

    def test_tableheaders_to_dict_by_header_name(self):
        nt = self.TestLoader.TableHeaders(
            NAME=True,
            CHOICE=True,
        )
        tl = self.TestLoader(None)
        self.assertTrue(
            {
                "Name": True,
                "Choice": True,
            },
            tl.tableheaders_to_dict_by_header_name(nt),
        )

    # Test that load_wrapper
    def test_load_wrapper_does_not_nest_AggregatedErrors_exceptions(self):
        # TODO: Implement
        pass

    def test_load_wrapper_summarizes_ConflictingValueError_as_ConflictingValueErrors(
        self,
    ):
        # TODO: Implement
        pass

    def test_load_wrapper_summarizes_RequiredValueError_and_RequiredValueErrors(self):
        # TODO: Implement
        pass

    def test_load_wrapper_summarizes_DuplicateValues_as_DuplicateValueErrors(self):
        # TODO: Implement
        pass

    def test_load_wrapper_summarizes_RequiredColumnValue_as_RequiredColumnValues(self):
        # TODO: Implement
        pass

    def test_load_wrapper_handles_defer_rollback(self):
        # TODO: Implement
        pass

    def test_load_wrapper_handles_DryRun(self):
        # TODO: Implement
        pass

    def test_load_wrapper_returns_record_counts(self):
        # TODO: Implement
        pass
