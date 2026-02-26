from collections import namedtuple
from typing import List

import pandas as pd
from django.core.exceptions import ValidationError
from django.db import IntegrityError, ProgrammingError, connection
from django.db.models import (
    RESTRICT,
    AutoField,
    CharField,
    ForeignKey,
    IntegerField,
    ManyToManyField,
    Model,
    Q,
    UniqueConstraint,
)
from django.test.utils import isolate_apps

from DataRepo.loaders.base.table_column import ColumnValue, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    ConflictingValueErrors,
    DBFieldVsFileColDeveloperWarning,
    DryRun,
    DuplicateHeaders,
    DuplicateValueErrors,
    DuplicateValues,
    InfileDatabaseError,
    MultipleConflictingValueMatches,
    NoLoadData,
    RequiredColumnValue,
    RequiredColumnValues,
    RequiredHeadersError,
    RequiredValueError,
    RequiredValueErrors,
    UnknownHeaders,
)


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
            opt_str = CharField()

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
            DataRequiredHeaders = ["NAME"]
            DataRequiredValues = DataRequiredHeaders
            DataUniqueColumnConstraints = [["NAME"]]
            FieldToDataHeaderKey = {mdl.__name__: {"name": "NAME", "choice": "CHOICE"}}
            DataColumnMetadata = DataTableHeaders(
                NAME=TableColumn.init_flat(name="Name Header"),
                CHOICE=TableColumn.init_flat(field=mdl.choice),
            )
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
            DataRequiredHeaders = ["NAME"]
            DataRequiredValues = DataRequiredHeaders
            DataUniqueColumnConstraints = [["NAME"], ["UFONE", "UFTWO"]]
            DataColumnTypes = {"NAME": str, "UFONE": str, "UFTWO": str}
            FieldToDataHeaderKey = {
                "TestUCModel": {"name": "NAME", "uf1": "UFONE", "uf2": "UFTWO"}
            }
            Models = [mdl]
            DataColumnMetadata = DataTableHeaders(
                NAME=TableColumn.init_flat(name="Name Header"),
                UFONE=TableColumn.init_flat(name="UFOne Header"),
                UFTWO=TableColumn.init_flat(name="UFTwo Header"),
            )

            def load_data(self):
                return None

        return TestUCLoader

    @classmethod
    def generate_test_model_with_unique_constraint_condition(cls):
        # Model used for testing
        class TestUCConditionModel(Model):
            id = AutoField(primary_key=True)
            name = CharField()
            file = IntegerField(null=True, blank=True)
            opt_val = CharField(null=True, blank=True)

            class Meta:
                app_label = "loader"
                constraints = [
                    UniqueConstraint(
                        # Name must be unique when file is null (and not unique when file is not null)
                        fields=["name"],
                        name="unique_name_when_file_null",
                        condition=Q(file__isnull=True),
                    ),
                    UniqueConstraint(
                        # "file" unique whether name is null or not
                        fields=["file"],
                        name="unique_file",
                    ),
                ]

        return TestUCConditionModel

    @classmethod
    def generate_uccondition_test_loader(cls, mdl):
        class TestUCConditionLoader(TableLoader):
            DataSheetName = "test"
            DataTableHeaders = namedtuple(
                "DataTableHeaders", ["NAME", "FILE", "OPTVAL"]
            )
            DataHeaders = DataTableHeaders(NAME="Name", FILE="File", OPTVAL="Optval")
            DataRequiredHeaders = ["NAME"]
            DataRequiredValues = DataRequiredHeaders
            DataUniqueColumnConstraints = [["NAME", "FILE"]]
            DataColumnTypes = {"NAME": str, "FILE": int, "OPTVAL": str}
            FieldToDataHeaderKey = {
                "TestUCConditionModel": {
                    "name": "NAME",
                    "file": "FILE",
                    "opt_val": "OPTVAL",
                }
            }
            Models = [mdl]
            DataColumnMetadata = DataTableHeaders(
                NAME=TableColumn.init_flat(name="Name Header"),
                FILE=TableColumn.init_flat(name="File Header"),
                OPTVAL=TableColumn.init_flat(name="OptVal Header"),
            )

            def load_data(self):
                return None

        return TestUCConditionLoader

    @classmethod
    def generate_test_related_models(cls):
        class TestRelatedModel(Model):
            id = AutoField(primary_key=True)
            name = CharField(unique=True)

            class Meta:
                app_label = "loader"

        class TestManyModel(Model):
            id = AutoField(primary_key=True)
            name = CharField(unique=True)

            class Meta:
                app_label = "loader"

        # A model that links to other models in 1:1 and M:M
        class TestConnectedModel(Model):
            id = AutoField(primary_key=True)
            name = CharField(unique=True)
            optval = CharField(null=True, blank=True)
            related = ForeignKey(
                to="loader.TestRelatedModel",
                on_delete=RESTRICT,
                related_name="tcm",
            )
            manyfield = ManyToManyField(
                to="loader.TestManyModel",
                related_name="tcms",
            )

            class Meta:
                app_label = "loader"

        # Create the temporary/dynamic test tables in the test database
        with connection.schema_editor() as schema_editor:
            for model in [TestRelatedModel, TestManyModel, TestConnectedModel]:
                schema_editor.create_model(model)

        return TestConnectedModel, TestRelatedModel, TestManyModel

    @classmethod
    def generate_test_related_loader(cls, mdl):
        class TestRelatedLoader(TableLoader):
            DataSheetName = "test"
            DataTableHeaders = namedtuple(
                "DataTableHeaders", ["NAME", "OPTVAL", "RELATED", "MANY"]
            )
            DataHeaders = DataTableHeaders(
                NAME="Name", OPTVAL="OptVal", RELATED="Related", MANY="Many"
            )
            DataRequiredHeaders = ["NAME"]
            DataRequiredValues = DataRequiredHeaders
            DataUniqueColumnConstraints = [["NAME"]]
            FieldToDataHeaderKey = {
                mdl.__name__: {
                    "name": "NAME",
                    "optval": "OPTVAL",
                    "related": "RELATED",
                    "manyfield": "MANY",
                }
            }
            DataColumnMetadata = DataTableHeaders(
                NAME=TableColumn.init_flat(name="Name Header"),
                OPTVAL=TableColumn.init_flat(field=mdl.optval),
                RELATED=TableColumn.init_flat(name="Related Header"),
                MANY=TableColumn.init_flat(name="Many Header"),
            )
            Models = [mdl]

            def load_data(self):
                return None

        return TestRelatedLoader

    def __init__(self, *args, **kwargs):
        # The test model and loader must be created for the entire class or instance.  I chose "instance" so that I
        # didn't need to put the generators in a separate class or at __main__ level.  If you try and generate them in
        # each test, the model will get destroyed after the test and generating it again silently fails.
        self.test_model_class = self.generate_test_model()
        self.test_loader_class = self.generate_test_loader(self.test_model_class)
        self.test_uc_model_class = self.generate_test_model_with_unique_constraints()
        self.test_uc_loader_class = self.generate_uc_test_loader(
            self.test_uc_model_class
        )
        self.test_ucc_model_class = (
            self.generate_test_model_with_unique_constraint_condition()
        )
        self.test_ucc_loader_class = self.generate_uccondition_test_loader(
            self.test_uc_model_class
        )
        super().__init__(*args, **kwargs)

    # handle_load_db_errors Tests
    def test_handle_load_db_errors_ve_choice(self):
        """Ensures handle_load_db_errors packages ValidationError about invalid choices"""
        tl = self.test_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        tl.handle_load_db_errors(
            ValidationError("3 is not a valid choice"),
            self.test_model_class,
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
        tl = self.test_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = {"name": "test2", "choice": "2"}
        self.test_model_class.objects.create(**recdict)
        # An integrity error requires a conflict, so:
        recdict["choice"] = "1"
        tl.handle_load_db_errors(
            IntegrityError("duplicate key value violates unique constraint"),
            self.test_model_class,
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
        tl = self.test_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = {"name": None, "choice": "2"}
        # An integrity error requires a conflict, so:
        tl.handle_load_db_errors(
            IntegrityError('null value in column "name" violates not-null constraint'),
            self.test_model_class,
            recdict,
        )
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            RequiredValueError, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertIn(
            "Value required for 'name' in the load file data.  Record extracted from row: 2.",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "This error only happens when related data failed to load",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "Fixing errors above this one will fix this error",
            str(tl.aggregated_errors_object.exceptions[0]),
        )

    def test_handle_load_db_errors_catches_RequiredColumnValue(self):
        """Ensures that handle_load_db_errors catches RequiredColumnValue errors."""
        tl = self.test_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = {"name": "test", "choice": "2"}
        # An integrity error requires a conflict, so:
        tl.handle_load_db_errors(
            RequiredColumnValue("NAME"), self.test_model_class, recdict
        )
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            RequiredColumnValue, type(tl.aggregated_errors_object.exceptions[0])
        )

    def test_handle_load_db_errors_raises_1_ve_with_same_dict(self):
        """Ensures that multiple ValidationErrors about the same dict are only buffered by handle_load_db_errors once"""
        tl = self.test_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        tl.handle_load_db_errors(
            ValidationError("3 is not a valid choice"),
            self.test_model_class,
            {"name": "test", "choice": "3"},
        )
        tl.set_row_index(1)
        tl.handle_load_db_errors(
            ValidationError("3 is not a valid choice"),
            self.test_model_class,
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
        tl = self.test_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = None
        # An integrity error requires a conflict, so:
        tl.handle_load_db_errors(
            AttributeError("Error"), self.test_model_class, recdict
        )
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            AttributeError, type(tl.aggregated_errors_object.exceptions[0])
        )

    def test_handle_load_db_errors_unique_constraint_condition(self):
        """Ensures that a unique constraint's condition is used when looking for conflicts.

        This indirectly tests get_inconsistencies, get_offending_unique_constraint_recs, and
        _filter_multiple_unique_constraint_matches
        """
        tl = self.test_ucc_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)

        # Create 2 existing records
        erec1 = self.test_ucc_model_class.objects.create(**{"name": "a"})
        erec1.full_clean()
        erec2 = self.test_ucc_model_class.objects.create(
            **{"name": "a", "file": 1, "opt_val": "b"}
        )
        erec2.full_clean()

        # Send a simulated IntegrityError to handle_load_db_errors
        newrecdict = {"name": "a", "file": 1}
        tl.handle_load_db_errors(
            IntegrityError("duplicate key value violates unique constraint"),
            self.test_ucc_model_class,
            newrecdict,
        )

        # Ensure 1 ConflictingValueError was buffered
        self.assertEqual(
            1,
            len(tl.aggregated_errors_object.exceptions),
            msg=(
                "Expected 1 ConflictingValueError, got: "
                f"{[type(e).__name__ for e in tl.aggregated_errors_object.exceptions]}"
            ),
        )
        self.assertIsInstance(
            tl.aggregated_errors_object.exceptions[0],
            ConflictingValueError,
            msg=(
                "Expected 1 ConflictingValueError, got: "
                f"{[type(e).__name__ for e in tl.aggregated_errors_object.exceptions]}"
            ),
        )

        # Ensure that the record that handle_load_db_errors identified is the correct one (it had to have used the
        # condition or else it would have found erec1 using the first constraint)
        cve: ConflictingValueError = tl.aggregated_errors_object.exceptions[0]
        self.assertEquivalent(erec2, cve.rec)

    def test_handle_load_db_errors_ambiguous_unique_constraint_condition(self):
        """Ensures that when multiple unique constraints match, a MultipleConflictingValueMatches is buffered.

        This indirectly tests get_inconsistencies, get_offending_unique_constraint_recs, and
        _filter_multiple_unique_constraint_matches
        """
        tl = self.test_ucc_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)

        # Create 2 existing records
        erec1 = self.test_ucc_model_class.objects.create(**{"name": "a"})
        erec1.full_clean()
        erec2 = self.test_ucc_model_class.objects.create(
            **{"name": "a", "file": 1, "opt_val": "b"}
        )
        erec2.full_clean()

        # Supplying ["opt_val": None] is technically unsupported by the TableLoader when using get_or_create, but that
        # is the one condition I found that would trigger the multiple unique constraint match issue.  There are
        # probably other ways to trigger it.
        newrecdict = {"name": "a", "file": 1, "opt_val": None}

        # Send a simulated IntegrityError to handle_load_db_errors
        tl.handle_load_db_errors(
            IntegrityError("duplicate key value violates unique constraint"),
            self.test_ucc_model_class,
            newrecdict,
        )

        # Ensure 1 ConflictingValueError was buffered
        self.assertEqual(
            1,
            len(tl.aggregated_errors_object.exceptions),
            msg=(
                "Expected 1 ConflictingValueError, got: "
                f"{[type(e).__name__ for e in tl.aggregated_errors_object.exceptions]}"
            ),
        )
        self.assertIsInstance(
            tl.aggregated_errors_object.exceptions[0],
            MultipleConflictingValueMatches,
        )

        # Ensure that the record that handle_load_db_errors identified is the correct one (it had to have used the
        # condition or else it would have found erec1 using the first constraint)
        mcvm: MultipleConflictingValueMatches = tl.aggregated_errors_object.exceptions[
            0
        ]
        self.assertTrue(any(erec2 == r for r, _, _ in mcvm.recs_diffs_cves))

    def test_get_inconsistencies_optional_and_related_fields(self):
        """Ensures that get_inconsistencies finds differences in optional fields when a user adds data to existing
        records and does not include differences with many-related models.  See PR #1713.
        """
        # TODO: See TODO note in the constructor
        (connected_model, related_model, many_model) = (
            self.generate_test_related_models()
        )
        test_related_loader = self.generate_test_related_loader(connected_model)

        # Create a loader object from which we will call get_inconsistencies
        tl = test_related_loader()

        # Create existing records (that ostensibly caused an IntegrityError exception), so that we have a rec to supply
        # to get_inconsistencies.  The end goal here is a 'connected_model' record that links to a 1:1 relation and an
        # M:M relation
        rmo = related_model.objects.create(name="test")
        mmo = many_model.objects.create(name="test")
        recdict = {"name": "test1", "optval": "a", "related": rmo}
        rec = connected_model.objects.create(**recdict)
        rec.manyfield.add(mmo)

        # An integrity error requires a conflict, and we want to test a difference with optional values, so:
        del recdict["optval"]

        # Get the inconsistencies - This should identify only the optval difference
        differences, num_same = tl.get_inconsistencies(rec, recdict)

        # Now test that both the difference in optval is reported and that there is no difference reported with respect
        # to the many-related model being null
        self.assertEquivalent({"optval": {"orig": "a", "new": None}}, differences)
        # Test that the number of matching field values between rec_dict and the actual existing record is correct
        self.assertEqual(2, num_same)

        # Add on a first-order check that a single ConflictingValueError was buffered
        self.assertEqual(
            ConflictingValueError, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(rec, tl.aggregated_errors_object.exceptions[0].rec)

    def test_get_inconsistencies_optional_value_added(self):
        """Ensures that get_inconsistencies finds differences in optional fields when a user adds data to existing
        records and does not include differences with many-related models.  See PR #1713.
        """
        # TODO: See TODO note in the constructor
        (connected_model, related_model, many_model) = (
            self.generate_test_related_models()
        )
        test_related_loader = self.generate_test_related_loader(connected_model)

        # Create a loader object from which we will call get_inconsistencies
        tl = test_related_loader()

        # Create existing records (that ostensibly caused an IntegrityError exception), so that we have a rec to supply
        # to get_inconsistencies.  The end goal here is a 'connected_model' record that links to a 1:1 relation and an
        # M:M relation
        rmo = related_model.objects.create(name="test2")
        mmo = many_model.objects.create(name="test2")
        recdict = {"name": "test2", "related": rmo}
        rec = connected_model.objects.create(**recdict)
        rec.manyfield.add(mmo)

        # An integrity error requires a conflict, and we want to test a difference with optional values, so:
        recdict["optval"] = "b"

        # Get the inconsistencies - This should identify only the optval difference
        differences, num_same = tl.get_inconsistencies(rec, recdict)

        # Now test that both the difference in optval is reported and that there is no difference reported with respect
        # to the many-related model being null
        self.assertEquivalent({"optval": {"orig": None, "new": "b"}}, differences)
        # Test that the number of matching field values between rec_dict and the actual existing record is correct
        self.assertEqual(2, num_same)

        # Add on a first-order check that a single ConflictingValueError was buffered
        self.assertEqual(
            ConflictingValueError, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(rec, tl.aggregated_errors_object.exceptions[0].rec)

    def test_check_for_inconsistencies_case1(self):
        """Ensures that check_for_inconsistencies correctly packages conflicts"""
        tl = self.test_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = {"name": "test2", "choice": "2"}
        self.test_model_class.objects.create(**recdict)
        rec = self.test_model_class.objects.get(**recdict)
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

    def test_check_for_inconsistencies_case2(self):
        """Ensures that check_for_inconsistencies correctly packages conflicts despite type differences, even if the
        field is not mapped or the type is not recorded."""
        tl = self.test_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        recdict = {"name": "test2", "choice": "2"}
        self.test_model_class.objects.create(**recdict)
        rec = self.test_model_class.objects.get(**recdict)
        # The loading code automatically casts, but when check_for_inconsistencies runs, it gets the uncast value,
        # where Excel or pandas autodetected a type (inaccurately)
        recdict["choice"] = 2
        errfound = tl.check_for_inconsistencies(rec, recdict)
        self.assertFalse(errfound)
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(1, tl.aggregated_errors_object.num_warnings)
        self.assertFalse(tl.aggregated_errors_object.is_fatal)
        self.assertFalse(tl.aggregated_errors_object.exceptions[0].is_fatal)
        self.assertIsInstance(
            tl.aggregated_errors_object.exceptions[0], DBFieldVsFileColDeveloperWarning
        )
        self.assertIn(
            "database ('2', a 'str') and the type of the value from the file ('2', a 'int')",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "cast to a string to compare and found to be equal",
            str(tl.aggregated_errors_object.exceptions[0]),
        )

    def test_check_for_inconsistencies_case3(self):
        """Ensures that check_for_inconsistencies correctly packages conflicts despite type differences without warning
        if the field is mapped and the type is recorded."""
        tl = self.test_uc_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        recdict = {"name": "test2", "uf1": "2"}
        self.test_uc_model_class.objects.create(**recdict)
        rec = self.test_uc_model_class.objects.get(**recdict)
        # The loading code automatically casts, but when check_for_inconsistencies runs, it gets the uncast value,
        # where Excel or pandas autodetected a type (inaccurately)
        recdict["uf1"] = 2
        errfound = tl.check_for_inconsistencies(rec, recdict)
        self.assertFalse(
            errfound,
            msg=(
                "Expected 0 exceptions, got: "
                f"{[type(e).__name__ + ': ' + str(e) for e in tl.aggregated_errors_object.exceptions]}"
            ),
        )
        # No error when the field is mapped to a column and the column's type is recorded
        self.assertEqual(0, len(tl.aggregated_errors_object.exceptions))

    def test_check_for_inconsistencies_case4(self):
        """Tests developer warnings about database/file value type arise when searching for inconsistencies.

        Requirements tested (from GitHub issue #1662):
            1. Programmer warnings must not be user-facing.
            8. A warning is buffered when the compared database field and file column value differ by type (other than
               NoneType)

        This test creates a model record with a str value in a field and calls check_for_inconsistencies, supplying it a
        rec_dict that has an int for that field.
        """
        table_loader = self.test_uc_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        table_loader.set_row_index(0)  # Converted to row 2 (header line is 1)
        recdict = {
            "name": "test4",
            "uf1": "1",
            "uf2": "2",
            "opt_str": "letters",  # This is the value we are testing
        }
        self.test_uc_model_class.objects.create(**recdict)
        rec = self.test_uc_model_class.objects.get(**recdict)

        # NOTE: The loading code automatically casts when it knows the column type, but when when that's not set in the
        # loader class, when check_for_inconsistencies runs, it gets the uncast value, where Excel or pandas
        # autodetected a type (inaccurately), e.g. the user enters a number for a string value in the DB.
        recdict["opt_str"] = 2
        errfound = table_loader.check_for_inconsistencies(rec, recdict)

        # Test Req 8. See docstring
        self.assertTrue(errfound)
        self.assertEqual(2, len(table_loader.aggregated_errors_object.exceptions))
        self.assertTrue(
            table_loader.aggregated_errors_object.exception_type_exists(
                DBFieldVsFileColDeveloperWarning
            )
        )
        # This error is also expected
        self.assertTrue(
            table_loader.aggregated_errors_object.exception_type_exists(
                ConflictingValueError
            )
        )

        self.assertIsInstance(
            table_loader.aggregated_errors_object.exceptions[0],
            DBFieldVsFileColDeveloperWarning,
        )
        dev_warning: DBFieldVsFileColDeveloperWarning = (
            table_loader.aggregated_errors_object.exceptions[0]
        )

        # Test Req 1. See docstring
        self.assertFalse(dev_warning.is_error)
        self.assertFalse(dev_warning.is_fatal)

    def test_check_for_inconsistencies_case5(self):
        """Tests developer warnings about database/file value type do not arise when one of the values is 'None'.

        Requirements tested (from GitHub issue #1662):
            7. This warning must no be risen when one of the compared values is None

        This test creates a model record with a str value in a field and calls check_for_inconsistencies, supplying it a
        rec_dict that has a NoneType for that field.
        """
        table_loader1 = self.test_uc_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        table_loader1.set_row_index(0)  # Converted to row 2 (header line is 1)

        # Case 1: DB value is null
        recdict = {
            "name": "test4",
            "uf1": "1",
            "uf2": "2",
            # Omitting opt_str makes it null in the DB
        }
        self.test_uc_model_class.objects.create(**recdict)
        rec = self.test_uc_model_class.objects.get(**recdict)

        # NOTE: The loading code automatically casts when it knows the column type, but when when that's not set in the
        # loader class, when check_for_inconsistencies runs, it gets the uncast value, where Excel or pandas
        # autodetected a type (inaccurately), e.g. the user enters a number for a string value in the DB.
        recdict["opt_str"] = "2"
        errfound = table_loader1.check_for_inconsistencies(rec, recdict)

        # We still expect a ConflictingValueError
        self.assertTrue(errfound)
        self.assertEqual(1, len(table_loader1.aggregated_errors_object.exceptions))
        self.assertTrue(
            table_loader1.aggregated_errors_object.exception_type_exists(
                ConflictingValueError
            )
        )
        self.assertFalse(
            table_loader1.aggregated_errors_object.exception_type_exists(
                DBFieldVsFileColDeveloperWarning
            )
        )

        # Case 2: File value is None
        table_loader2 = self.test_uc_loader_class()
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        table_loader2.set_row_index(0)  # Converted to row 2 (header line is 1)

        rec.opt_str = "2"
        rec.full_clean()
        rec.save()
        recdict["opt_str"] = None
        errfound = table_loader2.check_for_inconsistencies(rec, recdict)

        self.assertTrue(errfound)
        print(table_loader2.aggregated_errors_object.exceptions)
        self.assertEqual(1, len(table_loader2.aggregated_errors_object.exceptions))
        self.assertTrue(
            table_loader2.aggregated_errors_object.exception_type_exists(
                ConflictingValueError
            )
        )
        self.assertFalse(
            table_loader2.aggregated_errors_object.exception_type_exists(
                DBFieldVsFileColDeveloperWarning
            )
        )

    def test_model_field_to_column_type(self):
        tucl = self.test_uc_loader_class()
        type_returned = tucl.model_field_to_column_type(
            self.test_uc_model_class.__name__, "name"
        )
        self.assertEqual(str, type_returned)
        type_returned2 = tucl.model_field_to_column_type(
            self.test_uc_model_class.__name__, "unrecorded_field"
        )
        self.assertIsNone(type_returned2)

    # Method tests
    def test_get_load_stats(self):
        tl = self.test_loader_class()
        ls = tl.get_load_stats()
        self.assertDictEqual(
            {
                self.test_model_class.__name__: {
                    "created": 0,
                    "existed": 0,
                    "deleted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "errored": 0,
                    "warned": 0,
                }
            },
            ls,
        )

    def test_get_models(self):
        tl = self.test_loader_class()
        self.assertEqual([self.test_model_class], tl.get_models())

    def test_created(self):
        tl = self.test_loader_class()
        tl.created()
        self.assertDictEqual(
            {
                self.test_model_class.__name__: {
                    "created": 1,
                    "existed": 0,
                    "deleted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "errored": 0,
                    "warned": 0,
                }
            },
            tl.record_counts,
        )

    def test_existed(self):
        tl = self.test_loader_class()
        tl.existed()
        self.assertDictEqual(
            {
                self.test_model_class.__name__: {
                    "created": 0,
                    "existed": 1,
                    "deleted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "errored": 0,
                    "warned": 0,
                }
            },
            tl.record_counts,
        )

    def test_deleted(self):
        tl = self.test_loader_class()
        tl.deleted()
        self.assertDictEqual(
            {
                self.test_model_class.__name__: {
                    "created": 0,
                    "existed": 0,
                    "deleted": 1,
                    "updated": 0,
                    "skipped": 0,
                    "errored": 0,
                    "warned": 0,
                }
            },
            tl.record_counts,
        )

    def test_skipped(self):
        tl = self.test_loader_class()
        tl.skipped()
        self.assertDictEqual(
            {
                self.test_model_class.__name__: {
                    "created": 0,
                    "existed": 0,
                    "deleted": 0,
                    "updated": 0,
                    "skipped": 1,
                    "errored": 0,
                    "warned": 0,
                }
            },
            tl.record_counts,
        )

    def test_errored(self):
        tl = self.test_loader_class()
        tl.errored()
        self.assertDictEqual(
            {
                self.test_model_class.__name__: {
                    "created": 0,
                    "existed": 0,
                    "deleted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "errored": 1,
                    "warned": 0,
                }
            },
            tl.record_counts,
        )

    def test_warned(self):
        tl = self.test_loader_class()
        tl.warned()
        self.assertDictEqual(
            {
                self.test_model_class.__name__: {
                    "created": 0,
                    "existed": 0,
                    "deleted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "errored": 0,
                    "warned": 1,
                }
            },
            tl.record_counts,
        )

    def test__get_model_name(self):
        tl = self.test_loader_class()
        self.assertEqual(self.test_model_class.__name__, tl._get_model_name())

        # Check exception when models > 1
        class TestMultiModelLoader(self.test_loader_class):
            # Note, this does not define all columns. Just need any values to prevent errors.
            Models = [self.test_model_class, self.test_uc_model_class]

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
            DataRequiredHeaders = ["NAME"]
            DataRequiredValues = DataRequiredHeaders
            DataDefaultValues = DataTableHeaders(NAME="test", CHOICE="1")
            DataUniqueColumnConstraints = [["NAME"]]
            FieldToDataHeaderKey = {"TestModel": {"name": "NAME", "choice": "CHOICE"}}
            Models = [self.test_model_class]
            DataColumnMetadata = DataTableHeaders(
                NAME=TableColumn.init_flat(name="Name Header"),
                CHOICE=TableColumn.init_flat(name="Choice Header"),
            )

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
        tl = self.test_loader_class(df=pddata)
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
        tl = self.test_loader_class()
        tl.skip_row_indexes = [1, 9, 22]
        self.assertEqual([1, 9, 22], tl.get_skip_row_indexes())

    def test_add_skip_row_index(self):
        tl = self.test_loader_class()
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
            DataRequiredHeaders = ["NAME"]
            DataRequiredValues = DataRequiredHeaders
            DataUniqueColumnConstraints = [["NAME"]]
            FieldToDataHeaderKey = {"TestModel": {"name": "NAME", "choice": "CHOICE"}}
            Models = [self.test_model_class]
            DataColumnMetadata = DataTableHeaders(
                NAME=TableColumn.init_flat(name="Name Header"),
                CHOICE=TableColumn.init_flat(name="Choice Header"),
            )

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

    def test_check_unique_constraints(self, with_arg=False):
        pddata = pd.DataFrame.from_dict(
            {
                "Name": ["A", "A", "B", "C"],
                "uf1": ["x", "x", "y", "y"],
                "uf2": ["1", "2", "3", "3"],
            },
        )
        if with_arg:
            tucl = self.test_uc_loader_class()
            tucl.check_unique_constraints(pddata)
        else:
            tucl = self.test_uc_loader_class(df=pddata)
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

    def test_check_unique_constraints_with_arg(self):
        self.test_check_unique_constraints(True)

    def test_header_key_to_name(self):
        tucl = self.test_uc_loader_class()
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

    def test_get_column_types_default(self):
        tucl = self.test_uc_loader_class()
        td = tucl.get_column_types()
        expected = {
            "Name": str,
            "uf1": str,
            "uf2": str,
        }
        self.assertEqual(expected, td)

    def test_isnamedtuple(self):
        nt = self.test_loader_class.DataTableHeaders(
            NAME=True,
            CHOICE=True,
        )
        self.assertTrue(self.test_loader_class.isnamedtuple(nt))

    def test_initialize_metadata(self):
        tl = self.test_loader_class()
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
            DataColumnMetadata = DataTableHeaders(
                NAME=TableColumn.init_flat(name="Name Header"),
                CHOICE=TableColumn.init_flat(name="Choice Header"),
            )

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
                "\tattribute [TestInvalidLoader.DataHeaders] namedtuple required, NoneType set\n"
                "\tattribute [TestInvalidLoader.DataRequiredHeaders] N-dimensional list of strings required, "
                "but contains ['NoneType']\n"
                "\tattribute [TestInvalidLoader.DataRequiredValues] N-dimensional list of strings required, "
                "but contains ['NoneType']\n"
                "\tattribute [TestInvalidLoader.DataUniqueColumnConstraints] list required, NoneType set\n"
                "\tattribute [TestInvalidLoader.FieldToDataHeaderKey] dict required, NoneType set"
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
            DataRequiredHeaders = ["TEST"]
            DataRequiredValues = DataRequiredHeaders
            DataColumnTypes = {"TEST": str}
            DataDefaultValues = DataTableHeaders(TEST=5)
            DataUniqueColumnConstraints = [["TEST"]]
            FieldToDataHeaderKey = {"TestModel": {"name": "TEST"}}
            Models = [TestModel]
            DataColumnMetadata = DataTableHeaders(
                TEST=TableColumn.init_flat(name="Test Header")
            )

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
        tl = self.test_loader_class()
        # initialize_metadata is called in the constructor
        self.assertEqual(tl.DataDefaultValues, tl.get_defaults())

    def test_get_header_keys(self):
        tl = self.test_loader_class()
        self.assertEqual(list(tl.DataHeaders._asdict().keys()), tl.get_header_keys())

    def test_get_pretty_headers_defaults(self):
        tl = self.test_loader_class()
        self.assertEqual("Name*, Choice (* = Required)", tl.get_pretty_headers())

    def test_get_headers(self):
        tl = self.test_loader_class()
        self.assertEqual(tl.DataHeaders, tl.get_headers())

    def test_set_row_index(self):
        tl = self.test_loader_class()
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
        outdict, outlist = self.test_loader_class.get_one_column_dupes(
            pddata, "col2", ignore_row_idxs=[2]
        )
        self.assertEqual({"x": [0, 1]}, outdict)
        self.assertEqual([0, 1], outlist)

    def test_get_unique_constraints(self):
        unique_constraints: List[UniqueConstraint] = (
            self.test_loader_class.get_unique_constraints(self.test_uc_model_class)
        )
        self.assertEqual(1, len(unique_constraints))
        self.assertEqual(
            (
                "uf1",
                "uf2",
            ),
            unique_constraints[0].fields,
        )

    def test_get_non_auto_model_fields(self):
        expected = [
            "name",
            "uf1",
            "uf2",
            "opt_str",
        ]
        field_names = [
            f.name if hasattr(f, "name") else f.field_name
            for f in self.test_loader_class.get_non_auto_model_fields(
                self.test_uc_model_class
            )
        ]
        self.assertEqual(expected, field_names)

    def test_get_enumerated_fields(self):
        field_names = self.test_loader_class.get_enumerated_fields(
            self.test_model_class
        )
        self.assertEqual(["choice"], field_names)

    def test_get_unique_fields(self):
        field_names = self.test_loader_class.get_unique_fields(self.test_uc_model_class)
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
                "Can't instantiate abstract class TestEmptyLoader with abstract methods DataColumnMetadata, "
                "DataHeaders, DataRequiredHeaders, DataRequiredValues, DataSheetName, DataTableHeaders, "
                "DataUniqueColumnConstraints, FieldToDataHeaderKey, Models, load_data"
            ),
            str(ar.exception),
        )

    def test_is_skip_row(self):
        tl = self.test_loader_class()
        tl.skip_row_indexes = [3]
        self.assertTrue(tl.is_skip_row(3))
        self.assertFalse(tl.is_skip_row(0))

    def test_tableheaders_to_dict_by_header_name(self):
        nt = self.test_loader_class.DataTableHeaders(
            NAME=True,
            CHOICE=True,
        )
        tl = self.test_loader_class()
        self.assertTrue(
            {
                "Name": True,
                "Choice": True,
            },
            tl.tableheaders_to_dict_by_header_name(nt),
        )

    # Test that load_wrapper
    def test_load_wrapper_does_not_nest_AggregatedErrors(self):
        class TestNestedAesLoader(self.test_loader_class):
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
        tnal = TestNestedAesLoader(df=pddata)

        with self.assertRaises(AggregatedErrors) as ar:
            tnal.load_data()
        aes = ar.exception
        self.assertEqual(2, len(aes.exceptions))
        self.assertEqual(ValueError, type(aes.exceptions[0]))
        self.assertEqual("WARNING", str(aes.exceptions[0]))
        self.assertEqual(ValueError, type(aes.exceptions[1]))
        self.assertEqual("Nested", str(aes.exceptions[1]))

    def test_load_wrapper_summarizes_ConflictingValueErrors(self):
        class TestMultiCVELoader(self.test_loader_class):
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
        tmcl = TestMultiCVELoader(df=pddata)

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
            ),
            str(aes.exceptions[0]),
        )

    def test_load_wrapper_summarizes_RequiredValueErrors(self):
        class TestMultiRVELoader(self.test_loader_class):
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
        tmrl = TestMultiRVELoader(df=pddata)

        with self.assertRaises(AggregatedErrors) as ar:
            tmrl.load_data()
        aes = ar.exception
        # The 2 exceptions should be summarized as 1
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(RequiredValueErrors, type(aes.exceptions[0]))
        self.assertIn(
            "Required values found missing during loading", str(aes.exceptions[0])
        )
        self.assertIn("Column: [Name] on row(s): 2", str(aes.exceptions[0]))
        self.assertIn("Column: [Choice] on row(s): 3", str(aes.exceptions[0]))
        self.assertIn(
            "Errors like this only happen when related data failed to load",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "evidenced by the fact that the indicated column/rows have values",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "Fixing errors above this will fix this error", str(aes.exceptions[0])
        )

    def test_load_wrapper_summarizes_DuplicateValueErrors(self):
        class TestMultiDVELoader(self.test_loader_class):
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
        tmdl = TestMultiDVELoader(df=pddata)

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
        class TestMultiRCVLoader(self.test_loader_class):
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
        tmrl = TestMultiRCVLoader(df=pddata)

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
        class TestDeferedLoader(self.test_loader_class):
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
        tdl = TestDeferedLoader(df=pddata, defer_rollback=True)

        # There should be no record found initially
        self.assertEqual(
            0, self.test_model_class.objects.filter(name="A", choice=1).count()
        )

        # Expect an AggregatedErrors exception
        with self.assertRaises(AggregatedErrors) as ar:
            tdl.load_data()

        # To avoid overlooking another error, check the exception
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(ValueError, type(aes.exceptions[0]))
        self.assertEqual("Test", str(aes.exceptions[0]))

        # This get should not cause an exception because the record should have been created and kept
        self.test_model_class.objects.get(name="A", choice=1)

    def test_load_wrapper_handles_DryRun(self):
        class TestDryRunLoader(self.test_loader_class):
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
        tdl = TestDryRunLoader(df=pddata, dry_run=True)

        # There should be no record found initially
        self.assertEqual(
            0, self.test_model_class.objects.filter(name="A", choice=1).count()
        )

        # Expect a DryRun exception
        with self.assertRaises(DryRun):
            tdl.load_data()

        # Nothing should load in a dry run
        self.assertEqual(
            0, self.test_model_class.objects.filter(name="A", choice=1).count()
        )

    def test_load_wrapper_preserves_return(self):
        class TestStatsLoader(self.test_loader_class):
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
        tsl = TestStatsLoader(df=pddata)

        retval = tsl.load_data()

        self.assertEqual(42, retval)

    def test_set_headers(self):
        tl = self.test_loader_class()
        tl.set_headers({"NAME": "X", "CHOICE": "Selection"})
        expected = self.test_loader_class.DataTableHeaders(NAME="X", CHOICE="Selection")
        self.assertEqual(expected, tl.headers)

    def test__merge_headers(self):
        # User-supplied headers (i.e. from the command line) trump derived class custom headers
        tl = self.test_loader_class(
            user_headers={"NAME": "UsersDumbNameHeader", "CHOICE": "UsersChoice"}
        )
        tl._merge_headers({"NAME": "X", "CHOICE": "Selection"})
        expected = self.test_loader_class.DataTableHeaders(
            NAME="UsersDumbNameHeader", CHOICE="UsersChoice"
        )
        self.assertEqual(expected, tl.headers)

    def test_set_defaults(self):
        tl = self.test_loader_class()
        tl.set_defaults({"NAME": "one", "CHOICE": "two"})
        expected = self.test_loader_class.DataTableHeaders(NAME="one", CHOICE="two")
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
        tl = self.test_loader_class(df=df, data_sheet="Test", defaults_df=def_df)
        tl._merge_defaults({"NAME": "D"})
        expected = self.test_loader_class.DataTableHeaders(NAME="G", CHOICE=None)
        self.assertEqual(expected, tl.defaults)

    def test_isnamedtupletype(self):
        self.assertTrue(
            self.test_loader_class.isnamedtupletype(
                self.test_loader_class.DataTableHeaders
            )
        )
        self.assertFalse(
            self.test_loader_class.isnamedtupletype(self.test_loader_class.DataHeaders)
        )
        self.assertTrue(
            self.test_loader_class.isnamedtupletype(
                self.test_loader_class.DefaultsTableHeaders
            )
        )
        self.assertFalse(self.test_loader_class.isnamedtupletype(1))

    def test_check_dataframe_headers(self):
        df = pd.DataFrame.from_dict(
            {
                "Wrong": ["A", "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tl = self.test_loader_class(df=df)
        with self.assertRaises(AggregatedErrors) as ar:
            tl.check_dataframe_headers()
        aes = ar.exception
        self.assertEqual(2, len(aes.exceptions))
        self.assertEqual(RequiredHeadersError, type(aes.exceptions[0]))
        self.assertIn("Name", str(aes.exceptions[0]))
        self.assertEqual(UnknownHeaders, type(aes.exceptions[1]))
        self.assertIn("Wrong", str(aes.exceptions[1]))

        tl2 = self.test_loader_class()
        tl2.check_dataframe_headers()
        aes2 = tl2.aggregated_errors_object
        self.assertEqual(1, len(aes2.exceptions))
        self.assertEqual(NoLoadData, type(aes2.exceptions[0]))

    def test_check_dataframe_headers_defaults(self):
        tl = self.test_loader_class()
        # Setting tl.defaults_df manually so that it's not automatically checked via the constructor
        tl.defaults_df = pd.DataFrame.from_dict(
            {
                "Sheet Name": ["test"],
                "Column Header": ["Name"],
                "Wrong header": ["value"],
            },
        )
        with self.assertRaises(AggregatedErrors) as ar:
            tl.check_dataframe_headers(reading_defaults=True)
        aes = ar.exception
        self.assertEqual(2, len(aes.exceptions))
        self.assertEqual(RequiredHeadersError, type(aes.exceptions[0]))
        self.assertIn("Default Value", str(aes.exceptions[0]))
        self.assertEqual(UnknownHeaders, type(aes.exceptions[1]))
        self.assertIn("Wrong header", str(aes.exceptions[1]))

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
        tl = self.test_loader_class(df=df, data_sheet="Test", defaults_df=def_df)
        # Derived class defaults (just to ensure these aren't included)
        tl.set_defaults({"NAME": "one", "CHOICE": "two"})
        expected = {"Name": "G"}
        self.assertEqual(expected, tl.get_user_defaults())

    def test_header_name_to_key(self):
        tl = self.test_loader_class()
        self.assertEqual({"NAME": 2}, tl.header_name_to_key({"Name": 2}))

    def test_get_pretty_headers_headers(self):
        tl = self.test_loader_class()
        self.assertEqual(
            "MyName*, MyChoice (* = Required)",
            tl.get_pretty_headers(
                headers=self.test_loader_class.DataTableHeaders(
                    NAME="MyName",
                    CHOICE="MyChoice",
                )
            ),
        )

    def test_get_pretty_headers_markers(self):
        tl = self.test_loader_class()
        self.assertEqual("Name, Choice", tl.get_pretty_headers(markers=False))

    def test_get_pretty_headers_legend(self):
        tl = self.test_loader_class()
        self.assertEqual("Name*, Choice", tl.get_pretty_headers(legend=False))

    def test_get_pretty_headers_reqd_only(self):
        tl = self.test_loader_class()
        self.assertEqual("Name* (* = Required)", tl.get_pretty_headers(reqd_only=True))

    def test_get_pretty_headers_reqd_spec(self):
        tl = self.test_loader_class()
        self.assertEqual(
            "Choice*, Name (* = Required)", tl.get_pretty_headers(reqd_spec=["Choice"])
        )

    def test_get_pretty_headers_all_reqd(self):
        tl = self.test_loader_class()
        self.assertEqual(
            "(Name, Choice)^ (^ = Any Required)",
            tl.get_pretty_headers(reqd_spec=["Name", "Choice"], all_reqd=False),
        )

    def test__get_pretty_headers_helper(self):
        """Test that _get_pretty_headers_helper returns a decorated string of headers."""

        tl = self.test_loader_class()

        # a required
        self.assertEqual(
            "a*",
            tl._get_pretty_headers_helper(["a"]),
        )

        # a required (pointless _anded=False is reversed)
        self.assertEqual(
            "a*",
            tl._get_pretty_headers_helper(["a"], _anded=False),
        )

        # a, b, and c required
        self.assertEqual(
            "a*, b*, c*",
            tl._get_pretty_headers_helper(["a", "b", "c"]),
        )

        # a and (b or c) required
        self.assertEqual(
            "a*, (b, c)^",
            tl._get_pretty_headers_helper(["a", ["b", "c"]]),
        )

        # a and (b or c) required - but no all-required markers
        self.assertEqual(
            "a, (b, c)^",
            tl._get_pretty_headers_helper(["a", ["b", "c"]], markers=False),
        )

        # Either a or (b and c) required
        self.assertEqual(
            "(a, (b, c)*)^",
            tl._get_pretty_headers_helper(["a", ["b", "c"]], _anded=False),
        )

        # (a or b) and (b or c) required
        self.assertEqual(
            "(a, b)^, (b, c)^",
            tl._get_pretty_headers_helper([["a", "b"], ["b", "c"]]),
        )

        # (a and b) or (b and c) required
        self.assertEqual(
            "((a, b)*, (b, c)*)^",
            tl._get_pretty_headers_helper([["a", "b"], ["b", "c"]], _anded=False),
        )

    def test_get_missing_headers(self):
        """Test that get_missing_headers returns missing required headers (if any)."""

        tl = self.test_loader_class()

        # c and ((a and (d or c)) or (a and e)) are required
        # a and c supplied
        # Requirements met - None are missing, so None and True (irrelevant) are returned
        self.assertEqual(
            (None, True),
            tl.get_missing_headers(
                supd_headers=["a", "c"],
                reqd_headers=["c", [["a", ["d", "c"]], ["a", "e"]]],
            ),
        )

        # Setting the outer group to or'ed with _anded=False
        # b or (d or e)  # Note that the [[]] is and(or(...)), which equates to or(...) when the outer group is or
        # a and c supplied
        # Requirements not met: supply either b, d, or e to meet the requirements
        self.assertEqual(
            (["b", "d", "e"], False),
            tl.get_missing_headers(
                supd_headers=["a", "c"],
                reqd_headers=["b", [["d", "e"]]],
                _anded=False,
            ),
        )

        # c and (either (a and d) or (a and e)) are required
        # a and c supplied
        # Requirements not met: supply either d or e to meet the requirements
        self.assertEqual(
            (["d", "e"], False),
            tl.get_missing_headers(
                supd_headers=["a", "c"],
                reqd_headers=["c", [["a", "d"], ["a", "e"]]],
            ),
        )

        # c and (either a or (a and f)) are required
        # a and c supplied
        # Requirements met
        self.assertEqual(
            (None, True),
            tl.get_missing_headers(
                supd_headers=["a", "c"],
                reqd_headers=["c", ["a", ["d", "f"]]],
            ),
        )

        # c and (either (a and (d or f)) or (a and e)) are required
        # a and c supplied
        # Requirements not met: supply either d, f, or e to meet the requirements
        self.assertEqual(
            (["d", "f", "e"], False),
            tl.get_missing_headers(
                supd_headers=["a", "c"],
                reqd_headers=["c", [["a", ["d", "f"]], ["a", "e"]]],
            ),
        )

        # c and (either (a and (d or f)) or (a and e)) and g are required
        # a and c supplied
        # Requirements not met: supply either d, f, or e - and g to meet the requirements
        self.assertEqual(
            ([["d", "f", "e"], "g"], True),
            tl.get_missing_headers(
                supd_headers=["a", "c"],
                reqd_headers=["c", [["a", ["d", "f"]], ["a", "e"]], "g"],
            ),
        )

    def test_duplicate_required_header_in_or_group(self):
        """This test was added based on an error encountered when handling changes needed in the load_infusates branch,
        due to a rebase.  2 tests of files whose headers should have changed lead to a weird error about missing and
        unknown headers.  The header requirements were 2 or'ed and-groups, where there were 2 shared headers in each
        and-group, and one of those common headers was missing.  The resulting report of missing headers had duplicates
        listed.  I changed the code to only add a header if it wasn't already in the current group being added-to.  This
        test checks that the output is consolidated.  This test failed before the fix was implemented.
        """
        tl = self.test_loader_class()
        self.assertEqual(
            (["a"], True),
            tl.get_missing_headers(
                supd_headers=["f", "b", "c", "d", "e"],
                reqd_headers=[[["a", "b", "c", "d"], ["a", "e", "d"]]],
            ),
        )

    def test_header_keys_to_names(self):
        """Test that header_keys_to_names converts an N-dimensional list of header keys to an N-dimensional list of
        header names.
        """
        tl = self.test_loader_class()

        # One of the builtin N-dimensional headers keys list
        self.assertEqual(
            ["Name"],
            tl.header_keys_to_names(self.test_loader_class.DataRequiredHeaders),
        )

        # Custom N-dimensional headers keys list
        self.assertEqual(
            ["Name", ["Choice"]],
            tl.header_keys_to_names(["NAME", ["CHOICE"]]),
        )

        # Custom headers
        self.assertEqual(
            ["MyName"],
            tl.header_keys_to_names(
                ["NAME"],
                headers=self.test_loader_class.DataTableHeaders(
                    NAME="MyName",
                    CHOICE="MyChoice",
                ),
            ),
        )

    def test_get_invalid_types_from_ndim_strings(self):
        """Test that get_invalid_types_from_ndim_strings returns a non-repeating list of types (as strings) that are not
        str or list.
        """
        self.assertEqual(
            ["int", "bool", "float", "dict"],
            self.test_loader_class.get_invalid_types_from_ndim_strings(
                [1, [False, 1.2, "ok", 5.6], "ok", {"a": "b"}]
            ),
        )

    def test_flatten_ndim_strings(self):
        self.assertEqual(
            ["a", "b", "c", "d", "e", "f"],
            self.test_loader_class.flatten_ndim_strings(
                ["a", ["b", "c"], [["d", "e"], "f", "a"]]
            ),
        )

    def test_check_dataframe_values_data_no_defaults(self):
        """Test that check_dataframe_values buffers exceptions for rows with missing required values."""

        df = pd.DataFrame.from_dict(
            {
                "Name": [None, "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tl = self.test_loader_class(df=df)
        tl.check_dataframe_values()
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            RequiredColumnValue, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(
            "Value required for column(s) [Name] in row [2] in the load file data.",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        # check_dataframe_values must not change the current row
        self.assertIsNone(tl.row_index)
        self.assertIsNone(tl.rownum)

    def test_check_dataframe_values_data_with_defaults(self):
        """Test that check_dataframe_values doesn't buffer exceptions for rows when missing values are filled in with
        default values."""

        df = pd.DataFrame.from_dict(
            {
                "Name": [None, "B", "C"],
                "Choice": ["1", "2", "2"],
            },
        )
        tl = self.test_loader_class(df=df)
        tl.set_defaults({"NAME": "A"})
        tl.check_dataframe_values()
        self.assertEqual(0, len(tl.aggregated_errors_object.exceptions))
        # check_dataframe_values must not change the current row
        self.assertIsNone(tl.row_index)
        self.assertIsNone(tl.rownum)

    def test_check_dataframe_values_defaults(self):
        """Test that check_dataframe_values doesn't buffer exceptions for rows not missing required values from the
        defaults sheet."""

        defaults_df = pd.DataFrame.from_dict(
            {
                "Sheet Name": ["test"],
                "Column Header": ["Name"],
                "Default Value": ["A"],
            },
        )
        tl = self.test_loader_class(defaults_df=defaults_df)
        tl.check_dataframe_values(reading_defaults=True)
        self.assertEqual(0, len(tl.aggregated_errors_object.exceptions))
        # check_dataframe_values must not change the current row
        self.assertIsNone(tl.row_index)
        self.assertIsNone(tl.rownum)

    def test_check_dataframe_values_defaults_nulls(self):
        """Test that check_dataframe_values buffers exceptions for all rows with missing required values from the
        defaults sheet."""

        tl = self.test_loader_class()
        # Setting tl.defaults_df manually so that it's not automatically checked via the constructor
        tl.defaults_df = pd.DataFrame.from_dict(
            {
                "Sheet Name": [None],  # Required
                "Column Header": [None],  # Required
                "Default Value": [
                    1
                ],  # Optional - we need at least 1 value, otherwise the empty row is skipped
            },
        )
        tl.check_dataframe_values(reading_defaults=True)
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            RequiredColumnValue, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(
            "Value required for column(s) [Sheet Name, Column Header] in row [2] in the load file data.",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        # check_dataframe_values must not change the current row
        self.assertIsNone(tl.row_index)
        self.assertIsNone(tl.rownum)

    def test_get_missing_values(self):
        """Test that get_missing_values returns a list headers when given a row with missing required values."""

        tl = self.test_loader_class()

        df = pd.DataFrame.from_dict(
            {
                "Name": ["1"],
                "Choice": ["1"],
            },
        )
        for _, row in df.iterrows():
            pass

        self.assertEqual(
            (None, True),
            tl.get_missing_values(row=row),
        )

        df = pd.DataFrame.from_dict(
            {
                "Name": [None],
                "Choice": ["1"],
            },
        )
        for _, row in df.iterrows():
            pass

        self.assertEqual(
            (["Name"], True),
            tl.get_missing_values(row=row),
        )

    def test_get_ordered_display_headers(self):
        class TestOneDefLoader(self.test_loader_class):
            DataDefaultValues = self.test_loader_class.DataTableHeaders(
                NAME=None, CHOICE="1"
            )

        todl = TestOneDefLoader()
        self.assertEqual(["Name"], todl.get_ordered_display_headers())
        self.assertEqual(["Name", "Choice"], todl.get_ordered_display_headers(all=True))

        # Now reverse the order and define no defaults
        class TestRevOneDefLoader(self.test_loader_class):
            DataTableHeaders = namedtuple("DataTableHeaders", ["CHOICE", "NAME"])

        trodl = TestRevOneDefLoader()
        self.assertEqual(["Choice", "Name"], trodl.get_ordered_display_headers())

    def test_get_dataframe_template_empty(self):
        class TestOneDefLoader(self.test_loader_class):
            DataDefaultValues = self.test_loader_class.DataTableHeaders(
                NAME=None, CHOICE="1"
            )

        expected = {"Name": {}}

        todl = TestOneDefLoader()
        self.assertDictEqual(expected, todl.get_dataframe_template())

    def test_get_dataframe_template_populated(self):
        class TestOneDefLoader(self.test_loader_class):
            DataDefaultValues = self.test_loader_class.DataTableHeaders(
                NAME=None, CHOICE="1"
            )

        self.test_model_class.objects.create(name="A", choice=1)
        self.test_model_class.objects.create(name="B", choice=2)

        expected = {"Name": {0: "A", 1: "B"}}

        todl = TestOneDefLoader()
        self.assertEqual(str(expected), str(todl.get_dataframe_template(populate=True)))

    def test_get_dataframe_template_all_populated(self):
        class TestOneDefLoader(self.test_loader_class):
            DataDefaultValues = self.test_loader_class.DataTableHeaders(
                NAME=None, CHOICE="1"
            )

        self.test_model_class.objects.create(name="A", choice=1)
        self.test_model_class.objects.create(name="B", choice=2)

        expected = {
            "Name": {0: "A", 1: "B"},
            "Choice": {0: "1", 1: "2"},
        }

        todl = TestOneDefLoader()
        self.assertDictEqual(
            expected, todl.get_dataframe_template(all=True, populate=True)
        )

    def test_get_header_metadata(self):
        tl = self.test_loader_class()
        self.assertListEqual(
            ["Choice", "Name"], sorted(tl.get_header_metadata().keys())
        )
        # Just going to assert a single attribute is properly set
        self.assertEqual("TestModel Choice", tl.get_header_metadata()["Choice"].name)

    def test_constructor_no_positional_args(self):
        with self.assertRaises(AggregatedErrors) as ar:
            self.test_loader_class("bad")
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], ProgrammingError))
        self.assertIn(
            "expects 0 positional arguments, but got: 1", str(aes.exceptions[0])
        )

    def test_get_value_metadata(self):
        tl = self.test_loader_class()
        self.assertEqual(["Name", "Choice"], list(tl.get_value_metadata().keys()))
        self.assertEqual(
            2,
            len(
                [
                    v
                    for v in tl.get_value_metadata().values()
                    if isinstance(v, ColumnValue)
                ]
            ),
        )

    def test_get_column_metadata(self):
        tl = self.test_loader_class()
        self.assertEqual(["Name", "Choice"], list(tl.get_column_metadata().keys()))
        self.assertEqual(
            2,
            len(
                [
                    v
                    for v in tl.get_column_metadata().values()
                    if isinstance(v, TableColumn)
                ]
            ),
        )

    def test_update_load_stats(self):
        tl = self.test_loader_class()
        counts = {
            "UndocumentedModel": {  # Adding a model that's not in tl.Models (to support child loaders)
                "created": 10,
                "existed": 0,
                "deleted": 0,
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
            "TestModel": {
                "created": 0,
                "existed": 5,
                "deleted": 0,
                "updated": 1,
                "skipped": 2,
                "errored": 3,
                "warned": 4,
            },
        }
        tl.update_load_stats(counts)
        self.assertDictEqual(counts, tl.record_counts)
        new_counts = {
            "NewUndocumentedModel": {  # Adding a new model
                "created": 10,
                "existed": 0,
                "deleted": 0,
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
            "TestModel": {
                "created": 1,
                "existed": 2,
                "deleted": 0,
                "updated": 3,
                "skipped": 4,
                "errored": 5,
                "warned": 0,
            },
        }
        tl.update_load_stats(new_counts)
        expected = {
            "UndocumentedModel": {
                "created": 10,
                "existed": 0,
                "deleted": 0,
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
            "NewUndocumentedModel": {
                "created": 10,
                "existed": 0,
                "deleted": 0,
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
            "TestModel": {
                "created": 1,
                "existed": 7,
                "deleted": 0,
                "updated": 4,
                "skipped": 6,
                "errored": 8,
                "warned": 4,
            },
        }
        self.assertDictEqual(expected, tl.record_counts)

    def test_get_friendly_filename(self):
        tl1 = self.test_loader_class(file="/ugly/path/ugly.tsv")
        self.assertEqual("ugly.tsv", tl1.get_friendly_filename())
        tl2 = self.test_loader_class(file="/ugly/path/ugly.tsv", filename="pretty.tsv")
        self.assertEqual("pretty.tsv", tl2.get_friendly_filename())
        tl3 = self.test_loader_class(
            file="/ugly/path/ugly.tsv", filename="relative/path/pretty.tsv"
        )
        self.assertEqual("pretty.tsv", tl3.get_friendly_filename())

    def test__get_column_types(self):
        cts, _ = self.test_uc_loader_class._get_column_types()
        self.assertDictEqual({"Name": str, "uf1": str, "uf2": str}, cts)

    def test_get_column_types_custom(self):
        self.assertDictEqual(
            {
                "UsersDumbNameHeader": str,
                "UsersDumbFieldOneHeader": str,
                "UsersDumbFieldTwoHeader": str,
            },
            self.test_uc_loader_class(
                user_headers={
                    "NAME": "UsersDumbNameHeader",
                    "UFONE": "UsersDumbFieldOneHeader",
                    "UFTWO": "UsersDumbFieldTwoHeader",
                }
            ).get_column_types(),
        )


class MainTests(TracebaseTestCase):
    def test_flatten(self):
        """The applied use of this flatten method was/is to flatten ValidationError objects (which are iterable), but it
        can work on any (non-string/non-byte) iterable."""
        ve = ValidationError(
            [
                ValidationError(ValidationError("one")),
                ValidationError(
                    [
                        ValidationError("two"),
                        ValidationError("three"),
                    ]
                ),
            ]
        )
        self.assertEqual("['one', 'two', 'three']", str(ve))
