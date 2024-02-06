from collections import namedtuple

import pandas as pd
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db.models import AutoField, CharField, Model
from django.test.utils import isolate_apps

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    ConflictingValueError,
    InfileDatabaseError,
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
    def generate_test_loader(cls, mdl):
        class TestLoader(TraceBaseLoader):
            TableHeaders = namedtuple("TableHeaders", ["NAME", "CHOICE"])
            DefaultHeaders = TableHeaders(NAME="Name", CHOICE="Choice")
            RequiredHeaders = TableHeaders(NAME=True, CHOICE=False)
            RequiredValues = RequiredHeaders
            UniqueColumnConstraints = [["NAME"]]
            FieldToHeaderKey = {"TestModel": {"name": "NAME", "choice": "CHOICE"}}
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
        super().__init__(*args, **kwargs)

    # handle_load_db_errors Tests
    def test_handle_load_db_errors_ve_choice(self):
        """Ensures handle_load_db_errors packages ValidationError about invalid choices"""
        pddata = pd.DataFrame.from_dict({"NAME": ["test"], "CHOICE": ["3"]})
        tl = self.TestLoader(pddata)
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        try:
            raise ValidationError("3 is not a valid choice")
        except Exception as e:
            tl.handle_load_db_errors(e, self.TestModel, {"name": "test", "choice": "3"})
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

    def test_handle_load_db_errors_ie_unique(self):
        """Ensures handle_load_db_errors packages ValidationError about invalid choices"""
        pddata = pd.DataFrame.from_dict({"NAME": ["test2"], "CHOICE": ["2"]})
        tl = self.TestLoader(pddata)
        # Circumventing the need to call load_data, set what is needed to call handle_load_db_errors...
        tl.set_row_index(0)  # Converted to row 2 (header line is 1)
        # handle_load_db_errors queries for the existing record that caused the IntegrityError exception, so we need to
        # create one:
        recdict = {"name": "test2", "choice": "2"}
        self.TestModel.objects.create(**recdict)
        # An integrity error requires a conflict, so:
        recdict["choice"] = "1"
        try:
            raise IntegrityError("duplicate key value violates unique constraint")
        except Exception as e:
            tl.handle_load_db_errors(e, self.TestModel, recdict)
        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(
            ConflictingValueError, type(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertEqual(
            (
                "Conflicting field values encountered in row [2] in the load file data in TestModel record [{'id': 1, "
                "'name': 'test2', 'choice': '2'}]:\n"
                "\tchoice in\n"
                "\t\tdatabase: [2]\n"
                "\t\tfile: [1]"
            ),
            str(tl.aggregated_errors_object.exceptions[0]),
        )

    def test_handle_load_db_errors_catches_IntegrityError_containing_violates_not_null_constraint(
        self,
    ):
        # TODO: Implement
        pass

    def test_handle_load_db_errors_catches_RequiredColumnValue(self):
        # TODO: Implement
        pass

    def test_handle_load_db_errors_raises_ValidationErrors_with_the_same_dict_as_InfileDatabaseError_once(
        self,
    ):
        # TODO: Implement
        pass

    def test_handle_load_db_errors_raises_exception_unpackaged_if_rec_dict_is_None_or_has_no_keys(
        self,
    ):
        # TODO: Implement
        pass

    def test_check_for_inconsistencies(self):
        # TODO: Implement
        pass

    # Method tests
    def test_get_load_stats(self):
        # TODO: Implement
        pass

    def test_get_models(self):
        # TODO: Implement
        pass

    def test_created(self):
        # TODO: Implement
        pass

    def test_existed(self):
        # TODO: Implement
        pass

    def test_errored(self):
        # TODO: Implement
        pass

    def test__get_model_name(self):
        # TODO: Implement
        pass

    def test_get_defaults_dict_by_header_name(self):
        # TODO: Implement
        pass

    def test_getRowVal(self):
        # TODO: Implement
        pass

    def test_get_skip_row_indexes(self):
        # TODO: Implement
        pass

    def test_add_skip_row_index(self):
        # TODO: Implement
        pass

    def test_check_headers(self):
        # TODO: Implement
        pass

    def test_check_unique_constraints(self):
        # TODO: Implement
        pass

    def test_header_key_to_name(self):
        # TODO: Implement
        pass

    def test_get_column_types(self):
        # TODO: Implement
        pass

    def test_isnamedtuple(self):
        # TODO: Implement
        pass

    def test_initialize_metadata(self):
        # TODO: Implement
        pass

    def test_check_class_attributes(self):
        # TODO: Implement
        pass

    def test_get_defaults(self):
        # TODO: Implement
        pass

    def test_get_header_keys(self):
        # TODO: Implement
        pass

    def test_get_pretty_default_headers(self):
        # TODO: Implement
        pass

    def test_get_headers(self):
        # TODO: Implement
        pass

    def test_set_row_index(self):
        # TODO: Implement
        pass

    # apply_loader_wrapper tests
    def test_apply_loader_wrapper_checks_TableHeaders(self):
        # TODO: Implement
        pass

    def test_apply_loader_wrapper_checks_DefaultHeaders(self):
        # TODO: Implement
        pass

    def test_apply_loader_wrapper_checks_RequiredHeaders(self):
        # TODO: Implement
        pass

    def test_apply_loader_wrapper_checks_DefaultValues(self):
        # TODO: Implement
        pass

    def test_apply_loader_wrapper_checks_RequiredValues(self):
        # TODO: Implement
        pass

    def test_apply_loader_wrapper_checks_ColumnTypes(self):
        # TODO: Implement
        pass

    def test_apply_loader_wrapper_checks_UniqueColumnConstraints(self):
        # TODO: Implement
        pass

    def test_apply_loader_wrapper_checks_FieldToHeaderKey(self):
        # TODO: Implement
        pass

    def test_apply_loader_wrapper_checks_Models(self):
        # TODO: Implement
        pass

    def test_apply_loader_wrapper_checks_load_data(self):
        # TODO: Implement
        pass

    def test_is_skip_row(self):
        # TODO: Implement
        pass

    def test_tableheaders_to_dict_by_header_name(self):
        # TODO: Implement
        pass

    def test_check_header_names(self):
        # TODO: Implement
        pass

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
