from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class TraceBaseLoaderTests(TracebaseTestCase):
    # handle_load_db_errors Tests
    def test_handle_load_db_errors_catches_ValidationError_containing_is_not_a_valid_choice(
        self,
    ):
        # TODO: Implement
        pass

    def test_handle_load_db_errors_catches_IntegrityError_containing_duplicate_key_value_violates_unique(
        self,
    ):
        # TODO: Implement
        pass

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
