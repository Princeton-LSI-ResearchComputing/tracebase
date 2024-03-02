# from django.core.management import call_command

from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class LoadTracersCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    def test_names_only_ok(self):
        # TODO: Implement test
        pass

    def test_column_data_only_ok(self):
        # TODO: Implement test
        pass

    def test_name_and_column_mix_ok(self):
        # TODO: Implement test
        pass

    def test_name_with_multiple_numbers_error(self):
        # TODO: Implement test
        pass

    def test_number_with_multiple_names_error(self):
        # TODO: Implement test
        pass

    def test_number_with_multiple_compounds_error(self):
        # TODO: Implement test
        pass

    def test_dupe_isotopes_ok_when_second_isotopes_differ(self):
        # TODO: Implement test
        pass
