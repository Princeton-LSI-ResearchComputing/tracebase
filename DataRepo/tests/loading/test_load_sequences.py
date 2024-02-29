from django.core.management import call_command

from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class LoadSequencesCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    def test_load_sequences_excel_with_defaults(self):
        call_command(
            "load_sequences",
            infile="DataRepo/data/tests/submission_v3/study.xlsx",
        )

    def test_load_sequences_excel_without_defaults(self):
        call_command(
            "load_sequences",
            infile="DataRepo/data/tests/submission_v3/study_no_defs.xlsx",
        )

    def test_load_sequences_tsv_with_defaults(self):
        call_command(
            "load_sequences",
            infile="DataRepo/data/tests/submission_v3/sequences.tsv",
            defaults_file="DataRepo/data/tests/submission_v3/defaults.tsv",
        )

    def test_load_sequences_tsv_without_defaults(self):
        call_command(
            "load_sequences",
            infile="DataRepo/data/tests/submission_v3/sequences.tsv",
        )
