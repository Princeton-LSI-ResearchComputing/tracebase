from django.core.management import call_command

from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class LoadLCProtocolsCommandTests(TracebaseTestCase):
    def test_load_lcprotocols_excel_with_defaults(self):
        call_command(
            "load_lcprotocols",
            infile="DataRepo/data/tests/submission_v3/study.xlsx",
        )

    def test_load_lcprotocols_excel_without_defaults(self):
        call_command(
            "load_lcprotocols",
            infile="DataRepo/data/tests/submission_v3/study_no_defs.xlsx",
        )

    def test_load_lcprotocols_tsv(self):
        call_command(
            "load_lcprotocols",
            infile="DataRepo/data/tests/submission_v3/lcprotocols.tsv",
        )
