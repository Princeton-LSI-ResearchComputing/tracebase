from django.core.management import call_command

from DataRepo.models import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class LoadStudiesTests(TracebaseTestCase):
    def test_load_studies_command(self):
        self.assertEqual(Study.objects.count(), 0)
        call_command(
            "load_studies",
            infile="DataRepo/data/tests/small_obob/small_obob_study.xlsx",
        )
        self.assertEqual(Study.objects.count(), 1)
        rec = Study.objects.first()
        self.assertEqual("obf", rec.code)
        self.assertEqual("ob/ob Fasted", rec.name)
        self.assertEqual(
            "ob/ob and wildtype littermates were fasted 7 hours and infused with tracers",
            rec.description,
        )
