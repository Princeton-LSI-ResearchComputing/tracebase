from django.core.management import call_command

from DataRepo.models import CompoundSynonym
from DataRepo.templatetags.customtags import get_case_insensitive_synonyms
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class CustomTagsTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )

    def test_get_case_insensitive_synonyms(self):
        csqs = CompoundSynonym.objects.filter(name__icontains="glucose")
        qnames = list(csqs.values_list("name", flat=True))
        # Make sure there are expected case variants
        self.assertIn("glucose", qnames)
        self.assertIn("Glucose", qnames)
        # Expected case insensitive list:  ['D-Glucose', 'glucose', 'glucose-6-phosphate']
        # Input case variant list:  ['D-Glucose', 'Glucose', 'glucose', 'Glucose-6-phosphate', 'glucose-6-phosphate']
        csls = get_case_insensitive_synonyms(csqs)
        self.assertListEqual(csls, ["D-Glucose", "glucose", "glucose-6-phosphate"])
