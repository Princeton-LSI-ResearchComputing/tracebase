from DataRepo.models.researcher import could_be_variant_researcher
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class ResearcherTests(TracebaseTestCase):
    def test_could_be_variant_researcher_none_exist(self):
        self.assertFalse(could_be_variant_researcher("Gail"))

    def test_could_be_variant_researcher_match_exists(self):
        self.assertFalse(could_be_variant_researcher("Gail", ["Gail"]))

    def test_could_be_variant_researcher_no_match(self):
        self.assertTrue(could_be_variant_researcher("Gail", ["Gus"]))
