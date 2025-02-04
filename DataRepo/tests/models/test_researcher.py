from django.conf import settings
from django.core.management import call_command
from django.test import override_settings

from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.researcher import Researcher
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
class ResearcherTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    @MaintainedModel.no_autoupdates()
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table_blank_sample.xlsx"
            ),
        )
        cls.COMPOUNDS_COUNT = 2
        cls.MSRUN_SAMPLES_COUNT = 14
        super().setUpTestData()

    def test_could_be_variant_researcher_none_exist(self):
        self.assertFalse(Researcher.could_be_variant_researcher("Gail", []))

    def test_could_be_variant_researcher_defaults_exist(self):
        self.assertTrue(Researcher.could_be_variant_researcher("Gail"))

    def test_could_be_variant_researcher_match_exists(self):
        self.assertFalse(Researcher.could_be_variant_researcher("Gail", ["Gail"]))

    def test_could_be_variant_researcher_no_match(self):
        self.assertTrue(Researcher.could_be_variant_researcher("Gail", ["Gus"]))

    def test_leaderboards(self):
        expected_lbd = {
            "studies_leaderboard": [
                (Researcher(name="Xianfeng Zeng"), 1),
            ],
            "animals_leaderboard": [
                (Researcher(name="Xianfeng Zeng"), 1),
            ],
            "peakgroups_leaderboard": [
                (
                    Researcher(name="Xianfeng Zeng"),
                    self.COMPOUNDS_COUNT * self.MSRUN_SAMPLES_COUNT,
                ),
            ],
        }
        lbd = Researcher.leaderboard_data()
        for name, lst in expected_lbd.items():
            for i, tpl in enumerate(lst):
                researcher, score = tpl
                self.assertEqual(researcher, lbd[name][i].researcher)
                self.assertEqual(score, lbd[name][i].score)
