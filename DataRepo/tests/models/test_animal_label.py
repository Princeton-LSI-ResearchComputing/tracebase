from django.conf import settings
from django.core.management import call_command
from django.test import override_settings

from DataRepo.models.animal import Animal
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
class AnimalLabelTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

    @classmethod
    @MaintainedModel.no_autoupdates()
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/multiple_labels/animal_sample_table_v3.xlsx",
            exclude_sheets=["Peak Annotation Files"],
        )
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/multiple_labels/alafasted_cor.xlsx",
        )

        super().setUpTestData()

    @MaintainedModel.no_autoupdates()
    def test_serum_tracers_enrichment_fraction(self):
        anml = Animal.objects.get(name="xzl5")
        recs = anml.labels.all()
        outputc = recs.get(element__exact="C").serum_tracers_enrichment_fraction
        outputn = recs.get(element__exact="N").serum_tracers_enrichment_fraction
        self.assertEqual(2, recs.count())
        self.assertAlmostEqual(0.2235244143081364, outputc)
        self.assertAlmostEqual(0.30075567022988536, outputn)
