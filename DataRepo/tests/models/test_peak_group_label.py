from django.conf import settings
from django.core.management import call_command
from django.test import override_settings, tag

from DataRepo.models import Animal, Compound
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.peak_group import PeakGroup
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
class TracerRateTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob2/obob_animal_sample_table_v3.xlsx",
        )
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/small_obob2/obob_maven_c160_serum.xlsx",
        )

        # defining a primary animal object for repeated tests
        cls.animal = Animal.objects.get(name="970")
        cls.tracer = cls.animal.infusate.tracers.first()
        cls.element = cls.tracer.labels.first().element
        cls.pg = cls.animal.last_serum_tracer_peak_groups.first()
        cls.fpgl = cls.pg.labels.filter(element=cls.element).first()

        super().setUpTestData()

    @tag("fcirc")
    def test_peakgroup_is_tracer_label_compound_group(self):
        self.assertTrue(self.fpgl.is_tracer_label_compound_group)
        self.assertEqual(self.fpgl.peak_group.name, "C16:0")

    @tag("fcirc")
    def test_peakgroup_from_serum_sample(self):
        self.assertTrue(self.fpgl.from_serum_sample)

    @tag("fcirc")
    def test_peakgroup_can_compute_tracer_label_rates(self):
        self.assertTrue(self.fpgl.can_compute_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_body_weight_intact_tracer_label_rates(self):
        self.assertTrue(self.fpgl.can_compute_body_weight_intact_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_body_weight_average_tracer_label_rates(self):
        self.assertTrue(self.fpgl.can_compute_body_weight_average_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_intact_tracer_label_rates(self):
        self.assertTrue(self.fpgl.can_compute_intact_tracer_label_rates)

    @tag("fcirc")
    def test_peakgroup_can_compute_average_tracer_label_rates(self):
        self.assertTrue(self.fpgl.can_compute_average_tracer_label_rates)

    @tag("fcirc")
    def test_nontracer_peakgroup_calculation_attempts(self):
        nontracer_compound = Compound.objects.get(name="succinate")
        non_tracer_pg_label = (
            self.animal.last_serum_sample.msrun_samples.first()
            .peak_groups.get(compounds__exact=nontracer_compound)
            .labels.first()
        )
        # # but let's get a peakgroup for a compound we know is not the tracer
        # pgs = self.animal.last_serum_sample.peak_groups(nontracer_compound)
        # # should only be one in this specific case
        # non_tracer_pg = pgs[0]
        # tryptophan is not the tracer
        self.assertFalse(non_tracer_pg_label.is_tracer_label_compound_group)
        # and none of these should return a value
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_disappearance_intact_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_appearance_intact_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_disappearance_intact_per_animal)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_appearance_intact_per_animal)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_disappearance_average_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_appearance_average_per_gram)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_disappearance_average_per_animal)
        with self.assertWarns(UserWarning):
            self.assertIsNone(non_tracer_pg_label.rate_appearance_average_per_animal)

    @tag("fcirc")
    def test_last_serum_tracer_rate_disappearance_intact_per_gram(self):
        self.assertAlmostEqual(
            self.fpgl.rate_disappearance_intact_per_gram,
            38.83966501,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_appearance_intact_per_gram(self):
        self.assertAlmostEqual(
            self.fpgl.rate_appearance_intact_per_gram,
            34.35966501,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_disappearance_intact_per_animal(self):
        self.assertAlmostEqual(
            self.fpgl.rate_disappearance_intact_per_animal,
            1040.903022,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_appearance_intact_per_animal(self):
        self.assertAlmostEqual(
            self.fpgl.rate_appearance_intact_per_animal,
            920.8390222,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_disappearance_average_per_gram(self):
        self.assertAlmostEqual(
            self.fpgl.rate_disappearance_average_per_gram,
            37.36671487,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_appearance_average_per_gram(self):
        self.assertAlmostEqual(
            self.fpgl.rate_appearance_average_per_gram,
            32.88671487,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_disappearance_average_per_animal(self):
        # doublecheck weight, because test is not exact but test_tracer_Rd_avg_g was fine
        self.assertAlmostEqual(
            self.fpgl.rate_disappearance_average_per_animal,
            1001.427958,
            places=2,
        )

    @tag("fcirc")
    def test_last_serum_tracer_rate_appearance_average_per_animal(self):
        # Uses Animal.last_serum_sample and Sample.peak_groups
        pgl = (
            self.animal.last_serum_sample.last_tracer_peak_groups.first().labels.first()
        )
        self.assertAlmostEqual(
            pgl.rate_appearance_average_per_animal,
            881.3639585,
            places=2,
        )


@override_settings(CACHES=settings.TEST_CACHES)
class MultiLabelPeakGroupLabelTests(TracebaseTestCase):
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
    def test_enrichment_abundance(self):
        pg = PeakGroup.objects.filter(msrun_sample__sample__name="xzl5_panc").get(
            name="glutamine"
        )
        pgc = pg.labels.get(element__exact="C").enrichment_abundance
        pgn = pg.labels.get(element__exact="N").enrichment_abundance
        expectedc = 1369911.2746615328
        expectedn = 6571127.3714690255
        self.assertEqual(pg.labels.count(), 2)
        self.assertAlmostEqual(expectedc, pgc)
        self.assertAlmostEqual(expectedn, pgn)

    @MaintainedModel.no_autoupdates()
    def test_normalized_labeling_2_elements(self):
        pg = PeakGroup.objects.filter(msrun_sample__sample__name="xzl5_panc").get(
            name="glutamine"
        )
        pgc = pg.labels.get(element__exact="C").normalized_labeling
        pgn = pg.labels.get(element__exact="N").normalized_labeling
        expectedc = 0.06287501342027346
        expectedn = 0.2241489339907528
        self.assertEqual(pg.labels.count(), 2)
        self.assertAlmostEqual(expectedc, pgc)
        self.assertAlmostEqual(expectedn, pgn)
