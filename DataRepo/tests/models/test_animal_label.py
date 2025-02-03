from django.conf import settings
from django.core.management import call_command
from django.test import override_settings

from DataRepo.models import (
    Animal,
    AnimalLabel,
    MSRunSample,
    PeakData,
    PeakGroup,
    Tissue,
)
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
class AnimalLabelTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob2/obob_animal_sample_table_v3.xlsx",
        )
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob2/serum_lactate_sample_table.xlsx",
        )

        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/small_obob2/obob_maven_6eaas_inf.xlsx",
        )
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/small_obob2/obob_maven_6eaas_serum.xlsx",
        )

        cls.SERUM_COMPOUNDS_COUNT = 13

        cls.N_PEAKGROUP_LABELS = 66

        # defining a primary animal object for repeated tests
        cls.MAIN_SERUM_ANIMAL = Animal.objects.get(name="971")

        super().setUpTestData()

    @MaintainedModel.no_autoupdates()
    def test_serum_tracers_enrichment_fraction(self):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/multiple_labels/animal_sample_table_v3.xlsx",
            exclude_sheets=["Peak Annotation Files"],
        )
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/multiple_labels/alafasted_cor.xlsx",
        )

        anml = Animal.objects.get(name="xzl5")
        recs = anml.labels.all()
        outputc = recs.get(element__exact="C").serum_tracers_enrichment_fraction
        outputn = recs.get(element__exact="N").serum_tracers_enrichment_fraction
        self.assertEqual(2, recs.count())
        self.assertAlmostEqual(0.2235244143081364, outputc)
        self.assertAlmostEqual(0.30075567022988536, outputn)

    def test_last_serum_tracer_label_peak_groups_success(self):
        animal = self.MAIN_SERUM_ANIMAL
        last_serum_sample = animal.last_serum_sample
        peak_groups = PeakGroup.objects.filter(
            msrun_sample__sample__id__exact=last_serum_sample.id
        )
        # ALL the sample's PeakGroup objects in the QuerySet total 13
        self.assertEqual(peak_groups.count(), self.SERUM_COMPOUNDS_COUNT)
        # but if limited to only the tracer, it is just 1 object in the QuerySet
        sample_tracer_peak_groups = last_serum_sample.last_tracer_peak_groups
        self.assertEqual(sample_tracer_peak_groups.count(), 1)
        # and test that the Animal convenience method is equivalent for this
        # particular sample/animal
        pg = animal.labels.first().last_serum_tracer_label_peak_groups.first()
        self.assertEqual(sample_tracer_peak_groups.get().id, pg.id)

    def test_last_serum_tracer_label_peak_groups_empty(self):
        animal = self.MAIN_SERUM_ANIMAL
        last_serum_sample = animal.last_serum_sample
        # Sample->MSRunSample is a restricted relationship, so the MSRunSamples must be deleted before the sample can be
        # deleted
        serum_sample_msrun = MSRunSample.objects.filter(
            sample__name=last_serum_sample.name
        ).get()
        serum_sample_msrun.delete()
        """
        with the msrun_sample deleted, the 7 rows of prior peak data
        (test_sample_peak_data, above) are now 0/gone
        """
        peakdata = PeakData.objects.filter(
            peak_group__msrun_sample__sample__exact=last_serum_sample
        ).filter(
            peak_group__compounds__id=animal.infusate.tracers.first().compound.id,
        )
        self.assertEqual(peakdata.count(), 0)
        with self.assertWarns(UserWarning):
            last_serum_sample.delete()
        # with the sample deleted, there are no more serum records...
        # so if we refresh, with no cached final serum values...
        refeshed_animal = Animal.objects.get(name="971")
        refeshed_animal_label = refeshed_animal.labels.first()
        serum_samples = refeshed_animal.samples.filter(
            tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        )
        # so zero length list
        self.assertEqual(serum_samples.count(), 0)
        with self.assertWarns(UserWarning):
            self.assertEqual(
                refeshed_animal_label.last_serum_tracer_label_peak_groups.count(),
                0,
            )

    def test_animal_label_populated(self):
        """
        Ensure there is an animal label for every animal and labeled element combo (regardless of tracers/infusates)
        """
        self.assertEqual(AnimalLabel.objects.count(), 8)
