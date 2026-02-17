from datetime import datetime, timedelta

from django.conf import settings
from django.core.management import call_command
from django.test import override_settings, tag

from DataRepo.models import (
    Animal,
    ArchiveFile,
    Compound,
    DataFormat,
    DataType,
    LCMethod,
    MaintainedModel,
    MSRunSample,
    MSRunSequence,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupLabel,
    Sample,
    Tissue,
)
from DataRepo.models.hier_cached_model import set_cache
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import NoCommonLabel


@override_settings(CACHES=settings.TEST_CACHES)
class PeakGroupLabelTracerRateTests(TracebaseTestCase):
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
class PeakGroupLabelMultiLabelTests(TracebaseTestCase):
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


@override_settings(CACHES=settings.TEST_CACHES)
class PeakGroupLabelPropertyTests(TracebaseTestCase):
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

        cls.pg = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun_sample__sample__name="BAT-xz971")
            .filter(peak_annotation_file__filename="obob_maven_6eaas_inf.xlsx")
            .get()
        )

        super().setUpTestData()

    def assert_bat_glucose_calcs(self, pg, total, enrichfrac, enrichabund, normlabel):
        self.assertAlmostEqual(total, pg.total_abundance, places=3)
        self.assertAlmostEqual(enrichfrac, pg.labels.first().enrichment_fraction)
        self.assertAlmostEqual(
            enrichabund,
            pg.labels.first().enrichment_abundance,
            places=5,
        )
        self.assertAlmostEqual(normlabel, pg.labels.first().normalized_labeling)

    def test_enrichment_fraction(self):
        peak_group = self.pg
        peak_data = peak_group.peak_data.filter(labels__count=0).get()
        self.assertEqual(peak_data.raw_abundance, 8814287)
        self.assertAlmostEqual(peak_data.corrected_abundance, 9553199.89089051)
        self.assert_bat_glucose_calcs(
            peak_group, 9599112.684, 0.001555566789, 14932.06089, 0.009119978074
        )

    def test_enrichment_abundance(self):
        # Remove existing peak groups so that we can load alternate data that has no original sheet.
        PeakGroup.objects.filter(msrun_sample__sample__name="BAT-xz971").delete()
        PeakGroup.objects.filter(msrun_sample__sample__name="Liv-xz982").delete()
        # Load alternate data
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/small_obob2/obob_maven_6eaas_inf_corrected.csv",
        )
        # Get one of the peak groups that has no original data
        peak_group = (
            PeakGroup.objects.filter(compounds__name="glucose")
            .filter(msrun_sample__sample__name="BAT-xz971")
            .filter(peak_annotation_file__filename="obob_maven_6eaas_inf_corrected.csv")
            .get()
        )
        peak_data = peak_group.peak_data.filter(labels__count=0).get()
        # so some data is unavialable
        self.assertIsNone(peak_data.raw_abundance)
        self.assertIsNone(peak_data.med_mz)
        self.assertIsNone(peak_data.med_rt)
        # but presumably these are all computed from the corrected data
        self.assertAlmostEqual(peak_data.corrected_abundance, 9553199.891)
        self.assert_bat_glucose_calcs(
            peak_group, 9599112.684, 0.001555566789, 14932.06089, 0.009119978074
        )

    def test_normalized_labeling(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun_sample__sample__name="serum-xz971")
            .get()
        )
        peak_data = peak_group.peak_data.filter(labels__count=0).get()
        self.assertAlmostEqual(peak_data.raw_abundance, 205652.5)
        self.assertAlmostEqual(peak_data.corrected_abundance, 222028.365565823)
        self.assert_bat_glucose_calcs(
            peak_group, 267686.902436353, 0.1705669439, 45658.53687, 1
        )

    def make_msrun_sequence(self):
        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")

        seq = MSRunSequence(
            researcher="John Doe",
            date=datetime.now(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
            lc_method=lcm,
        )
        seq.full_clean()
        seq.save()
        return seq

    def make_msrun_sample(self, seq, sample):
        mstype = DataType.objects.get(code="ms_data")
        rawfmt = DataFormat.objects.get(code="ms_raw")
        mzxfmt = DataFormat.objects.get(code="mzxml")
        rawrec = ArchiveFile.objects.create(
            filename="test.raw",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c5",
            data_type=mstype,
            data_format=rawfmt,
        )
        mzxrec = ArchiveFile.objects.create(
            filename="test.mzxml",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c4",
            data_type=mstype,
            data_format=mzxfmt,
        )
        msrs = MSRunSample(
            msrun_sequence=seq,
            sample=sample,
            polarity=MSRunSample.POSITIVE_POLARITY,
            ms_raw_file=rawrec,
            ms_data_file=mzxrec,
        )
        msrs.full_clean()
        msrs.save()
        return msrs

    @MaintainedModel.no_autoupdates()
    def test_enrichment_fraction_no_peak_labeled_elements(self):
        # This creates an animal with a nitrogen-labeled tracer (among others)
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/animal_sample_table_labeled_elements_v3.xlsx",
        )

        # Retrieve a sample associated with an animal that has a tracer with only a nitrogen label
        sample = Sample.objects.get(name__exact="test_animal_2_sample_1")
        # Get the sequence created by the load
        seq = MSRunSequence.objects.get(researcher="george")

        msrs = self.make_msrun_sample(seq, sample)

        ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
        accucor_format = DataFormat.objects.get(code="accucor")
        peak_annotation_file = ArchiveFile.objects.create(
            filename="test_data_file",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
            data_type=ms_peak_annotation,
            data_format=accucor_format,
        )

        pg = PeakGroup(
            name="lactate",
            peak_annotation_file=peak_annotation_file,
            msrun_sample=msrs,
        )
        pg.save()

        # Add a compound to the peak group that does not have a nitrogen
        cpd = Compound.objects.get(name="lactate", formula="C3H6O3")
        pg.compounds.add(cpd)

        # make sure we get only 1 labeled element of nitrogen
        self.assertEqual(
            ["N"],
            sample.animal.infusate.tracer_labeled_elements,
            msg="Make sure the tracer labeled elements are set for the animal this peak group is linked to.",
        )

        # Create the peak group label that would be created if the accucor/isocorr data was loaded
        PeakGroupLabel.objects.create(
            peak_group=pg,
            element="N",
        )

        # Now try to trigger a NoCommonLabel exception
        with self.assertRaises(
            NoCommonLabel,
            msg=(
                "PeakGroup lactate found associated with measured compounds: [lactate] that does not contain labeled "
                "element C (from the tracers in the infusate [methionine-(15N1)[200]])."
            ),
        ):
            pg.labels.first().enrichment_fraction  # pylint: disable=no-member

    def test_enrichment_fraction_missing_formula_warning(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun_sample__sample__name="serum-xz971")
            .get()
        )
        peak_group.formula = None
        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().enrichment_fraction)

    def test_enrichment_fraction_formula_missing_label_error(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun_sample__sample__name="serum-xz971")
            .get()
        )
        peak_group.formula = "H2O"
        with self.assertRaises(NoCommonLabel):
            peak_group.labels.first().enrichment_fraction

    def test_enrichment_fraction_missing_formula_return_is_none(self):
        peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun_sample__sample__name="serum-xz971")
            .get()
        )

        for peak_data in peak_group.peak_data.all():
            for pdl in peak_data.labels.all():
                pdl.delete()
            peak_data.save()

        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().enrichment_fraction)

    def test_normalized_labeling_latest_serum(self):
        peak_group = self.pg
        first_serum_sample = Sample.objects.filter(name="serum-xz971").get()
        second_serum_sample = Sample.objects.create(
            date=first_serum_sample.date,
            name="serum-xz971.2",
            researcher=first_serum_sample.researcher,
            animal=first_serum_sample.animal,
            time_collected=first_serum_sample.time_collected + timedelta(minutes=1),
            tissue=first_serum_sample.tissue,
        )

        serum_samples = first_serum_sample.animal.samples.filter(
            tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        )
        # there should now be 2 serum samples for this animal
        self.assertEqual(serum_samples.count(), 2)
        last_serum_sample = first_serum_sample.animal.last_serum_sample
        # and the final one should now be the second one (just created)
        self.assertEqual(last_serum_sample.name, second_serum_sample.name)

        seq = self.make_msrun_sequence()

        msrs = self.make_msrun_sample(seq, second_serum_sample)

        second_serum_peak_group = PeakGroup.objects.create(
            name=peak_group.name,
            formula=peak_group.formula,
            peak_annotation_file=peak_group.peak_annotation_file,
            msrun_sample=msrs,
        )
        second_serum_peak_group.compounds.add(
            peak_group.msrun_sample.sample.animal.infusate.tracers.first().compound
        )
        first_serum_peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun_sample__sample__name="serum-xz971")
            .get()
        )
        first_serum_peak_group_label = first_serum_peak_group.labels.first()
        PeakGroupLabel.objects.create(
            peak_group=second_serum_peak_group,
            element=first_serum_peak_group_label.element,
        )
        for orig_peak_data in first_serum_peak_group.peak_data.all():
            pdr = PeakData.objects.create(
                raw_abundance=orig_peak_data.raw_abundance,
                corrected_abundance=orig_peak_data.corrected_abundance,
                peak_group=second_serum_peak_group,
                med_mz=orig_peak_data.med_mz,
                med_rt=orig_peak_data.med_rt,
            )
            PeakDataLabel.objects.create(
                peak_data=pdr,
                element=orig_peak_data.labels.first().element,
                mass_number=orig_peak_data.labels.first().mass_number,
                count=orig_peak_data.labels.first().count,
            )
        second_peak_data = second_serum_peak_group.peak_data.order_by(
            "labels__count"
        ).last()
        second_peak_data.corrected_abundance = 100
        second_peak_data.save()
        self.assertEqual(peak_group.labels.count(), 1, msg="Assure load was complete")
        self.assertAlmostEqual(
            peak_group.labels.first().normalized_labeling, 3.455355083
        )

    def test_normalized_labeling_latest_serum_no_peakgroup(self):
        """
        The calculation of any peak group's normalized labeling utilizes the serum's enrichment fraction of each of the
        tracer peak groups involved.  This test messes with those tracer peak groups and the serum samples to make sure
        it uses the right serum tracer peak groups and issues an error if they are missing.
        """
        peak_group = self.pg
        first_serum_sample = Sample.objects.filter(name="serum-xz971").get()
        second_serum_sample = Sample.objects.create(
            date=first_serum_sample.date,
            name="serum-xz971.3",
            researcher=first_serum_sample.researcher,
            animal=first_serum_sample.animal,
            tissue=first_serum_sample.tissue,
            time_collected=first_serum_sample.time_collected + timedelta(minutes=1),
        )

        serum_samples = first_serum_sample.animal.samples.filter(
            tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        )
        last_serum_sample = first_serum_sample.animal.last_serum_sample
        # there should now be 2 serum samples for this animal
        self.assertEqual(serum_samples.count(), 2)
        # and the final one should now be the second one (just created)
        self.assertEqual(last_serum_sample.name, second_serum_sample.name)

        # Confirm the original calculated normalized labeling using the existing final serum sample
        self.assertAlmostEqual(
            0.00911997807399377,
            peak_group.labels.first().normalized_labeling,
        )

        # Create a later msrun_sample with the later serum sample (but no peak group)
        seq = self.make_msrun_sequence()
        msrs = self.make_msrun_sample(seq, second_serum_sample)

        # DO NOT CREATE A PEAKGROUP FOR THE TRACER
        self.assertEqual(peak_group.labels.count(), 1, msg="Assure load was complete")
        # With the new logic of obtaining the last instance of a peak group among serum samples, this should still
        # produce a calculation even though the last serum sample doesn't have a peak group for the tracer. It will
        # just use the one from the first
        self.assertAlmostEqual(
            0.00911997807399377,
            peak_group.labels.first().normalized_labeling,
        )

        # Now add a peak group to the new last serum sample and change the corrected abundance to confirm it uses the
        # new last sample's peak group
        second_serum_peak_group = PeakGroup.objects.create(
            name=peak_group.name,
            formula=peak_group.formula,
            msrun_sample=msrs,
            peak_annotation_file=peak_group.peak_annotation_file,
        )
        first_serum_peak_group = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun_sample__sample__name="serum-xz971")
            .get()
        )
        second_serum_peak_group.compounds.add(
            peak_group.msrun_sample.sample.animal.infusate.tracers.first().compound
        )
        PeakGroupLabel.objects.create(
            peak_group=second_serum_peak_group,
            element=peak_group.labels.first().element,
        )
        # We do not need to add a peak group label (i.e. make it missing), because it's not used in this calculation
        for orig_peak_data in first_serum_peak_group.peak_data.all():
            pdr = PeakData.objects.create(
                raw_abundance=orig_peak_data.raw_abundance,
                corrected_abundance=orig_peak_data.corrected_abundance,
                peak_group=second_serum_peak_group,
                med_rt=orig_peak_data.med_rt,
                med_mz=orig_peak_data.med_mz,
            )
            PeakDataLabel.objects.create(
                peak_data=pdr,
                element=orig_peak_data.labels.first().element,
                count=orig_peak_data.labels.first().count,
                mass_number=orig_peak_data.labels.first().mass_number,
            )
        second_peak_data = second_serum_peak_group.peak_data.order_by(
            "labels__count"
        ).last()
        second_peak_data.corrected_abundance = 100
        second_peak_data.save()
        # Now confirm the different calculated value
        self.assertAlmostEqual(
            3.4553550826083774, peak_group.labels.first().normalized_labeling
        )

        # Now let's delete both peak groups and confirm the value can no longer be calculated and that a warning is
        # issued

        # Now let's delete the first serum peak group's peak group label record that still exists
        first_serum_peak_group.delete()
        second_serum_peak_group.delete()
        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().normalized_labeling)

    def test_peak_group_label_populated(self):
        self.assertEqual(PeakGroupLabel.objects.count(), self.N_PEAKGROUP_LABELS)

    def test_normalized_labeling_missing_serum_peak_group(self):
        peak_group = self.pg
        peak_group_serum = (
            PeakGroup.objects.filter(compounds__name="lysine")
            .filter(msrun_sample__sample__name="serum-xz971")
            .get()
        )
        peak_group_serum.delete()

        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().normalized_labeling)

    def test_normalized_labeling_missing_serum_sample(self):
        peak_group = self.pg
        serum_sample_msrun = MSRunSample.objects.filter(
            sample__name="serum-xz971"
        ).get()
        serum_sample_msrun.delete()
        serum_sample = Sample.objects.filter(name="serum-xz971").get()
        serum_sample.delete()

        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group.labels.first().normalized_labeling)

    def test_no_calc_errors_when_pg_total_abundance_zero(self):
        # Test various calculations do not raise exceptions when total_abundance is zero
        peak_group = self.pg
        seq = self.make_msrun_sequence()
        msrs = self.make_msrun_sample(seq, peak_group.msrun_sample.sample)

        peak_group_zero = PeakGroup.objects.create(
            name=peak_group.name
            + "_stereoisomer",  # To avoid unique constraint violation
            formula=peak_group.formula,
            msrun_sample=msrs,
            peak_annotation_file=peak_group.peak_annotation_file,
        )

        labeled_elems = []
        for orig_peak_data in peak_group.peak_data.all():
            pd = PeakData.objects.create(
                raw_abundance=0,
                corrected_abundance=0,
                peak_group=peak_group_zero,
                med_mz=orig_peak_data.med_mz,
                med_rt=orig_peak_data.med_rt,
            )
            # Fraction is not defined when total_abundance is zero
            self.assertIsNone(pd.fraction)
            for orig_peak_label in orig_peak_data.labels.all():
                if orig_peak_label.element not in labeled_elems:
                    labeled_elems.append(orig_peak_label.element)
                PeakDataLabel.objects.create(
                    peak_data=pd,
                    element=orig_peak_label.element,
                    mass_number=orig_peak_label.mass_number,
                    count=orig_peak_label.count,
                )
        for pgl in labeled_elems:
            PeakGroupLabel.objects.create(
                peak_group=peak_group_zero,
                element=pgl,
            )

        with self.assertWarns(UserWarning):
            self.assertIsNone(peak_group_zero.labels.first().enrichment_fraction)
        self.assertIsNone(peak_group_zero.labels.first().enrichment_abundance)
        self.assertIsNone(peak_group_zero.labels.first().normalized_labeling)
        self.assertEqual(peak_group_zero.total_abundance, 0)

    def test_is_tracer_label_compound_group(self):
        # get a non tracer compound from a serum sample
        sample = Sample.objects.get(name="serum-xz971")
        pg = (
            sample.msrun_samples.last()
            .peak_groups.filter(name__exact="tryptophan")
            .last()
        )
        pgl = pg.labels.first()
        self.assertFalse(pgl.is_tracer_label_compound_group)

    def test_can_compute_tracer_label_rates_true(self):
        # get a tracer compound from a  sample
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.last_tracer_peak_groups.last()
        pgl = pg.labels.first()
        self.assertTrue(pgl.can_compute_tracer_label_rates)

    def test_can_compute_tracer_label_rates_false_no_rate(self):
        # get a tracer compound from a  sample
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.last_tracer_peak_groups.last()
        animal = pg.animal
        # but if the animal infusion_rate is not defined...
        orig_tir = animal.infusion_rate
        animal.infusion_rate = None
        animal.save()
        pgf = animal.labels.first().last_serum_tracer_label_peak_groups.first()
        pglf = pgf.labels.first()
        with self.assertWarns(UserWarning):
            self.assertFalse(pglf.can_compute_tracer_label_rates)
        # revert
        animal.infusion_rate = orig_tir
        animal.save()

    def test_can_compute_tracer_label_rates_false_no_conc(self):
        # get a tracer compound from a sample
        sample = Sample.objects.get(name="serum-xz971")
        pg = sample.last_tracer_peak_groups.last()
        animal = pg.animal
        al = animal.labels.first()
        pgf = al.last_serum_tracer_label_peak_groups.last()
        pglf = pgf.labels.first()
        # but if the animal tracer_concentration is not defined...
        set_cache(pglf, "tracer_concentration", None)
        with self.assertWarns(UserWarning):
            self.assertFalse(pglf.can_compute_intact_tracer_label_rates)

    def test_can_compute_body_weight_intact_tracer_label_rates_true(self):
        animal = self.MAIN_SERUM_ANIMAL
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        self.assertTrue(pgl.can_compute_body_weight_intact_tracer_label_rates)

    def test_can_compute_body_weight_tracer_label_rates_false(self):
        animal = self.MAIN_SERUM_ANIMAL
        orig_bw = animal.body_weight
        animal.body_weight = None
        animal.save()
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        with self.assertWarns(UserWarning):
            self.assertFalse(pgl.can_compute_body_weight_intact_tracer_label_rates)
        # revert
        animal.body_weight = orig_bw
        animal.save()

    def test_can_compute_intact_tracer_label_rates_true(self):
        animal = self.MAIN_SERUM_ANIMAL
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        self.assertTrue(pgl.can_compute_intact_tracer_label_rates)

    def test_can_compute_intact_tracer_label_rates_false(self):
        animal = self.MAIN_SERUM_ANIMAL
        tracer = animal.infusate.tracers.first()
        pg = animal.last_serum_tracer_peak_groups.first()
        pgid = pg.id
        tracer_labeled_count = tracer.labels.first().count
        intact_peakdata = pg.peak_data.filter(labels__count=tracer_labeled_count).get()
        intact_peakdata_label = intact_peakdata.labels.get(
            count__exact=tracer_labeled_count
        )
        # set to something crazy, or None
        intact_peakdata_label.count = 42
        intact_peakdata_label.save()
        pgf = PeakGroup.objects.get(id=pgid)
        pglf = pgf.labels.first()
        with self.assertWarns(UserWarning):
            self.assertFalse(pglf.can_compute_intact_tracer_label_rates)
        # revert
        intact_peakdata_label.count = tracer_labeled_count
        intact_peakdata_label.save()

    def test_can_compute_average_tracer_label_rates_true(self):
        animal = self.MAIN_SERUM_ANIMAL
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        self.assertTrue(pgl.can_compute_average_tracer_label_rates)

    def test_can_compute_average_tracer_label_rates_false(self):
        # need to invalidate the computed/cached enrichment_fraction, somehow
        animal = self.MAIN_SERUM_ANIMAL
        pg = animal.last_serum_tracer_peak_groups.first()
        pgl = pg.labels.first()
        set_cache(pgl, "enrichment_fraction", None)
        with self.assertWarns(UserWarning):
            self.assertFalse(pgl.can_compute_average_tracer_label_rates)
