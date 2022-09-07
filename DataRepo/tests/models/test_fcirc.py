from datetime import datetime
from datetime import timedelta

from django.conf import settings
from django.core.management import call_command
from django.test import override_settings, tag

from DataRepo.models import Sample, Animal, MSRun, PeakGroupSet, PeakGroup, PeakGroupLabel, FCirc, Protocol
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
@tag("multi_working")
class FCircTests(TracebaseTestCase):

    def setUp(self):
        # Get an animal (assuming it has an infusate/tracers/etc)
        animal = Animal.objects.last()
        # Get its last serum sample
        lss = animal.last_serum_sample

        # For the validity of the test, assert there exist FCirc records and that they are for the last peak groups
        self.assertTrue(lss.fcircs.count() > 0)
        for fco in lss.fcircs.all():
            self.assertTrue(fco.is_last)

        # Now create a new last serum sample (without any peak groups)
        tissue = lss.tissue
        tc = lss.time_collected + timedelta(seconds=1)
        nlss = Sample.objects.create(animal=animal, name=lss.name + "_2", tissue=tissue, time_collected=tc)
        print(f"Created new last serum sample: {nlss.name} in tissue: {tissue.name}")
        self.lss = lss
        self.newlss = nlss

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table_serum_only.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        # call_command(
        #     "load_accucor_msruns",
        #     protocol="Default",
        #     accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
        #     date="2021-06-03",
        #     researcher="Michael Neinast",
        #     new_researcher=True,
        # )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_serum.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
        )

    def test_new_serum_leaves_is_last_unchanged(self):
        """
        Issue #460, test 3.1.1.
        3. Updates of FCirc.is_last, Sample.is_serum_sample, and Animal.last_serum_sample are triggered by themselves
           and by changes to models down to PeakGroup.
          1. Create a new serum sample whose time collected is later than existing serum samples.
            1. Confirm all FCirc.is_last values are unchanged.
        """
        # Assert that the old last serum sample still has the last tracer peakgroups
        for fco in self.lss.fcircs.all():
            self.assertTrue(fco.is_last)

    def test_new_tracer_peak_group_updates_all_is_last(self):
        """
        Issue #460, test 3.1.1.
        3. Updates of FCirc.is_last, Sample.is_serum_sample, and Animal.last_serum_sample are triggered by themselves
           and by changes to models down to PeakGroup.
          2. Create a new msrun whose date is later than the msrun of the new serum sample (created above), a new
             tracer peak group in the new serum sample (created above), and related peak group labels.
            1. Confirm all FCirc.is_last values related to the old serum sample are now false.
        """
        print("STARTING FCIRC TEST")
        # Create new protocol, msrun, peak group, and peak group labels
        ptl = Protocol.objects.create(
            name="p1",
            description="p1desc",
            category=Protocol.MSRUN_PROTOCOL,
        )
        msr = MSRun.objects.create(
            researcher="Anakin Skywalker",
            date=datetime.now(),
            sample=self.newlss,
            protocol=ptl,
        )
        pgs = PeakGroupSet.objects.create(filename="testing_dataset_file")
        print("\n\nADDING NEW PEAKGROUP...\n")
        for tracer in self.lss.animal.infusate.tracers.all():
            pg = PeakGroup.objects.create(
                name=tracer.compound.name,
                formula=tracer.compound.formula,
                msrun=msr,
                peak_group_set=pgs,
            )
            print(f"Added new peak group (id: {pg.id}) before compound: {tracer.compound.name} added")
            pg.compounds.add(tracer.compound)
            pg.save()
            print(f"Added new peak group (id: {pg.id}) for compound: {tracer.compound.name}")
            for label in self.lss.animal.labels.all():
                PeakGroupLabel.objects.create(peak_group=pg, element=label.element)

        print(f"THERE ARE {Sample.objects.count()} SAMPLE RECORDS and {PeakGroup.objects.filter(name='lysine').count()} PEAKGROUP RECORDS")
        # Assert that the old last serum sample's is_last is now false
        for fco in self.lss.fcircs.all():
            print(f"FCirc.{fco.id}.is_last_serum_peak_group() output = {fco.is_last_serum_peak_group()} and FCirc.{fco.id}.is_last = {fco.is_last}")
            # Assert that the method output is correct
            self.assertFalse(fco.is_last_serum_peak_group())
            # Assert that the field was updated
            self.assertFalse(fco.is_last)

        # Create new FCirc records
        for tracer in self.lss.animal.infusate.tracers.all():
            for label in tracer.labels.all():
                FCirc.objects.create(serum_sample=self.newlss, tracer=tracer, element=label.element)

        # Assert that the new last serum sample's is_last is true
        for fco in self.newlss.fcircs.all():
            self.assertTrue(fco.is_last)

    def test_maintained_model_relation(self):
        """
        Issue #460, test 4.
        4. Ability to propagate changes without a function decorator if no maintained fields are present

        We will do this by asserting that there's no function decorator for PeakGroup.  If there isn't, and
        test_new_tracer_peak_group_updates_all_is_last passes, then requirement(/test) 4 works.
        """
        maint_fld_funcs = [x for x in PeakGroup.get_my_updaters() if x["update_function"] is not None]
        self.assertEqual(
            0,
            len(maint_fld_funcs),
            msg=(
                "No maintained_field_function decorators means that propagation works (if "
                "test_new_tracer_peak_group_updates_all_is_last passes)"
            )
        )
        maint_mdl_rltns = [x for x in PeakGroup.get_my_updaters() if x["update_function"] is None]
        self.assertEqual(
            1,
            len(maint_mdl_rltns),
            msg=(
                "A class maintained_model_relation decorator implies that propagation works (if "
                "test_new_tracer_peak_group_updates_all_is_last passes)"
            )
        )
