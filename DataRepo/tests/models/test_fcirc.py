from datetime import datetime, timedelta

from django.conf import settings
from django.core.management import call_command
from django.test import override_settings

from DataRepo.models import (
    Animal,
    FCirc,
    MSRun,
    PeakGroup,
    PeakGroupLabel,
    PeakGroupSet,
    Protocol,
    Sample,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
class FCircTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()

        # For the validity of the test, assert there exist FCirc records and that they are for the last peak groups
        self.assertTrue(self.lss.fcircs.count() > 0)
        for fco in self.lss.fcircs.all():
            self.assertTrue(fco.is_last)

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
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_serum.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
        )

        # Get an animal (assuming it has an infusate/tracers/etc)
        animal = Animal.objects.last()
        # Get its last serum sample
        lss = animal.last_serum_sample

        # Now create a new last serum sample (without any peak groups)
        tissue = lss.tissue
        tc = lss.time_collected + timedelta(seconds=1)
        nlss = Sample.objects.create(
            animal=animal, name=lss.name + "_2", tissue=tissue, time_collected=tc
        )

        cls.lss = lss
        cls.newlss = nlss

        super().setUpTestData()

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
        for tracer in self.lss.animal.infusate.tracers.all():
            pg = PeakGroup.objects.create(
                name=tracer.compound.name,
                formula=tracer.compound.formula,
                msrun=msr,
                peak_group_set=pgs,
            )
            pg.compounds.add(tracer.compound)
            # We don't need to call pg.save() here because I added an m2m handler to make .add() calls trigger a save.
            for label in self.lss.animal.labels.all():
                PeakGroupLabel.objects.create(peak_group=pg, element=label.element)

        # Assert that the old last serum sample's is_last is now false
        for fco in self.lss.fcircs.all():
            # Assert that the method output is correct
            self.assertFalse(fco.is_last_serum_peak_group())
            # Assert that the field was updated
            self.assertFalse(fco.is_last)

        self.create_newlss_fcirc_recs()

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
        maint_fld_funcs = [
            x for x in PeakGroup.get_my_updaters() if x["update_function"] is not None
        ]
        self.assertEqual(
            0,
            len(maint_fld_funcs),
            msg=(
                "No maintained_field_function decorators means that propagation works (if "
                "test_new_tracer_peak_group_updates_all_is_last passes)"
            ),
        )
        maint_mdl_rltns = [
            x for x in PeakGroup.get_my_updaters() if x["update_function"] is None
        ]
        self.assertEqual(
            1,
            len(maint_mdl_rltns),
            msg=(
                "A class maintained_model_relation decorator implies that propagation works (if "
                "test_new_tracer_peak_group_updates_all_is_last passes)"
            ),
        )

    def create_newlss_fcirc_recs(self):
        # Create FCirc records for self.newlss
        for tracer in self.lss.animal.infusate.tracers.all():
            for label in tracer.labels.all():
                FCirc.objects.create(
                    serum_sample=self.newlss, tracer=tracer, element=label.element
                )

    def test_serum_validity_valid(self):
        # Deleting the newlss should make lss valid because the actual last serum sample has peakgroups and the newlss
        # does not.  This depends on autoupdates to update Animal.last_serum_sample
        self.newlss.delete()
        for fcr in self.lss.fcircs.all():
            self.assertTrue(fcr.serum_validity["valid"])
            self.assertIn(
                "No significant problems found", fcr.serum_validity["message"]
            )
            self.assertEqual("good", fcr.serum_validity["level"])
            self.assertEqual("000000000", fcr.serum_validity["bitcode"])

    def test_serum_validity_no_peakgroup(self):
        self.create_newlss_fcirc_recs()

        self.assertTrue(self.newlss.fcircs.count() > 0)
        for fcr in self.newlss.fcircs.all():
            self.assertFalse(fcr.serum_validity["valid"])
            self.assertIn("No serum", fcr.serum_validity["message"])
            self.assertEqual("error", fcr.serum_validity["level"])

            # The bits in the following test explained...
            # 1 - srmsmpl_has_no_trcr_pgs - 1 = No peak groups exist for this serum sample/tracer combo.
            # 0 - last_trcr_pg_but_smpl_tmclctd_is_none_amng_many - 0 = "I" am either a serum sample holding a "not
            #                                                           last" peakgroup for my tracer or I have a time
            #                                                           collected.
            # 0 - srmsmpl_has_no_trcr_pgs - 0 = My serum sample has the last peakgroup for my tracer and I'm the last
            #                                   serum sample.
            # 0 - sib_of_last_smpl_tmclctd_is_none - 0 = There are either no other serum samples or they all have a
            #                                            time_collected.
            # 0 - prev_smpl_tmclctd_is_none_amng_many - 0 = There is either only 1 serum sample or (I'm not last and I
            #                                               have a time_collected).
            # 0 - msr_date_is_none_and_many_msrs_for_smpl - 0 = This FCirc record's serum sample either has only 1
            #                                                   MSRun or its date has a value.
            # 1 - overall - 1 = Status is not "good" overall.
            # 0 - tmclctd_is_none_but_only1_smpl - 0 = There are either multiple serum samples or there is 1 and it has
            #                                          a time collected.
            # 0 - msr_date_is_none_but_only1_msr_for_smpl - 0 = There are either many MSRuns for this serum sample or
            #                                                   there is 1 & it has a date.
            self.assertEqual("100000100", fcr.serum_validity["bitcode"])

    def test_serum_validity_no_time_collected(self):
        # When we null the time collected for lss, newlss is still the last serun sample, but the fcirc record for the
        # original lss is still the "last" one with peak groups - because we haven't added any peak groups to newlss
        # for this test

        tcbak = self.lss.time_collected
        self.lss.time_collected = None
        self.lss.save()

        for fcr in self.lss.fcircs.all():
            self.assertFalse(fcr.serum_validity["valid"])
            self.assertIn(
                "The sample time collected is not set for this last serum tracer peak group",
                fcr.serum_validity["message"],
            )
            self.assertEqual("error", fcr.serum_validity["level"])

            # The bits in the following test explained...
            # 0 - srmsmpl_has_no_trcr_pgs - 0 = Peak groups exist for this serum sample/tracer combo.
            # 1 - last_trcr_pg_but_smpl_tmclctd_is_none_amng_many - 1 = "I" am the serum sample for the last peakgroup
            #                                                           for this tracer, other serum samples exist, and
            #                                                           "my" time collected is null.
            # 1 - srmsmpl_has_no_trcr_pgs - 1 = My serum sample has the last peakgroup for my tracer but I'm not the
            #                                   last serum sample.
            # 0 - sib_of_last_smpl_tmclctd_is_none - 0 = There are either no other serum samples or they all have a
            #                                            time_collected.
            # 0 - prev_smpl_tmclctd_is_none_amng_many - 0 = There is either only 1 serum sample or (I'm not last and I
            #                                               have a time_collected).
            # 0 - msr_date_is_none_and_many_msrs_for_smpl - 0 = This FCirc record's serum sample either has only 1
            #                                                   MSRun or its date has a value.
            # 1 - overall - 1 = Status is not "good" overall.
            # 0 - tmclctd_is_none_but_only1_smpl - 0 = There are either multiple serum samples or there is 1 and it has
            #                                          a time collected.
            # 0 - msr_date_is_none_but_only1_msr_for_smpl - 0 = There are either many MSRuns for this serum sample or
            #                                                   there is 1 & it has a date.
            self.assertEqual("011000100", fcr.serum_validity["bitcode"])

        self.lss.time_collected = tcbak
        self.lss.save()

    def test_serum_validity_sibling_has_null_time_collected(self):
        tcbak = self.newlss.time_collected
        self.newlss.time_collected = None
        self.newlss.save()

        self.create_newlss_fcirc_recs()

        # Now lss is the last serum sample because it has a time_collected, and it has to be used for the FCirc
        # calculations because only it has peak groups, but another serum sample exists with a null time_collected.
        # This creates a warning state.

        self.assertTrue(self.newlss.fcircs.count() > 0)
        for fcr in self.lss.fcircs.all():
            self.assertTrue(fcr.is_last)
            self.assertFalse(fcr.serum_validity["valid"])
            self.assertIn(
                "may not actually be the last one", fcr.serum_validity["message"]
            )
            self.assertEqual("warn", fcr.serum_validity["level"])

            # The bits in the following test explained...
            # 0 - srmsmpl_has_no_trcr_pgs - 0 = Peak groups exist for this serum sample/tracer combo.
            # 0 - last_trcr_pg_but_smpl_tmclctd_is_none_amng_many - 0 = "I" am either a serum sample holding a "not
            #                                                           last" peakgroup for my tracer or I have a time
            #                                                           collected.
            # 0 - srmsmpl_has_no_trcr_pgs - 0 = My serum sample has the last peakgroup for my tracer and I'm the last
            #                                   serum sample.
            # 1 - sib_of_last_smpl_tmclctd_is_none - 1 = There are multiple serum samples and some don't have a
            #                                            time_collected.
            # 0 - prev_smpl_tmclctd_is_none_amng_many - 0 = There is either only 1 serum sample or (I'm not last and I
            #                                               have a time_collected).
            # 0 - msr_date_is_none_and_many_msrs_for_smpl - 0 = This FCirc record's serum sample either has only 1
            #                                                   MSRun or its date has a value.
            # 1 - overall - 1 = Status is not "good" overall.
            # 0 - tmclctd_is_none_but_only1_smpl - 0 = There are either multiple serum samples or there is 1 and it has
            #                                          a time collected.
            # 0 - msr_date_is_none_but_only1_msr_for_smpl - 0 = There are either many MSRuns for this serum sample or
            #                                                   there is 1 & it has a date.
            self.assertEqual("000100100", fcr.serum_validity["bitcode"])

        self.newlss.time_collected = tcbak
        self.newlss.save()

    def test_serum_validity_previous_time_collected_is_null(self):
        tcbak = self.newlss.time_collected
        self.newlss.time_collected = None
        self.newlss.save()

        # To get to the prev_smpl_tmclctd_is_none_amng_many state of 1, there must exist peakgroups for newlss
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
        for tracer in self.lss.animal.infusate.tracers.all():
            pg = PeakGroup.objects.create(
                name=tracer.compound.name,
                formula=tracer.compound.formula,
                msrun=msr,
                peak_group_set=pgs,
            )
            pg.compounds.add(tracer.compound)
            # We don't need to call pg.save() here because I added an m2m handler to make .add() calls trigger a save.
            for label in self.lss.animal.labels.all():
                PeakGroupLabel.objects.create(peak_group=pg, element=label.element)

        self.create_newlss_fcirc_recs()

        # newlss is the last serum sample and it has peak groups, but lss has to be used for the FCirc
        # calculations because only it has a time_collected.  This creates a warning state.

        self.assertTrue(self.newlss.fcircs.count() > 0)
        for fcr in self.newlss.fcircs.all():
            self.assertFalse(fcr.serum_validity["valid"])
            self.assertIn(
                "The sample time collected is not set for this previous",
                fcr.serum_validity["message"],
            )
            self.assertEqual("warn", fcr.serum_validity["level"])

            # The bits in the following test explained...
            # 0 - srmsmpl_has_no_trcr_pgs - 0 = Peak groups exist for this serum sample/tracer combo.
            # 0 - last_trcr_pg_but_smpl_tmclctd_is_none_amng_many - 0 = "I" am either a serum sample holding a "not
            #                                                           last" peakgroup for my tracer or I have a time
            #                                                           collected.
            # 0 - srmsmpl_has_no_trcr_pgs - 0 = My serum sample has the last peakgroup for my tracer and I'm the last
            #                                   serum sample.
            # 0 - sib_of_last_smpl_tmclctd_is_none - 1 = There are multiple serum samples and some don't have a
            #                                            time_collected.
            # 1 - prev_smpl_tmclctd_is_none_amng_many - 1 = There are many serum samples and my time_collected is null.
            # 0 - msr_date_is_none_and_many_msrs_for_smpl - 0 = This FCirc record's serum sample either has only 1
            #                                                   MSRun or its date has a value.
            # 1 - overall - 1 = Status is not "good" overall.
            # 0 - tmclctd_is_none_but_only1_smpl - 0 = There are either multiple serum samples or there is 1 and it has
            #                                          a time collected.
            # 0 - msr_date_is_none_but_only1_msr_for_smpl - 0 = There are either many MSRuns for this serum sample or
            #                                                   there is 1 & it has a date.
            self.assertEqual("000010100", fcr.serum_validity["bitcode"])

        self.newlss.time_collected = tcbak
        self.newlss.save()
