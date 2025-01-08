import json

from django.core.management import call_command
from django.test import tag
from django.urls import reverse

from DataRepo.models import (
    Animal,
    ArchiveFile,
    Compound,
    Infusate,
    MSRunSample,
    MSRunSequence,
    PeakData,
    PeakGroup,
    Sample,
    Study,
    Tissue,
)
from DataRepo.models.maintained_model import (
    MaintainedModel,
    MaintainedModelCoordinator,
    UncleanBufferError,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


def assert_coordinator_state_is_initialized():
    # Obtain all coordinators that exist
    all_coordinators = [MaintainedModel._get_default_coordinator()]
    all_coordinators.extend(MaintainedModel._get_coordinator_stack())
    if 1 != len(all_coordinators):
        raise ValueError(
            f"Before setting up test data, there are {len(all_coordinators)} (not 1) MaintainedModelCoordinators."
        )
    if all_coordinators[0].auto_update_mode != "always":
        raise ValueError(
            "Before setting up test data, the default coordinator is not in always autoupdate mode."
        )
    if 0 != all_coordinators[0].buffer_size():
        raise UncleanBufferError()


class ViewTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls, disabled_coordinator=False):
        call_command("legacy_load_study", "DataRepo/data/tests/tissues/loading.yaml")
        cls.ALL_TISSUES_COUNT = 37

        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/small_obob/small_obob_compounds.tsv",
        )
        cls.ALL_COMPOUNDS_COUNT = 3

        if not disabled_coordinator:
            # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after
            # itself
            assert_coordinator_state_is_initialized()

        call_command(
            "legacy_load_samples",
            "DataRepo/data/tests/small_obob/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/data/tests/small_obob2/sample_table_headers.yaml",
        )
        # not counting the header and BLANK samples
        cls.ALL_SAMPLES_COUNT = 15
        # not counting the header and the BLANK animal
        cls.ALL_ANIMALS_COUNT = 1

        call_command(
            "legacy_load_accucor_msruns",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
        )
        cls.INF_COMPOUNDS_COUNT = 2
        cls.INF_SAMPLES_COUNT = 14
        cls.INF_PEAKDATA_ROWS = 11
        cls.INF_PEAKGROUP_COUNT = cls.INF_COMPOUNDS_COUNT * cls.INF_SAMPLES_COUNT

        call_command(
            "legacy_load_accucor_msruns",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            accucor_file=(
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_serum/"
                "small_obob_maven_6eaas_serum.xlsx"
            ),
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=False,
        )
        cls.SERUM_COMPOUNDS_COUNT = 3
        cls.SERUM_SAMPLES_COUNT = 1
        cls.SERUM_PEAKDATA_ROWS = 13
        cls.SERUM_PEAKGROUP_COUNT = cls.SERUM_COMPOUNDS_COUNT * cls.SERUM_SAMPLES_COUNT

        cls.ALL_SEQUENCES_COUNT = 1

        super().setUpTestData()

    def setUp(self):
        # Load data and buffer autoupdates before each test
        self.assert_coordinator_state_is_initialized()
        super().setUp()

    def tearDown(self):
        self.assert_coordinator_state_is_initialized()
        super().tearDown()

    def assert_coordinator_state_is_initialized(
        self, msg="MaintainedModelCoordinators are in the default state."
    ):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        # Make sure there is only the default coordinator
        self.assertEqual(
            1,
            len(all_coordinators),
            msg=msg + "  The coordinator_stack should be empty.",
        )
        # Make sure that its mode is "always"
        self.assertEqual(
            "always",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode should be 'always'.",
        )
        # Make sure that the buffer is empty to start
        for coordinator in all_coordinators:
            self.assertEqual(
                0, coordinator.buffer_size(), msg=msg + "  The buffer should be empty."
            )

    @tag("compound")
    def test_compound_list(self):
        response = self.client.get(reverse("compound_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/compound_list.html")
        self.assertEqual(
            len(response.context["compound_list"]), self.ALL_COMPOUNDS_COUNT
        )

    @tag("compound")
    def test_compound_detail(self):
        lysine = Compound.objects.filter(name="lysine").get()
        response = self.client.get(reverse("compound_detail", args=[lysine.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/compound_detail.html")
        self.assertEqual(response.context["compound"].name, "lysine")

    @tag("compound")
    def test_compound_detail_404(self):
        c = Compound.objects.order_by("id").last()
        response = self.client.get(reverse("compound_detail", args=[c.id + 1]))
        self.assertEqual(response.status_code, 404)

    @tag("compound")
    def test_infusate_detail(self):
        infusate = Infusate.objects.filter(
            tracers__compound__name__icontains="lysine"
        ).first()
        response = self.client.get(reverse("infusate_detail", args=[infusate.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/infusate_detail.html")

    @tag("compound")
    def test_infusate_detail_404(self):
        inf = Infusate.objects.order_by("id").last()
        response = self.client.get(reverse("infusate_detail", args=[inf.id + 1]))
        self.assertEqual(response.status_code, 404)

    @tag("compound")
    def test_infusate_list(self):
        response = self.client.get(reverse("infusate_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/infusate_list.html")
        self.assertEqual(len(response.context["infusate_list"]), 1)
        self.assertEqual(len(response.context["df"]), 1)

    @tag("study")
    def test_study_list(self):
        response = self.client.get(reverse("study_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/study_list.html")
        self.assertEqual(len(response.context["study_list"]), 1)
        self.assertEqual(len(response.context["df"]), 1)

    @tag("study")
    def test_study_summary(self):
        response = self.client.get("/DataRepo/studies/study_summary/")
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/study_summary.html")

    @tag("study")
    def test_study_detail(self):
        obob_fasted = Study.objects.filter(name="obob_fasted").get()
        response = self.client.get(reverse("study_detail", args=[obob_fasted.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/study_detail.html")
        self.assertEqual(response.context["study"].name, "obob_fasted")
        self.assertEqual(len(response.context["stats_df"]), 1)

    @tag("study")
    def test_study_detail_404(self):
        s = Study.objects.order_by("id").last()
        response = self.client.get(reverse("study_detail", args=[s.id + 1]))
        self.assertEqual(response.status_code, 404)

    @tag("animal")
    def test_animal_list(self):
        response = self.client.get(reverse("animal_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/animal_list.html")
        self.assertEqual(len(response.context["animal_list"]), self.ALL_ANIMALS_COUNT)
        self.assertEqual(len(response.context["df"]), self.ALL_ANIMALS_COUNT)

    @tag("animal")
    def test_animal_detail(self):
        a1 = Animal.objects.filter(name="971").get()
        response = self.client.get(reverse("animal_detail", args=[a1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/animal_detail.html")
        self.assertEqual(response.context["animal"].name, "971")
        self.assertEqual(len(response.context["df"]), self.ALL_SAMPLES_COUNT)

    @tag("animal")
    def test_animal_detail_404(self):
        a = Animal.objects.order_by("id").last()
        response = self.client.get(reverse("animal_detail", args=[a.id + 1]))
        self.assertEqual(response.status_code, 404)

    @tag("tissue")
    def test_tissue_list(self):
        response = self.client.get(reverse("tissue_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/tissue_list.html")
        self.assertEqual(len(response.context["tissue_list"]), self.ALL_TISSUES_COUNT)

    @tag("tissue")
    def test_tissue_detail(self):
        t1 = Tissue.objects.filter(name="brown_adipose_tissue").get()
        response = self.client.get(reverse("tissue_detail", args=[t1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/tissue_detail.html")
        self.assertEqual(response.context["tissue"].name, "brown_adipose_tissue")

    @tag("tissue")
    def test_tissue_detail_404(self):
        t = Tissue.objects.order_by("id").last()
        response = self.client.get(reverse("tissue_detail", args=[t.id + 1]))
        self.assertEqual(response.status_code, 404)

    @tag("sample")
    def test_sample_list(self):
        response = self.client.get("/DataRepo/samples/")
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/sample_list.html")
        self.assertEqual(len(response.context["sample_list"]), self.ALL_SAMPLES_COUNT)
        self.assertEqual(len(response.context["df"]), self.ALL_SAMPLES_COUNT)

    @tag("sample")
    def test_sample_list_per_animal(self):
        a1 = Animal.objects.filter(name="971").get()
        s1 = Sample.objects.filter(animal_id=a1.id)
        response = self.client.get("/DataRepo/samples/?animal_id=" + str(a1.pk))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/sample_list.html")
        self.assertEqual(len(response.context["sample_list"]), s1.count())
        self.assertEqual(len(response.context["df"]), s1.count())

    @tag("sample")
    def test_sample_detail(self):
        s1 = Sample.objects.filter(name="BAT-xz971").get()
        response = self.client.get(reverse("sample_detail", args=[s1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/sample_detail.html")
        self.assertEqual(response.context["sample"].name, "BAT-xz971")

    @tag("sample")
    def test_sample_detail_404(self):
        s = Sample.objects.order_by("id").last()
        response = self.client.get(reverse("sample_detail", args=[s.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_msrun_sample_list(self):
        response = self.client.get(reverse("msrunsample_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/msrunsample_list.html")
        self.assertEqual(len(response.context["msrun_samples"]), self.ALL_SAMPLES_COUNT)

    def test_msrun_sequence_list(self):
        response = self.client.get(reverse("msrunsequence_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/msrunsequence_list.html")
        self.assertEqual(len(response.context["sequences"]), self.ALL_SEQUENCES_COUNT)

    def test_msrun_sample_detail(self):
        ms1 = MSRunSample.objects.filter(sample__name="BAT-xz971").get()
        response = self.client.get(reverse("msrunsample_detail", args=[ms1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/msrunsample_detail.html")
        self.assertEqual(response.context["msrun_sample"].sample.name, "BAT-xz971")

    def test_msrun_sample_detail_404(self):
        ms = MSRunSample.objects.order_by("id").last()
        response = self.client.get(reverse("msrunsample_detail", args=[ms.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_msrun_sequence_detail(self):
        ms1 = MSRunSequence.objects.filter(
            msrun_samples__sample__name="BAT-xz971"
        ).get()
        response = self.client.get(reverse("msrunsequence_detail", args=[ms1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/msrunsequence_detail.html")
        self.assertEqual(
            self.ALL_SAMPLES_COUNT, response.context["sequence"].msrun_samples.count()
        )

    def test_msrun_sequence_detail_404(self):
        ms = MSRunSequence.objects.order_by("id").last()
        response = self.client.get(reverse("msrunsequence_detail", args=[ms.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_archive_file_list(self):
        response = self.client.get(reverse("archive_file_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/archive_file_list.html")
        self.assertEqual(len(response.context["archive_file_list"]), 2)

    def test_archive_file_detail(self):
        af1 = ArchiveFile.objects.filter(
            filename="small_obob_maven_6eaas_inf.xlsx"
        ).get()
        response = self.client.get(reverse("archive_file_detail", args=[af1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/archive_file_detail.html")
        self.assertEqual(
            response.context["archivefile"].filename, "small_obob_maven_6eaas_inf.xlsx"
        )

    def test_archive_file_detail_404(self):
        af = ArchiveFile.objects.order_by("id").last()
        response = self.client.get(reverse("archive_file_detail", args=[af.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_peakgroup_list(self):
        response = self.client.get("/DataRepo/peakgroups/")
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/peakgroup_list.html")
        self.assertEqual(
            len(response.context["peakgroup_list"]),
            self.INF_PEAKGROUP_COUNT + self.SERUM_PEAKGROUP_COUNT,
        )

    def test_peakgroup_list_per_msrun_sample(self):
        ms1 = MSRunSample.objects.filter(sample__name="BAT-xz971").get()
        pg1 = PeakGroup.objects.filter(msrun_sample_id=ms1.id)
        response = self.client.get(
            "/DataRepo/peakgroups/?msrun_sample_id=" + str(ms1.pk)
        )
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/peakgroup_list.html")
        self.assertEqual(len(response.context["peakgroup_list"]), pg1.count())

    def test_peakgroup_detail(self):
        ms1 = MSRunSample.objects.filter(sample__name="BAT-xz971").get()
        pg1 = PeakGroup.objects.filter(msrun_sample_id=ms1.id).first()
        response = self.client.get(reverse("peakgroup_detail", args=[pg1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/peakgroup_detail.html")
        self.assertEqual(response.context["peakgroup"].name, pg1.name)

    def test_peakgroup_detail_404(self):
        pg = PeakGroup.objects.order_by("id").last()
        response = self.client.get(reverse("peakgroup_detail", args=[pg.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_peakdata_list(self):
        """
        the total rows loaded may be greater than total rows in file for each sample,
        since rows are created during loading for missing labeled_count
        """
        response = self.client.get("/DataRepo/peakdata/")
        pd = PeakData.objects.all()
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/peakdata_list.html")
        self.assertEqual(len(response.context["peakdata_list"]), pd.count())

    def test_peakdata_list_per_peakgroup(self):
        pg1 = PeakGroup.objects.filter(msrun_sample__sample__name="serum-xz971").first()
        pd1 = PeakData.objects.filter(peak_group_id=pg1.pk)
        response = self.client.get("/DataRepo/peakdata/?peak_group_id=" + str(pg1.pk))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/peakdata_list.html")
        self.assertEqual(len(response.context["peakdata_list"]), pd1.count())

    def test_search_advanced_browse(self):
        """
        Load the advanced search page in browse mode and make sure the mode is added to the context data
        """
        response = self.client.get(
            "/DataRepo/search_advanced/?mode=browse&format=pdtemplate"
        )
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/search/query.html")
        self.assertEqual(response.context["mode"], "browse")
        self.assertEqual(response.context["format"], "pdtemplate")

    def test_search_advanced_search(self):
        """
        Load the advanced search page in the default search mode and make sure the mode is added to the context data
        """
        response = self.client.get("/DataRepo/search_advanced/")
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/search/query.html")
        self.assertEqual(response.context["mode"], "search")

    def get_advanced_search_inputs(self):
        asform = {
            "fmt": "pgtemplate",
            "form-TOTAL_FORMS": "3",
            "form-INITIAL_FORMS": "0",
            "form-0-pos": "pgtemplate-PeakGroups-selected.0-all-False.0",
            "form-0-fld": "msrun_sample__sample__tissue__name",
            "form-0-ncmp": "iexact",
            "form-0-val": "Brain",
            "form-0-units": "identity",
            "form-1-pos": "pdtemplate-PeakData.0-all-False.0",
            "form-1-fld": "labels__element",
            "form-1-ncmp": "iexact",
            "form-1-units": "identity",
            "form-2-pos": "fctemplate-FCirc.0-all-False.0",
            "form-2-fld": "msrun_sample__sample__animal__name",
            "form-2-ncmp": "iexact",
            "form-2-units": "identity",
        }
        qry = self.get_advanced_qry()
        dlform = {
            "form-TOTAL_FORMS": "1",
            "form-INITIAL_FORMS": "0",
            "qryjson": json.dumps(qry),
        }
        return [asform, qry, dlform]

    def get_advanced_qry(self):
        """
        Create a simple advanced query
        """
        return {
            "selectedtemplate": "pgtemplate",
            "searches": {
                "pgtemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "fld": "msrun_sample__sample__tissue__name",
                                "ncmp": "iexact",
                                "static": "",
                                "val": "Brain",
                                "units": "identity",
                            }
                        ],
                    },
                    "name": "PeakGroups",
                },
                "pdtemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "ncmp": "iexact",
                                "static": "",
                                "fld": "labels__element",
                                "val": "",
                                "units": "identity",
                            }
                        ],
                    },
                    "name": "PeakData",
                },
                "fctemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "fld": "msrun_sample__sample__animal__name",
                                "ncmp": "iexact",
                                "static": "",
                                "val": "",
                                "units": "identity",
                            }
                        ],
                    },
                    "name": "FCirc",
                },
            },
        }

    def test_search_advanced_valid(self):
        """
        Do a simple advanced search and make sure the results are correct
        """
        qs = PeakGroup.objects.filter(
            msrun_sample__sample__tissue__name__iexact="Brain"
        ).prefetch_related("msrun_sample__sample__animal__studies")
        [filledform, qry, ignore] = self.get_advanced_search_inputs()
        response = self.client.post("/DataRepo/search_advanced/", filledform)
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/search/query.html")
        self.assertEqual(len(response.context["res"]), qs.count())
        self.assertEqual(qry, response.context["qry"])

    def test_search_advanced_invalid(self):
        """
        Do a simple advanced search and make sure the results are correct
        """
        [invalidform, qry, ignore] = self.get_advanced_search_inputs()
        # Make the form invalid
        invalidform.pop("form-0-val", None)
        # Expected response difference:
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0]["val"] = ""
        response = self.client.post("/DataRepo/search_advanced/", invalidform)
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/search/query.html")
        self.assertEqual(len(response.context["res"]), 0)
        self.assertEqual(qry, response.context["qry"])

    def test_search_advanced_tsv(self):
        """
        Download a simple advanced search and make sure the results are correct
        """
        [filledform, qry, dlform] = self.get_advanced_search_inputs()
        response = self.client.post("/DataRepo/search_advanced_tsv/", dlform)

        # Response content settings
        self.assertEqual(response.get("Content-Type"), "application/text")
        self.assertEqual(response.status_code, 200)
        # Cannot use assertContains here for non-http response - it will complain about a missing status_code
        contentdisp = response.get("Content-Disposition")
        self.assertTrue("attachment" in contentdisp)
        self.assertTrue("PeakGroups" in contentdisp)
        self.assertTrue(".tsv" in contentdisp)


class ViewNullToleranceTests(ViewTests):
    """
    This class inherits from the ViewTests class above and overrides the setUpTestData method to load without auto-
    updates.

    All super tests are executed.  Those that are broken are overridden here to have something to apply the broken tags
    to.
    """

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData(disabled_coordinator=True)

    @classmethod
    def setUpClass(self):
        # Silently dis-allow auto-updates by adding a disabled coordinator
        disabled_coordinator = MaintainedModelCoordinator("disabled")
        MaintainedModel._add_coordinator(disabled_coordinator)
        super().setUpClass()

    def setUp(self):
        # Load data and buffer autoupdates before each test
        self.assert_coordinator_state_is_initialized()
        super().setUp()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        MaintainedModel._reset_coordinators()

    def assert_coordinator_state_is_initialized(
        self, msg="MaintainedModelCoordinators are in the default state."
    ):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        # Make sure there is only the default coordinator
        self.assertEqual(
            2,
            len(all_coordinators),
            msg=msg + "  The coordinator_stack should have the disabled coordinator.",
        )
        # Make sure that its mode is "always"
        self.assertEqual(
            "always",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode should be 'always'.",
        )
        # Make sure that the buffer is empty to start
        for coordinator in all_coordinators:
            self.assertEqual(
                0, coordinator.buffer_size(), msg=msg + "  The buffer should be empty."
            )

    def test_study_list(self):
        """Make sure this page works when infusate/tracer, and/or tracer label names are None"""
        super().test_study_list()

    def test_study_detail(self):
        """Make sure this page works when infusate/tracer, and/or tracer label names are None"""
        super().test_study_detail()
