import json
import os.path
import tempfile

from django.conf import settings
from django.core.management import call_command
from django.test import override_settings, tag
from django.urls import reverse

from DataRepo.models import (
    Animal,
    ArchiveFile,
    Compound,
    Infusate,
    MSRun,
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
from DataRepo.models.utilities import get_all_models
from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    TracebaseTransactionTestCase,
)
from DataRepo.views import DataValidationView


def assert_coordinator_state_is_initialized():
    # Obtain all coordinators that exist
    all_coordinators = [MaintainedModel._get_default_coordinator()]
    all_coordinators.extend(MaintainedModel._get_coordinator_stack())
    if 1 != len(all_coordinators):
        raise ValueError(
            f"Before setting up test data, there are {len(all_coordinators)} MaintainedModelCoordinators."
        )
    if all_coordinators[0].auto_update_mode != "immediate":
        raise ValueError(
            "Before setting up test data, the default coordinator is not in immediate autoupdate mode."
        )
    if 0 != all_coordinators[0].buffer_size():
        raise UncleanBufferError()


class ViewTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls, disabled_coordinator=False):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        cls.ALL_TISSUES_COUNT = 37

        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
        )
        cls.ALL_COMPOUNDS_COUNT = 3

        if not disabled_coordinator:
            # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after
            # itself
            assert_coordinator_state_is_initialized()

        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
            skip_cache_updates=True,
        )
        # not counting the header and BLANK samples
        cls.ALL_SAMPLES_COUNT = 15
        # not counting the header and the BLANK animal
        cls.ALL_ANIMALS_COUNT = 1

        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
            skip_cache_updates=True,
        )
        cls.INF_COMPOUNDS_COUNT = 2
        cls.INF_SAMPLES_COUNT = 14
        cls.INF_PEAKDATA_ROWS = 11
        cls.INF_PEAKGROUP_COUNT = cls.INF_COMPOUNDS_COUNT * cls.INF_SAMPLES_COUNT

        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_serum.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=False,
            skip_cache_updates=True,
        )
        cls.SERUM_COMPOUNDS_COUNT = 3
        cls.SERUM_SAMPLES_COUNT = 1
        cls.SERUM_PEAKDATA_ROWS = 13
        cls.SERUM_PEAKGROUP_COUNT = cls.SERUM_COMPOUNDS_COUNT * cls.SERUM_SAMPLES_COUNT

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
        # Make sure that its mode is "immediate"
        self.assertEqual(
            "immediate",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode is 'immediate'.",
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

    def test_msrun_list(self):
        response = self.client.get(reverse("msrun_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/msrun_list.html")
        self.assertEqual(len(response.context["msrun_list"]), self.ALL_SAMPLES_COUNT)

    def test_msrun_detail(self):
        ms1 = MSRun.objects.filter(sample__name="BAT-xz971").get()
        response = self.client.get(reverse("msrun_detail", args=[ms1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/msrun_detail.html")
        self.assertEqual(response.context["msrun"].sample.name, "BAT-xz971")

    def test_msrun_detail_404(self):
        ms = MSRun.objects.order_by("id").last()
        response = self.client.get(reverse("msrun_detail", args=[ms.id + 1]))
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

    def test_peakgroup_list_per_msrun(self):
        ms1 = MSRun.objects.filter(sample__name="BAT-xz971").get()
        pg1 = PeakGroup.objects.filter(msrun_id=ms1.id)
        response = self.client.get("/DataRepo/peakgroups/?msrun_id=" + str(ms1.pk))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/peakgroup_list.html")
        self.assertEqual(len(response.context["peakgroup_list"]), pg1.count())

    def test_peakgroup_detail(self):
        ms1 = MSRun.objects.filter(sample__name="BAT-xz971").get()
        pg1 = PeakGroup.objects.filter(msrun_id=ms1.id).first()
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
        pg1 = PeakGroup.objects.filter(msrun__sample__name="serum-xz971").first()
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
            "form-0-fld": "msrun__sample__tissue__name",
            "form-0-ncmp": "iexact",
            "form-0-val": "Brain",
            "form-0-units": "identity",
            "form-1-pos": "pdtemplate-PeakData.0-all-False.0",
            "form-1-fld": "labels__element",
            "form-1-ncmp": "iexact",
            "form-1-units": "identity",
            "form-2-pos": "fctemplate-FCirc.0-all-False.0",
            "form-2-fld": "msrun__sample__animal__name",
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
                                "fld": "msrun__sample__tissue__name",
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
                                "fld": "msrun__sample__animal__name",
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
            msrun__sample__tissue__name__iexact="Brain"
        ).prefetch_related("msrun__sample__animal__studies")
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
        # Make sure that its mode is "immediate"
        self.assertEqual(
            "immediate",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode is 'immediate'.",
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


class ValidationViewTests(TracebaseTransactionTestCase):
    """
    Note, without the TransactionTestCase (derived) class (and the with transaction.atomic block below), the infusate-
    related model managers produce the following error:
        django.db.transaction.TransactionManagementError: An error occurred in the current transaction. You can't
        execute queries until the end of the 'atomic' block.
    ...associated with the outer atomic transaction of any normal test case.  See:
    https://stackoverflow.com/questions/21458387/transactionmanagementerror-you-cant-execute-queries-until-the-end-of-the-atom
    """

    fixtures = ["data_types.yaml", "data_formats.yaml"]

    def assert_coordinator_state_is_initialized(
        self, msg="MaintainedModelCoordinators are in the default state."
    ):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        # Make sure there is only the default coordinator
        self.assertEqual(
            1, len(all_coordinators), msg=msg + "  The coordinator_stack is empty."
        )
        # Make sure that its mode is "immediate"
        self.assertEqual(
            "immediate",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode is 'immediate'.",
        )
        # Make sure that the buffer is empty to start
        for coordinator in all_coordinators:
            self.assertEqual(
                0, coordinator.buffer_size(), msg=msg + "  The buffer is empty."
            )

    @classmethod
    def initialize_databases(cls):
        # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after itself
        assert_coordinator_state_is_initialized()

        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )

    @classmethod
    def clear_database(cls):
        """
        Clears out the contents of the supplied database and confirms it's empty.
        """
        # Note that get_all_models is implemented to return the models in an order that facilitates this deletion
        for mdl in get_all_models():
            mdl.objects.all().delete()
        # Make sure the database is actually empty so that the tests are meaningful
        sum = cls.sum_record_counts()
        assert sum == 0

    @classmethod
    def sum_record_counts(cls):
        record_counts = cls.get_record_counts()
        sum = 0
        for cnt in record_counts:
            sum += cnt
        return sum

    @classmethod
    def get_record_counts(cls):
        record_counts = []
        for mdl in get_all_models():
            record_counts.append(mdl.objects.all().count())
        return record_counts

    def test_validate_view(self):
        """
        Do a simple validation view test
        """
        response = self.client.get(reverse("validate"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/validate_submission.html")

    def test_validate_files_good(self):
        """
        Do a file validation test
        """
        # Load the necessary tissues & compounds for a successful test
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )

        # Files/inputs we will test
        sf = "DataRepo/example_data/data_submission_good/animal_sample_table.xlsx"
        afs = [
            "DataRepo/example_data/data_submission_good/accucor1.xlsx",
            "DataRepo/example_data/data_submission_good/accucor2.xlsx",
        ]

        sfkey = "animal_sample_table.xlsx"
        af1key = "accucor1.xlsx"
        af2key = "accucor2.xlsx"

        # Test the get_validation_results function
        # This call indirectly tests that ValidationView.validate_stody returns a MultiLoadStatus object on success
        # It also indirectly ensures that create_yaml(dir) puts a loading.yaml file in the dir
        [results, valid, exceptions, ne, nw] = self.validate_some_files(sf, afs)

        # There is a researcher named "anonymous", but that name is ignored
        self.assertTrue(
            valid, msg=f"There should be no errors in any file: {exceptions}"
        )

        # The sample file's researcher is "Anonymous" and it's not in the database, but the researcher check ignores
        # researchers named "anonymous" (case-insensitive)
        self.assertEqual("PASSED", results[sfkey])
        self.assertEqual(0, len(exceptions[sfkey]))

        # Check the accucor file details
        self.assert_accucor_files_pass([af1key, af2key], results, exceptions)

    @override_settings(DEBUG=True)
    def test_validate_files_with_sample_warning(self):
        """
        Do a file validation test
        """
        self.initialize_databases()

        # Load some data that should cause a researcher warning during validation (an unknown researcher error will not
        # be raised if there are no researchers loaded in the database)
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
            validate=True,
            skip_cache_updates=True,
        )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
            validate=True,
            skip_cache_updates=True,
        )

        # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after itself
        self.assert_coordinator_state_is_initialized()

        # Files/inputs we will test
        sf = "DataRepo/example_data/data_submission_sample_unkres_acc_good/animal_sample_table.xlsx"
        afs = [
            "DataRepo/example_data/data_submission_sample_unkres_acc_good/accucor1.xlsx",
            "DataRepo/example_data/data_submission_sample_unkres_acc_good/accucor2.xlsx",
        ]

        sfkey = "animal_sample_table.xlsx"
        af1key = "accucor1.xlsx"
        af2key = "accucor2.xlsx"

        # Test the get_validation_results function
        [
            results,
            valid,
            exceptions,
            num_errors,
            num_warnings,
        ] = self.validate_some_files(sf, afs)

        if settings.DEBUG:
            print(
                f"VALID: {valid}\nALL RESULTS: {results}\nALL EXCEPTIONS: {exceptions}"
            )

        # NOTE: When the unknown researcher error is raised, the sample table load would normally be rolled back.  The
        # subsequent accucor load would then fail (to find any more errors), because it can't find the same names in
        # the database.  Sample table loader needs to raise the exception to communicate the issues to the validate
        # interface, so in validation mode, it raises the exception outside of the atomic transaction block, which
        # won't rollback the erroneous load, so the validation code wraps everything in an outer atomic transaction and
        # rolls back everything at the end.

        # There is a researcher named "George Costanza" that should be unknown, making the overall status false.  Any
        # error or warning will cause is_valid to be false
        self.assertFalse(
            valid,
            msg=(
                "Should be valid. The 'George Costanza' researcher should cause a warning, so there should be 1 "
                f"warning: [{exceptions}] for the sample file."
            ),
        )

        # The sample file's researcher is "Anonymous" and it's not in the database, but the researcher check ignores
        # researchers named "anonymous" (case-insensitive)
        self.assertEqual(
            "WARNING",
            results[sfkey],
            msg=f"There should only be 1 warning for file {sfkey}: {exceptions[sfkey]}",
        )
        self.assertEqual(0, num_errors[sfkey])
        self.assertEqual(1, num_warnings[sfkey])
        self.assertEqual("UnknownResearcherError", exceptions[sfkey][0]["type"])

        # Check the accucor file details
        self.assert_accucor_files_pass([af1key, af2key], results, exceptions)

    def test_databases_unchanged(self):
        """
        Test to ensure that validating user submitted data does not change either database
        """
        self.clear_database()
        self.initialize_databases()

        # Get initial record counts for all models
        tb_init_counts = self.get_record_counts()
        coordinator = MaintainedModel._get_current_coordinator()
        pre_load_maintained_values = coordinator.get_all_maintained_field_values(
            "DataRepo.models"
        )

        sample_file = (
            "DataRepo/example_data/data_submission_good/animal_sample_table.xlsx"
        )
        accucor_files = [
            "DataRepo/example_data/data_submission_good/accucor1.xlsx",
            "DataRepo/example_data/data_submission_good/accucor2.xlsx",
        ]

        self.validate_some_files(sample_file, accucor_files)

        # Get record counts for all models
        tb_post_counts = self.get_record_counts()
        post_load_maintained_values = coordinator.get_all_maintained_field_values(
            "DataRepo.models"
        )

        self.assertListEqual(tb_init_counts, tb_post_counts)
        self.assertEqual(pre_load_maintained_values, post_load_maintained_values)

    def test_tissues_load(self):
        """
        Test to ensure that tissues load in both databases by default
        """
        self.clear_database()
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        self.assertGreater(Tissue.objects.all().count(), 0)

    @override_settings(VALIDATION_ENABLED=False)
    def test_validate_view_disabled_redirect(self):
        """
        Do a simple validation view test when validation is disabled
        """
        response = self.client.get(reverse("validate"))
        self.assertEqual(
            response.status_code, 302, msg="Make sure the view is redirected"
        )

    @override_settings(VALIDATION_ENABLED=False)
    def test_validate_view_disabled_template(self):
        """
        Do a simple validation view test when validation is disabled
        """
        response = self.client.get(reverse("validate"), follow=True)
        self.assertTemplateUsed(response, "validation_disabled.html")

    def test_accucor_validation_error(self):
        self.clear_database()
        self.initialize_databases()

        sample_file = "DataRepo/example_data/small_dataset/small_obob_animal_and_sample_table.xlsx"
        accucor_files = [
            "DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_req_prefix.xlsx",
        ]
        sfkey = "small_obob_animal_and_sample_table.xlsx"
        afkey = "small_obob_maven_6eaas_inf_req_prefix.xlsx"
        [
            results,
            valid,
            exceptions,
            num_errors,
            num_warnings,
        ] = self.validate_some_files(sample_file, accucor_files)

        self.assertFalse(valid)

        # Sample file
        self.assertTrue(sfkey in results)
        self.assertEqual(
            "PASSED",
            results[sfkey],
            msg=f"There should be no exceptions for file {sfkey}: {exceptions[afkey][0]}",
        )

        self.assertTrue(sfkey in exceptions)
        self.assertEqual(0, num_errors[sfkey])
        self.assertEqual(0, num_warnings[sfkey])

        # Accucor file
        self.assertTrue(afkey in results)
        self.assertEqual("FAILED", results[afkey])

        self.assertTrue(
            afkey in exceptions,
            msg=f"{afkey} should be a key in the exceptions dict.  Its keys are: {exceptions.keys()}",
        )
        self.assertEqual(1, num_errors[afkey])
        self.assertEqual("NoSamplesError", exceptions[afkey][0]["type"])
        self.assertEqual(0, num_warnings[afkey])

    def validate_some_files(self, sample_file, accucor_files):
        # Test the get_validation_results function
        vo = DataValidationView()
        vo.set_files(sample_file, accucor_files)
        # Now try validating the load files
        valid, results, exceptions, _ = vo.get_validation_results()

        file_keys = []
        file_keys.append(os.path.basename(sample_file))
        for afile in accucor_files:
            file_keys.append(os.path.basename(afile))

        for file_key in file_keys:
            self.assertTrue(file_key in results)
            self.assertTrue(file_key in exceptions)

        num_errors = {}
        num_warnings = {}
        for file in exceptions.keys():
            num_errors[file] = 0
            num_warnings[file] = 0
            for exc in exceptions[file]:
                if exc["is_error"]:
                    num_errors[file] += 1
                else:
                    num_warnings[file] += 1

        if settings.DEBUG:
            print(
                f"VALID: {valid}\nALL RESULTS: {results}\nALL EXCEPTIONS: {exceptions}\nNUM ERRORS: {num_errors}\n"
                f"NUM WARNING: {num_warnings}"
            )

        return results, valid, exceptions, num_errors, num_warnings

    def assert_accucor_files_pass(self, accucor_file_keys, results, exceptions):
        for afkey in accucor_file_keys:
            # There may be missing samples, but they should be ignored if they contain the substring "blank".  (The
            # user should not be bothered with a warning they cannot do anything about.)  We are checking in validate
            # mode, but if we weren't, an exception would have been raised.
            self.assertTrue(afkey in results)
            self.assertEqual("PASSED", results[afkey])

            self.assertTrue(afkey in exceptions)
            self.assertEqual(0, len(exceptions[afkey]))

    def create_valid_dvv(self, tmpdir):
        basic_loading_data = {
            "protocols": None,  # Added by self.add_sample_data()
            "animals_samples_treatments": {
                "table": None,  # Added by self.add_sample_data()
                "skip_researcher_check": False,
            },
            "accucor_data": {
                "accucor_files": [
                    # {
                    #     "name": None,  # Added by self.add_ms_data()
                    #     "isocorr_format": False,  # Set by self.add_ms_data()
                    # },
                ],
                "msrun_protocol": "Default",
                "date": "1972-11-24",
                "researcher": "anonymous",
                "new_researcher": False,
            },
        }

        sf = "DataRepo/example_data/data_submission_good/animal_sample_table.xlsx"
        afs = [
            "DataRepo/example_data/data_submission_good/accucor1.xlsx",
            "DataRepo/example_data/data_submission_good/accucor2.xlsx",
        ]

        dvv = DataValidationView()
        dvv.set_files(sf, afs)

        sfn = os.path.basename(sf)
        sfp = os.path.join(tmpdir, str(sfn))
        afps = []
        for af in afs:
            afn = os.path.basename(af)
            afps.append(os.path.join(tmpdir, str(afn)))

        return dvv, basic_loading_data, sfp, afps

    def test_add_sample_data(self):
        """Test add_sample_data(dict, tmpdir) adds dict["protocols"] and dict["animals_samples_treatments"]["table"]"""
        tmpdir_obj = tempfile.TemporaryDirectory()
        tmpdir = tmpdir_obj.name

        dvv, basic_loading_data, sf, afs = self.create_valid_dvv(tmpdir)

        dvv.add_sample_data(basic_loading_data, tmpdir)
        self.assertEqual(sf, basic_loading_data["protocols"])
        self.assertEqual(sf, basic_loading_data["animals_samples_treatments"]["table"])

    def test_add_ms_data(self):
        """
        Test add_ms_data(dict, tmpdir, files, filenames, is_isocorr) adds dict["accucor_data"]["accucor_files"].append(
            {
                "name": fp,
                "isocorr_format": is_isocorr,
            }
        )
        """
        tmpdir_obj = tempfile.TemporaryDirectory()
        tmpdir = tmpdir_obj.name

        dvv, basic_loading_data, sf, afs = self.create_valid_dvv(tmpdir)

        dvv.add_ms_data(
            basic_loading_data,
            tmpdir,
            dvv.accucor_files,
            [],
            False,
        )
        self.assertEqual(
            afs[0], basic_loading_data["accucor_data"]["accucor_files"][0]["name"]
        )
        self.assertFalse(
            basic_loading_data["accucor_data"]["accucor_files"][0]["isocorr_format"]
        )
        self.assertEqual(
            afs[1], basic_loading_data["accucor_data"]["accucor_files"][1]["name"]
        )
        self.assertFalse(
            basic_loading_data["accucor_data"]["accucor_files"][1]["isocorr_format"]
        )
