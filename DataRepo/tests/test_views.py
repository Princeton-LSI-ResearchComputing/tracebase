import json

from django.conf import settings
from django.core.management import call_command
from django.db import transaction
from django.test import override_settings, tag
from django.urls import reverse

from DataRepo.models import (
    Animal,
    Compound,
    CompoundSynonym,
    Infusate,
    MSRun,
    PeakData,
    PeakGroup,
    PeakGroupSet,
    Sample,
    Study,
    Tissue,
)
from DataRepo.models.maintained_model import (
    UncleanBufferError,
    buffer_size,
    disable_buffering,
    enable_buffering,
)
from DataRepo.models.utilities import get_all_models
from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    TracebaseTransactionTestCase,
)
from DataRepo.views import DataValidationView


class ViewTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        cls.ALL_TISSUES_COUNT = 37

        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
        )
        cls.ALL_COMPOUNDS_COUNT = 3

        # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after itself
        if buffer_size() > 0:
            raise UncleanBufferError()

        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
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
        )
        cls.SERUM_COMPOUNDS_COUNT = 3
        cls.SERUM_SAMPLES_COUNT = 1
        cls.SERUM_PEAKDATA_ROWS = 13
        cls.SERUM_PEAKGROUP_COUNT = cls.SERUM_COMPOUNDS_COUNT * cls.SERUM_SAMPLES_COUNT

        super().setUpTestData()

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

    def test_peakgroupset_list(self):
        response = self.client.get(reverse("peakgroupset_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/peakgroupset_list.html")
        self.assertEqual(len(response.context["peakgroupset_list"]), 2)

    def test_peakgroupset_detail(self):
        pgs1 = PeakGroupSet.objects.filter(
            filename="small_obob_maven_6eaas_inf.xlsx"
        ).get()
        response = self.client.get(reverse("peakgroupset_detail", args=[pgs1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/peakgroupset_detail.html")
        self.assertEqual(
            response.context["peakgroupset"].filename, "small_obob_maven_6eaas_inf.xlsx"
        )

    def test_peakgroupset_detail_404(self):
        pgs = PeakGroupSet.objects.order_by("id").last()
        response = self.client.get(reverse("peakgroupset_detail", args=[pgs.id + 1]))
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

    def test_validate_files(self):
        """
        Do a file validation test
        """
        # Load the necessary tissues & compounds for a successful test
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )

        results, errors = validate_some_files(self)

        self.assertTrue(len(errors["data_submission_accucor1.xlsx"]) == 0)
        self.assertTrue(len(errors["data_submission_accucor2.xlsx"]) == 0)
        self.assertEqual(results["data_submission_accucor1.xlsx"], "PASSED")
        self.assertEqual(results["data_submission_accucor2.xlsx"], "PASSED")


class ViewNullToleranceTests(ViewTests):
    """
    This class inherits from the ViewTests class above and overrides the setUpTestData method to load without auto-
    updates.

    All super tests are executed.  Those that are broken are overridden here to have something to apply the broken tags
    to.
    """

    @classmethod
    def setUpTestData(cls):
        # Silently dis-allow auto-updates by disabling buffering
        disable_buffering()
        super().setUpTestData()
        enable_buffering()

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

    @classmethod
    def initialize_databases(cls):
        # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after itself
        if buffer_size() > 0:
            raise UncleanBufferError()

        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )

    @classmethod
    def clear_database(cls, db):
        """
        Clears out the contents of the supplied database and confirms it's empty.
        """
        # Note that get_all_models is implemented to return the models in an order that facilitates this deletion
        for mdl in get_all_models():
            mdl.objects.using(db).all().delete()
        # Make sure the database is actually empty so that the tests are meaningful
        sum = cls.sum_record_counts(db)
        assert sum == 0

    @classmethod
    def sum_record_counts(cls, db):
        record_counts = cls.get_record_counts(db)
        sum = 0
        for cnt in record_counts:
            sum += cnt
        return sum

    @classmethod
    def get_record_counts(cls, db):
        record_counts = []
        for mdl in get_all_models():
            record_counts.append(mdl.objects.using(db).all().count())
        return record_counts

    def test_validate_view(self):
        """
        Do a simple validation view test
        """
        response = self.client.get(reverse("validate"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/validate_submission.html")

    @override_settings(DEBUG=True)
    def test_validate_files(self):
        """
        Do a file validation test
        """
        self.initialize_databases()

        # Load some data that should cause a researcher warning during validation
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
        )

        # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after itself
        if buffer_size() > 0:
            raise UncleanBufferError()

        results, errors = validate_some_files(self)
        self.assertTrue(
            len(errors["data_submission_accucor1.xlsx"]) == 0,
            msg=f"Should be no errors, but got [{', '.join(errors['data_submission_accucor1.xlsx'])}]",
        )
        self.assertTrue(
            len(errors["data_submission_accucor2.xlsx"]) == 0,
            msg=f"Should be no errors, but got [{', '.join(errors['data_submission_accucor2.xlsx'])}]",
        )
        self.assertEqual(results["data_submission_accucor1.xlsx"], "PASSED")
        self.assertEqual(results["data_submission_accucor2.xlsx"], "PASSED")

    def test_databases_unchanged(self):
        """
        Test to ensure that validating user submitted data does not change either database
        """
        self.clear_database(settings.TRACEBASE_DB)
        self.clear_database(settings.VALIDATION_DB)
        self.initialize_databases()

        # Get initial record counts for all models
        tb_init_counts = self.get_record_counts(settings.TRACEBASE_DB)
        vd_init_counts = self.get_record_counts(settings.VALIDATION_DB)

        # Files/inputs we will test
        animal_sample_dict = {}
        animal_sample_dict[
            "data_submission_animal_sample_table.xlsx"
        ] = "DataRepo/example_data/data_submission_animal_sample_table.xlsx"
        accucor_dict = {
            "data_submission_accucor1.xlsx": "DataRepo/example_data/data_submission_accucor1.xlsx",
            "data_submission_accucor2.xlsx": "DataRepo/example_data/data_submission_accucor2.xlsx",
        }

        # Test the validate_load_files function
        vo = DataValidationView()
        vo.validate_load_files(animal_sample_dict, accucor_dict)

        # Get record counts for all models
        tb_post_counts = self.get_record_counts(settings.TRACEBASE_DB)
        vd_post_counts = self.get_record_counts(settings.VALIDATION_DB)

        self.assertListEqual(tb_init_counts, tb_post_counts)
        self.assertListEqual(vd_init_counts, vd_post_counts)

    def test_compounds_load_in_both_dbs(self):
        """
        Test to ensure that compounds load in both databases by default
        """
        self.clear_database(settings.TRACEBASE_DB)
        self.clear_database(settings.VALIDATION_DB)
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
        )
        self.assertGreater(
            Compound.objects.using(settings.TRACEBASE_DB).all().count(), 0
        )
        self.assertGreater(
            CompoundSynonym.objects.using(settings.TRACEBASE_DB).all().count(), 0
        )

    def test_tissues_load_in_both_dbs(self):
        """
        Test to ensure that tissues load in both databases by default
        """
        self.clear_database(settings.TRACEBASE_DB)
        self.clear_database(settings.VALIDATION_DB)
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        self.assertGreater(Tissue.objects.using(settings.TRACEBASE_DB).all().count(), 0)

    def test_only_tracebase_loaded(self):
        """
        Test to ensure that the validation database is never loaded with samples, animals, and accucor data by default
        """
        with transaction.atomic():
            tb_init_sum = self.sum_record_counts(settings.TRACEBASE_DB)
            vd_init_sum = self.sum_record_counts(settings.VALIDATION_DB)
            self.clear_database(settings.TRACEBASE_DB)
            self.clear_database(settings.VALIDATION_DB)
            self.initialize_databases()
            tb_init_sum = self.sum_record_counts(settings.TRACEBASE_DB)
            vd_init_sum = self.sum_record_counts(settings.VALIDATION_DB)
            call_command(
                "load_samples",
                "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
                sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
            )
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
                date="2021-06-03",
                researcher="Michael Neinast",
                new_researcher=True,
            )
            tb_post_sum = self.sum_record_counts(settings.TRACEBASE_DB)
            vd_post_sum = self.sum_record_counts(settings.VALIDATION_DB)
            self.assertGreater(tb_post_sum, tb_init_sum)
            self.assertEqual(vd_post_sum, vd_init_sum)

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


def validate_some_files(testobj):
    # Files/inputs we will test
    animal_sample_dict = {}
    animal_sample_dict[
        "data_submission_animal_sample_table.xlsx"
    ] = "DataRepo/example_data/data_submission_animal_sample_table.xlsx"
    accucor_dict = {
        "data_submission_accucor1.xlsx": "DataRepo/example_data/data_submission_accucor1.xlsx",
        "data_submission_accucor2.xlsx": "DataRepo/example_data/data_submission_accucor2.xlsx",
    }

    # Test the validate_load_files function
    vo = DataValidationView()
    # Now try validating the load files
    [results, valid, errors] = vo.validate_load_files(animal_sample_dict, accucor_dict)

    # Note that even though the Study "Notes" header is missing from the input file, we don't expect to encounter
    # that error before the researcher warning because that column value is optional

    # Check the sample file details
    testobj.assertTrue("data_submission_animal_sample_table.xlsx" in results)
    testobj.assertTrue("data_submission_animal_sample_table.xlsx" in errors)

    # The researcher warning technically results in a validation failure (because it's an exception), but it's the
    # last possible check on the file on purpose so that everything else is guaranteed to be OK
    testobj.assertFalse(valid)

    # There should only be a warning about the researcher in the sample file not existing - no other errors
    testobj.assertEqual(
        "WARNING",
        results["data_submission_animal_sample_table.xlsx"],
    )
    testobj.assertEqual(1, len(errors["data_submission_animal_sample_table.xlsx"]))

    # Check the accucor file details
    testobj.assertTrue("data_submission_accucor1.xlsx" in results)
    testobj.assertTrue("data_submission_accucor1.xlsx" in errors)
    testobj.assertTrue("data_submission_accucor2.xlsx" in results)
    testobj.assertTrue("data_submission_accucor2.xlsx" in errors)

    return results, errors
