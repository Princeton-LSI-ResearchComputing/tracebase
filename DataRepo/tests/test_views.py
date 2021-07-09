from django.core.management import call_command
from django.test import TestCase
from django.urls import reverse

from DataRepo.models import (
    Animal,
    Compound,
    MSRun,
    PeakData,
    PeakGroup,
    PeakGroupSet,
    Protocol,
    Sample,
    Study,
)


class ViewTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_compounds",
            "DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
        )
        cls.ALL_COMPOUNDS_COUNT = 3

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
            researcher="Michael",
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
            researcher="Michael",
        )
        cls.SERUM_COMPOUNDS_COUNT = 3
        cls.SERUM_SAMPLES_COUNT = 1
        cls.SERUM_PEAKDATA_ROWS = 13
        cls.SERUM_PEAKGROUP_COUNT = cls.SERUM_COMPOUNDS_COUNT * cls.SERUM_SAMPLES_COUNT

    def test_home_url_exists_at_desired_location(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)

    def test_home_url_accessible_by_name(self):
        response = self.client.get(reverse("home"))
        self.assertEqual(response.status_code, 200)

    def test_home_uses_correct_template(self):
        response = self.client.get(reverse("home"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "home.html")

    def test_compound_list(self):
        response = self.client.get(reverse("compound_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/compound_list.html")
        self.assertEqual(
            len(response.context["compound_list"]), self.ALL_COMPOUNDS_COUNT
        )

    def test_compound_detail(self):
        lysine = Compound.objects.filter(name="lysine").get()
        response = self.client.get(reverse("compound_detail", args=[lysine.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/compound_detail.html")
        self.assertEqual(response.context["compound"].name, "lysine")

    def test_compound_detail_404(self):
        c = Compound.objects.order_by("id").last()
        response = self.client.get(reverse("compound_detail", args=[c.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_study_list(self):
        response = self.client.get(reverse("study_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/study_list.html")
        self.assertEqual(len(response.context["study_list"]), 1)

    def test_study_detail(self):
        obob_fasted = Study.objects.filter(name="obob_fasted").get()
        response = self.client.get(reverse("study_detail", args=[obob_fasted.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/study_detail.html")
        self.assertEqual(response.context["study"].name, "obob_fasted")

    def test_study_detail_404(self):
        s = Study.objects.order_by("id").last()
        response = self.client.get(reverse("study_detail", args=[s.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_protocol_list(self):
        response = self.client.get(reverse("protocol_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/protocol_list.html")
        self.assertEqual(len(response.context["protocol_list"]), 1)

    def test_protocol_detail(self):
        p1 = Protocol.objects.filter(name="Default").get()
        response = self.client.get(reverse("protocol_detail", args=[p1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/protocol_detail.html")
        self.assertEqual(response.context["protocol"].name, "Default")

    def test_protocol_detail_404(self):
        p = Protocol.objects.order_by("id").last()
        response = self.client.get(reverse("protocol_detail", args=[p.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_animal_list(self):
        response = self.client.get(reverse("animal_list"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/animal_list.html")
        self.assertEqual(len(response.context["animal_list"]), self.ALL_ANIMALS_COUNT)

    def test_animal_detail(self):
        a1 = Animal.objects.filter(name="971").get()
        response = self.client.get(reverse("animal_detail", args=[a1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/animal_detail.html")
        self.assertEqual(response.context["animal"].name, "971")

    def test_animal_detail_404(self):
        a = Animal.objects.order_by("id").last()
        response = self.client.get(reverse("animal_detail", args=[a.id + 1]))
        self.assertEqual(response.status_code, 404)

    def test_sample_list(self):
        response = self.client.get("/DataRepo/samples/")
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/sample_list.html")
        self.assertEqual(len(response.context["sample_list"]), self.ALL_SAMPLES_COUNT)

    def test_sample_list_per_animal(self):
        a1 = Animal.objects.filter(name="971").get()
        s1 = Sample.objects.filter(animal_id=a1.id)
        response = self.client.get("/DataRepo/samples/?animal_id=" + str(a1.pk))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/sample_list.html")
        self.assertEqual(len(response.context["sample_list"]), s1.count())

    def test_sample_detail(self):
        s1 = Sample.objects.filter(name="BAT-xz971").get()
        response = self.client.get(reverse("sample_detail", args=[s1.id]))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/sample_detail.html")
        self.assertEqual(response.context["sample"].name, "BAT-xz971")

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
        response = self.client.get("/DataRepo/search_advanced/?mode=browse&format=pdtemplate")
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/search_advanced.html")
        self.assertEqual(response.context["mode"], "browse")
        self.assertEqual(response.context["format"], "pdtemplate")

    def test_search_advanced_search(self):
        """
        Load the advanced search page in the default search mode and make sure the mode is added to the context data
        """
        response = self.client.get("/DataRepo/search_advanced/")
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/search_advanced.html")
        self.assertEqual(response.context["mode"], "search")

    def test_search_advanced_valid(self):
        """
        Do a simple advanced search and make sure the results are correct
        """
        qs = PeakData.objects.filter(
            peak_group__msrun__sample__tissue__name__iexact="Brain"
        ).prefetch_related("peak_group__msrun__sample__animal__studies")
        filledform = {
            "fmt": "pgtemplate",
            "form-TOTAL_FORMS": "2",
            "form-INITIAL_FORMS": "0",
            "form-0-pos": "pgtemplate-PeakGroups-selected.0-all.0",
            "form-0-fld": "peak_group__msrun__sample__tissue__name",
            "form-0-ncmp": "iexact",
            "form-0-val": "Brain",
            "form-1-pos": "pdtemplate-PeakData.0-all.0",
            "form-1-fld": "labeled_element",
            "form-1-ncmp": "iexact",
        }
        qry = {
            "selectedtemplate": "pgtemplate",
            "searches": {
                "pgtemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "fld": "peak_group__msrun__sample__tissue__name",
                                "ncmp": "iexact",
                                "val": "Brain"
                            }
                        ]
                    },
                    "name": "PeakGroups"
                },
                "pdtemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "fld": "labeled_element",
                                "ncmp": "iexact",
                                "val": ""
                            }
                        ]
                    },
                    "name": "PeakData"
                }
            }
        }
        response = self.client.post("/DataRepo/search_advanced/", filledform)
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/search_advanced.html")
        self.assertEqual(len(response.context["res"]), qs.count())
        self.assertEqual(response.context["qry"], qry)

    def test_search_advanced_invalid(self):
        """
        Do a simple advanced search and make sure the results are correct
        """
        invalidform = {
            "fmt": "pgtemplate",
            "form-TOTAL_FORMS": "2",
            "form-INITIAL_FORMS": "0",
            "form-0-pos": "pgtemplate-PeakGroups-selected.0-all.0",
            "form-0-fld": "peak_group__msrun__sample__tissue__name",
            "form-0-ncmp": "iexact",
            "form-1-pos": "pdtemplate-PeakData.0-all.0",
            "form-1-fld": "labeled_element",
            "form-1-ncmp": "iexact",
        }
        qry = {
            "selectedtemplate": "pgtemplate",
            "searches": {
                "pgtemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "fld": "peak_group__msrun__sample__tissue__name",
                                "ncmp": "iexact",
                                "val": ""
                            }
                        ]
                    },
                    "name": "PeakGroups"
                },
                "pdtemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "fld": "labeled_element",
                                "ncmp": "iexact",
                                "val": ""
                            }
                        ]
                    },
                    "name": "PeakData"
                }
            }
        }
        response = self.client.post("/DataRepo/search_advanced/", invalidform)
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/search_advanced.html")
        self.assertEqual(len(response.context["res"]), 0)
        self.assertEqual(response.context["qry"], qry)
