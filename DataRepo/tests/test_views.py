import json

from django.core.management import call_command
from django.test import TestCase, tag
from django.urls import reverse

from DataRepo.compositeviews import BaseAdvancedSearchView, BaseSearchView
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
    Tissue,
)
from DataRepo.views import (
    DataValidationView,
    constructAdvancedQuery,
    createNewBasicQuery,
    getAllBrowseData,
    getJoinedRecFieldValue,
    isQryObjValid,
    manyToManyFilter,
    pathStepToPosGroupType,
    performQuery,
    rootToFormatInfo,
    searchFieldToDisplayField,
)


class ViewTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "tissues.yaml")
        cls.ALL_TISSUES_COUNT = 35

        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
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

    def test_home_card_attr_list(self):
        # spot check: counts, urls for card attributes
        animal_count = Animal.objects.all().count()
        tissue_count = Tissue.objects.all().count()
        sample_count = Sample.objects.all().count()
        accucor_file_count = PeakGroupSet.objects.all().count()
        compound_count = Compound.objects.all().count()
        tracer_count = (
            Animal.objects.exclude(tracer_compound_id__isnull=True)
            .order_by("tracer_compound_id")
            .values_list("tracer_compound_id")
            .distinct("tracer_compound_id")
            .count()
        )
        comp_url = reverse("compound_list")
        accucor_file_url = reverse("peakgroupset_list")
        advance_search_url = reverse("search_advanced")
        response = self.client.get(reverse("home"))
        self.assertEqual(animal_count, self.ALL_ANIMALS_COUNT)
        self.assertEqual(tissue_count, self.ALL_TISSUES_COUNT)
        self.assertEqual(sample_count, self.ALL_SAMPLES_COUNT)
        self.assertEqual(accucor_file_count, 2)
        self.assertEqual(compound_count, self.ALL_COMPOUNDS_COUNT)
        self.assertEqual(tracer_count, 1)
        self.assertEqual(comp_url, "/DataRepo/compounds/")
        self.assertEqual(accucor_file_url, "/DataRepo/peakgroupsets/")
        self.assertEqual(advance_search_url, "/DataRepo/search_advanced/")
        self.assertEqual(len(response.context["card_rows"]), 2)

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
            "form-1-pos": "pdtemplate-PeakData.0-all-False.0",
            "form-1-fld": "labeled_element",
            "form-1-ncmp": "iexact",
            "form-2-pos": "fctemplate-FCirc.0-all-False.0",
            "form-2-fld": "msrun__sample__animal__name",
            "form-2-ncmp": "iexact",
        }
        qry = {
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
                                "fld": "",
                                "val": "",
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
                            }
                        ],
                    },
                    "name": "FCirc",
                },
            },
        }
        dlform = {
            "form-TOTAL_FORMS": "1",
            "form-INITIAL_FORMS": "0",
            "qryjson": json.dumps(qry),
        }
        return [asform, qry, dlform]

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
        self.assertEqual(response.context["qry"], qry)

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
        self.assertEqual(response.context["qry"], qry)

    def test_pathStepToPosGroupType_inner_node(self):
        """
        Convert "0-all" to [0, "all"]
        """
        [pos, gtype, static] = pathStepToPosGroupType("0-all-True")
        self.assertEqual(pos, 0)
        self.assertEqual(gtype, "all")
        self.assertTrue(not static)

    def test_pathStepToPosGroupType_leaf_node(self):
        """
        Convert "0" to [0, None]
        """
        [pos, gtype, static] = pathStepToPosGroupType("0")
        self.assertEqual(pos, 0)
        self.assertEqual(gtype, None)
        self.assertEqual(static, False)

    def test_rootToFormatInfo_selected(self):
        """
        Convert "pgtemplate-PeakGroups-selected" to ["pgtemplate", "PeakGroups", True]
        """
        [format, name, sel] = rootToFormatInfo("pgtemplate-PeakGroups-selected")
        self.assertEqual(format, "pgtemplate")
        self.assertEqual(name, "PeakGroups")
        self.assertEqual(sel, True)

    def test_rootToFormatInfo_unselected(self):
        """
        Convert "pdtemplate-PeakData" to ["pdtemplate", "PeakData", False]
        """
        [format, name, sel] = rootToFormatInfo("pdtemplate-PeakData")
        self.assertEqual(format, "pdtemplate")
        self.assertEqual(name, "PeakData")
        self.assertEqual(sel, False)

    def test_search_advanced_tsv(self):
        """
        Download a simple advanced search and make sure the results are correct
        """
        qs = PeakGroup.objects.filter(
            msrun__sample__tissue__name__iexact="Brain"
        ).prefetch_related("msrun__sample__animal__studies")
        expected_newline_count = qs.count() + 6
        # This query returns 2 rows and the tsv has 22 tabs. We add 22 more for the header
        expected_tab_count = qs.count() * 22 + 22
        [filledform, qry, dlform] = self.get_advanced_search_inputs()
        response = self.client.post("/DataRepo/search_advanced_tsv/", dlform)

        # Response content settings
        self.assertTemplateUsed(response, "DataRepo/search/downloads/download.tsv")
        self.assertTemplateUsed(response, "DataRepo/search/downloads/peakgroups.tsv")
        self.assertEqual(response.get("Content-Type"), "application/text")
        self.assertEqual(response.status_code, 200)
        # Cannot use assertContains here for non-http response - it will complain about a missing status_code
        contentdisp = response.get("Content-Disposition")
        self.assertTrue("attachment" in contentdisp)
        self.assertTrue("PeakGroups" in contentdisp)
        self.assertTrue(".tsv" in contentdisp)
        # Line count
        self.assertContains(response, "\n", count=expected_newline_count)
        self.assertContains(response, "\t", count=expected_tab_count)
        # Header tests
        # The header includes the download time
        self.assertContains(response, "# Download Time: ", count=1)
        # The header includes the advanced search qry
        self.assertContains(response, "# Advanced Search Query: {", count=1)
        # The qry in the header contains the search term and is in json format
        self.assertContains(response, "'val': 'Brain'", count=1)
        # Header row starts with the sample column
        self.assertContains(response, "Sample\t", count=1)
        # qry sent to the downloaded file header is correct
        self.assertEqual(response.context["qry"], qry)

    def test_getAllBrowseData(self):
        """
        Test that test_getAllBrowseData returns all data for the selected format.
        """
        basv_metadata = BaseAdvancedSearchView()
        pf = "msrun__sample__animal__studies"
        qs = PeakGroup.objects.all().prefetch_related(pf)
        res = getAllBrowseData("pgtemplate", basv_metadata)
        self.assertEqual(res.count(), qs.count())

    def get_basic_qry_inputs(self):
        qs = PeakGroup.objects.all().prefetch_related("msrun__sample__animal__studies")
        tval = str(qs[0].msrun.sample.animal.studies.all()[0].id)
        empty_tree = {
            "type": "group",
            "val": "all",
            "static": False,
            "queryGroup": [
                {
                    "type": "query",
                    "pos": "",
                    "static": False,
                    "ncmp": "",
                    "fld": "",
                    "val": "",
                }
            ],
        }
        qry = {
            "selectedtemplate": "pgtemplate",
            "searches": {
                "pgtemplate": {
                    "name": "PeakGroups",
                    "tree": {
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "static": False,
                                "ncmp": "iexact",
                                "fld": "msrun__sample__animal__studies__name",
                                "val": "obob_fasted",
                            }
                        ],
                    },
                },
                "pdtemplate": {"name": "PeakData", "tree": empty_tree},
                "fctemplate": {"name": "Fcirc", "tree": empty_tree},
            },
        }
        return tval, qry

    def test_createNewBasicQuery(self):
        """
        Test createNewBasicQuery creates a correct qry
        """
        tval, qry = self.get_basic_qry_inputs()
        basv_metadata = BaseAdvancedSearchView()
        mdl = "Study"
        fld = "id"
        cmp = "iexact"
        val = tval
        fmt = "pgtemplate"
        newqry = createNewBasicQuery(basv_metadata, mdl, fld, cmp, val, fmt)
        self.maxDiff = None
        self.assertEqual(newqry, qry)

    def test_searchFieldToDisplayField(self):
        """
        Test that searchFieldToDisplayField converts Study.id to Study.name
        """
        [tval, qry] = self.get_basic_qry_inputs()
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0][
            "fld"
        ] = "msrun__sample__animal__studies__id"
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0]["val"] = tval
        basv_metadata = BaseAdvancedSearchView()
        mdl = "Study"
        fld = "id"
        val = tval
        fmt = "pgtemplate"
        dfld, dval = searchFieldToDisplayField(basv_metadata, mdl, fld, val, fmt, qry)
        self.assertEqual(dfld, "name")
        self.assertEqual(dval, "obob_fasted")

    def test_getJoinedRecFieldValue(self):
        """
        Test that getJoinedRecFieldValue gets a value from a joined table
        """
        basv_metadata = BaseAdvancedSearchView()
        fmt = "pgtemplate"
        mdl = "Animal"
        fld = "feeding_status"
        pf = "msrun__sample__animal__studies"
        recs = PeakGroup.objects.all().prefetch_related(pf)
        val = getJoinedRecFieldValue(recs, basv_metadata, fmt, mdl, fld, fld, "Fasted")
        self.assertEqual(val, "Fasted")

    def test_constructAdvancedQuery_performQuery(self):
        """
        Test that performQuery returns a correct queryset
        """
        [ignoreas, qry, ignoredl] = self.get_advanced_search_inputs()
        q_exp = constructAdvancedQuery(qry)
        basv_metadata = BaseAdvancedSearchView()
        pf = [
            "msrun__sample__tissue",
            "msrun__sample__animal__tracer_compound",
            "msrun__sample__animal__studies",
        ]
        res = performQuery(q_exp, "pgtemplate", basv_metadata)
        qs = PeakGroup.objects.filter(
            msrun__sample__tissue__name__iexact="Brain"
        ).prefetch_related(*pf)
        self.assertEqual(res.count(), qs.count())

    def test_isQryObjValid(self):
        """
        Test that isQryObjValid correctly validates an object.
        """
        [ignoreas, qry, ignoredl] = self.get_advanced_search_inputs()
        basv_metadata = BaseAdvancedSearchView()
        valid = isQryObjValid(qry, basv_metadata.getFormatNames().keys())
        self.assertEqual(valid, True)
        qry.pop("selectedtemplate")
        invalid = isQryObjValid(qry, basv_metadata.getFormatNames().keys())
        self.assertEqual(invalid, False)

    def test_cv_getSearchFieldChoices(self):
        """
        Test getSearchFieldChoices
        """
        basv_metadata = BaseAdvancedSearchView()
        fmt = "pgtemplate"
        res = basv_metadata.getSearchFieldChoices(fmt)
        choices = (
            ("msrun__sample__animal__name", "Animal"),
            ("msrun__sample__animal__body_weight", "Body Weight (g)"),
            ("msrun__sample__animal__diet", "Diet"),
            ("msrun__sample__animal__feeding_status", "Feeding Status"),
            ("formula", "Formula"),
            ("msrun__sample__animal__genotype", "Genotype"),
            ("compounds__synonyms__name", "Measured Compound"),
            ("name", "Peak Group"),
            ("peak_group_set__filename", "Peak Group Set Filename"),
            ("msrun__sample__name", "Sample"),
            ("msrun__sample__animal__sex", "Sex"),
            ("msrun__sample__animal__studies__name", "Study"),
            ("msrun__sample__tissue__name", "Tissue"),
            ("msrun__sample__animal__tracer_compound__name", "Tracer Compound"),
            (
                "msrun__sample__animal__tracer_infusion_concentration",
                "Tracer Infusion Concentration (mM)",
            ),
            (
                "msrun__sample__animal__tracer_infusion_rate",
                "Tracer Infusion Rate (ul/min/g)",
            ),
            ("msrun__sample__animal__tracer_labeled_atom", "Tracer Labeled Element"),
            ("msrun__sample__animal__treatment__name", "Treatment"),
        )
        self.assertEqual(res, choices)

    def test_cv_getKeyPathList(self):
        """
        Test getKeyPathList
        """
        basv_metadata = BaseAdvancedSearchView()
        fmt = "pgtemplate"
        mdl = "Animal"
        res = basv_metadata.getKeyPathList(fmt, mdl)
        kpl = ["msrun", "sample", "animal"]
        self.assertEqual(res, kpl)

    def test_cv_getPrefetches(self):
        """
        Test getPrefetches
        """
        basv_metadata = BaseAdvancedSearchView()
        fmt = "pgtemplate"
        res = basv_metadata.getPrefetches(fmt)
        pfl = [
            "peak_group_set",
            "compounds__synonyms",
            "msrun__sample__tissue",
            "msrun__sample__animal__treatment",
            "msrun__sample__animal__tracer_compound",
            "msrun__sample__animal__studies",
        ]
        self.assertEqual(res, pfl)

    def test_cv_getModels(self):
        """
        Test getModels
        """
        basv_metadata = BaseAdvancedSearchView()
        fmt = "pgtemplate"
        res = basv_metadata.getModels(fmt)
        ml = [
            "PeakGroupSet",
            "CompoundSynonym",
            "PeakGroup",
            "Protocol",
            "Sample",
            "Tissue",
            "Animal",
            "Compound",
            "Study",
        ]
        self.assertEqual(res, ml)

    def test_cv_getSearchFields(self):
        """
        Test getSearchFields
        """
        basv_metadata = BaseAdvancedSearchView()
        fmt = "pgtemplate"
        mdl = "Animal"
        res = basv_metadata.getSearchFields(fmt, mdl)
        sfd = {
            "id": "msrun__sample__animal__id",
            "name": "msrun__sample__animal__name",
            "genotype": "msrun__sample__animal__genotype",
            "body_weight": "msrun__sample__animal__body_weight",
            "sex": "msrun__sample__animal__sex",
            "diet": "msrun__sample__animal__diet",
            "feeding_status": "msrun__sample__animal__feeding_status",
            "tracer_labeled_atom": "msrun__sample__animal__tracer_labeled_atom",
            "tracer_infusion_rate": "msrun__sample__animal__tracer_infusion_rate",
            "tracer_infusion_concentration": "msrun__sample__animal__tracer_infusion_concentration",
        }
        self.assertEqual(res, sfd)

    def test_cv_getDisplayFields(self):
        """
        Test getDisplayFields
        """
        basv_metadata = BaseAdvancedSearchView()
        fmt = "pgtemplate"
        mdl = "Animal"
        res = basv_metadata.getDisplayFields(fmt, mdl)
        # Note the difference with the 'id' field - which is not a displayed field
        dfd = {
            "id": "name",
            "name": "name",
            "genotype": "genotype",
            "body_weight": "body_weight",
            "age": "age",
            "sex": "sex",
            "diet": "diet",
            "feeding_status": "feeding_status",
            "tracer_labeled_atom": "tracer_labeled_atom",
            "tracer_infusion_rate": "tracer_infusion_rate",
            "tracer_infusion_concentration": "tracer_infusion_concentration",
        }
        self.assertEqual(res, dfd)

    def test_cv_getFormatNames(self):
        """
        Test getFormatNames
        """
        basv_metadata = BaseAdvancedSearchView()
        res = basv_metadata.getFormatNames()
        fnd = {
            "pgtemplate": "PeakGroups",
            "pdtemplate": "PeakData",
            "fctemplate": "Fcirc",
        }
        self.assertEqual(res, fnd)

    def test_cv_formatNameOrKeyToKey(self):
        """
        Test formatNameOrKeyToKey
        """
        basv_metadata = BaseAdvancedSearchView()
        fmt = "PeakGroups"
        res = basv_metadata.formatNameOrKeyToKey(fmt)
        self.assertEqual(res, "pgtemplate")

    def test_validate_view(self):
        """
        Do a simple validation view test
        """
        response = self.client.get(reverse("validate"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "DataRepo/validate_submission.html")

    def test_validate_files(self):
        """
        Do a file validation test
        """
        # Load the necessary compounds for a successful test
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )

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
        [results, valid, errors] = vo.validate_load_files(
            animal_sample_dict, accucor_dict
        )

        # Note that even though the Study "Notes" header is missing from the input file, we don't expect to encounter
        # that error before the researcher warning because that column value is optional

        # Check the sample file details
        self.assertTrue("data_submission_animal_sample_table.xlsx" in results)
        self.assertTrue("data_submission_animal_sample_table.xlsx" in errors)

        # The researcher warning technically results in a validation failure (because it's an exception), but it's the
        # last possible check on the file on purpose so that everything else is guaranteed to be OK
        self.assertTrue(not valid)

        self.assertEqual(results["data_submission_animal_sample_table.xlsx"], "WARNING")
        self.assertTrue(len(errors["data_submission_animal_sample_table.xlsx"]) == 1)
        self.assertTrue(
            "1 researchers from the sample file: [Anonymous]"
            in errors["data_submission_animal_sample_table.xlsx"][0]
        )

        # Check the accucor file details
        self.assertTrue("data_submission_accucor1.xlsx" in results)
        self.assertTrue("data_submission_accucor1.xlsx" in errors)
        self.assertTrue("data_submission_accucor2.xlsx" in results)
        self.assertTrue("data_submission_accucor2.xlsx" in errors)
        self.assertTrue(len(errors["data_submission_accucor1.xlsx"]) == 0)
        self.assertTrue(len(errors["data_submission_accucor2.xlsx"]) == 0)
        self.assertEqual(results["data_submission_accucor1.xlsx"], "PASSED")
        self.assertEqual(results["data_submission_accucor2.xlsx"], "PASSED")

    def test_manyToManyFilter(self):
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table_2ndstudy.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        qry = {
            "selectedtemplate": "pgtemplate",
            "searches": {
                "pgtemplate": {
                    "tree": {
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "static": False,
                                "ncmp": "icontains",
                                "fld": "msrun__sample__animal__studies__name",
                                "val": "obob_fasted",
                            }
                        ],
                    },
                    "name": "PeakGroups",
                },
                "pdtemplate": {"name": "PeakData", "tree": {}},
                "fctemplate": {"name": "Fcirc", "tree": {}},
            },
        }
        pgrecs = PeakGroup.objects.filter(
            msrun__sample__animal__studies__name__icontains="obob_fasted"
        )
        studyrecs = pgrecs[0].msrun.sample.animal.studies.all()
        mm_lookup = {"msrun__sample__animal__studies": studyrecs[0]}
        result = manyToManyFilter(pgrecs[0], mm_lookup, qry)
        self.assertTrue(result)
        mm_lookup = {"msrun__sample__animal__studies": studyrecs[1]}
        result = manyToManyFilter(pgrecs[0], mm_lookup, qry)
        self.assertTrue(not result)


@tag("search_choices")
class SearchFieldChoicesTests(TestCase):
    def test_get_all_comparison_choices(self):
        base_search_view = BaseSearchView()

        all_ncmp_choices = (
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("lt", "<"),
            ("lte", "<="),
            ("gt", ">"),
            ("gte", ">="),
            ("not_isnull", "has a value (ie. is not None)"),
            ("isnull", "does not have a value (ie. is None)"),
            ("icontains", "contains"),
            ("not_icontains", "does not contain"),
            ("istartswith", "starts with"),
            ("not_istartswith", "does not start with"),
            ("iendswith", "ends with"),
            ("not_iendswith", "does not end with"),
        )
        self.assertEqual(base_search_view.getAllComparisonChoices(), all_ncmp_choices)
