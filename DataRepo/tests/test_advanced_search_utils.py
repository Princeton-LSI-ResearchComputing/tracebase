from django.core.management import call_command

from DataRepo.advanced_search_utils import (
    createNewBasicQuery,
    getAllBrowseData,
    getJoinedRecFieldValue,
    isQryObjValid,
    isValidQryObjPopulated,
    pathStepToPosGroupType,
    performQuery,
    rootToFormatInfo,
    searchFieldToDisplayField,
)
from DataRepo.compositeviews import BaseAdvancedSearchView
from DataRepo.models import PeakGroup
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class AdvancedSearchUtilsTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        cls.ALL_TISSUES_COUNT = 36

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
                                "fld": "labeled_element",
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

    def get_advanced_qry2(self):
        """
        Modify the query returned by get_advanced_qry to include search terms on 2 M:M related tables in a sub-group.
        """
        qry = self.get_advanced_qry()
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0][
            "fld"
        ] = "msrun__sample__name"
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0]["val"] = "BAT-xz971"
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"].append(
            {
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
                    },
                    {
                        "type": "query",
                        "pos": "",
                        "static": False,
                        "ncmp": "iexact",
                        "fld": "compounds__synonyms__name",
                        "val": "glucose",
                    },
                ],
            }
        )
        return qry

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

    def test_performQuery(self):
        """
        Test that performQuery returns a correct queryset
        """
        qry = self.get_advanced_qry()
        basv_metadata = BaseAdvancedSearchView()
        pf = [
            "msrun__sample__tissue",
            "msrun__sample__animal__tracer_compound",
            "msrun__sample__animal__studies",
        ]
        res, cnt = performQuery(qry, "pgtemplate", basv_metadata)
        qs = PeakGroup.objects.filter(
            msrun__sample__tissue__name__iexact="Brain"
        ).prefetch_related(*pf)
        self.assertEqual(cnt, qs.count())

    def test_performQuery_distinct(self):
        """
        Test that performQuery returns no duplicate root table records when M:M tables queried with multiple matches.
        """
        qry = self.get_advanced_qry2()
        basv_metadata = BaseAdvancedSearchView()
        res, cnt = performQuery(qry, "pgtemplate", basv_metadata)
        qs = (
            PeakGroup.objects.filter(msrun__sample__name__iexact="BAT-xz971")
            .filter(msrun__sample__animal__studies__name__iexact="obob_fasted")
            .filter(compounds__synonyms__name__iexact="glucose")
        )
        # Ensure the test is working by ensuring the number of records without distinct is larger
        self.assertTrue(cnt < qs.count())
        self.assertEqual(cnt, 1)

    def test_isQryObjValid(self):
        """
        Test that isQryObjValid correctly validates a qry object.
        """
        qry = self.get_advanced_qry()
        basv_metadata = BaseAdvancedSearchView()
        isvalid = isQryObjValid(qry, basv_metadata.getFormatNames().keys())
        self.assertEqual(isvalid, True)
        qry.pop("selectedtemplate")
        isvalid = isQryObjValid(qry, basv_metadata.getFormatNames().keys())
        self.assertEqual(isvalid, False)

    def test_isValidQryObjPopulated(self):
        """
        Test that isValidQryObjPopulated correctly interprets the population of a subgroup.
        """
        qry = self.get_advanced_qry2()
        isvalid = isValidQryObjPopulated(qry)
        self.assertEqual(isvalid, True)
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][1]["queryGroup"] = []
        isvalid = isValidQryObjPopulated(qry)
        self.assertEqual(isvalid, False)

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

    def test_getAllBrowseData(self):
        """
        Test that test_getAllBrowseData returns all data for the selected format.
        """
        basv_metadata = BaseAdvancedSearchView()
        pf = "msrun__sample__animal__studies"
        qs = PeakGroup.objects.all().prefetch_related(pf)
        res, cnt = getAllBrowseData("pgtemplate", basv_metadata)
        self.assertEqual(cnt, qs.count())

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
        dfld, dval = searchFieldToDisplayField(basv_metadata, mdl, fld, val, qry)
        self.assertEqual(dfld, "name")
        self.assertEqual(dval, "obob_fasted")
