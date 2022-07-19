from copy import deepcopy

from django.core.management import call_command
from django.db.models import F, Q
from django.test import tag

from DataRepo.formats.dataformat import Format, splitCommon, splitPathName
from DataRepo.formats.dataformat_group_query import (
    appendFilterToGroup,
    constructAdvancedQuery,
    createFilterCondition,
    createFilterGroup,
    extractFldPaths,
    isQryObjValid,
    isValidQryObjPopulated,
    pathStepToPosGroupType,
    rootToFormatInfo,
)
from DataRepo.formats.peakgroups_dataformat import PeakGroupsFormat
from DataRepo.formats.search_group import SearchGroup
from DataRepo.models import CompoundSynonym, PeakGroup
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class FormatsTests(TracebaseTestCase):
    maxDiff = None

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table_2ndstudy.tsv",
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
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_serum.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=False,
        )

    def getQueryObject(self):
        return {
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

    def getQueryObject2(self):
        qry = deepcopy(self.getQueryObject())
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"].append(
            {
                "type": "query",
                "pos": "",
                "static": False,
                "ncmp": "icontains",
                "fld": "compounds__synonyms__name",
                "val": "glucose",
            }
        )
        return qry

    def getPdtemplateChoicesTuple(self):
        return (
            ("peak_group__msrun__sample__animal__name", "Animal"),
            ("peak_group__msrun__sample__animal__body_weight", "Body Weight (g)"),
            ("corrected_abundance", "Corrected Abundance"),
            ("peak_group__msrun__sample__animal__diet", "Diet"),
            ("peak_group__msrun__sample__animal__feeding_status", "Feeding Status"),
            ("peak_group__formula", "Formula"),
            ("peak_group__msrun__sample__animal__genotype", "Genotype"),
            ("labeled_count", "Labeled Count"),
            ("labeled_element", "Labeled Element"),
            (
                "peak_group__compounds__synonyms__name",
                "Measured Compound (Any Synonym)",
            ),
            ("peak_group__compounds__name", "Measured Compound (Primary Synonym)"),
            ("med_mz", "Median M/Z"),
            ("med_rt", "Median RT"),
            ("peak_group__name", "Peak Group"),
            ("peak_group__peak_group_set__filename", "Peak Group Set Filename"),
            ("raw_abundance", "Raw Abundance"),
            ("peak_group__msrun__sample__name", "Sample"),
            ("peak_group__msrun__sample__animal__sex", "Sex"),
            ("peak_group__msrun__sample__animal__studies__name", "Study"),
            ("peak_group__msrun__sample__tissue__name", "Tissue"),
            (
                "peak_group__msrun__sample__animal__infusate__tracers__compound__name",
                "Tracer Compound (Primary Synonym)",
            ),
            (
                "peak_group__msrun__sample__animal__tracer_infusion_concentration",
                "Tracer Infusion Concentration (mM)",
            ),
            (
                "peak_group__msrun__sample__animal__infusion_rate",
                "Tracer Infusion Rate (ul/min/g)",
            ),
            ("peak_group__msrun__sample__animal__treatment__name", "Treatment"),
        )

    def getPgtemplateChoicesTuple(self):
        return (
            ("msrun__sample__animal__name", "Animal"),
            ("msrun__sample__animal__body_weight", "Body Weight (g)"),
            ("msrun__sample__animal__diet", "Diet"),
            ("msrun__sample__animal__feeding_status", "Feeding Status"),
            ("formula", "Formula"),
            ("msrun__sample__animal__genotype", "Genotype"),
            ("compounds__synonyms__name", "Measured Compound (Any Synonym)"),
            ("compounds__name", "Measured Compound (Primary Synonym)"),
            ("name", "Peak Group"),
            ("peak_group_set__filename", "Peak Group Set Filename"),
            ("msrun__sample__name", "Sample"),
            ("msrun__sample__animal__sex", "Sex"),
            ("msrun__sample__animal__studies__name", "Study"),
            ("msrun__sample__tissue__name", "Tissue"),
            (
                "msrun__sample__animal__tracer_compound__name",
                "Tracer Compound (Primary Synonym)",
            ),
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

    def test_getSearchFieldChoicesDict(self):
        basv = SearchGroup()
        sfcd = basv.getSearchFieldChoicesDict()
        sfcd_expected = {
            "fctemplate": (
                ("msrun__sample__animal__name", "Animal"),
                ("msrun__sample__animal__body_weight", "Body Weight (g)"),
                ("msrun__sample__animal__diet", "Diet"),
                ("msrun__sample__animal__feeding_status", "Feeding Status"),
                ("msrun__sample__animal__genotype", "Genotype"),
                ("msrun__sample__animal__sex", "Sex"),
                ("msrun__sample__animal__studies__name", "Study"),
                (
                    "msrun__sample__time_collected",
                    "Time Collected (hh:mm:ss since infusion)",
                ),
                ("msrun__sample__animal__tracer_compound__name", "Tracer Compound"),
                (
                    "msrun__sample__animal__tracer_infusion_concentration",
                    "Tracer Infusion Concentration (mM)",
                ),
                (
                    "msrun__sample__animal__tracer_infusion_rate",
                    "Tracer Infusion Rate (ul/min/g)",
                ),
                (
                    "msrun__sample__animal__tracer_labeled_atom",
                    "Tracer Labeled Element",
                ),
                ("msrun__sample__animal__treatment__name", "Treatment"),
            ),
            "pdtemplate": self.getPdtemplateChoicesTuple(),
            "pgtemplate": self.getPgtemplateChoicesTuple(),
        }
        self.assertDictEqual(sfcd, sfcd_expected)

    def test_getAllSearchFieldChoices(self):
        basv = SearchGroup()
        sfct = basv.getAllSearchFieldChoices()
        sfct_expected = self.getPgtemplateChoicesTuple()
        sfct_expected += self.getPdtemplateChoicesTuple()
        sfct_expected += (
            (
                "msrun__sample__time_collected",
                "Time Collected (hh:mm:ss since infusion)",
            ),
        )
        self.assertTupleEqual(sfct, sfct_expected)

    def test_extractFldPaths(self):
        qry = self.getQueryObject()
        paths = extractFldPaths(qry)
        expected_paths = ["msrun__sample__animal__studies"]
        self.assertEqual(paths, expected_paths)

    def test_splitCommon_hascommon(self):
        fld_path = "msrun__sample__animal__studies"
        reroot_path = "msrun__sample__animal__tracer_compound"
        common_path, remainder = splitCommon(fld_path, reroot_path)
        self.assertEqual(common_path, "msrun__sample__animal")
        self.assertEqual(remainder, "studies")

    def test_splitCommon_nocommon(self):
        fld_path = "msrun__sample__animal__studies"
        reroot_path = "compounds__synonyms"
        common_path, remainder = splitCommon(fld_path, reroot_path)
        self.assertEqual(common_path, "")
        self.assertEqual(remainder, "msrun__sample__animal__studies")

    def test_splitPathName(self):
        path, name = splitPathName("msrun__sample__animal__treatment__name")
        self.assertEqual(path, "msrun__sample__animal__treatment")
        self.assertEqual(name, "name")

    def test_reRootFieldPath(self):
        fld = "msrun__sample__animal__studies__name"
        reroot_instance_name = "CompoundSynonym"
        pgsv = PeakGroupsFormat()
        rerooted_fld = pgsv.reRootFieldPath(fld, reroot_instance_name)
        expected_fld = "compound__peak_groups__msrun__sample__animal__studies__name"
        self.assertEqual(rerooted_fld, expected_fld)

    def test_reRootQry(self):
        qry = self.getQueryObject2()
        qry_backup = deepcopy(qry)
        basv = SearchGroup()
        new_qry = basv.reRootQry("pgtemplate", qry, "MeasuredCompound")
        expected_qry = deepcopy(self.getQueryObject2())
        expected_qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0][
            "fld"
        ] = "peak_groups__msrun__sample__animal__studies__name"
        expected_qry["searches"]["pgtemplate"]["tree"]["queryGroup"][1][
            "fld"
        ] = "synonyms__name"
        self.assertEqual(qry, qry_backup, msg="qry must be left unchanged")
        self.assertEqual(new_qry, expected_qry)

    def test_pathToModelInstanceName(self):
        pgsv = PeakGroupsFormat()
        mi = pgsv.pathToModelInstanceName("msrun__sample__animal__studies")
        self.assertEqual(mi, "Study")

    def test_getTrueJoinPrefetchPathsAndQrys(self):
        qry = self.getQueryObject2()
        basv = SearchGroup()
        fmt = "pgtemplate"

        # Set all split_rows values to False for the test, then...
        mdl_inst = "MeasuredCompound"
        pgsv = basv.modeldata[fmt]
        for inst in pgsv.model_instances.keys():
            pgsv.model_instances[inst]["manytomany"]["split_rows"] = False
        # Set only MeasuredCompound's split_rows=True for the test
        pgsv.model_instances[mdl_inst]["manytomany"]["split_rows"] = True

        qry["searches"][fmt]["tree"]["queryGroup"][1]["fld"] = "compounds__name"
        qry["searches"][fmt]["tree"]["queryGroup"][1]["val"] = "citrate"
        prefetches = basv.getTrueJoinPrefetchPathsAndQrys(qry)
        expected_prefetches = [
            "msrun__sample__animal__tracer_compound",
            "msrun__sample__animal__treatment",
            "msrun__sample__animal__studies",
            "msrun__sample__tissue",
            [
                "compounds",
                {
                    "searches": {
                        "fctemplate": {
                            "name": "Fcirc",
                            "tree": {},
                        },
                        "pdtemplate": {
                            "name": "PeakData",
                            "tree": {},
                        },
                        "pgtemplate": {
                            "name": "PeakGroups",
                            "tree": {
                                "queryGroup": [
                                    {
                                        "fld": "peak_groups__msrun__sample__animal__studies__name",
                                        "ncmp": "icontains",
                                        "pos": "",
                                        "static": False,
                                        "type": "query",
                                        "val": "obob_fasted",
                                    },
                                    {
                                        "fld": "name",
                                        "ncmp": "icontains",
                                        "pos": "",
                                        "static": False,
                                        "type": "query",
                                        "val": "citrate",
                                    },
                                ],
                                "static": False,
                                "type": "group",
                                "val": "all",
                            },
                        },
                    },
                    "selectedtemplate": fmt,
                },
                "Compound",
            ],
            "compounds__synonyms",
            "peak_group_set",
        ]

        self.assertEqual(prefetches, expected_prefetches)

    def test_getFullJoinAnnotations(self):
        basv = SearchGroup()
        fmt = "pgtemplate"
        annot_name = "compound"

        # Set all split_rows values to False for the test, then...
        mdl_inst = "MeasuredCompound"
        pgsv = basv.modeldata[fmt]
        for inst in pgsv.model_instances.keys():
            pgsv.model_instances[inst]["manytomany"]["split_rows"] = False
        # Set only MeasuredCompound's split_rows=True and annot_name="compound" for the test
        pgsv.model_instances[mdl_inst]["manytomany"]["split_rows"] = True
        pgsv.model_instances[mdl_inst]["manytomany"]["root_annot_fld"] = annot_name

        # Do the test
        annots = basv.getFullJoinAnnotations(fmt)
        expected_annots = [{annot_name: F("compounds__pk")}]
        self.assertEqual(annots, expected_annots)

    def test_getDistinctFields(self):
        basv = SearchGroup()
        fmt = "pgtemplate"
        order_by = "name"

        # Turn off all split_rows for the test, then...
        mdl_inst = "MeasuredCompound"
        pgsv = basv.modeldata[fmt]
        for inst in pgsv.model_instances.keys():
            pgsv.model_instances[inst]["manytomany"]["split_rows"] = False
        # Set only MeasuredCompound's split_rows value to True for the test
        pgsv.model_instances[mdl_inst]["manytomany"]["split_rows"] = True

        distincts = basv.getDistinctFields(fmt, order_by)
        expected_distincts = [order_by, "pk", "compounds__name", "compounds__pk"]
        self.assertEqual(distincts, expected_distincts)

    def test_getDistinctFields_split_all(self):
        """
        Ensures that meta ordering fields are expanded to real database fields.  I.e. it tests that the third returned
        field is "compounds__synonyms__compound__name" and not "compounds__synonyms__compound"
        """
        pgf = PeakGroupsFormat()
        self.assertIn(
            "compound",
            CompoundSynonym._meta.__dict__["ordering"],
            msg="CompoundSynonym must have 'compound' in meta.ordering for the next assertion to be meaningful",
        )
        distincts = pgf.getDistinctFields(split_all=True)
        expected_distincts = [
            "name",
            "pk",
            "compounds__synonyms__compound__name",
            "compounds__synonyms__name",
            "compounds__synonyms__pk",
            "compounds__name",
            "compounds__pk",
            "msrun__sample__animal__studies__name",
            "msrun__sample__animal__studies__pk",
        ]
        self.assertEqual(distincts, expected_distincts)

    def test_getFKModelName(self):
        pgf = PeakGroupsFormat()
        mdl_name = pgf.getFKModelName(CompoundSynonym(), "compound")
        self.assertEqual(mdl_name, "Compound")

    def test_getOrderByFields_instance(self):
        pgsv = PeakGroupsFormat()
        mdl_inst = "MeasuredCompound"

        order_bys = pgsv.getOrderByFields(mdl_inst_nm=mdl_inst)
        expected_order_bys = ["name"]
        self.assertEqual(order_bys, expected_order_bys)

    def test_getOrderByFields_model(self):
        pgsv = PeakGroupsFormat()
        mdl = "Compound"

        order_bys = pgsv.getOrderByFields(model_name=mdl)
        expected_order_bys = ["name"]
        self.assertEqual(order_bys, expected_order_bys)

    def test_getOrderByFields_both(self):
        pgsv = PeakGroupsFormat()
        mdl_inst = "MeasuredCompound"
        mdl = "Compound"

        with self.assertRaises(
            Exception, msg="mdl_inst_nm and model_name are mutually exclusive options."
        ):
            pgsv.getOrderByFields(mdl_inst_nm=mdl_inst, model_name=mdl)

    def test_getOrderByFields_neither(self):
        pgsv = PeakGroupsFormat()
        mdl_inst = "MeasuredCompound"
        mdl = "Compound"

        with self.assertRaises(
            Exception, msg="Either a model instance name or model name is required."
        ):
            pgsv.getOrderByFields(mdl_inst_nm=mdl_inst, model_name=mdl)

    def test_createFilterGroup(self):
        got = createFilterGroup()
        expected = {
            "type": "group",
            "val": "all",
            "static": False,
            "queryGroup": [],
        }
        self.assertEqual(got, expected)

    def test_createFilterCondition(self):
        fld = "fldtest"
        ncmp = "ncmptest"
        val = "valtest"
        got = createFilterCondition(fld, ncmp, val)
        expected = {
            "type": "query",
            "pos": "",
            "static": False,
            "fld": fld,
            "ncmp": ncmp,
            "val": val,
        }
        self.assertEqual(got, expected)

    def test_appendFilterToGroup(self):
        fld = "fldtest"
        ncmp = "ncmptest"
        val = "valtest"
        got = appendFilterToGroup(
            createFilterGroup(), createFilterCondition(fld, ncmp, val)
        )
        expected = {
            "type": "group",
            "val": "all",
            "static": False,
            "queryGroup": [
                {
                    "type": "query",
                    "pos": "",
                    "static": False,
                    "fld": fld,
                    "ncmp": ncmp,
                    "val": val,
                }
            ],
        }
        self.assertEqual(got, expected)

    def test_getStatsParams(self):
        pgsv = PeakGroupsFormat()
        stats = pgsv.getStatsParams()
        got = stats[2]
        expected_i2 = {
            "displayname": "Measured Compounds",
            "distincts": ["compounds__name"],
            "filter": None,
        }
        self.assertEqual(got, expected_i2)

    def test_getAllBrowseData(self):
        """
        Test that test_getAllBrowseData returns all data for the selected format.
        """
        basv_metadata = SearchGroup()
        pf = "msrun__sample__animal__studies"
        qs = PeakGroup.objects.all().prefetch_related(pf)
        res, cnt, stats = basv_metadata.getAllBrowseData("pgtemplate")
        self.assertEqual(cnt, qs.count())

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
        basv_metadata = SearchGroup()
        mdl = "Study"
        fld = "id"
        cmp = "iexact"
        val = tval
        fmt = "pgtemplate"
        newqry = basv_metadata.createNewBasicQuery(mdl, fld, cmp, val, fmt)
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
        basv_metadata = SearchGroup()
        mdl = "Study"
        fld = "id"
        val = tval
        dfld, dval = basv_metadata.searchFieldToDisplayField(mdl, fld, val, qry)
        self.assertEqual(dfld, "name")
        self.assertEqual(dval, "obob_fasted")

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

    def getExpectedStats(self):
        return {
            "available": True,
            "data": {
                "Animals": {
                    "count": 1,
                    "filter": None,
                    "sample": [
                        {
                            "cnt": 2,
                            "val": "971",
                        },
                    ],
                },
                "Feeding Statuses": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "Fasted"}],
                },
                "Infusion Concentrations": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "23.2"}],
                },
                "Infusion Rates": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "0.11"}],
                },
                "Labeled Elements": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "C"}],
                },
                "Measured Compounds": {
                    "count": 2,
                    "filter": None,
                    "sample": [
                        {"cnt": 1, "val": "glucose"},
                        {"cnt": 1, "val": "lactate"},
                    ],
                },
                "Samples": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "Br-xz971"}],
                },
                "Studies": {
                    "count": 2,
                    "filter": None,
                    "sample": [
                        {"cnt": 2, "val": "obob_fasted"},
                        {"cnt": 2, "val": "small_obob"},
                    ],
                },
                "Tissues": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "brain"}],
                },
                "Tracer Compounds": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "lysine"}],
                },
            },
            "populated": True,
            "show": True,
        }

    def test_performQueryStats(self):
        """
        Test that performQuery returns a correct stats structure
        """
        basv = SearchGroup()
        qry = self.get_advanced_qry()
        res, cnt, stats = basv.performQuery(qry, "pgtemplate", generate_stats=True)
        expected_stats = self.getExpectedStats()
        self.assertEqual(stats, expected_stats)

    def test_getQueryStats(self):
        """
        Test that getQueryStats returns a correct stats structure
        """
        basv = SearchGroup()
        qry = self.get_advanced_qry()
        res, cnt, ignore_stats = basv.performQuery(
            qry, "pgtemplate", generate_stats=True
        )
        got = basv.getQueryStats(res, qry["selectedtemplate"])
        full_stats = self.getExpectedStats()
        expected = full_stats["data"]
        self.assertEqual(got, expected)

    def test_constructAdvancedQuery(self):
        """
        Test that constructAdvancedQuery returns a correct Q expression
        """
        qry = self.get_advanced_qry()
        q_exp = constructAdvancedQuery(qry)
        expected_q = Q(msrun__sample__tissue__name__iexact="Brain")
        self.assertEqual(q_exp, expected_q)

    def test_performQuery(self):
        """
        Test that performQuery returns a correct queryset
        """
        qry = self.get_advanced_qry()
        basv_metadata = SearchGroup()
        pf = [
            "msrun__sample__tissue",
            "msrun__sample__animal__tracer_compound",
            "msrun__sample__animal__studies",
        ]
        res, cnt, stats = basv_metadata.performQuery(
            qry, "pgtemplate", generate_stats=False
        )
        qs = PeakGroup.objects.filter(
            msrun__sample__tissue__name__iexact="Brain"
        ).prefetch_related(*pf)
        self.assertEqual(cnt, qs.count())
        expected_stats = {
            "available": True,
            "data": {},
            "populated": False,
            "show": False,
        }
        self.assertEqual(stats, expected_stats)

    def test_performQuery_distinct(self):
        """
        Test that performQuery returns no duplicate root table records when M:M tables queried with multiple matches.
        """
        qry = self.get_advanced_qry2()
        basv_metadata = SearchGroup()
        res, cnt, stats = basv_metadata.performQuery(qry, "pgtemplate")
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
        basv_metadata = SearchGroup()
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

    def test_getJoinedRecFieldValue(self):
        """
        Test that getJoinedRecFieldValue gets a value from a joined table
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        mdl = "Animal"
        fld = "feeding_status"
        pf = "msrun__sample__animal__studies"
        recs = PeakGroup.objects.all().prefetch_related(pf)
        val = basv_metadata.getJoinedRecFieldValue(recs, fmt, mdl, fld, fld, "Fasted")
        self.assertEqual(val, "Fasted")

    def test_cv_getSearchFieldChoices(self):
        """
        Test getSearchFieldChoices
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        res = basv_metadata.getSearchFieldChoices(fmt)
        choices = (
            ("msrun__sample__animal__name", "Animal"),
            ("msrun__sample__animal__body_weight", "Body Weight (g)"),
            ("msrun__sample__animal__diet", "Diet"),
            ("msrun__sample__animal__feeding_status", "Feeding Status"),
            ("formula", "Formula"),
            ("msrun__sample__animal__genotype", "Genotype"),
            ("compounds__synonyms__name", "Measured Compound (Any Synonym)"),
            ("compounds__name", "Measured Compound (Primary Synonym)"),
            ("name", "Peak Group"),
            ("peak_group_set__filename", "Peak Group Set Filename"),
            ("msrun__sample__name", "Sample"),
            ("msrun__sample__animal__sex", "Sex"),
            ("msrun__sample__animal__studies__name", "Study"),
            ("msrun__sample__tissue__name", "Tissue"),
            (
                "msrun__sample__animal__tracer_compound__name",
                "Tracer Compound (Primary Synonym)",
            ),
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
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        mdl = "Animal"
        res = basv_metadata.getKeyPathList(fmt, mdl)
        kpl = ["msrun", "sample", "animal"]
        self.assertEqual(res, kpl)

    def test_cv_getPrefetches(self):
        """
        Test getPrefetches
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        res = basv_metadata.getPrefetches(fmt)
        pfl = [
            "msrun__sample__animal__tracer_compound",
            "msrun__sample__animal__treatment",
            "msrun__sample__animal__studies",
            "msrun__sample__tissue",
            "compounds__synonyms",
            "peak_group_set",
        ]
        self.assertEqual(pfl, res)

    def test_cv_getModelInstances(self):
        """
        Test getModelInstances
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        res = basv_metadata.getModelInstances(fmt)
        ml = [
            "PeakGroupSet",
            "CompoundSynonym",
            "PeakGroup",
            "Protocol",
            "Sample",
            "Tissue",
            "Animal",
            "TracerCompound",
            "MeasuredCompound",
            "Study",
        ]
        self.assertEqual(res, ml)

    def test_cv_getSearchFields(self):
        """
        Test getSearchFields
        """
        basv_metadata = SearchGroup()
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
        basv_metadata = SearchGroup()
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
        basv_metadata = SearchGroup()
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
        basv_metadata = SearchGroup()
        fmt = "PeakGroups"
        res = basv_metadata.formatNameOrKeyToKey(fmt)
        self.assertEqual(res, "pgtemplate")

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


@tag("search_choices")
class SearchFieldChoicesTests(TracebaseTestCase):
    def test_get_all_comparison_choices(self):
        base_search_view = Format()

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
