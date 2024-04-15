from copy import deepcopy
from typing import Dict

from django.core.management import call_command
from django.db.models import F, Q, Value
from django.test import tag
from parameterized import parameterized

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
from DataRepo.formats.peakdata_dataformat import PeakDataFormat
from DataRepo.formats.peakgroups_dataformat import PeakGroupsFormat
from DataRepo.formats.search_group import SearchGroup
from DataRepo.models import CompoundSynonym, FCirc, MaintainedModel, PeakGroup
from DataRepo.models.utilities import get_model_by_name
from DataRepo.templatetags.customtags import get_many_related_rec
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class FormatsTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]
    maxDiff = None
    orig_split_rows: Dict[str, str] = {}

    def setUp(self):
        super().setUp()
        self.addCleanup(self.restore_split_rows)

    @classmethod
    @MaintainedModel.no_autoupdates()
    def setUpTestData(cls):
        call_command("loaddata", "lc_methods")
        call_command("load_study", "DataRepo/data/tests/tissues/loading.yaml")
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/small_obob/small_obob_compounds.tsv",
        )
        call_command(
            "load_samples",
            "DataRepo/data/tests/small_obob/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/data/tests/small_obob2/sample_table_headers.yaml",
        )
        call_command(
            "load_samples",
            "DataRepo/data/tests/small_obob/small_obob_sample_table_2ndstudy.tsv",
            sample_table_headers="DataRepo/data/tests/small_obob2/sample_table_headers.yaml",
        )
        call_command(
            "load_accucor_msruns",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
            polarity="positive",
        )
        call_command(
            "load_accucor_msruns",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_serum.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=False,
            polarity="positive",
        )
        basv = SearchGroup()
        for fmt in basv.modeldata.keys():
            cls.orig_split_rows[fmt] = {}
            for inst in basv.modeldata[fmt].model_instances.keys():
                cls.orig_split_rows[fmt][inst] = basv.modeldata[fmt].model_instances[
                    inst
                ]["manyrelated"]["split_rows"]
        super().setUpTestData()

    def restore_split_rows(self):
        """
        Some tests manipulate the basv.modeldata[fmt].model_instances[inst]["manyrelated"]["split_rows"] value to test
        various functionality.  These manipulations of class variables persist from test to test, so if manipulated,
        this method should be called to restore their original values.
        """
        basv = SearchGroup()
        for fmt in basv.modeldata.keys():
            for inst in basv.modeldata[fmt].model_instances.keys():
                basv.modeldata[fmt].model_instances[inst]["manyrelated"][
                    "split_rows"
                ] = self.orig_split_rows[fmt][inst]

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
                                "fld": "sample__animal__studies__name",
                                "val": "obob_fasted",
                                "units": "identity",
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
                "units": "identity",
            }
        )
        return qry

    def getPdtemplateChoicesTuple(self):
        return (
            ("peak_group__sample__animal__age", "Age"),
            ("peak_group__sample__animal__name", "Animal"),
            (
                "peak_group__sample__animal__body_weight",
                "Body Weight (g)",
            ),
            ("corrected_abundance", "Corrected Abundance"),
            ("peak_group__sample__animal__diet", "Diet"),
            (
                "peak_group__sample__animal__feeding_status",
                "Feeding Status",
            ),
            ("peak_group__formula", "Formula"),
            ("peak_group__sample__animal__genotype", "Genotype"),
            ("peak_group__sample__animal__infusate__name", "Infusate"),
            (
                "peak_group__sample__animal__infusion_rate",
                "Infusion Rate (ul/min/g)",
            ),
            ("labels__count", "Labeled Count"),
            ("labels__element", "Labeled Element"),
            ("peak_group__ms_data_file__filename", "MZ Data Filename"),
            (
                "peak_group__msrun_sequence__researcher",
                "Mass Spec Operator",
            ),
            (
                "peak_group__msrun_sequence__instrument",
                "Mass Spectrometer Name",
            ),
            (
                "peak_group__compounds__synonyms__name",
                "Measured Compound (Any Synonym)",
            ),
            ("peak_group__compounds__name", "Measured Compound (Primary Synonym)"),
            ("med_mz", "Median M/Z"),
            ("med_rt", "Median RT"),
            ("peak_group__peak_annotation_file__filename", "Peak Annotation Filename"),
            ("peak_group__name", "Peak Group"),
            ("peak_group__polarity", "Polarity"),
            ("peak_group__ms_raw_file__filename", "RAW Data Filename"),
            ("raw_abundance", "Raw Abundance"),
            ("peak_group__sample__name", "Sample"),
            ("peak_group__sample__animal__sex", "Sex"),
            ("peak_group__sample__animal__studies__name", "Study"),
            (
                "peak_group__sample__time_collected",
                "Time Collected (since infusion)",
            ),
            ("peak_group__sample__tissue__name", "Tissue"),
            (
                "peak_group__sample__animal__infusate__tracers__name",
                "Tracer",
            ),
            (
                "peak_group__sample__animal__infusate__tracers__compound__name",
                "Tracer Compound (Primary Synonym)",
            ),
            (
                "peak_group__sample__animal__infusate__tracer_links__concentration",
                "Tracer Concentration (mM)",
            ),
            ("peak_group__sample__animal__treatment__name", "Treatment"),
        )

    def getPgtemplateChoicesTuple(self):
        return (
            ("sample__animal__age", "Age"),
            ("sample__animal__name", "Animal"),
            ("sample__animal__body_weight", "Body Weight (g)"),
            ("compounds__synonyms__name", "Compound (Measured) (Any Synonym)"),
            ("compounds__name", "Compound (Measured) (Primary Synonym)"),
            (
                "sample__animal__infusate__tracers__compound__name",
                "Compound (Tracer) (Primary Synonym)",
            ),
            ("sample__animal__diet", "Diet"),
            ("sample__animal__feeding_status", "Feeding Status"),
            ("formula", "Formula"),
            ("sample__animal__genotype", "Genotype"),
            ("sample__animal__infusate__name", "Infusate"),
            ("sample__animal__infusion_rate", "Infusion Rate (ul/min/g)"),
            ("labels__element", "Labeled Element"),
            ("ms_data_file__filename", "MZ Data Filename"),
            ("msrun_sequence__researcher", "Mass Spec Operator"),
            ("msrun_sequence__instrument", "Mass Spectrometer Name"),
            ("peak_annotation_file__filename", "Peak Annotation Filename"),
            ("name", "Peak Group"),
            ("polarity", "Polarity"),
            ("ms_raw_file__filename", "RAW Data Filename"),
            ("sample__name", "Sample"),
            ("sample__animal__sex", "Sex"),
            ("sample__animal__studies__name", "Study"),
            (
                "sample__time_collected",
                "Time Collected (since infusion)",
            ),
            ("sample__tissue__name", "Tissue"),
            ("sample__animal__infusate__tracers__name", "Tracer"),
            (
                "sample__animal__infusate__tracer_links__concentration",
                "Tracer Concentration (mM)",
            ),
            ("sample__animal__treatment__name", "Treatment"),
        )

    def getFctemplateChoicesTuple(self):
        return (
            ("serum_sample__animal__name", "Animal"),
            ("serum_sample__animal__age", "Animal Age"),
            ("serum_sample__animal__body_weight", "Body Weight (g)"),
            ("serum_sample__animal__diet", "Diet"),
            ("serum_sample__animal__feeding_status", "Feeding Status"),
            ("serum_sample__animal__genotype", "Genotype"),
            ("serum_sample__animal__infusion_rate", "Infusion Rate (ul/min/g)"),
            ("is_last", "Is Last Serum Tracer Peak Group"),
            ("element", "Peak Group Labeled Element"),
            ("serum_sample__animal__sex", "Sex"),
            ("serum_sample__animal__studies__name", "Study"),
            (
                "serum_sample__time_collected",
                "Time Collected (since infusion)",
            ),
            ("tracer__name", "Tracer"),
            ("tracer__compound__name", "Tracer Compound (Primary Synonym)"),
            (
                "serum_sample__animal__infusate__tracer_links__concentration",
                "Tracer Concentration (mM)",
            ),
            ("serum_sample__animal__treatment__name", "Treatment"),
        )

    def assertIsAFcUnitsLookupDict(self, fld_units_lookup):
        self.assertEqual(31, len(fld_units_lookup.keys()))
        # Path should be prepended to the field name
        self.assertIsNone(fld_units_lookup["serum_sample__animal__genotype"])
        # Each value should be a dict with the units, this one having 15 keys
        self.assertEqual(15, len(fld_units_lookup["serum_sample__animal__age"].keys()))
        # This "native" unit type has 5 keys: name, example, convert, pyconvert, and about
        self.assertEqual(
            5, len(fld_units_lookup["serum_sample__animal__age"]["identity"].keys())
        )
        # Check the name (displayed in the units select list)
        self.assertEqual(
            "n.n{units},...",
            fld_units_lookup["serum_sample__animal__age"]["identity"]["name"],
        )
        # Check the example (shown as a placeholder in the val field)
        self.assertEqual(
            "1w,1d,1:01:01.1",
            fld_units_lookup["serum_sample__animal__age"]["identity"]["example"],
        )
        # The convert key should be a function
        self.assertEqual(
            "function",
            type(
                fld_units_lookup["serum_sample__animal__age"]["identity"]["convert"]
            ).__name__,
        )
        # Check the about value
        expected_about = (
            "Values can be entered using the following format pattern: `[n{units}{:|,}]*hh:mm:ss[.f]`, where units "
            "can be:\n\n- c[enturies]\n- decades\n- y[ears]\n- months\n- w[eeks]\n- d[ays]\n- h[ours]\n- m[inutes]\n"
            "- s[econds]\n- milliseconds\n- microseconds\n\nIf milli/micro-seconds are not included, the last 3 units "
            "(hours, minutes, and seconds) do not need to be specified.\n\nExamples:\n\n- 1w,1d,1:01:01.1\n- 1 year, "
            "3 months\n- 2:30\n- 2 days, 11:29:59.999"
        )
        self.assertEqual(
            expected_about,
            fld_units_lookup["serum_sample__animal__age"]["identity"]["about"],
        )
        # The convert function should modify the value to the format needed by the database
        self.assertEqual(
            "14w", fld_units_lookup["serum_sample__animal__age"]["weeks"]["convert"](14)
        )

    def assertIsAPgUnitsLookupDict(self, fld_units_lookup):
        # There should be 39 fields with units lookups
        self.assertEqual(46, len(fld_units_lookup.keys()))
        # Path should be prepended to the field name
        self.assertIsNone(fld_units_lookup["sample__animal__genotype"])
        # Each value should be a dict with the units, this one having 15 keys
        self.assertEqual(
            15, len(fld_units_lookup["sample__animal__age"].keys())
        )
        # This "native" unit type has 5 keys: name, example, convert, pyconvert, and about
        self.assertEqual(
            5,
            len(
                fld_units_lookup["sample__animal__age"]["identity"].keys()
            ),
        )
        # Check the name (displayed in the units select list)
        self.assertEqual(
            "n.n{units},...",
            fld_units_lookup["sample__animal__age"]["identity"]["name"],
        )
        # Check the example (shown as a placeholder in the val field)
        self.assertEqual(
            "1w,1d,1:01:01.1",
            fld_units_lookup["sample__animal__age"]["identity"][
                "example"
            ],
        )
        # The convert key should be a function
        self.assertEqual(
            "function",
            type(
                fld_units_lookup["sample__animal__age"]["identity"][
                    "convert"
                ]
            ).__name__,
        )

        # Each value should be a dict with the units, this one having 15 keys
        self.assertEqual(
            15, len(fld_units_lookup["sample__time_collected"].keys())
        )
        # This "native" unit type has 5 keys: name, example, convert, pyconvert, and about
        self.assertEqual(
            5,
            len(
                fld_units_lookup["sample__time_collected"][
                    "identity"
                ].keys()
            ),
        )
        # Check the name (displayed in the units select list)
        self.assertEqual(
            "n.n{units},...",
            fld_units_lookup["sample__time_collected"]["identity"][
                "name"
            ],
        )
        # Check the example (shown as a placeholder in the val field)
        self.assertEqual(
            "1w,1d,1:01:01.1",
            fld_units_lookup["sample__time_collected"]["identity"][
                "example"
            ],
        )
        # The convert key should be a function
        self.assertEqual(
            "function",
            type(
                fld_units_lookup["sample__time_collected"]["identity"][
                    "convert"
                ]
            ).__name__,
        )

        # Check the about value
        expected_about = (
            "Values can be entered using the following format pattern: `[n{units}{:|,}]*hh:mm:ss[.f]`, where units "
            "can be:\n\n- c[enturies]\n- decades\n- y[ears]\n- months\n- w[eeks]\n- d[ays]\n- h[ours]\n- m[inutes]\n"
            "- s[econds]\n- milliseconds\n- microseconds\n\nIf milli/micro-seconds are not included, the last 3 units "
            "(hours, minutes, and seconds) do not need to be specified.\n\nExamples:\n\n- 1w,1d,1:01:01.1\n- 1 year, "
            "3 months\n- 2:30\n- 2 days, 11:29:59.999"
        )
        self.assertEqual(
            expected_about,
            fld_units_lookup["sample__animal__age"]["identity"]["about"],
        )
        # The convert function should modify the value to the format needed by the database
        self.assertEqual(
            "14w",
            fld_units_lookup["sample__animal__age"]["weeks"]["convert"](
                14
            ),
        )

    def test_getSearchFieldChoicesDict(self):
        basv = SearchGroup()
        sfcd = basv.getSearchFieldChoicesDict()
        sfcd_expected = {
            "fctemplate": self.getFctemplateChoicesTuple(),
            "pdtemplate": self.getPdtemplateChoicesTuple(),
            "pgtemplate": self.getPgtemplateChoicesTuple(),
        }
        self.assertDictEqual(sfcd_expected, sfcd)

    def test_getAllSearchFieldChoices(self):
        basv = SearchGroup()
        sfct = basv.getAllSearchFieldChoices()
        sfct_expected = self.getPgtemplateChoicesTuple()
        sfct_expected += tuple(
            tuple(x)
            for x in self.getPdtemplateChoicesTuple()
            if x != ("labels__element", "Labeled Element")
        )
        sfct_expected += self.getFctemplateChoicesTuple()
        self.assertTupleEqual(sfct_expected, sfct)

    def test_extractFldPaths(self):
        qry = self.getQueryObject()
        paths = extractFldPaths(qry)
        expected_paths = ["sample__animal__studies"]
        self.assertEqual(expected_paths, paths)

    def test_splitCommon_hascommon(self):
        fld_path = "sample__animal__studies"
        reroot_path = "sample__animal__tracer_compound"
        common_path, remainder = splitCommon(fld_path, reroot_path)
        self.assertEqual(common_path, "sample__animal")
        self.assertEqual("studies", remainder)

    def test_splitCommon_nocommon(self):
        fld_path = "sample__animal__studies"
        reroot_path = "compounds__synonyms"
        common_path, remainder = splitCommon(fld_path, reroot_path)
        self.assertEqual(common_path, "")
        self.assertEqual("sample__animal__studies", remainder)

    def test_splitPathName(self):
        path, name = splitPathName("sample__animal__treatment__name")
        self.assertEqual(path, "sample__animal__treatment")
        self.assertEqual("name", name)

    def test_reRootFieldPath(self):
        fld = "sample__animal__studies__name"
        reroot_instance_name = "CompoundSynonym"
        pgsv = PeakGroupsFormat()
        rerooted_fld = pgsv.reRootFieldPath(fld, reroot_instance_name)
        expected_fld = (
            "compound__peak_groups__sample__animal__studies__name"
        )
        self.assertEqual(expected_fld, rerooted_fld)

    def test_reRootQry(self):
        qry = self.getQueryObject2()
        qry_backup = deepcopy(qry)
        basv = SearchGroup()
        new_qry = basv.reRootQry("pgtemplate", qry, "MeasuredCompound")
        expected_qry = deepcopy(self.getQueryObject2())
        expected_qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0][
            "fld"
        ] = "peak_groups__sample__animal__studies__name"
        expected_qry["searches"]["pgtemplate"]["tree"]["queryGroup"][1][
            "fld"
        ] = "synonyms__name"
        self.assertEqual(qry, qry_backup, msg="qry must be left unchanged")
        self.assertEqual(expected_qry, new_qry)

    def test_pathToModelInstanceName(self):
        pgsv = PeakGroupsFormat()
        mi = pgsv.pathToModelInstanceName("sample__animal__studies")
        self.assertEqual("Study", mi)

    def test_getTrueJoinPrefetchPathsAndQrys(self):
        qry = self.getQueryObject2()
        basv = SearchGroup()
        fmt = "pgtemplate"

        # Set all split_rows values to False for the test, then...
        mdl_inst = "MeasuredCompound"
        pgsv = basv.modeldata[fmt]
        for inst in pgsv.model_instances.keys():
            pgsv.model_instances[inst]["manyrelated"]["split_rows"] = False
        # Set only MeasuredCompound's split_rows=True for the test
        pgsv.model_instances[mdl_inst]["manyrelated"]["split_rows"] = True

        qry["searches"][fmt]["tree"]["queryGroup"][1]["fld"] = "compounds__name"
        qry["searches"][fmt]["tree"]["queryGroup"][1]["val"] = "citrate"
        prefetches = basv.getTrueJoinPrefetchPathsAndQrys(qry)
        expected_prefetches = [
            "sample__animal__infusate__tracers__compound",
            "sample__animal__treatment",
            "sample__animal__studies",
            "sample__tissue",
            "msrun_sequence",
            "msrun_sample__ms_data_file",
            "msrun_sample__ms_raw_file",
            "peak_annotation_file",
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
                                        "fld": "peak_groups__sample__animal__studies__name",
                                        "ncmp": "icontains",
                                        "pos": "",
                                        "static": False,
                                        "type": "query",
                                        "val": "obob_fasted",
                                        "units": "identity",
                                    },
                                    {
                                        "fld": "name",
                                        "ncmp": "icontains",
                                        "pos": "",
                                        "static": False,
                                        "type": "query",
                                        "val": "citrate",
                                        "units": "identity",
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
            "labels",
        ]

        self.assertEqual(11, len(prefetches))
        self.assertEqual("list", type(prefetches).__name__)
        self.assertEqual(expected_prefetches[0:8], prefetches[0:8])
        self.assertEqual(expected_prefetches[9:3], prefetches[9:3])
        self.assertEqual(expected_prefetches[8][0:3], prefetches[8][0:3])
        self.assertIsAPgUnitsLookupDict(prefetches[8][3])

        # Should be called after tearDown()
        # self.restore_split_rows()

    def test_getFullJoinAnnotations(self):
        basv = SearchGroup()
        fmt = "pgtemplate"
        annot_name = "compound"

        # Set all split_rows values to False for the test, then...
        mdl_inst = "MeasuredCompound"
        pgsv = basv.modeldata[fmt]
        for inst in pgsv.model_instances.keys():
            pgsv.model_instances[inst]["manyrelated"]["split_rows"] = False
        # Set only MeasuredCompound's split_rows=True and annot_name="compound" for the test
        pgsv.model_instances[mdl_inst]["manyrelated"]["split_rows"] = True
        pgsv.model_instances[mdl_inst]["manyrelated"]["root_annot_fld"] = annot_name

        # Do the test
        annots = basv.getFullJoinAnnotations(fmt)
        expected_annots = [
            {
                "peak_group_label": Value("")
            },  # Empty string annotation when split_rows is False
            {"compound": F("compounds__pk")},
            {"study": Value("")},
        ]
        self.assertEqual(
            str(expected_annots),
            str(annots),
            msg="Only the split_rows=True annotations have field values",
        )

        # Should be called after tearDown()
        # self.restore_split_rows()

    def test_getDistinctFields(self):
        basv = SearchGroup()
        fmt = "pgtemplate"
        order_by = "name"

        # Turn off all split_rows for the test, then...
        mdl_inst = "MeasuredCompound"
        pgsv = basv.modeldata[fmt]
        for inst in pgsv.model_instances.keys():
            pgsv.model_instances[inst]["manyrelated"]["split_rows"] = False
        # Set only MeasuredCompound's split_rows value to True for the test
        pgsv.model_instances[mdl_inst]["manyrelated"]["split_rows"] = True

        distincts = basv.getDistinctFields(fmt, order_by)
        expected_distincts = [
            order_by,
            "pk",
            "compounds__name",
            "compounds__pk",
        ]
        self.assertEqual(expected_distincts, distincts)

        # Should be called after tearDown()
        # self.restore_split_rows()

    def test_getDistinctFields_split_all(self):
        """
        Ensures that meta ordering fields are expanded to real database fields.  I.e. it tests that fields from every
        M:M model (WRT root) like "compounds__synonyms__compound" are dereferenced to the field from that model's
        Meta.ordering, like "compounds__synonyms__compound__name".

        It also tests that every model instance whose model has ["manyrelated"]["manytomany"] as True is included in
        the returned field set.
        """
        pgf = PeakGroupsFormat()
        self.assertIn(
            "compound",
            CompoundSynonym._meta.__dict__["ordering"],
            msg="CompoundSynonym must have 'compound' in meta.ordering for the next assertion to be meaningful",
        )
        distincts = pgf.getDistinctFields(split_all=True)
        # This includes fields expanded from every M:M model
        expected_distincts = [
            "name",
            "pk",
            "labels__peak_group__name",
            "labels__element",
            "labels__pk",
            "sample__animal__infusate__name",
            "sample__animal__infusate__tracer_links__tracer__name",
            "sample__animal__infusate__tracer_links__concentration",
            "sample__animal__infusate__tracer_links__pk",
            "sample__animal__infusate__tracers__name",
            "sample__animal__infusate__tracers__pk",
            "sample__animal__infusate__tracers__compound__name",
            "sample__animal__infusate__tracers__compound__pk",
            "compounds__name",
            "compounds__pk",
            "compounds__synonyms__compound__name",
            "compounds__synonyms__name",
            "compounds__synonyms__pk",
            "sample__animal__studies__name",
            "sample__animal__studies__pk",
        ]
        self.assertEqual(expected_distincts, distincts)

    def test_getFKModelName(self):
        pgf = PeakGroupsFormat()
        mdl_name = pgf.getFKModelName(CompoundSynonym(), "compound")
        self.assertEqual("Compound", mdl_name)

    def test_getOrderByFields_instance(self):
        pgsv = PeakDataFormat()
        mdl_inst = "PeakData"
        mdl = get_model_by_name(mdl_inst)

        # Retreive any custom ordering
        self.assertEqual(
            ["peak_group", "-corrected_abundance"],
            mdl._meta.__dict__["ordering"],
            msg=(
                "Ensure that the peak_group field is present (because it's a foreign key that should be converted to "
                "the default field) and that the corrected abundance field has a negative sign so that the following "
                "test is meaningful."
            ),
        )
        order_bys = pgsv.getOrderByFields(mdl_inst_nm=mdl_inst)
        expected_order_bys = ["peak_group__name", "corrected_abundance"]
        self.assertEqual(expected_order_bys, order_bys)

    def test_getOrderByFields_model(self):
        pgsv = PeakGroupsFormat()
        mdl = "Compound"

        order_bys = pgsv.getOrderByFields(model_name=mdl)
        expected_order_bys = ["name"]
        self.assertEqual(expected_order_bys, order_bys)

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
        self.assertEqual(expected, got)

    def test_createFilterCondition(self):
        fld = "fldtest"
        ncmp = "ncmptest"
        val = "valtest"
        units = "unitstest"
        got = createFilterCondition(fld, ncmp, val, units)
        expected = {
            "type": "query",
            "pos": "",
            "static": False,
            "fld": fld,
            "ncmp": ncmp,
            "val": val,
            "units": units,
        }
        self.assertEqual(expected, got)

    def test_appendFilterToGroup(self):
        fld = "fldtest"
        ncmp = "ncmptest"
        val = "valtest"
        units = "unitstest"
        got = appendFilterToGroup(
            createFilterGroup(), createFilterCondition(fld, ncmp, val, units)
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
                    "units": units,
                }
            ],
        }
        self.assertEqual(expected, got)

    def test_getStatsParams(self):
        pgsv = PeakGroupsFormat()
        stats = pgsv.getStatsParams()
        got = stats[2]
        expected_i2 = {
            "displayname": "Measured Compounds",
            "distincts": ["compounds__name"],
            "filter": None,
        }
        self.assertEqual(expected_i2, got)

    def test_getAllBrowseData(self):
        """
        Test that test_getAllBrowseData returns all data for the selected format.
        """
        basv_metadata = SearchGroup()
        pf = "sample__animal__studies"
        qs = PeakGroup.objects.all().prefetch_related(pf)
        res, cnt, stats = basv_metadata.getAllBrowseData("pgtemplate")
        self.assertEqual(qs.count(), cnt)

    def get_basic_qry_inputs(self):
        qs = PeakGroup.objects.all().prefetch_related(
            "sample__animal__studies"
        )
        tval = str(qs[0].sample.animal.studies.all()[0].id)
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
                    "units": "",
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
                                "fld": "sample__animal__studies__name",
                                "val": "obob_fasted",
                                "units": "identity",
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
        units = "identity"
        newqry = basv_metadata.createNewBasicQuery(mdl, fld, cmp, val, fmt, units)
        self.maxDiff = None
        self.assertEqual(qry, newqry)

    def test_searchFieldToDisplayField(self):
        """
        Test that searchFieldToDisplayField converts Study.id to Study.name
        """
        [tval, qry] = self.get_basic_qry_inputs()
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0][
            "fld"
        ] = "sample__animal__studies__id"
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
                                "fld": "sample__tissue__name",
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
                                "fld": "sample__animal__name",
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

    def get_advanced_qry2(self):
        """
        Modify the query returned by get_advanced_qry to include search terms on 2 M:M related tables in a sub-group.
        """
        qry = self.get_advanced_qry()
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0][
            "fld"
        ] = "sample__name"
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
                        "fld": "sample__animal__studies__name",
                        "val": "obob_fasted",
                        "units": "identity",
                    },
                    {
                        "type": "query",
                        "pos": "",
                        "static": False,
                        "ncmp": "iexact",
                        "fld": "compounds__synonyms__name",
                        "val": "glucose",
                        "units": "identity",
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
                "Tracer Concentrations": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "lysine:23.2"}],
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
        self.assertEqual(expected_stats, stats)

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
        self.assertEqual(expected, got)

    def test_constructAdvancedQuery(self):
        """
        Test that constructAdvancedQuery returns a correct Q expression
        """
        qry = self.get_advanced_qry()
        q_exp = constructAdvancedQuery(qry)
        expected_q = Q(sample__tissue__name__iexact="Brain")
        self.assertEqual(expected_q, q_exp)

    def test_performQuery(self):
        """
        Test that performQuery returns a correct queryset
        """
        qry = self.get_advanced_qry()
        basv_metadata = SearchGroup()
        pf = [
            "sample__tissue",
            "sample__animal__tracer_compound",
            "sample__animal__studies",
        ]
        res, cnt, stats = basv_metadata.performQuery(
            qry, "pgtemplate", generate_stats=False
        )
        qs = PeakGroup.objects.filter(
            sample__tissue__name__iexact="Brain"
        ).prefetch_related(*pf)
        self.assertEqual(cnt, qs.count())
        expected_stats = {
            "available": True,
            "data": {},
            "populated": False,
            "show": False,
        }
        self.assertEqual(expected_stats, stats)

    def test_performQuery_distinct(self):
        """
        Test that performQuery returns no duplicate root table records when M:M tables queried with multiple matches.
        """
        qry = self.get_advanced_qry2()
        basv_metadata = SearchGroup()
        res, cnt, stats = basv_metadata.performQuery(qry, "pgtemplate")
        qs = (
            PeakGroup.objects.filter(sample__name__iexact="BAT-xz971")
            .filter(sample__animal__studies__name__iexact="obob_fasted")
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
        pf = "sample__animal__studies"
        recs = PeakGroup.objects.all().prefetch_related(pf)
        val = basv_metadata.getJoinedRecFieldValue(recs, fmt, mdl, fld, fld, "Fasted")
        self.assertEqual("Fasted", val)

    def test_cv_getSearchFieldChoices(self):
        """
        Test getSearchFieldChoices
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        res = basv_metadata.getSearchFieldChoices(fmt)
        choices = (
            ("sample__animal__age", "Age"),
            ("sample__animal__name", "Animal"),
            ("sample__animal__body_weight", "Body Weight (g)"),
            ("compounds__synonyms__name", "Compound (Measured) (Any Synonym)"),
            ("compounds__name", "Compound (Measured) (Primary Synonym)"),
            (
                "sample__animal__infusate__tracers__compound__name",
                "Compound (Tracer) (Primary Synonym)",
            ),
            ("sample__animal__diet", "Diet"),
            ("sample__animal__feeding_status", "Feeding Status"),
            ("formula", "Formula"),
            ("sample__animal__genotype", "Genotype"),
            ("sample__animal__infusate__name", "Infusate"),
            ("sample__animal__infusion_rate", "Infusion Rate (ul/min/g)"),
            ("labels__element", "Labeled Element"),
            ("msrun_sample__ms_data_file__filename", "MZ Data Filename"),
            ("msrun_sequence__researcher", "Mass Spec Operator"),
            ("msrun_sequence__instrument", "Mass Spectrometer Name"),
            ("peak_annotation_file__filename", "Peak Annotation Filename"),
            ("name", "Peak Group"),
            ("msrun_sample__polarity", "Polarity"),
            ("msrun_sample__ms_raw_file__filename", "RAW Data Filename"),
            ("sample__name", "Sample"),
            ("sample__animal__sex", "Sex"),
            ("sample__animal__studies__name", "Study"),
            ("sample__time_collected", "Time Collected (since infusion)"),
            ("sample__tissue__name", "Tissue"),
            ("sample__animal__infusate__tracers__name", "Tracer"),
            (
                "sample__animal__infusate__tracer_links__concentration",
                "Tracer Concentration (mM)",
            ),
            ("sample__animal__treatment__name", "Treatment"),
        )
        self.assertEqual(choices, res)

    def test_cv_getKeyPathList(self):
        """
        Test getKeyPathList
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        mdl = "Animal"
        res = basv_metadata.getKeyPathList(fmt, mdl)
        kpl = ["sample", "animal"]
        self.assertEqual(kpl, res)

    def test_cv_getPrefetches(self):
        """
        Test getPrefetches (which should not return the infusatetracer through model and return paths in order of
        descending length)
        """
        basv_metadata = SearchGroup()
        fmt = "pdtemplate"
        res = basv_metadata.getPrefetches(fmt)
        pfl = [
            "peak_group__sample__animal__infusate__tracers__compound",
            "peak_group__sample__animal__treatment",
            "peak_group__sample__animal__studies",
            "peak_group__sample__tissue",
            "peak_group__msrun_sequence",
            "peak_group__msrun_sample__ms_data_file",
            "peak_group__msrun_sample__ms_raw_file",
            "peak_group__peak_annotation_file",
            "peak_group__compounds__synonyms",
            "labels",
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
            "PeakAnnotationFile",
            "PeakGroup",
            "PeakGroupLabel",
            "Protocol",
            "Sample",
            "Tissue",
            "Animal",
            "Infusate",
            "InfusateTracer",
            "Tracer",
            "TracerCompound",
            "MeasuredCompound",
            "CompoundSynonym",
            "Study",
            "MSRunSequence",
            "MSRunSample",
            "MZFile",
            "RAWFile",
        ]
        self.assertEqual(ml, res)

    def test_cv_getSearchFields(self):
        """
        Test getSearchFields
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        mdl = "Animal"
        res = basv_metadata.getSearchFields(fmt, mdl)
        sfd = {
            "id": "sample__animal__id",
            "name": "sample__animal__name",
            "genotype": "sample__animal__genotype",
            "age": "sample__animal__age",
            "body_weight": "sample__animal__body_weight",
            "sex": "sample__animal__sex",
            "diet": "sample__animal__diet",
            "feeding_status": "sample__animal__feeding_status",
            "infusion_rate": "sample__animal__infusion_rate",
        }
        self.assertEqual(sfd, res)

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
            "infusion_rate": "infusion_rate",
        }
        self.assertEqual(dfd, res)

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
        self.assertEqual(fnd, res)

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

    @MaintainedModel.no_autoupdates()
    def test_fcirc_performQuery_tracer_links_1to1(self):
        """
        This test ensures that when we perform any query on the fcirc format, the means of limiting each row to a
        single tracer works.  I.e. Calling get_many_related_rec with the tracer links and the annotated field (defined
        by root_annot_fld to be "tracer_link") returns a list contaoining a single record.
        """
        # Make sure there are multiple tracers
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/consolidated_tracebase_compound_list.tsv",
            verbosity=2,
        )
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/small_multitracer/animal_sample_table.xlsx",
        )
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/data/tests/small_multitracer/animal_sample_table.xlsx"
            ),
            skip_researcher_check=True,
        )
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/data/tests/small_multitracer/6eaafasted1_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
            polarity="positive",
        )
        call_command(
            "load_accucor_msruns",
            accucor_file="DataRepo/data/tests/small_multitracer/bcaafasted_cor.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Xianfeng Zeng",
            new_researcher=False,
            isocorr_format=True,
            polarity="positive",
        )

        format = "fctemplate"
        sg = SearchGroup()

        # Make sure the built in annotation field for this model instance is "tracer_link"
        annotfld = sg.modeldata["fctemplate"].model_instances["InfusateTracer"][
            "manyrelated"
        ]["root_annot_fld"]
        self.assertEqual("tracer_link", annotfld)

        # Perform the query
        (qs, junk1, junk2) = sg.performQuery(fmt=format)

        # Make sure there are results
        self.assertTrue(qs.count() > 0)

        num_multitracer_recs = 0
        for r in qs.all():
            # Make sure there are infusate records with multiple tracers
            if r.serum_sample.animal.infusate.tracer_links.count() > 1:
                num_multitracer_recs += 1

            # Obtain the true join records
            recs = get_many_related_rec(
                r.serum_sample.animal.infusate.tracer_links.all(), r.tracer_link
            )

            # Make sure there's only 1 related record (1:1)
            self.assertEqual(1, len(recs))

            # Make sure it's the correct record
            self.assertEqual(r.tracer.id, recs[0].tracer.id)

        # Make sure that there were some infusates with multiple tracers
        self.assertTrue(
            num_multitracer_recs > 0, msg="Make sure the test above has meaning"
        )

        # Make sure that getRootQuerySet was overridden to make:
        #   tracer__id = serum_sample__animal__infusate__tracer_links__tracer__id
        # so that the number of FCirc records is equal to the number of queryset records when splitting on
        # InfusateTracer records.  This affects only the result count displayed on the page.
        self.assertEqual(FCirc.objects.count(), qs.count())

    def test_getFieldUnitsDict(self):
        """
        Spot check a few dicts
        """
        sg = SearchGroup()
        fld_units_dict = sg.getFieldUnitsDict()
        self.assertEqual(3, len(fld_units_dict.keys()))
        expected_element_dict = {
            "choices": (("identity", "identity"),),
            "default": "identity",
            "metadata": {
                "identity": {
                    "about": None,
                    "example": None,
                },
            },
            "units": "identity",
        }
        self.assertEqual(expected_element_dict, fld_units_dict["fctemplate"]["element"])
        expected_age_dict = {
            "choices": (
                ("months", "months"),
                ("weeks", "weeks"),
                ("days", "days"),
                ("hours", "hours"),
            ),
            "default": "weeks",
            "metadata": {
                "days": {
                    "about": None,
                    "example": "1.0",
                },
                "hours": {
                    "about": None,
                    "example": "1.0",
                },
                "months": {
                    "about": None,
                    "example": "1.0",
                },
                "weeks": {
                    "about": None,
                    "example": "1.0",
                },
            },
            "units": "postgres_interval",
        }
        self.assertEqual(
            expected_age_dict, fld_units_dict["fctemplate"]["serum_sample__animal__age"]
        )
        self.assertEqual(31, len(fld_units_dict["fctemplate"].keys()))
        self.assertEqual(47, len(fld_units_dict["pgtemplate"].keys()))
        self.assertEqual(51, len(fld_units_dict["pdtemplate"].keys()))

    def test_getAllFieldUnitsChoices(self):
        sg = SearchGroup()
        fld_units_choices = sg.getAllFieldUnitsChoices()
        expected = (
            ("identity", "identity"),
            ("calendartime", "ny,nm,nw,nd"),
            ("clocktime", "clocktime (hh:mm[:ss])"),
            ("millennia", "millennia"),
            ("centuries", "centuries"),
            ("decades", "decades"),
            ("years", "years"),
            ("months", "months"),
            ("weeks", "weeks"),
            ("days", "days"),
            ("hours", "hours"),
            ("minutes", "minutes"),
            ("seconds", "seconds"),
            ("milliseconds", "milliseconds"),
            ("microseconds", "microseconds"),
        )
        self.assertEqual(expected, fld_units_choices)

    def test_getFieldUnitsLookup(self):
        format = "fctemplate"
        sg = SearchGroup()
        fld_units_lookup = sg.getFieldUnitsLookup(format)
        self.assertIsAFcUnitsLookupDict(fld_units_lookup)


@tag("search_choices")
class SearchFieldChoicesTests(TracebaseTestCase):
    def test_get_all_comparison_choices(self):
        base_search_view = Format()

        all_ncmp_choices = (
            ("exact", "is"),
            ("not_exact", "is not"),
            ("lt", "<"),
            ("lte", "<="),
            ("gt", ">"),
            ("gte", ">="),
            ("not_isnull", "has a value (ie. is not None)"),
            ("isnull", "does not have a value (ie. is None)"),
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("icontains", "contains"),
            ("not_icontains", "does not contain"),
            ("istartswith", "starts with"),
            ("not_istartswith", "does not start with"),
            ("iendswith", "ends with"),
            ("not_iendswith", "does not end with"),
        )
        self.assertEqual(all_ncmp_choices, base_search_view.getAllComparisonChoices())


class DataFormatTests(TracebaseTestCase):
    archive_file_instances = [
        [
            "PeakAnnotationFile",
            "PeakAnnotationFile",
            "ArchiveFile",
        ],
        ["RAWFile", "RAWFile", "ArchiveFile"],
        ["MZFile", "MZFile", "ArchiveFile"],
    ]

    @parameterized.expand(archive_file_instances)
    def test_PeakGroupsFormat_getModelFromInstance(self, name, instance, model):
        pgsv = PeakGroupsFormat()
        res = pgsv.getModelFromInstance(instance)
        self.assertEqual(res, model)

    @parameterized.expand(archive_file_instances)
    def test_PeakDataFormat_getModelFromInstance(self, name, instance, model):
        pgsv = PeakDataFormat()
        res = pgsv.getModelFromInstance(instance)
        self.assertEqual(res, model)
