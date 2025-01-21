from copy import deepcopy
from typing import Dict

from django.core.management import call_command
from django.db.models import F, Value

from DataRepo.formats.dataformat_group import (
    ConditionallyRequiredArgumentError,
    UnsupportedDistinctCombo,
)
from DataRepo.formats.search_group import SearchGroup
from DataRepo.models.fcirc import FCirc
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.peak_group import PeakGroup
from DataRepo.templatetags.customtags import get_many_related_rec
from DataRepo.tests.formats.formats_test_base import FormatsTestCase
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class DataformatGroupMainTests(TracebaseTestCase):
    """Test class for DataRepo.formats.dataformat_group.__main__"""

    def test_ConditionallyRequiredArgumentError(self):
        """Test __main__.ConditionallyRequiredArgumentError"""
        ConditionallyRequiredArgumentError()

    def test_UnsupportedDistinctCombo(self):
        """Test __main__.UnsupportedDistinctCombo"""
        udc = UnsupportedDistinctCombo(["a", "b", "c"])
        self.assertIn(
            "Unsupported combination of distinct fields: ['a', 'b', 'c']", str(udc)
        )


class FormatGroupTests(FormatsTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]
    orig_split_rows: Dict[str, str] = {}

    def setUp(self):
        super().setUp()
        self.addCleanup(self.restore_split_rows)

    @classmethod
    @MaintainedModel.no_autoupdates()
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_no_newsample.xlsx",
        )
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_no_newsample_2ndstudy.xlsx",
            exclude_sheets=["Peak Annotation Files"],
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

    def assertIsAPgUnitsLookupDict(self, fld_units_lookup):
        # There should be 39 fields with units lookups
        self.assertEqual(45, len(fld_units_lookup.keys()))
        # Path should be prepended to the field name
        self.assertIsNone(fld_units_lookup["msrun_sample__sample__animal__genotype"])
        # Each value should be a dict with the units, this one having 15 keys
        self.assertEqual(
            15, len(fld_units_lookup["msrun_sample__sample__animal__age"].keys())
        )
        # This "native" unit type has 5 keys: name, example, convert, pyconvert, and about
        self.assertEqual(
            5,
            len(
                fld_units_lookup["msrun_sample__sample__animal__age"]["identity"].keys()
            ),
        )
        # Check the name (displayed in the units select list)
        self.assertEqual(
            "n.n{units},...",
            fld_units_lookup["msrun_sample__sample__animal__age"]["identity"]["name"],
        )
        # Check the example (shown as a placeholder in the val field)
        self.assertEqual(
            "1w,1d,1:01:01.1",
            fld_units_lookup["msrun_sample__sample__animal__age"]["identity"][
                "example"
            ],
        )
        # The convert key should be a function
        self.assertEqual(
            "function",
            type(
                fld_units_lookup["msrun_sample__sample__animal__age"]["identity"][
                    "convert"
                ]
            ).__name__,
        )

        # Each value should be a dict with the units, this one having 15 keys
        self.assertEqual(
            15, len(fld_units_lookup["msrun_sample__sample__time_collected"].keys())
        )
        # This "native" unit type has 5 keys: name, example, convert, pyconvert, and about
        self.assertEqual(
            5,
            len(
                fld_units_lookup["msrun_sample__sample__time_collected"][
                    "identity"
                ].keys()
            ),
        )
        # Check the name (displayed in the units select list)
        self.assertEqual(
            "n.n{units},...",
            fld_units_lookup["msrun_sample__sample__time_collected"]["identity"][
                "name"
            ],
        )
        # Check the example (shown as a placeholder in the val field)
        self.assertEqual(
            "1w,1d,1:01:01.1",
            fld_units_lookup["msrun_sample__sample__time_collected"]["identity"][
                "example"
            ],
        )
        # The convert key should be a function
        self.assertEqual(
            "function",
            type(
                fld_units_lookup["msrun_sample__sample__time_collected"]["identity"][
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
            fld_units_lookup["msrun_sample__sample__animal__age"]["identity"]["about"],
        )
        # The convert function should modify the value to the format needed by the database
        self.assertEqual(
            "14w",
            fld_units_lookup["msrun_sample__sample__animal__age"]["weeks"]["convert"](
                14
            ),
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

    def get_basic_qry_inputs(self):
        qs = PeakGroup.objects.all().prefetch_related(
            "msrun_sample__sample__animal__studies"
        )
        study_name = "Small OBOB"
        tval = str(
            qs[0].msrun_sample.sample.animal.studies.filter(name=study_name)[0].id
        )
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
                                "fld": "msrun_sample__sample__animal__studies__name",
                                "val": study_name,
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

    def test_reRootQry(self):
        qry = self.getQueryObject2()
        qry_backup = deepcopy(qry)
        basv = SearchGroup()
        new_qry = basv.reRootQry("pgtemplate", qry, "MeasuredCompound")
        expected_qry = deepcopy(self.getQueryObject2())
        expected_qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0][
            "fld"
        ] = "peak_groups__msrun_sample__sample__animal__studies__name"
        expected_qry["searches"]["pgtemplate"]["tree"]["queryGroup"][1][
            "fld"
        ] = "synonyms__name"
        self.assertEqual(qry, qry_backup, msg="qry must be left unchanged")
        self.assertEqual(expected_qry, new_qry)

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
            "msrun_sample__sample__animal__infusate__tracers__compound",
            "msrun_sample__sample__msrun_samples__ms_data_file",
            "msrun_sample__sample__msrun_samples__ms_raw_file",
            "msrun_sample__sample__animal__treatment",
            "msrun_sample__sample__animal__studies",
            "msrun_sample__sample__tissue",
            "msrun_sample__msrun_sequence",
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
                                        "fld": "peak_groups__msrun_sample__sample__animal__studies__name",
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
            {"mzdatafl": Value("")},
            {"mzrawfl": Value("")},
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

    def test_getAllBrowseData(self):
        """
        Test that test_getAllBrowseData returns all data for the selected format.
        """
        basv_metadata = SearchGroup()
        pf = "msrun_sample__sample__animal__studies"
        qs = PeakGroup.objects.all().prefetch_related(pf)
        _, cnt, _ = basv_metadata.getAllBrowseData("pgtemplate")
        self.assertEqual(qs.count(), cnt)

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
        self.assertEqual(qry, newqry)

    def test_searchFieldToDisplayField(self):
        """
        Test that searchFieldToDisplayField converts Study.id to Study.name
        """
        [tval, qry] = self.get_basic_qry_inputs()
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0][
            "fld"
        ] = "msrun_sample__sample__animal__studies__id"
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0]["val"] = tval
        basv_metadata = SearchGroup()
        mdl = "Study"
        fld = "id"
        val = tval
        dfld, dval = basv_metadata.searchFieldToDisplayField(mdl, fld, val, qry)
        self.assertEqual(dfld, "name")
        self.assertEqual(dval, "Small OBOB")

    def test_performQuery_stats1(self):
        """
        Test that performQuery returns a correct queryset
        """
        qry = self.get_advanced_qry()
        basv_metadata = SearchGroup()
        pf = [
            "msrun_sample__sample__tissue",
            "msrun_sample__sample__animal__tracer_compound",
            "msrun_sample__sample__animal__studies",
        ]
        _, cnt, stats = basv_metadata.performQuery(
            qry, "pgtemplate", generate_stats=False
        )
        qs = PeakGroup.objects.filter(
            msrun_sample__sample__tissue__name__iexact="Brain"
        ).prefetch_related(*pf)
        self.assertEqual(cnt, qs.count())
        expected_stats = {
            "available": True,
            "data": {},
            "populated": False,
            "show": False,
        }
        self.assertEqual(expected_stats, stats)

    def test_performQuery_stats2(self):
        """
        Test that performQuery returns a correct stats structure
        """
        basv = SearchGroup()
        qry = self.get_advanced_qry()
        _, _, stats = basv.performQuery(qry, "pgtemplate", generate_stats=True)
        for mdl in stats["data"].keys():
            stats["data"][mdl]["sample"] = sorted(
                stats["data"][mdl]["sample"], key=lambda d: d["val"]
            )
        expected_stats = self.getExpectedStats()
        self.assertDictEqual(expected_stats, stats)

    def test_performQuery_distinct(self):
        """
        Test that performQuery returns no duplicate root table records when M:M tables queried with multiple matches.
        """
        qry = self.get_advanced_qry2()
        basv_metadata = SearchGroup()
        _, cnt, _ = basv_metadata.performQuery(qry, "pgtemplate")
        qs = (
            PeakGroup.objects.filter(msrun_sample__sample__name__iexact="BAT-xz971")
            .filter(msrun_sample__sample__animal__studies__name__iexact="obob_fasted")
            .filter(compounds__synonyms__name__iexact="glucose")
        )
        # Ensure the test is working by ensuring the number of records without distinct is larger
        self.assertTrue(cnt < qs.count())
        self.assertEqual(cnt, 1)

    @MaintainedModel.no_autoupdates()
    def test_performQuery_fcirc_tracer_links_1to1(self):
        """
        This test ensures that when we perform any query on the fcirc format, the means of limiting each row to a
        single tracer works.  I.e. Calling get_many_related_rec with the tracer links and the annotated field (defined
        by root_annot_fld to be "tracer_link") returns a list contaoining a single record.
        """
        # Make sure there are multiple tracers
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_multitracer/study.xlsx",
        )

        format = "fctemplate"
        sg = SearchGroup()

        # Make sure the built in annotation field for this model instance is "tracer_link"
        annotfld = sg.modeldata["fctemplate"].model_instances["InfusateTracer"][
            "manyrelated"
        ]["root_annot_fld"]
        self.assertEqual("tracer_link", annotfld)

        # Perform the query
        qs, _, _ = sg.performQuery(fmt=format)

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

    def test_getQueryStats_full(self):
        """
        Test that getQueryStats returns a correct stats structure
        """
        basv = SearchGroup()
        qry = self.get_advanced_qry()
        res, _, _ = basv.performQuery(qry, "pgtemplate", generate_stats=True)
        got, based_on = basv.getQueryStats(res, qry["selectedtemplate"])
        for mdl in got.keys():
            got[mdl]["sample"] = sorted(got[mdl]["sample"], key=lambda d: d["val"])
        full_stats = self.getExpectedStats()
        expected = full_stats["data"]
        self.assertEqual(expected, got)
        self.assertIsNone(based_on)

    def test_getQueryStats_truncated(self):
        """Test that getQueryStats returns truncated results when not enough time"""
        basv = SearchGroup()
        qry = self.get_advanced_qry()
        res, _, _ = basv.performQuery(qry, "pgtemplate", generate_stats=True)
        got, based_on = basv.getQueryStats(
            res,
            qry["selectedtemplate"],
            # A time limit of 0 seconds will produce 1 result because the elapsed time if checked at the bottom of the
            # for loop that iterates over the queryset
            time_limit_secs=0,
        )
        full_stats = self.getExpectedStats()
        expected = full_stats["data"]
        self.assertNotEqual(expected, got)
        self.assertEqual("* Based on 5.56% of the data (truncated for time)", based_on)

    def test_getJoinedRecFieldValue(self):
        """
        Test that getJoinedRecFieldValue gets a value from a joined table
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        mdl = "Animal"
        fld = "feeding_status"
        pf = "msrun_sample__sample__animal__studies"
        recs = PeakGroup.objects.all().prefetch_related(pf)
        val = basv_metadata.getJoinedRecFieldValue(recs, fmt, mdl, fld, fld, "Fasted")
        self.assertEqual("Fasted", val)

    def test_getSearchFieldChoices(self):
        """
        Test getSearchFieldChoices
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        res = basv_metadata.getSearchFieldChoices(fmt)
        choices = (
            ("msrun_sample__sample__animal__age", "Age"),
            ("msrun_sample__sample__animal__name", "Animal"),
            ("msrun_sample__sample__animal__body_weight", "Body Weight (g)"),
            ("compounds__synonyms__name", "Compound (Measured) (Any Synonym)"),
            ("compounds__name", "Compound (Measured) (Primary Synonym)"),
            (
                "msrun_sample__sample__animal__infusate__tracers__compound__name",
                "Compound (Tracer) (Primary Synonym)",
            ),
            ("msrun_sample__sample__animal__diet", "Diet"),
            ("msrun_sample__sample__animal__feeding_status", "Feeding Status"),
            ("formula", "Formula"),
            ("msrun_sample__sample__animal__genotype", "Genotype"),
            ("msrun_sample__sample__animal__infusate__name", "Infusate"),
            ("msrun_sample__sample__animal__infusion_rate", "Infusion Rate (ul/min/g)"),
            ("labels__element", "Labeled Element"),
            (
                "msrun_sample__sample__msrun_samples__ms_data_file__filename",
                "MZ Data Filename",
            ),
            ("msrun_sample__msrun_sequence__researcher", "Mass Spec Operator"),
            ("msrun_sample__msrun_sequence__instrument", "Mass Spectrometer Name"),
            ("peak_annotation_file__filename", "Peak Annotation Filename"),
            ("name", "Peak Group"),
            (
                "msrun_sample__sample__msrun_samples__ms_raw_file__filename",
                "RAW Data Filename",
            ),
            ("msrun_sample__sample__name", "Sample"),
            ("msrun_sample__sample__animal__sex", "Sex"),
            ("msrun_sample__sample__animal__studies__name", "Study"),
            ("msrun_sample__sample__time_collected", "Time Collected (since infusion)"),
            ("msrun_sample__sample__tissue__name", "Tissue"),
            ("msrun_sample__sample__animal__infusate__tracers__name", "Tracer"),
            (
                "msrun_sample__sample__animal__infusate__tracer_links__concentration",
                "Tracer Concentration (mM)",
            ),
            ("msrun_sample__sample__animal__treatment__name", "Treatment"),
        )
        self.assertEqual(choices, res)

    def test_getKeyPathList(self):
        """
        Test getKeyPathList
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        mdl = "Animal"
        res = basv_metadata.getKeyPathList(fmt, mdl)
        kpl = ["msrun_sample", "sample", "animal"]
        self.assertEqual(kpl, res)

    def test_getPrefetches(self):
        """
        Test getPrefetches (which should not return the infusatetracer through model and return paths in order of
        descending length)
        """
        basv_metadata = SearchGroup()
        fmt = "pdtemplate"
        res = basv_metadata.getPrefetches(fmt)
        pfl = [
            "peak_group__msrun_sample__sample__animal__infusate__tracers__compound",
            "peak_group__msrun_sample__sample__msrun_samples__ms_data_file",
            "peak_group__msrun_sample__sample__msrun_samples__ms_raw_file",
            "peak_group__msrun_sample__sample__animal__treatment",
            "peak_group__msrun_sample__sample__animal__studies",
            "peak_group__msrun_sample__sample__tissue",
            "peak_group__msrun_sample__msrun_sequence",
            "peak_group__peak_annotation_file",
            "peak_group__compounds__synonyms",
            "labels",
        ]
        self.assertEqual(pfl, res)

    def test_getModelInstances(self):
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
            "MZFile",
            "RAWFile",
        ]
        self.assertEqual(ml, res)

    def test_getSearchFields(self):
        """
        Test getSearchFields
        """
        basv_metadata = SearchGroup()
        fmt = "pgtemplate"
        mdl = "Animal"
        res = basv_metadata.getSearchFields(fmt, mdl)
        sfd = {
            "id": "msrun_sample__sample__animal__id",
            "name": "msrun_sample__sample__animal__name",
            "genotype": "msrun_sample__sample__animal__genotype",
            "age": "msrun_sample__sample__animal__age",
            "body_weight": "msrun_sample__sample__animal__body_weight",
            "sex": "msrun_sample__sample__animal__sex",
            "diet": "msrun_sample__sample__animal__diet",
            "feeding_status": "msrun_sample__sample__animal__feeding_status",
            "infusion_rate": "msrun_sample__sample__animal__infusion_rate",
        }
        self.assertEqual(sfd, res)

    def test_getDisplayFields(self):
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

    def test_getFormatNames(self):
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

    def test_formatNameOrKeyToKey(self):
        """
        Test formatNameOrKeyToKey
        """
        basv_metadata = SearchGroup()
        fmt = "PeakGroups"
        res = basv_metadata.formatNameOrKeyToKey(fmt)
        self.assertEqual(res, "pgtemplate")

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
        self.assertEqual(46, len(fld_units_dict["pgtemplate"].keys()))
        self.assertEqual(50, len(fld_units_dict["pdtemplate"].keys()))

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

    def test_getSearchFieldChoicesDict(self):
        basv = SearchGroup()
        sfcd = basv.getSearchFieldChoicesDict()
        sfcd_expected = {
            "fctemplate": self.getFctemplateChoicesTuple(),
            "pdtemplate": self.getPdtemplateChoicesTuple(),
            "pgtemplate": self.getPgtemplateChoicesTuple(),
        }
        self.assertDictEqual(sfcd_expected, sfcd)
