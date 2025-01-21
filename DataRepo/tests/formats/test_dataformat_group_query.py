from django.db.models import Q

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
    splitCommon,
    splitPathName,
)
from DataRepo.formats.search_group import SearchGroup
from DataRepo.tests.formats.formats_test_base import FormatsTestCase


class DataformatGroupQueryMainTests(FormatsTestCase):

    def test_extractFldPaths(self):
        qry = self.getQueryObject()
        paths = extractFldPaths(qry)
        expected_paths = ["msrun_sample__sample__animal__studies"]
        self.assertEqual(expected_paths, paths)

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

    def test_constructAdvancedQuery(self):
        """
        Test that constructAdvancedQuery returns a correct Q expression
        """
        qry = self.get_advanced_qry()
        q_exp = constructAdvancedQuery(qry)
        expected_q = Q(msrun_sample__sample__tissue__name__iexact="Brain")
        self.assertEqual(expected_q, q_exp)

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

    def test_splitCommon_hascommon(self):
        fld_path = "msrun_sample__sample__animal__studies"
        reroot_path = "msrun_sample__sample__animal__tracer_compound"
        common_path, remainder = splitCommon(fld_path, reroot_path)
        self.assertEqual(common_path, "msrun_sample__sample__animal")
        self.assertEqual("studies", remainder)

    def test_splitCommon_nocommon(self):
        fld_path = "msrun_sample__sample__animal__studies"
        reroot_path = "compounds__synonyms"
        common_path, remainder = splitCommon(fld_path, reroot_path)
        self.assertEqual(common_path, "")
        self.assertEqual("msrun_sample__sample__animal__studies", remainder)

    def test_splitPathName(self):
        path, name = splitPathName("msrun_sample__sample__animal__treatment__name")
        self.assertEqual(path, "msrun_sample__sample__animal__treatment")
        self.assertEqual("name", name)
