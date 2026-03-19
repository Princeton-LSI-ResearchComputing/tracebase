from django.db.models import Q

from DataRepo.formats.dataformat_group_query import (
    append_filter_to_group,
    construct_advanced_query,
    create_filter_condition,
    create_filter_group,
    extract_fld_paths,
    is_qry_obj_valid,
    is_valid_qry_obj_populated,
    path_step_to_pos_group_type,
    root_to_format_info,
    split_common,
    split_path_name,
)
from DataRepo.formats.search_group import SearchGroup
from DataRepo.tests.formats.formats_test_base import FormatsTestCase


class DataformatGroupQueryMainTests(FormatsTestCase):

    def test_extract_fld_paths(self):
        qry = self.get_query_object()
        paths = extract_fld_paths(qry)
        expected_paths = ["msrun_sample__sample__animal__studies"]
        self.assertEqual(expected_paths, paths)

    def test_create_filter_group(self):
        got = create_filter_group()
        expected = {
            "type": "group",
            "val": "all",
            "static": False,
            "queryGroup": [],
        }
        self.assertEqual(expected, got)

    def test_create_filter_condition(self):
        fld = "fldtest"
        ncmp = "ncmptest"
        val = "valtest"
        units = "unitstest"
        got = create_filter_condition(fld, ncmp, val, units)
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

    def test_append_filter_to_group(self):
        fld = "fldtest"
        ncmp = "ncmptest"
        val = "valtest"
        units = "unitstest"
        got = append_filter_to_group(
            create_filter_group(), create_filter_condition(fld, ncmp, val, units)
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

    def test_construct_advanced_query(self):
        """
        Test that constructAdvancedQuery returns a correct Q expression
        """
        qry = self.get_advanced_qry()
        q_exp = construct_advanced_query(qry)
        expected_q = Q(msrun_sample__sample__tissue__name__iexact="Brain")
        self.assertEqual(expected_q, q_exp)

    def test_is_qry_obj_valid(self):
        """
        Test that isQryObjValid correctly validates a qry object.
        """
        qry = self.get_advanced_qry()
        basv_metadata = SearchGroup()
        isvalid = is_qry_obj_valid(qry, basv_metadata.get_format_names().keys())
        self.assertEqual(isvalid, True)
        qry.pop("selectedtemplate")
        isvalid = is_qry_obj_valid(qry, basv_metadata.get_format_names().keys())
        self.assertEqual(isvalid, False)

    def test_is_valid_qry_obj_populated(self):
        """
        Test that isValidQryObjPopulated correctly interprets the population of a subgroup.
        """
        qry = self.get_advanced_qry2()
        isvalid = is_valid_qry_obj_populated(qry)
        self.assertEqual(isvalid, True)
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][1]["queryGroup"] = []
        isvalid = is_valid_qry_obj_populated(qry)
        self.assertEqual(isvalid, False)

    def test_path_step_to_pos_group_type_inner_node(self):
        """
        Convert "0-all" to [0, "all"]
        """
        [pos, gtype, static] = path_step_to_pos_group_type("0-all-True")
        self.assertEqual(pos, 0)
        self.assertEqual(gtype, "all")
        self.assertTrue(not static)

    def test_path_step_to_pos_group_type_leaf_node(self):
        """
        Convert "0" to [0, None]
        """
        [pos, gtype, static] = path_step_to_pos_group_type("0")
        self.assertEqual(pos, 0)
        self.assertEqual(gtype, None)
        self.assertEqual(static, False)

    def test_root_to_format_info_selected(self):
        """
        Convert "pgtemplate-PeakGroups-selected" to ["pgtemplate", "PeakGroups", True]
        """
        [format, name, sel] = root_to_format_info("pgtemplate-PeakGroups-selected")
        self.assertEqual(format, "pgtemplate")
        self.assertEqual(name, "PeakGroups")
        self.assertEqual(sel, True)

    def test_root_to_format_info_unselected(self):
        """
        Convert "pdtemplate-PeakData" to ["pdtemplate", "PeakData", False]
        """
        [format, name, sel] = root_to_format_info("pdtemplate-PeakData")
        self.assertEqual(format, "pdtemplate")
        self.assertEqual(name, "PeakData")
        self.assertEqual(sel, False)

    def test_split_common_hascommon(self):
        fld_path = "msrun_sample__sample__animal__studies"
        reroot_path = "msrun_sample__sample__animal__tracer_compound"
        common_path, remainder = split_common(fld_path, reroot_path)
        self.assertEqual(common_path, "msrun_sample__sample__animal")
        self.assertEqual("studies", remainder)

    def test_split_common_nocommon(self):
        fld_path = "msrun_sample__sample__animal__studies"
        reroot_path = "compounds__synonyms"
        common_path, remainder = split_common(fld_path, reroot_path)
        self.assertEqual(common_path, "")
        self.assertEqual("msrun_sample__sample__animal__studies", remainder)

    def test_split_path_name(self):
        path, name = split_path_name("msrun_sample__sample__animal__treatment__name")
        self.assertEqual(path, "msrun_sample__sample__animal__treatment")
        self.assertEqual("name", name)
