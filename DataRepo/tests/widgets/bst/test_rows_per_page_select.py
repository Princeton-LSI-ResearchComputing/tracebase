from typing import Dict

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.widgets.bst.rows_per_page_select import BSTRowsPerPageSelect


class BSTRowsPerPageSelectTests(TracebaseTestCase):
    def test_constructor(self):
        # Test defaults
        rpps1 = BSTRowsPerPageSelect(10)
        self.assertEqual("paginate_by", rpps1.select_name)
        self.assertEqual("rows-per-page-option", rpps1.option_name)
        self.assertEqual(10, rpps1.total_rows)
        self.assertEqual(5, rpps1._selected_label)
        self.assertEqual(5, rpps1.selected)
        self.assertEqual([5, 0], rpps1.page_sizes)

        # Test all args (and tests that the selected opt is added to the list if absent)
        rpps2 = BSTRowsPerPageSelect(
            10, selected=10, select_name="rpp", option_name="rpp_opt"
        )
        self.assertEqual("rpp", rpps2.select_name)
        self.assertEqual("rpp_opt", rpps2.option_name)
        self.assertEqual(10, rpps2.total_rows)
        self.assertEqual(10, rpps2._selected_label)
        self.assertEqual(10, rpps2.selected)
        self.assertEqual([5, 10, 0], rpps2.page_sizes)

        # Test that the selected opt is added to the list if absent, and that it is sorted
        rpps3 = BSTRowsPerPageSelect(19, selected=7)
        self.assertEqual(19, rpps3.total_rows)
        self.assertEqual(7, rpps3._selected_label)
        self.assertEqual(7, rpps3.selected)
        self.assertEqual([5, 7, 10, 15, 0], rpps3.page_sizes)

    def test_filter_page_sizes(self):
        rpps = BSTRowsPerPageSelect(10)
        self.assertEqual([5, 0], rpps.filter_page_sizes())
        rpps.total_rows = 10000
        self.assertEqual(
            [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0], rpps.filter_page_sizes()
        )
        rpps.total_rows = 30
        self.assertEqual([5, 10, 15, 20, 25, 0], rpps.filter_page_sizes())
        rpps.total_rows = 30
        rpps.selected = 50
        self.assertEqual([5, 10, 15, 20, 25, 50, 0], rpps.filter_page_sizes())

    def test_str(self):
        rpps = BSTRowsPerPageSelect(10)
        self.assertIn('<span class="page-size">5</span>', str(rpps))
        self.assertNotIn('data-value="10"', str(rpps))
        self.assertIn('data-value="5"', str(rpps))
        self.assertIn("selected\n    >5</a>", str(rpps))
        self.assertIn('data-value="0"\n    \n    >ALL</a>', str(rpps))

        rpps2 = BSTRowsPerPageSelect(100, selected=25)
        self.assertIn('data-value="5"', str(rpps2))
        self.assertIn('data-value="10"', str(rpps2))
        self.assertIn('data-value="15"', str(rpps2))
        self.assertIn('data-value="50"', str(rpps2))
        self.assertIn('data-value="5"', str(rpps2))
        self.assertNotIn('data-value="100"', str(rpps2))
        self.assertIn("selected\n    >25</a>", str(rpps2))

    def test_get_context(self):
        rpps = BSTRowsPerPageSelect(60)
        context: Dict[str, dict] = rpps.get_context("select", 10, None)
        self.assertIn("option_name", context["widget"].keys())
        self.assertEqual("rows-per-page-option", context["widget"]["option_name"])
        cnt = 0
        for i, (_, group_choices, _) in enumerate(context["widget"]["optgroups"]):
            for j in range(len(group_choices)):
                cnt += 1
                self.assertIn(
                    "option_name", context["widget"]["optgroups"][i][1][j].keys()
                )
                self.assertEqual(
                    "rows-per-page-option",
                    context["widget"]["optgroups"][i][1][j]["option_name"],
                )
        # 5, 10, 15, 20, 25, 50, 0 -> 7 sizes
        self.assertEqual(7, cnt)

    def test_custom_opt_name(self):
        rpps = BSTRowsPerPageSelect(60, option_name="rpp")
        context: Dict[str, dict] = rpps.get_context("select", 10, None)
        self.assertEqual("rpp", context["widget"]["option_name"])
        for i, (_, group_choices, _) in enumerate(context["widget"]["optgroups"]):
            for j in range(len(group_choices)):
                self.assertEqual(
                    "rpp", context["widget"]["optgroups"][i][1][j]["option_name"]
                )
