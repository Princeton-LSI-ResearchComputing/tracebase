from django.template.loader import render_to_string
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst.utils import SizedPaginator


@override_settings(DEBUG=True)
class PaginatorTemplateTests(TracebaseTestCase):
    """Tests for the paginator.html template tied to SizedPaginator in DataRepo.views.models.bst.utils"""

    template_name = SizedPaginator.template_name

    def render_template(self, sized_paginator: SizedPaginator):
        page_obj = sized_paginator.get_page(sized_paginator.cur_page)
        context = {"page_obj": page_obj}
        return render_to_string(self.template_name, context)

    def get_massaged_template_str(self, template_str: str):
        lines = template_str.splitlines()
        non_empty_lines = [f"{line}\n" for line in lines if line.strip()]
        return "".join(non_empty_lines)

    def assert_substrings(self, expected_substrings: list, template_str: str):
        for expected in expected_substrings:
            # assertIn has really ugly failure output.  assertTrue with msg set is better
            self.assertTrue(
                expected in template_str,
                msg=f"'{expected}' not found in:\n{self.get_massaged_template_str(template_str)}",
            )

    def assert_substrings_in_order(self, expected_substrings: list, template_str: str):
        pos = 0
        for i, expected in enumerate(expected_substrings):
            try:
                pos = template_str[pos:].index(expected)
            except ValueError:
                if expected not in template_str:
                    # assertIn has really ugly failure output.  assertTrue with msg set is better
                    self.assertTrue(
                        expected in template_str,
                        msg=(
                            f"Substring {i + 1} out of {len(expected_substrings)}: '{expected}' not found in the "
                            f"expected order in:\n{self.get_massaged_template_str(template_str)}"
                        ),
                    )

    def test_totals(self):
        """Tests to ensure that the total and raw total are present"""
        sp = SizedPaginator(
            2,  # total
            [1, 2],  # Fake object_list
            15,  # per_page
            raw_total=5,
        )
        template_str = self.render_template(sp)
        self.assert_substrings(
            ['of <span title="Unfiltered: 5">2</span>'], template_str
        )

    def test_size_select_presence(self):
        """Test that the size select list is only present when there are more than the smallest page size."""
        sp1 = SizedPaginator(
            2,  # total
            [1, 2],  # object_list
            15,  # per_page
        )
        template_str = self.render_template(sp1)
        self.assertNotIn('class="page-list"', template_str)

        # Size select list present
        sp2 = SizedPaginator(
            2,  # total
            [1, 2],  # object_list
            1,  # per_page
        )
        template_str = self.render_template(sp2)
        self.assert_substrings(['class="page-list"'], template_str)

    def test_start_stop_info(self):
        """test that the bounds of the visible records are present"""
        sp = SizedPaginator(
            99,  # total
            [i for i in range(1, 100)],  # object_list (99 of them)
            10,  # per_page
            page=3,
        )
        template_str = self.render_template(sp)
        self.assert_substrings(["Showing 21", "to 30"], template_str)

    def test_page_controls_presence(self):
        """Test that the pagination controls are present when there are multiple pages"""
        sp = SizedPaginator(
            2,  # total
            [1, 2],  # object_list
            1,  # per_page
        )
        template_str = self.render_template(sp)
        self.assert_substrings(["Pagination Control"], template_str)

    def test_next_prev(self):
        """Test that the rev/next controls are present when needed."""
        # Test next
        sp = SizedPaginator(
            2,  # total
            [1, 2],  # object_list
            1,  # per_page
            page=1,  # cur_page
        )
        template_str = self.render_template(sp)
        self.assert_substrings(['<a href="?page=2&limit=1"', "&raquo;"], template_str)

        # Test prev
        sp = SizedPaginator(
            2,  # total
            [1, 2],  # object_list
            1,  # per_page
            page=2,  # cur_page
        )
        template_str = self.render_template(sp)
        # Cur page is 2, so there should be a link to page 1
        self.assert_substrings(['<a href="?page=1&limit=1"', "&laquo;"], template_str)

    def test_first_last(self):
        """Test that the first/last page controls are present when needed."""
        sp = SizedPaginator(
            9,  # total
            [i for i in range(1, 10)],  # object_list (9 of them)
            1,  # per_page
            page=5,
        )
        template_str = self.render_template(sp)
        self.assert_substrings(
            [
                '<a href="?page=1&limit=1"',
                '<a href="?page=9&limit=1"',
            ],
            template_str,
        )

    def test_ellipses(self):
        """Test that the ellipses controls are present when needed."""
        # Assert no ellipses when no gaps in the flank
        sp = SizedPaginator(
            9,  # total
            [i for i in range(1, 10)],  # object_list (9 of them)
            1,  # per_page
            page=5,
        )
        template_str = self.render_template(sp)
        self.assertNotIn("&hellip;", template_str)

        # Assert both ellipses when 2 gaps
        sp = SizedPaginator(
            11,  # total
            [i for i in range(1, 12)],  # object_list (11 of them)
            1,  # per_page
            page=6,
        )
        template_str = self.render_template(sp)
        self.assert_substrings_in_order(
            [
                "&hellip;",
                "&hellip;",
            ],
            template_str,
        )

    def test_current_flank(self):
        """Test that the current page is denoted and that the flanking page controls are present when needed."""
        sp = SizedPaginator(
            11,  # total
            [i for i in range(1, 12)],  # object_list (11 of them)
            1,  # per_page
            page=6,
        )

        template_str = self.render_template(sp)

        self.assertNotIn('<a href="?page=2&limit=1"', template_str)
        self.assert_substrings_in_order(
            [
                '<a href="?page=3&limit=1"',
                '<a href="?page=4&limit=1"',
                '<a href="?page=5&limit=1"',
                "<span>6 <span",
                '<a href="?page=7&limit=1"',
                '<a href="?page=8&limit=1"',
                '<a href="?page=9&limit=1"',
            ],
            template_str,
        )
        self.assertNotIn('<a href="?page=10&limit=1"', template_str)
