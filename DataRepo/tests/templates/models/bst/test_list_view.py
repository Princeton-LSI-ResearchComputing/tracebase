from django.db.models import CharField
from django.db.models.functions import Upper
from django.http import HttpRequest
from django.template.loader import render_to_string

from DataRepo.tests.templates.models.bst.base_template_test import (
    BaseTemplateTests,
    BTTAnimalTestModel,
    BTTStudyTestModel,
)
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.many_related_group import BSTColumnGroup
from DataRepo.views.models.bst.list_view import BSTListView


class StudyLV(BSTListView):
    model = BTTStudyTestModel
    annotations = {"description": Upper("desc", output_field=CharField())}
    exclude = ["id", "desc"]


class AnimalWithMultipleStudyColsLV(BSTListView):
    model = BTTAnimalTestModel
    column_ordering = ["name", "desc", "treatment", "studies__name", "studies__desc"]
    exclude = ["id", "studies"]

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            columns=[
                BSTColumnGroup(
                    BSTManyRelatedColumn(
                        "studies__name", AnimalWithMultipleStudyColsLV.model
                    ),
                    BSTManyRelatedColumn(
                        "studies__desc", AnimalWithMultipleStudyColsLV.model
                    ),
                ),
            ],
            **kwargs,
        )


class BSTListViewTests(BaseTemplateTests):

    list_view_template = "models/bst/list_view.html"

    def render_list_view_template(self, view: BSTListView):
        view.object_list = view.get_queryset()[:]
        context = view.get_context_data()
        return render_to_string(self.list_view_template, context)

    def get_massaged_template_str(self, template_str: str):
        pos = template_str.index('<div class="container-fluid main-content">')
        lines = template_str[pos:].splitlines()
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

    def test_script_imports(self):
        request = HttpRequest()
        slv = StudyLV(request=request)
        template_str = self.render_list_view_template(slv)
        expected_substrings = [
            '<script id="warnings" type="application/json">[]</script>',
            '<script id="cookie_resets" type="application/json">[]</script>',
            '<script src="js/bst/sorter.js"></script>',
            '<script src="js/bst/filterer.js"></script>',
        ]
        self.assert_substrings(expected_substrings, template_str)

    def test_warnings(self):
        request = HttpRequest()
        slv = StudyLV(request=request)
        slv.warnings.append("THIS IS A WARNING")
        slv.warnings.append("THIS IS A SECOND WARNING")
        template_str = self.render_list_view_template(slv)
        expected = (
            '<script id="warnings" type="application/json">'
            '["THIS IS A WARNING", "THIS IS A SECOND WARNING"]'
            "</script>"
        )
        self.assertTrue(
            expected in template_str,
            msg=f"'{expected}' not found in:\n{self.get_massaged_template_str(template_str)}",
        )

    def test_cookie_resets(self):
        request = HttpRequest()
        slv = StudyLV(request=request)
        slv.cookie_resets.append("test")
        template_str = self.render_list_view_template(slv)
        expected = (
            '<script id="cookie_resets" type="application/json">["test"]</script>'
        )
        self.assertTrue(
            expected in template_str,
            msg=f"'{expected}' not found in:\n{self.get_massaged_template_str(template_str)}",
        )

    def test_id_title(self):
        request = HttpRequest()
        slv = StudyLV(request=request)
        template_str = self.render_list_view_template(slv)
        expected_substrings = [
            "<h4>BTT Study Test Models</h4>",
            'id="StudyLV"',
        ]
        self.assert_substrings(expected_substrings, template_str)

    def test_default_table_attributes(self):
        request = HttpRequest()
        slv = StudyLV(request=request)
        template_str = self.render_list_view_template(slv)
        expected_substrings = [
            '<table class="table table-sm table-hover table-bordered table-responsive-xl table-striped"',
            'data-toggle="table"',
            'data-buttons-align="left"',
            'data-buttons-class="primary"',
            'data-buttons="customButtonsFunction"',
            "data-export-types=\"['csv', 'txt', 'excel']\"",
            'data-export-data-type="all"',
            'data-filter-control="true"',
            'data-search="true"',
            'data-search-align="left"',
            'data-search-on-enter-key="true"',
            'data-show-search-clear-button="true"',
            'data-show-multi-sort="true"',
            'data-show-columns="true"',
            'data-show-columns-toggle-all="true"',
            'data-show-fullscreen="true"',
            'data-show-export="false">',
        ]
        self.assert_substrings(expected_substrings, template_str)
        unexpected_substrings = [
            'data-search-text=""',
            'data-sort-name="None"',
            'data-sort-order="asc"',
        ]
        for unexpected in unexpected_substrings:
            # assertNotIn has really ugly failure output.  assertFalse with msg set is better
            self.assertFalse(
                unexpected in template_str,
                msg=f"Unexpectedly found '{unexpected}' in:\n{self.get_massaged_template_str(template_str)}",
            )

    def test_search(self):
        request = HttpRequest()
        request.COOKIES.update(
            {f"{StudyLV.__name__}-{StudyLV.search_cookie_name}": "test"}
        )
        slv = StudyLV(request=request)
        template_str = self.render_list_view_template(slv)
        expected = 'data-search-text="test"'
        # assertIn has really ugly failure output.  assertTrue with msg set is better
        self.assertTrue(
            expected in template_str,
            msg=f"'{expected}' not found in:\n{self.get_massaged_template_str(template_str)}",
        )

    def test_column_order(self):
        request = HttpRequest()

        slv = StudyLV(request=request)
        template_str = self.render_list_view_template(slv)
        expected_ordered_substrings = [
            '<th data-field="name"',
            '<th data-field="animals_mm_count"',
            '<th data-field="animals"',
            '<th data-field="description"',
            # NOTE: The default row order is determined by the model's ordering (which is descending)
            '<td class="table-cell">',
            "S2",
            "</td>",
            '<td class="table-cell">',
            "1",
            "</td>",
            '<td class="table-cell">',
            "BTTAnimalTestModel object (2)",
            "</td>",
            '<td class="table-cell">',
            "S2",
            "</td>",
            "</tr>",
            "<tr>",
            '<td class="table-cell">',
            "S1",
            "</td>",
            '<td class="table-cell">',
            "2",
            "</td>",
            '<td class="table-cell">',
            'BTTAnimalTestModel object (1); <br class="cell-wrap">',
            "BTTAnimalTestModel object (2)",
            "</td>",
            '<td class="table-cell">',
            "S1",
            "</td>",
        ]
        self.assert_substrings_in_order(expected_ordered_substrings, template_str)

    def test_row_order(self):
        request = HttpRequest()

        # Default sort
        slv = StudyLV(request=request)
        template_str = self.render_list_view_template(slv)
        expected_ordered_substrings = [
            # NOTE: The default order is determined by the model's ordering (which is descending)
            "S2",
            "S1",
        ]
        self.assert_substrings_in_order(expected_ordered_substrings, template_str)

        # Explicit descending sort
        request.COOKIES.update(
            {
                f"{StudyLV.__name__}-{StudyLV.sortcol_cookie_name}": "name",
                f"{StudyLV.__name__}-{StudyLV.asc_cookie_name}": "false",
            }
        )
        slv1 = StudyLV(request=request)
        template_str = self.render_list_view_template(slv1)
        expected_substrings = [
            'data-sort-name="name"',
            'data-sort-order="desc"',
        ]
        self.assert_substrings(expected_substrings, template_str)
        expected_ordered_substrings = ["S2", "S1"]
        self.assert_substrings_in_order(expected_ordered_substrings, template_str)

        # Explicit ascending sort
        request.COOKIES.update(
            {
                f"{StudyLV.__name__}-{StudyLV.sortcol_cookie_name}": "name",
                f"{StudyLV.__name__}-{StudyLV.asc_cookie_name}": "true",
            }
        )
        slv2 = StudyLV(request=request)
        template_str = self.render_list_view_template(slv2)
        expected_substrings = [
            'data-sort-name="name"',
            'data-sort-order="asc"',
        ]
        self.assert_substrings(expected_substrings, template_str)
        expected_ordered_substrings = ["S1", "S2"]
        self.assert_substrings_in_order(expected_ordered_substrings, template_str)

        # Default ascending sort (derived from the model's ordering?)
        request.COOKIES = {f"{StudyLV.__name__}-{StudyLV.sortcol_cookie_name}": "name"}
        slv3 = StudyLV(request=request)
        template_str = self.render_list_view_template(slv3)
        expected_substrings = [
            'data-sort-name="name"',
            'data-sort-order="asc"',
        ]
        self.assert_substrings(expected_substrings, template_str)
        expected_ordered_substrings = ["S1", "S2"]
        self.assert_substrings_in_order(expected_ordered_substrings, template_str)

    def test_paginator_added(self):
        request = HttpRequest()
        slv1 = StudyLV(request=request)
        template_str = self.get_massaged_template_str(
            self.render_list_view_template(slv1)
        )
        self.assertIn("Start BSTListView Pagination", template_str)
