from contextlib import contextmanager
from io import StringIO
from unittest.mock import patch

from django.http import HttpRequest
from django.template.loader import render_to_string

from DataRepo.tests.templates.models.bst.base_template_test import (
    BaseTemplateTests,
    BTTStudyTestModel,
)
from DataRepo.views.models.bst.export import BSTExportedListView
from DataRepo.views.models.bst.exporters.base import BSTExportView


class StudyELV(BSTExportedListView):
    model = BTTStudyTestModel


class StudyBSTCSVExportView(BSTExportView):
    name = "CSV"
    content_type = "text/csv"
    buffer_class = StringIO
    extension = "csv"
    view_name = "tsv_exp_list_view"

    def buffer_file(self, source_view: BSTExportedListView, header_content: str):
        pass


@contextmanager
def patch_exporter_classes(*exporter_classes):
    with patch.object(
        BSTExportView,
        "get_exporter_classes",
        return_value=list(exporter_classes),
    ):
        yield


class BSTExportViewTemplateTests(BaseTemplateTests):

    export_view_template = "models/bst/download_metadata_header.txt"

    def render_export_header_view_template(
        self, view: BSTExportView, source_view: BSTExportedListView
    ):
        source_view.object_list = source_view.get_queryset()[:]
        context = view.get_header_context(source_view)
        if hasattr(view, "request"):
            # Simulate a request
            context["request"] = view.request
        return render_to_string(self.export_view_template, context)

    def assert_substrings(self, expected_substrings: list, template_str: str):
        for expected in expected_substrings:
            # assertIn has really ugly failure output.  assertTrue with msg set is better
            self.assertTrue(
                expected in template_str,
                msg=f"'{expected}' not found in:\n{template_str}",
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
                            f"expected order in:\n{template_str}"
                        ),
                    )

    def test_header_render_full(self):
        request = HttpRequest()

        # Explicit descending sort
        request.COOKIES.update(
            {
                f"{StudyELV.__name__}-{StudyELV.sortcol_cookie_name}": "name",
                f"{StudyELV.__name__}-{StudyELV.asc_cookie_name}": "true",
                f"{StudyELV.__name__}-{StudyELV.search_cookie_name}": "S1",
                f"{StudyELV.__name__}-{StudyELV.filter_cookie_name}-name": "S1",
            }
        )

        with patch_exporter_classes(StudyBSTCSVExportView):
            source_view = StudyELV(request=request)
            source_view.init_interface()

        view = StudyBSTCSVExportView()
        view.init_export(source_view)

        template_str = self.render_export_header_view_template(view, source_view)

        expected_ordered_substrings = [
            "# BTT Study Test Models",
            "# Time: ",
            "# Rows: 1",
            "# Global Search Term: 'S1'",
            "# Column Filters: ",
            "#   name: 'S1'",
            "# Sort: name, ascending",
        ]
        self.assert_substrings_in_order(expected_ordered_substrings, template_str)

    def test_header_render_empty(self):
        request = HttpRequest()

        with patch_exporter_classes(StudyBSTCSVExportView):
            source_view = StudyELV(request=request)
            source_view.init_interface()

        view = StudyBSTCSVExportView()
        view.init_export(source_view)

        template_str = self.render_export_header_view_template(view, source_view)

        expected_ordered_substrings = [
            "# BTT Study Test Models",
            "# Time: ",
            "# Rows: 2",
            "# Global Search Term: None",
            "# Column Filters: None",
            "# Sort: name, ascending",  # Default
        ]
        self.assert_substrings_in_order(expected_ordered_substrings, template_str)
