from django.db.models import CharField
from django.db.models.functions import Lower
from django.template.loader import render_to_string

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.filterer.annotation import (
    BSTAnnotFilterer,
)


class ThTemplateTests(TracebaseTestCase):
    """Test that BST th template renders correctly.

    Uses BSTAnnotColumn, because the features are the same but no model is required.
    """

    th_template = "models/bst/th.html"

    def render_th_template(self, column):
        # Ignoring leading/trailing whitespace characters from the template code...
        return render_to_string(self.th_template, {"column": column}).strip()

    def test_th_basic(self):
        col = BSTAnnotColumn("colname", Lower("name", output_field=CharField()))
        html = self.render_th_template(col)
        self.assertIn("<th", html)
        self.assertIn('data-field="colname"', html)
        self.assertIn('data-valign="top"', html)
        self.assertIn('data-filter-control="input"', html)
        self.assertIn('data-filter-custom-search="djangoFilterer"', html)
        self.assertNotIn("data-filter-data", html)
        self.assertNotIn("data-filter-default", html)
        self.assertIn('data-sortable="true"', html)
        self.assertIn('data-sorter="djangoSorter"', html)
        self.assertIn('data-visible="true"', html)
        self.assertIn("Colname", html)

    def test_th_booleans(self):
        col = BSTAnnotColumn(
            "colname",
            Lower("name", output_field=CharField()),
            searchable=False,
            sortable=False,
            visible=False,
        )
        html = self.render_th_template(col)
        self.assertNotIn("data-filter-control", html)
        self.assertNotIn("data-filter-custom-search", html)
        self.assertNotIn("data-filter-data", html)
        self.assertNotIn("data-filter-default", html)
        self.assertNotIn("data-sortable", html)
        self.assertNotIn("data-sorter", html)
        self.assertIn('data-visible="false"', html)

    def test_th_client_filterer(self):
        col = BSTAnnotColumn(
            "colname",
            Lower("name", output_field=CharField()),
            filterer="someCustomFilterer",
        )
        html = self.render_th_template(col)
        # TODO: Implement the client-mode concept just in javascript.  See #1561
        # Then change this to test that it sets "someCustomFilterer"
        self.assertIn('data-filter-custom-search="djangoFilterer"', html)

    def test_th_input_method_select(self):
        col = BSTAnnotColumn(
            "colname",
            Lower("name", output_field=CharField()),
            filterer=BSTAnnotFilterer(
                "colname",
                input_method=BSTAnnotFilterer.INPUT_METHODS.SELECT,
                choices={"1": "1", "2": "2"},
            ),
        )
        html = self.render_th_template(col)
        self.assertIn('data-filter-control="select"', html)
        self.assertIn(
            'data-filter-data="json:{&quot;1&quot;: &quot;1&quot;, &quot;2&quot;: &quot;2&quot;}"',
            html,
        )

    def test_th_initial_filter(self):
        col = BSTAnnotColumn(
            "colname",
            Lower("name", output_field=CharField()),
            filterer=BSTAnnotFilterer("colname", initial="searchterm"),
        )
        html = self.render_th_template(col)
        self.assertIn('data-filter-default="searchterm"', html)
