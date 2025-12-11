from django.template.loader import render_to_string

from DataRepo.models.compound import Compound
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst.column.field import BSTColumn


class HMDBIDTemplateTests(TracebaseTestCase):

    value_template = "models/compound/hmdb_id.html"

    def get_massaged_template_str(self, template_str: str):
        """Removes empty lines (containing only whitespace)"""
        lines = template_str.splitlines()
        non_empty_lines = [f"{line}\n" for line in lines if line.strip()]
        return "".join(non_empty_lines)

    def render_value_template(self, context):
        return self.get_massaged_template_str(
            render_to_string(self.value_template, context)
        )

    def test_fake_hmdb_id_hidden(self):
        hmbd_col = BSTColumn("hmdb_id", Compound)

        real = Compound.objects.create(
            name="1-Methylhistidine", formula="C7H11N3O2", hmdb_id="HMDB0000001"
        )
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template({"column": hmbd_col, "object": real}).strip()
        self.assertIn("HMDB0000001", html)
        self.assertIn("<a", html)
        self.assertNotIn("None", html)

        fake = Compound.objects.create(
            name="Some Random Compound", formula="C2H6O", hmdb_id="FakeHMDB001"
        )
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template({"column": hmbd_col, "object": fake}).strip()
        self.assertIn("None", html)
        self.assertNotIn("FakeHMDB001", html)
        self.assertNotIn("<a", html)
