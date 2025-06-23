from django.db.models import CharField
from django.db.models.functions import Lower
from django.template.loader import render_to_string
from django.test import override_settings

from DataRepo.tests.templates.models.bst.base_template_test import (
    BaseTemplateTests,
    BTTAnimalTestModel,
)
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.field import BSTColumn
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn


class TdTemplateTests(BaseTemplateTests):
    """Test that BST value template renders correctly."""

    td_template = "models/bst/td.html"

    def render_td_template(self, context):
        return render_to_string(self.td_template, context)

    def test_bst_annot_column(self):
        lowtreatcol = BSTAnnotColumn(
            "lowername", Lower("treatment__name", output_field=CharField())
        )
        annots = {lowtreatcol.name: lowtreatcol.converter}
        rec = BTTAnimalTestModel.objects.annotate(**annots).get(name="A1")
        context = {
            "column": lowtreatcol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_td_template(context).strip()
        self.assertIn('<td class="table-cell-nobr">', html)
        self.assertIn("t1", html)

    def test_bst_column_field(self):
        namecol = BSTColumn("name", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": namecol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_td_template(context).strip()
        self.assertIn('<td class="table-cell-nobr">', html)
        self.assertIn("A1", html)

    def test_bst_column_object(self):
        namecol = BSTColumn("treatment", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": namecol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_td_template(context).strip()
        self.assertIn('<a href="thisisaurl">T1</a>', html)
        self.assertIn('<td class="table-cell-nobr">', html)

    def test_bst_related_column_field(self):
        treatdesccol = BSTRelatedColumn("treatment__desc", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": treatdesccol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_td_template(context).strip()
        self.assertIn('<td class="table-cell-nobr">', html)
        self.assertIn("t1", html)

    def test_bst_related_column_object(self):
        treatcol = BSTRelatedColumn("treatment", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": treatcol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_td_template(context).strip()
        self.assertIn('<td class="table-cell-nobr">', html)
        self.assertIn('<a href="thisisaurl">T1</a>', html)

    @override_settings(DEBUG=True)
    def test_bst_many_related_column_field(self):
        studydesccol = BSTManyRelatedColumn("studies__desc", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A2").first()
        studydesccol.set_list_attr(rec, [s.desc for s in rec.studies.all()])
        context = {
            "column": studydesccol,
            "object": rec,
        }
        self.assertEqual("models/bst/value_list.html", studydesccol.value_template)
        # Ignoring whitespace from the template code...
        html = (
            self.render_td_template(context).strip().replace("\n", "").replace("  ", "")
        )
        # NOTE: The descending order here is due to the manual subrecs query and the model's ordering.
        # In BSTListView, applying the column's ordering happens via the get_user_queryset.
        self.assertIn('<td class="table-cell-nobr">', html)
        self.assertIn('>s2; </span><br class="cell-wrap"><span class="nobr">s1', html)

    @override_settings(DEBUG=True)
    def test_bst_many_related_column_object(self):
        studycol = BSTManyRelatedColumn("studies", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A2").first()
        studycol.set_list_attr(rec, [s for s in rec.studies.all()])
        context = {
            "column": studycol,
            "object": rec,
        }
        self.assertEqual("models/bst/value_list.html", studycol.value_template)
        # Ignoring whitespace from the template code...
        html = (
            self.render_td_template(context).strip().replace("\n", "").replace("  ", "")
        )
        # NOTE: The descending order here is due to the manual subrecs query and the model's ordering.
        # In BSTListView, applying the column's ordering happens via the get_user_queryset.
        self.assertIn('<td class="table-cell-nobr">', html)
        # This avoids matching the primary key, which is not durable from test to test
        self.assertIn("BTTStudyTestModel object (", html)
        self.assertIn(
            '); </span><br class="cell-wrap"><span class="nobr">BTTStudyTestModel object (',
            html,
        )
