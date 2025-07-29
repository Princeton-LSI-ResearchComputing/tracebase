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


class ValueTemplateTests(BaseTemplateTests):
    """Test that BST value template renders correctly."""

    value_template = "models/bst/value.html"
    value_list_template = "models/bst/value_list.html"

    def render_value_template(self, context):
        return render_to_string(self.value_template, context)

    def render_value_list_template(self, context):
        return render_to_string(self.value_list_template, context)

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
        html = self.render_value_template(context).strip()
        self.assertEqual("t1", html)

    def test_bst_column_field(self):
        namecol = BSTColumn("name", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": namecol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertEqual("A1", html)

    def test_bst_column_object(self):
        namecol = BSTColumn("treatment", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": namecol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertEqual('<a href="thisisaurl">T1</a>', html)

    def test_bst_related_column_field(self):
        treatdesccol = BSTRelatedColumn("treatment__desc", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": treatdesccol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertEqual("t1", html)

    def test_bst_related_column_object(self):
        treatcol = BSTRelatedColumn("treatment", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": treatcol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertEqual('<a href="thisisaurl">T1</a>', html)

    @override_settings(DEBUG=True)
    def test_bst_many_related_column_field(self):
        studydesccol = BSTManyRelatedColumn("studies__desc", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A2").first()
        studydesccol.set_list_attr(rec, [s.desc for s in rec.studies.all()])
        context = {
            "column": studydesccol,
            "object": rec,
        }
        # Ignoring whitespace from the template code...
        html = (
            self.render_value_list_template(context)
            .strip()
            .replace("\n", "")
            .replace("  ", "")
        )
        # NOTE: The descending order here is due to the manual subrecs query and the model's ordering.
        # In BSTListView, applying the column's ordering happens via the get_user_queryset.
        self.assertEqual('s2; <br class="cell-wrap">s1', html)

    @override_settings(DEBUG=True)
    def test_bst_many_related_column_object(self):
        studycol = BSTManyRelatedColumn("studies", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A2").first()
        studycol.set_list_attr(rec, [s for s in rec.studies.all()])
        context = {
            "column": studycol,
            "object": rec,
        }
        # Ignoring whitespace from the template code...
        html = (
            self.render_value_list_template(context)
            .strip()
            .replace("\n", "")
            .replace("  ", "")
        )
        # This avoids matching the primary key, which is not durable from test to test
        self.assertIn("BTTStudyTestModel object (", html)
        self.assertIn('); <br class="cell-wrap">BTTStudyTestModel object (', html)
