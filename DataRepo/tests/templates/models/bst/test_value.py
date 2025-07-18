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

    def get_massaged_template_str(self, template_str: str):
        """Removes empty lines (containing only whitespace)"""
        lines = template_str.splitlines()
        non_empty_lines = [f"{line}\n" for line in lines if line.strip()]
        return "".join(non_empty_lines)

    def render_value_template(self, context):
        return self.get_massaged_template_str(
            render_to_string(self.value_template, context)
        )

    def render_value_list_template(self, context):
        return self.get_massaged_template_str(
            render_to_string(self.value_list_template, context)
        )

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
        self.assertIn('<span class="nobr">', html)
        self.assertIn("t1", html)

    def test_bst_column_field(self):
        namecol = BSTColumn("name", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "object": rec,
            "column": namecol,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertIn('<span class="nobr">', html)
        self.assertIn("A1", html)

    def test_bst_column_object(self):
        # Test that a representative field is used
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        namecol = BSTRelatedColumn("treatment", BTTAnimalTestModel)
        context = {
            "column": namecol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertIn('<span class="nobr">', html)
        self.assertIn('<a href="thisisaurl">T1</a>', html)

        # Test that __str__ method without a get_absolute_url method is used
        rec2 = BTTAnimalTestModel.objects.filter(name="A2").first()
        housecol = BSTRelatedColumn("housing", BTTAnimalTestModel)
        context2 = {
            "column": housecol,
            "object": rec2,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html2 = self.render_value_template(context2).strip()
        self.assertIn('<span class="nobr">', html2)
        self.assertIn("BTTHousingTestModel object (", html2)
        self.assertNotIn("<a href=", html2)

    def test_bst_related_column_field(self):
        treatdesccol = BSTRelatedColumn("treatment__desc", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": treatdesccol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertIn('<span class="nobr">', html)
        self.assertIn("t1", html)

    def test_bst_related_column_object(self):
        treatcol = BSTRelatedColumn("treatment", BTTAnimalTestModel)
        rec = BTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": treatcol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertIn('<span class="nobr">', html)
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
        # Ignoring whitespace from the template code...
        html = (
            self.render_value_list_template(context)
            .strip()
            .replace("\n", "")
            .replace("  ", "")
        )
        # NOTE: The descending order here is due to the manual subrecs query and the model's ordering.
        # In BSTListView, applying the column's ordering happens via the get_user_queryset.
        self.assertEqual(
            '<span class="nobr">s2; </span><br class="cell-wrap"><span class="nobr">s1</span>',
            html,
        )

    @override_settings(DEBUG=True)
    def test_bst_many_related_column_object(self):
        rec = BTTAnimalTestModel.objects.filter(name="A2").first()

        # Test that the display_field is used, i.e. instead of the default __str__ generated value, e.g.:
        # 'BTTStudyTestModel object (1)', i.e. the Study model has a unique name field, which becomes the representative
        studycol = BSTManyRelatedColumn("studies", BTTAnimalTestModel)
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
        self.assertEqual(
            '<span class="nobr">S2; </span><br class="cell-wrap"><span class="nobr">S1</span>',
            html,
        )

        # Test that a default __str__ is used when there's no representative display field (i.e. the friend model has no
        # unique field).
        friendcol = BSTManyRelatedColumn("friends", BTTAnimalTestModel)
        friendcol.set_list_attr(rec, [s for s in rec.friends.all()])
        context2 = {
            "column": friendcol,
            "object": rec,
        }
        # Ignoring whitespace from the template code...
        html2 = (
            self.render_value_list_template(context2)
            .strip()
            .replace("\n", "")
            .replace("  ", "")
        )
        # The primary key is not durable, so we avoid matching the ID, e.g. the '1' in 'BTTFriendTestModel object (1)'
        self.assertIn('<span class="nobr">BTTFriendTestModel object (', html2)
