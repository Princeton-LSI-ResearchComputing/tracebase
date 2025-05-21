from django.db.models import CASCADE, CharField, ForeignKey, ManyToManyField
from django.db.models.functions import Lower
from django.template.loader import render_to_string
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.field import BSTColumn
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn

VTTStudyTestModel = create_test_model(
    "VTTStudyTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": [Lower("name").desc()]},
        ),
    },
)

VTTAnimalTestModel = create_test_model(
    "VTTAnimalTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
        "studies": ManyToManyField(
            to="loader.VTTStudyTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.VTTTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
        ),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": ["-name"]},
        ),
    },
)

VTTTreatmentTestModel = create_test_model(
    "VTTTreatmentTestModel",
    {"name": CharField(unique=True), "desc": CharField()},
    attrs={
        "get_absolute_url": lambda _: "thisisaurl",
        "__str__": lambda slf: slf.name,
    },
)


class ValueTemplateTests(TracebaseTestCase):
    """Test that BST value template renders correctly."""

    value_template = "models/bst/value.html"
    value_list_template = "models/bst/value_list.html"

    @classmethod
    def setUpTestData(cls):
        cls.t1 = VTTTreatmentTestModel.objects.create(name="T1", desc="t1")
        cls.t2 = VTTTreatmentTestModel.objects.create(name="oddball", desc="t2")
        cls.s1 = VTTStudyTestModel.objects.create(name="S1", desc="s1")
        cls.s2 = VTTStudyTestModel.objects.create(name="S2", desc="s2")
        cls.a1 = VTTAnimalTestModel.objects.create(
            name="A1", desc="a1", treatment=cls.t1
        )
        cls.a1.studies.add(cls.s1)
        cls.a2 = VTTAnimalTestModel.objects.create(
            name="A2", desc="a2", treatment=cls.t2
        )
        cls.a2.studies.add(cls.s1)
        cls.a2.studies.add(cls.s2)
        super().setUpTestData()

    def render_value_template(self, context):
        return render_to_string(self.value_template, context)

    def render_value_list_template(self, context):
        return render_to_string(self.value_list_template, context)

    def test_bst_annot_column(self):
        lowtreatcol = BSTAnnotColumn(
            "lowername", Lower("treatment__name", output_field=CharField())
        )
        annots = {lowtreatcol.name: lowtreatcol.converter}
        rec = VTTAnimalTestModel.objects.annotate(**annots).get(name="A1")
        context = {
            "column": lowtreatcol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertEqual("t1", html)

    def test_bst_column_field(self):
        namecol = BSTColumn("name", VTTAnimalTestModel)
        rec = VTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": namecol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertEqual("A1", html)

    def test_bst_column_object(self):
        namecol = BSTColumn("treatment", VTTAnimalTestModel)
        rec = VTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": namecol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertEqual('<a href="thisisaurl">T1</a>', html)

    def test_bst_related_column_field(self):
        treatdesccol = BSTRelatedColumn("treatment__desc", VTTAnimalTestModel)
        rec = VTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": treatdesccol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertEqual("t1", html)

    def test_bst_related_column_object(self):
        treatcol = BSTRelatedColumn("treatment", VTTAnimalTestModel)
        rec = VTTAnimalTestModel.objects.filter(name="A1").first()
        context = {
            "column": treatcol,
            "object": rec,
        }
        # Ignoring leading/trailing whitespace characters from the template code...
        html = self.render_value_template(context).strip()
        self.assertEqual('<a href="thisisaurl">T1</a>', html)

    @override_settings(DEBUG=True)
    def test_bst_many_related_column_field(self):
        studydesccol = BSTManyRelatedColumn("studies__desc", VTTAnimalTestModel)
        rec = VTTAnimalTestModel.objects.filter(name="A2").first()
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
        studycol = BSTManyRelatedColumn("studies", VTTAnimalTestModel)
        rec = VTTAnimalTestModel.objects.filter(name="A2").first()
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
        # NOTE: The descending order here is due to the manual subrecs query and the model's ordering.
        # In BSTListView, applying the column's ordering happens via the get_user_queryset.
        self.assertEqual(
            'VTTStudyTestModel object (2); <br class="cell-wrap">VTTStudyTestModel object (1)',
            html,
        )
