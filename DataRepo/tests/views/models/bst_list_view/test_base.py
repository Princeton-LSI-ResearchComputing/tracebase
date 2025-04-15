from django.db.models import (
    CASCADE,
    CharField,
    Count,
    ForeignKey,
    ManyToManyField,
)
from django.db.models.functions import Lower
from django.http import HttpRequest
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst_list_view.base import BSTBaseListView
from DataRepo.views.models.bst_list_view.column.annotation import (
    BSTAnnotColumn,
)
from DataRepo.views.models.bst_list_view.column.field import BSTColumn
from DataRepo.views.models.bst_list_view.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst_list_view.column.many_related_group import (
    BSTColumnGroup,
)
from DataRepo.views.models.bst_list_view.column.related_field import (
    BSTRelatedColumn,
)

BSTLVStudyTestModel = create_test_model(
    "BSTLVStudyTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": [Lower("-name")]},
        ),
    },
)

BSTLVAnimalTestModel = create_test_model(
    "BSTLVAnimalTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
        "studies": ManyToManyField(
            to="loader.BSTLVStudyTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.BSTLVTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
        ),
    },
)

BSTLVTreatmentTestModel = create_test_model(
    "BSTLVTreatmentTestModel",
    {"name": CharField(unique=True), "desc": CharField()},
)


class StudyLV(BSTBaseListView):
    model = BSTLVStudyTestModel


class AnimalLV(BSTBaseListView):
    model = BSTLVAnimalTestModel


class AnimalNoStudiesLV(BSTBaseListView):
    model = BSTLVAnimalTestModel
    exclude = ["id", "studies"]


@override_settings(DEBUG=True)
class BSTListViewTests(TracebaseTestCase):

    @TracebaseTestCase.assertNotWarns()
    def test_init_success_no_cookies(self):
        blv = BSTBaseListView()
        blv.request = HttpRequest()

        self.assertEqual([], blv.ordering)
        self.assertIsNone(blv.search)
        self.assertEqual({}, blv.filters)
        self.assertEqual({}, blv.visibles)
        self.assertIsNone(blv.sortcol)
        self.assertTrue(blv.asc)
        self.assertFalse(blv.ordered)
        self.assertEqual(0, blv.total)
        self.assertEqual(0, blv.raw_total)
        self.assertEqual(15, blv.limit)
        self.assertEqual({}, blv.column_settings)
        self.assertEqual([], blv.warnings)
        self.assertEqual({}, blv.columns)
        self.assertEqual({}, blv.groups)

    @TracebaseTestCase.assertNotWarns()
    def test_init_success_cookies(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                "StudyLV-visible-name": "true",
                "StudyLV-visible-desc": "false",
                "StudyLV-filter-name": "",
                "StudyLV-filter-desc": "description",
                "StudyLV-search": "",
                "StudyLV-sortcol": "name",
                "StudyLV-asc": "false",
            }
        )
        request.GET.update({"limit": "20"})
        blv = StudyLV(request=request)

        self.assertEqual([Lower("-name")], blv.ordering)
        self.assertIsNone(blv.search)
        self.assertEqual({"desc": "description"}, blv.filters)
        self.assertEqual({"name": True, "desc": False}, blv.visibles)
        self.assertEqual("name", blv.sortcol)
        self.assertFalse(blv.asc)
        self.assertTrue(blv.ordered)
        self.assertEqual(20, blv.limit)
        self.assertEqual(0, blv.total)
        self.assertEqual(0, blv.raw_total)
        self.assertEqual([], blv.warnings)
        self.assertEqual({}, blv.column_settings)
        self.assertEqual(
            {
                "name": BSTColumn("name", BSTLVStudyTestModel),
                "desc": BSTColumn("desc", BSTLVStudyTestModel),
                "animals": BSTManyRelatedColumn("animals", BSTLVStudyTestModel),
            },
            blv.columns,
        )
        self.assertEqual({}, blv.groups)

    @TracebaseTestCase.assertNotWarns()
    def test_init_warnings(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                "StudyLV-visible-name": "true",
                "StudyLV-visible-desc": "wrong",
                "StudyLV-filter-stale": "description",
            }
        )
        with self.assertWarns(DeveloperWarning):
            blv = StudyLV(request=request)

        self.assertEqual({"stale": "description"}, blv.filters)
        self.assertEqual({"name": True}, blv.visibles)
        self.assertTrue(blv.asc)
        self.assertFalse(blv.ordered)
        self.assertEqual(15, blv.limit)
        self.assertEqual(0, blv.total)
        self.assertEqual(0, blv.raw_total)
        self.assertEqual(
            [
                "Invalid 'visible' cookie value encountered for column 'desc': 'wrong'.  "
                "Clearing cookie 'StudyLV-visible-desc'."
            ],
            blv.warnings,
        )
        self.assertEqual(["StudyLV-visible-desc"], blv.cookie_resets)

    def test_model_title_plural(self):
        self.assertEqual("BSTLV Study Test Models", StudyLV.model_title_plural)

    def test_model_title(self):
        self.assertEqual("BSTLV Study Test Model", StudyLV.model_title)

    def test_init_column_settings_list_supplied_for_columns(self):
        blv = BSTBaseListView()

        with self.assertRaises(TypeError) as ar:
            # not str, dict, BSTBaseColumn, or BSTColumnGroup -> TypeError
            blv.init_column_settings([1])
        self.assertIn(
            "When supplying a list of all columns' settings", str(ar.exception)
        )
        self.assertIn(
            "value's type must be one of [str, dict, BSTBaseColumn, or BSTColumnGroup]",
            str(ar.exception),
        )
        self.assertIn(
            "value of the column settings at index '0' was 'int'", str(ar.exception)
        )

        blv.init_column_settings(
            [
                "field1",  # str -> "field1": {}
                # dict with field_path -> self.column_settings["field2"]: {}
                {"field_path": "field2"},
                # dict with name, model and visible -> self.column_settings["field3"]: {"visible": False}
                {"name": "field3", "model": BSTLVStudyTestModel, "visible": False},
                BSTColumn(
                    "name", BSTLVStudyTestModel
                ),  # BSTBaseColumn -> self.column_settings[colobj.name]: colobj
                BSTColumnGroup(  # BSTColumnGroup -> self.column_settings[colobj.name] = colobj
                    BSTManyRelatedColumn("animals__name", BSTLVStudyTestModel),
                    BSTManyRelatedColumn("animals__desc", BSTLVStudyTestModel),
                ),
            ],
        )
        self.assertEqual(
            set(["field1", "field2", "field3", "name", "animals_group"]),
            set(blv.column_settings.keys()),
        )
        self.assertEqual({}, blv.column_settings["field1"])
        self.assertEqual({}, blv.column_settings["field2"])
        self.assertEqual({"visible": False}, blv.column_settings["field3"])
        self.assertEqual(
            BSTColumn("name", BSTLVStudyTestModel), blv.column_settings["name"]
        )
        self.assertIsInstance(blv.column_settings["animals_group"], BSTColumnGroup)

    def test_init_column_settings_dict_supplied_for_columns(self):
        blv = BSTBaseListView()

        with self.assertRaises(TypeError) as ar:
            # not str, dict, BSTBaseColumn, or BSTColumnGroup -> TypeError
            blv.init_column_settings({"field1": 1})
        self.assertIn(
            "When supplying a dict of all columns' settings", str(ar.exception)
        )
        self.assertIn(
            "type must be one of [str, dict, BSTBaseColumn, or BSTColumnGroup]",
            str(ar.exception),
        )
        self.assertIn(
            "value of the column settings at key 'field1' was 'int'.", str(ar.exception)
        )

        with self.assertRaises(ValueError) as ar:
            # not str, dict, BSTBaseColumn, or BSTColumnGroup -> TypeError
            blv.init_column_settings({"field1": "otherfield"})
        self.assertIn("The column settings key 'field1'", str(ar.exception))
        self.assertIn(
            "must be identical to the field_path string provided 'otherfield'",
            str(ar.exception),
        )

        blv.init_column_settings(
            {
                "field1": "field1",  # str -> "field1": {}
                # dict with field_path -> self.column_settings["field2"]: {}
                "field2": {"field_path": "field2"},
                # dict with name, model and visible -> self.column_settings["field3"]: {"visible": False}
                "field3": {
                    "name": "field3",
                    "model": BSTLVStudyTestModel,
                    "visible": False,
                },
                "name": BSTColumn(
                    "name", BSTLVStudyTestModel
                ),  # BSTBaseColumn -> self.column_settings[colobj.name]: colobj
                "animals_group": BSTColumnGroup(  # BSTColumnGroup -> self.column_settings[colobj.name] = colobj
                    BSTManyRelatedColumn("animals__name", BSTLVStudyTestModel),
                    BSTManyRelatedColumn("animals__desc", BSTLVStudyTestModel),
                ),
            },
        )
        self.assertEqual({}, blv.column_settings["field1"])
        self.assertEqual(
            set(["field1", "field2", "field3", "name", "animals_group"]),
            set(blv.column_settings.keys()),
        )
        self.assertEqual({"visible": False}, blv.column_settings["field3"])
        self.assertEqual({}, blv.column_settings["field2"])
        self.assertEqual(
            BSTColumn("name", BSTLVStudyTestModel), blv.column_settings["name"]
        )
        self.assertIsInstance(blv.column_settings["animals_group"], BSTColumnGroup)

    def test_prepare_column_kwargs(self):
        blv = BSTBaseListView()

        with self.assertRaises(KeyError) as ar:
            # dict with no name or field_path key -> KeyError
            blv.prepare_column_kwargs({"x": "y"})
        self.assertIn("When supplying column settings in a dict", str(ar.exception))
        self.assertIn(
            "must have either a 'name' or 'field_path' key", str(ar.exception)
        )
        self.assertIn("['x'].", str(ar.exception))

        with self.assertRaises(ValueError) as ar:
            # dict with no name or field_path key -> KeyError
            blv.prepare_column_kwargs({"name": "field1"}, settings_name="nomatch")
        self.assertIn(
            "When supplying all columns' settings as a dict containing dicts",
            str(ar.exception),
        )
        self.assertIn(
            "each column's settings dict must contain a 'name' or 'field_path' key",
            str(ar.exception),
        )
        self.assertIn(
            "value 'field1' must be identical to the outer dict key 'nomatch'",
            str(ar.exception),
        )

        d1 = {"field_path": "field2"}
        self.assertEqual("field2", blv.prepare_column_kwargs(d1))
        self.assertEqual({}, d1)
        d2 = {"name": "field3", "model": BSTLVStudyTestModel, "visible": False}
        self.assertEqual("field3", blv.prepare_column_kwargs(d2))
        self.assertEqual({"visible": False}, d2)
        d3 = {"field_path": "field2"}
        self.assertEqual(
            "field2", blv.prepare_column_kwargs(d3, settings_name="field2")
        )
        self.assertEqual({}, d3)
        d4 = {"name": "field3", "model": BSTLVStudyTestModel, "visible": False}
        self.assertEqual(
            "field3", blv.prepare_column_kwargs(d4, settings_name="field3")
        )
        self.assertEqual({"visible": False}, d4)

    def test_init_column_ordering(self):
        slv = StudyLV()
        # Defaults case (excludes "id", added by create_test_model)
        slv.column_ordering = []
        slv.init_column_ordering()
        self.assertEqual(["name", "desc", "animals"], slv.column_ordering)

        # User added a related column
        slv.column_settings = {"animals__desc": {}}
        slv.column_ordering = []
        slv.init_column_ordering()
        self.assertEqual(
            ["name", "desc", "animals", "animals__desc"], slv.column_ordering
        )

        # User changes exclude (id appears in order defined, added by create_test_model)
        slv.exclude = ["animals"]
        slv.column_settings = {}
        slv.column_ordering = []
        slv.init_column_ordering()
        self.assertEqual(["name", "desc", "id"], slv.column_ordering)

    @TracebaseTestCase.assertNotWarns(DeveloperWarning)
    def test_add_to_column_ordering(self):
        slv = StudyLV()
        slv.add_to_column_ordering("animals__desc")
        self.assertEqual("animals__desc", slv.column_ordering[-1])

        # Should warn and ignore if column is excluded
        with self.assertWarns(DeveloperWarning) as aw:
            slv.add_to_column_ordering("id")
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Ignoring attempt to add an excluded column 'id'",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Override the 'exclude' class attribute to include this column",
            str(aw.warnings[0].message),
        )
        self.assertIn("Current excludes: ['id']", str(aw.warnings[0].message))
        self.assertNotIn("id", slv.column_ordering)

        # Should not warn if _warn=False (asserted by decorator above)
        slv.add_to_column_ordering("id", _warn=False)

    def test_init_columns(self):
        alv = AnimalNoStudiesLV(
            [
                {"name": "study_count", "converter": Count("studies")},
                "treatment__desc",
                BSTColumnGroup(
                    BSTManyRelatedColumn("studies__name", BSTLVAnimalTestModel),
                    BSTManyRelatedColumn("studies__desc", BSTLVAnimalTestModel),
                ),
            ]
        )
        alv.columns = {}
        # Re-init the column_settings for study_count, because the forst pass would have removed the name:
        print(f"settings {alv.column_settings}")
        alv.init_columns()
        self.assertDictEqual(
            {
                "studies__name": BSTManyRelatedColumn(
                    "studies__name", BSTLVAnimalTestModel
                ),
                "studies__desc": BSTManyRelatedColumn(
                    "studies__desc", BSTLVAnimalTestModel
                ),
                "treatment": BSTRelatedColumn("treatment", BSTLVAnimalTestModel),
                "treatment__desc": BSTRelatedColumn(
                    "treatment__desc", BSTLVAnimalTestModel
                ),
                "desc": BSTColumn("desc", BSTLVAnimalTestModel),
                "name": BSTColumn("name", BSTLVAnimalTestModel),
                "study_count": BSTAnnotColumn("study_count", Count("studies")),
            },
            alv.columns,
        )

    def test_init_column(self):
        alv = AnimalLV()
        alv.columns = {}
        alv.init_column("treatment__desc")
        self.assertEqual(
            BSTRelatedColumn("treatment__desc", BSTLVAnimalTestModel),
            alv.columns["treatment__desc"],
        )
        alv.column_settings["study_count"] = {"converter": Count("studies")}
        alv.init_column("study_count")
        self.assertEqual(
            BSTAnnotColumn("study_count", Count("studies")), alv.columns["study_count"]
        )

    def test_init_column_group(self):
        alv = AnimalNoStudiesLV()
        alv.column_settings["studies_group"] = BSTColumnGroup(
            BSTManyRelatedColumn("studies__name", BSTLVAnimalTestModel),
            BSTManyRelatedColumn("studies__desc", BSTLVAnimalTestModel),
        )
        size_before = len(alv.columns.keys())
        group = BSTColumnGroup(
            BSTManyRelatedColumn("studies__name", BSTLVAnimalTestModel),
            BSTManyRelatedColumn("studies__desc", BSTLVAnimalTestModel),
        )
        alv.init_column_group(group)
        self.assertEqual(size_before + 2, len(alv.columns.keys()))
        # NOTE: Creating a BSTManyRelatedColumn on the fly will not be equal to the one added to columns, because the
        # group constructor modifies it (so they sort the same).
        self.assertEqual(group.columns[0], alv.columns["studies__name"])
        self.assertEqual(group.columns[1], alv.columns["studies__desc"])
        self.assertDictEqual(
            {
                "studies__name": group,
                "studies__desc": group,
            },
            alv.groups,
        )
