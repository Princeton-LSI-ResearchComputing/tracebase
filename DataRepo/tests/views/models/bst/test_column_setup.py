from django.db import ProgrammingError
from django.db.models import (
    CASCADE,
    CharField,
    Count,
    ForeignKey,
    IntegerField,
    ManyToManyField,
    Value,
)
from django.db.models.functions import Lower
from django.http import HttpRequest
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.field import BSTColumn
from DataRepo.views.models.bst.column.filterer.field import BSTFilterer
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.many_related_group import BSTColumnGroup
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn
from DataRepo.views.models.bst.column_setup import (
    BSTBaseDetailView,
    BSTBaseListView,
)


class BSTBaseViewTests(TracebaseTestCase):
    # NOTE: BSTBaseView's methods are tested via its derived classes
    pass


BSTBLVStudyTestModel = create_test_model(
    "BSTBLVStudyTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
    },
    attrs={
        "get_absolute_url": lambda self: f"/DataRepo/study/{self.pk}/",
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": [Lower("name").desc()]},
        ),
    },
)

BSTBLVAnimalTestModel = create_test_model(
    "BSTBLVAnimalTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
        "studies": ManyToManyField(
            to="loader.BSTBLVStudyTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.BSTBLVTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
        ),
    },
)

BSTBLVSampleTestModel = create_test_model(
    "BSTBLVSampleTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "animal": ForeignKey(
            to="loader.BSTBLVAnimalTestModel",
            related_name="samples",
            on_delete=CASCADE,
        ),
    },
)

BSTBLVTreatmentTestModel = create_test_model(
    "BSTBLVTreatmentTestModel",
    {"name": CharField(unique=True), "desc": CharField()},
)


class BSTBaseDetailViewTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        cls.tr = BSTBLVTreatmentTestModel.objects.create(id=1, name="T1", desc="t1")
        BSTBLVAnimalTestModel.objects.create(name="A1", treatment=cls.tr)
        super().setUpTestData()

    def test_BSTBaseDetailView(self):
        class TreatmentDetailView(BSTBaseDetailView):
            model = BSTBLVTreatmentTestModel
            exclude = ["animals"]

        bbdv = TreatmentDetailView()
        self.assertEqual(BSTBLVTreatmentTestModel, bbdv.model)
        self.assertEqual(3, len(bbdv.columns.keys()))

    def test_get_context_data(self):
        class TreatmentDetailView(BSTBaseDetailView):
            model = BSTBLVTreatmentTestModel
            exclude = ["animals"]

        bbdv = TreatmentDetailView()
        bbdv.object = self.tr
        self.assertEqual(
            set(
                [
                    "bstblvtreatmenttestmodel",
                    "columns",
                    "model",
                    "table_id",
                    "table_name",
                    "view",
                    "object",
                    "above_template",
                    "below_template",
                ]
            ),
            set(list(bbdv.get_context_data().keys())),
        )


class StudyBLV(BSTBaseListView):
    model = BSTBLVStudyTestModel


class AnimalBLV(BSTBaseListView):
    model = BSTBLVAnimalTestModel


class AnimalNoStudiesBLV(BSTBaseListView):
    model = BSTBLVAnimalTestModel
    exclude = ["id", "studies"]


class AnimalWithMultipleStudyColsBLV(BSTBaseListView):
    model = BSTBLVAnimalTestModel
    column_ordering = ["name", "desc", "treatment", "studies__name", "studies__desc"]
    exclude = ["id", "studies"]


@override_settings(DEBUG=True)
class BSTBaseListViewTests(TracebaseTestCase):

    @TracebaseTestCase.assertNotWarns()
    def test_init_success_no_cookies(self):
        blv = BSTBaseListView()
        blv.request = HttpRequest()

        self.assertEqual(["id"], blv.ordering)
        self.assertIsNone(blv.search_term)
        self.assertEqual({}, blv.filter_terms)
        self.assertEqual({}, blv.visibles)
        self.assertIsNone(blv.sort_name)
        self.assertTrue(blv.asc)
        self.assertFalse(blv.ordered)
        self.assertEqual(15, blv.limit)
        self.assertEqual({}, blv.column_settings)
        self.assertEqual([], blv.warnings)
        self.assertEqual({}, blv.columns)
        self.assertEqual({}, blv.groups)
        self.assertIsNone(blv.representative_column)

    @TracebaseTestCase.assertNotWarns()
    def test_init_success_cookies(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                "StudyBLV-visible-name": "true",
                "StudyBLV-visible-desc": "false",
                "StudyBLV-sortcol": "name",
                "StudyBLV-asc": "false",
                "StudyBLV-filter-name": "",
                "StudyBLV-filter-desc": "description",
                "StudyBLV-search": "",
            }
        )
        request.GET.update({"limit": "20"})
        slv = StudyBLV(request=request)
        slv.init_interface()

        self.assertEqual(
            "[OrderBy(Lower(F(name)), descending=True)]", str(slv.ordering)
        )
        self.assertIsNone(slv.search_term)
        self.assertEqual({"desc": "description"}, slv.filter_terms)
        self.assertEqual({"name": True, "desc": False}, slv.visibles)
        self.assertEqual("name", slv.sort_name)
        self.assertFalse(slv.asc)
        self.assertTrue(slv.ordered)
        self.assertEqual(20, slv.limit)
        self.assertEqual([], slv.warnings)
        self.assertDictEquivalent(
            {
                "animals_mm_count": BSTAnnotColumn(
                    "animals_mm_count",
                    Count("animals", output_field=IntegerField(), distinct=True),
                    model=BSTBLVStudyTestModel,
                    header="Animals Count",
                    filterer="strictFilterer",
                    sorter="numericSorter",
                    tooltip="Count of related BSTBLVAnimalTestModel records.",
                )
            },
            slv.column_settings,
        )
        self.assertDictEquivalent(
            {
                "name": BSTColumn("name", BSTBLVStudyTestModel, linked=True),
                "desc": BSTColumn(
                    "desc",
                    BSTBLVStudyTestModel,
                    # A filter was set for the 'desc' column.  The filter term was 'description', so we expect a
                    # BSTFilterer object with that initial value
                    filterer=BSTFilterer(
                        "desc", BSTBLVStudyTestModel, initial="description"
                    ),
                    visible=False,
                ),
                "animals_mm_count": BSTAnnotColumn(
                    "animals_mm_count",
                    Count("animals", output_field=IntegerField(), distinct=True),
                    model=BSTBLVStudyTestModel,
                    header="Animals Count",
                    filterer="strictFilterer",
                    sorter="numericSorter",
                    tooltip="Count of related BSTBLVAnimalTestModel records.",
                ),
                "animals": BSTManyRelatedColumn("animals", BSTBLVStudyTestModel),
            },
            slv.columns,
        )
        self.assertEqual({}, slv.groups)
        self.assertIsNotNone(slv.representative_column)
        self.assertEqual("name", slv.representative_column.name)
        self.assertTrue(slv.representative_column.linked)

    @TracebaseTestCase.assertNotWarns()
    def test_init_warnings(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                "StudyBLV-visible-name": "true",
                "StudyBLV-visible-desc": "wrong",
                "StudyBLV-filter-stale": "description",
            }
        )
        with self.assertWarns(DeveloperWarning):
            blv = StudyBLV(request=request)
            blv.init_interface()

        # "stale": "description" is removed from the filter_terms dict because it did not exist as a column.
        self.assertEqual({}, blv.filter_terms)
        self.assertEqual({"name": True}, blv.visibles)
        self.assertTrue(blv.asc)
        self.assertFalse(blv.ordered)
        self.assertEqual(15, blv.limit)
        self.assertEqual(
            [
                "Invalid 'visible' cookie value encountered for column 'desc': 'wrong'.  Resetting cookie.",
                "Invalid 'filter' column encountered: 'stale'.  Resetting filter cookie.",
            ],
            blv.warnings,
        )
        self.assertEqual(["visible-desc", "filter-stale"], blv.cookie_resets)

    def test_init_class_attr_column_settings(self):
        """This tests that you can use the column_settings class attribute to avoid extending the constructor"""

        class StudyBLVcs(BSTBaseListView):
            model = BSTBLVStudyTestModel
            column_settings = {"name": {"filterer": {"choices": ["1", "2"]}}}

        sblvcs = StudyBLVcs()
        self.assertIn("name", sblvcs.column_settings.keys())
        self.assertEqual(
            ["1", "2"], sblvcs.column_settings["name"]["filterer"]["choices"]
        )
        self.assertEquivalent(
            BSTColumn(
                "name",
                BSTBLVStudyTestModel,
                filterer=BSTFilterer("name", BSTBLVStudyTestModel, choices=["1", "2"]),
                linked=True,
            ),
            sblvcs.columns["name"],
        )

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
                {"name": "field3", "model": BSTBLVStudyTestModel, "visible": False},
                BSTColumn(
                    "name", BSTBLVStudyTestModel
                ),  # BSTBaseColumn -> self.column_settings[colobj.name]: colobj
                BSTColumnGroup(  # BSTColumnGroup -> self.column_settings[colobj.name] = colobj
                    BSTManyRelatedColumn("animals__name", BSTBLVStudyTestModel),
                    BSTManyRelatedColumn("animals__desc", BSTBLVStudyTestModel),
                ),
            ],
            clear=True,
        )
        self.assertEqual(
            set(
                [
                    "field1",
                    "field2",
                    "field3",
                    "name",
                    "animals_group",
                    "animals__name",
                    "animals__desc",
                ]
            ),
            set(blv.column_settings.keys()),
        )
        self.assertEqual({}, blv.column_settings["field1"])
        self.assertEqual({}, blv.column_settings["field2"])
        self.assertEqual({"visible": False}, blv.column_settings["field3"])
        self.assertEqual(
            BSTColumn("name", BSTBLVStudyTestModel), blv.column_settings["name"]
        )
        self.assertIsInstance(blv.column_settings["animals_group"], BSTColumnGroup)

    def test_init_column_settings_dict_supplied_for_columns(self):
        blv = BSTBaseListView()

        with self.assertRaises(TypeError) as ar:
            # not str, dict, BSTBaseColumn, or BSTColumnGroup -> TypeError
            blv.init_column_settings({"field1": 1}, clear=True)
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
            blv.init_column_settings({"field1": "otherfield"}, clear=True)
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
                    "model": BSTBLVStudyTestModel,
                    "visible": False,
                },
                "name": BSTColumn(
                    "name", BSTBLVStudyTestModel
                ),  # BSTBaseColumn -> self.column_settings[colobj.name]: colobj
                "animals_group": BSTColumnGroup(  # BSTColumnGroup -> self.column_settings[colobj.name] = colobj
                    BSTManyRelatedColumn("animals__name", BSTBLVStudyTestModel),
                    BSTManyRelatedColumn("animals__desc", BSTBLVStudyTestModel),
                ),
            },
            clear=True,
        )
        self.assertEqual({}, blv.column_settings["field1"])
        self.assertEqual(
            set(
                [
                    "field1",
                    "field2",
                    "field3",
                    "name",
                    "animals_group",
                    "animals__name",
                    "animals__desc",
                ]
            ),
            set(blv.column_settings.keys()),
        )
        self.assertEqual({"visible": False}, blv.column_settings["field3"])
        self.assertEqual({}, blv.column_settings["field2"])
        self.assertEqual(
            BSTColumn("name", BSTBLVStudyTestModel), blv.column_settings["name"]
        )
        self.assertIsInstance(blv.column_settings["animals_group"], BSTColumnGroup)

    def test_init_column_settings_dict_and_annotation(self):
        """This tests that an annotation can be in both the column_settings dict and in the annotations, as long as both
        don't specify the converter"""
        blv = BSTBaseListView()
        blv.annotations = {"annot": Value("no error expected")}

        # test = No error
        blv.init_column_settings(
            {"annot": {"visible": False}},
            clear=True,
        )

        # test = error when BSTAnnotColumn supplied
        with self.assertRaises(ProgrammingError) as ar:
            # not str, dict, BSTBaseColumn, or BSTColumnGroup -> TypeError
            blv.init_column_settings(
                {"annot": BSTAnnotColumn("annot", Value("error expected"))}, clear=True
            )
        self.assertIn(
            "Multiple column settings defined for annotation column 'annot'",
            str(ar.exception),
        )
        self.assertIn("'dict' and 'BSTAnnotColumn' were supplied.", str(ar.exception))

        # test = error when converter supplied
        with self.assertRaises(ProgrammingError) as ar:
            # not str, dict, BSTBaseColumn, or BSTColumnGroup -> TypeError
            blv.init_column_settings(
                {"annot": {"converter": Value("error expected")}}, clear=True
            )
        self.assertIn("Multiple BSTAnnotColumn converters", str(ar.exception))
        self.assertIn("Class default: 'Value('no error expected')'", str(ar.exception))
        self.assertIn(
            "Supplied via the constructor: 'Value('error expected')'.",
            str(ar.exception),
        )

        # test = No error when set in the class attribute (simulated)
        blv.column_settings = {"annot": {"visible": False}}
        blv.init_column_settings()

        # test = error when BSTAnnotColumn set in the class attribute (simulated)
        blv.column_settings = {
            "annot": BSTAnnotColumn("annot", Value("error expected"))
        }
        with self.assertRaises(ProgrammingError) as ar:
            # not str, dict, BSTBaseColumn, or BSTColumnGroup -> TypeError
            blv.init_column_settings()
        self.assertIn(
            "The annotation column 'annot' has been defined twice", str(ar.exception)
        )
        self.assertIn(
            "BSTBaseListView.column_settings and once in BSTBaseListView.annotations.",
            str(ar.exception),
        )

        # test = error when converter set in the class attribute (simulated)
        blv.column_settings = {"annot": {"converter": Value("error expected")}}
        with self.assertRaises(ProgrammingError) as ar:
            # not str, dict, BSTBaseColumn, or BSTColumnGroup -> TypeError
            blv.init_column_settings()
        self.assertIn(
            "The annotation column 'annot' has been defined twice", str(ar.exception)
        )
        self.assertIn(
            "BSTBaseListView.column_settings and once in BSTBaseListView.annotations",
            str(ar.exception),
        )

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
        d2 = {"name": "field3", "model": BSTBLVStudyTestModel, "visible": False}
        self.assertEqual("field3", blv.prepare_column_kwargs(d2))
        self.assertEqual({"visible": False}, d2)
        d3 = {"field_path": "field2"}
        self.assertEqual(
            "field2", blv.prepare_column_kwargs(d3, settings_name="field2")
        )
        self.assertEqual({}, d3)
        d4 = {"name": "field3", "model": BSTBLVStudyTestModel, "visible": False}
        self.assertEqual(
            "field3", blv.prepare_column_kwargs(d4, settings_name="field3")
        )
        self.assertEqual({"visible": False}, d4)

    def test_init_column_ordering(self):
        slv = StudyBLV()
        # Defaults case (excludes "id", added by create_test_model)
        slv.column_ordering = []
        slv.init_column_ordering()
        self.assertEqual(
            ["name", "desc", "animals_mm_count", "animals"], slv.column_ordering
        )

        # User added a related column
        slv.column_settings = {"animals__desc": {}}
        slv.column_ordering = []
        slv.init_column_ordering()
        self.assertEqual(
            ["name", "desc", "animals_mm_count", "animals", "animals__desc"],
            slv.column_ordering,
        )

        # User changes exclude (id appears in order defined, added by create_test_model)
        slv.exclude = ["animals"]
        slv.column_settings = {}
        slv.column_ordering = []
        slv.init_column_ordering()
        self.assertNotIn("animals_mm_count", slv.column_ordering)

        # Check that MR count col inserted before first MR column relation when self.column_ordering contains an MR
        # relation that is not the first in the path
        class SampleBLV(BSTBaseListView):
            model = BSTBLVSampleTestModel
            column_ordering = ["name", "animal__studies__name", "animal__studies__desc"]
            exclude = ["id", "animal", "animal__studies"]

        mlv = SampleBLV()
        # Reset the column_ordering to isolate it
        mlv.column_ordering = ["name", "animal__studies__name", "animal__studies__desc"]
        mlv.init_column_ordering()
        self.assertEqual(
            [
                "name",
                "animal_studies_mm_count",
                "animal__studies__name",
                "animal__studies__desc",
            ],
            mlv.column_ordering,
        )

    @TracebaseTestCase.assertNotWarns(DeveloperWarning)
    def test_add_to_column_ordering(self):
        slv = StudyBLV()
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
        group = BSTColumnGroup(
            BSTManyRelatedColumn("studies__name", BSTBLVAnimalTestModel),
            BSTManyRelatedColumn("studies__desc", BSTBLVAnimalTestModel),
        )
        alv = AnimalNoStudiesBLV(
            [
                "treatment__desc",
                group,
            ]
        )
        alv.columns = {}
        # NOTE: This was changed to remove references to studies_mm_count because the way the count columns are added
        # was changed in a way that allows the derived class to supply settings for the count columns, so the count
        # column key in the settings does not exist.  It is left clear for custom settings to be supplied.
        alv.init_columns()
        self.assertEqual(
            set(
                [
                    "studies_mm_count",
                    "studies__name",
                    "studies__desc",
                    "treatment",
                    "treatment__desc",
                    "desc",
                    "name",
                    "samples",
                    "samples_mm_count",
                ]
            ),
            set(list(alv.columns.keys())),
        )
        # NOTE: Groups modify their columns, so we must equate with the ones in the group.
        self.assertEqual(group.columns[0], alv.columns["studies__name"])
        self.assertEqual(group.columns[1], alv.columns["studies__desc"])
        self.assertEqual(
            BSTRelatedColumn("treatment", BSTBLVAnimalTestModel),
            alv.columns["treatment"],
        )
        self.assertEqual(
            BSTRelatedColumn("treatment__desc", BSTBLVAnimalTestModel),
            alv.columns["treatment__desc"],
        )
        self.assertEqual(BSTColumn("desc", BSTBLVAnimalTestModel), alv.columns["desc"])
        self.assertEqual(BSTColumn("name", BSTBLVAnimalTestModel), alv.columns["name"])
        self.assertEquivalent(
            BSTAnnotColumn(
                "studies_mm_count",
                Count("studies", output_field=IntegerField(), distinct=True),
                model=BSTBLVAnimalTestModel,
                header="Studies Count",
                filterer="strictFilterer",
                sorter="numericSorter",
                tooltip="Count of related BSTBLVStudyTestModel records.",
            ),
            alv.columns["studies_mm_count"],
        )

    def test_init_column(self):
        alv = AnimalBLV()
        alv.columns = {}
        alv.init_column("treatment__desc")
        self.assertEqual(
            BSTRelatedColumn("treatment__desc", BSTBLVAnimalTestModel),
            alv.columns["treatment__desc"],
        )
        alv.column_settings["study_count"] = {"converter": Count("studies")}
        alv.init_column("study_count")
        self.assertEquivalent(
            BSTAnnotColumn(
                "study_count", Count("studies"), model=BSTBLVAnimalTestModel
            ),
            alv.columns["study_count"],
        )

    # TODO: Account for the warnings about get_absolute_url not being in the model.
    # @TracebaseTestCase.assertNotWarns()
    def test_get_column_name(self):
        alv = AnimalBLV()
        self.assertEqual("field1", alv.get_column_name("field1"))
        self.assertEqual("field1", alv.get_column_name({"name": "field1"}))
        self.assertEqual(
            "desc", alv.get_column_name(BSTColumn("desc", BSTBLVAnimalTestModel))
        )
        self.assertEqual(
            "studies_group",
            alv.get_column_name(
                BSTColumnGroup(
                    BSTManyRelatedColumn("studies__name", BSTBLVAnimalTestModel),
                    BSTManyRelatedColumn("studies__desc", BSTBLVAnimalTestModel),
                )
            ),
        )
        with self.assertRaises(TypeError) as ar:
            alv.get_column_name(1, 0)
        # States the context of the problem
        self.assertIn("list of all columns' settings", str(ar.exception))
        # States the requirement
        self.assertIn("value's type must be one of", str(ar.exception))
        # Shows the required data
        # Suggests how to fix it
        self.assertIn(
            "[str, dict, BSTBaseColumn, or BSTColumnGroup]", str(ar.exception)
        )
        # Explains the problem
        self.assertIn("value of the column settings", str(ar.exception))
        # Shows the problem scope
        self.assertIn("at index '0'", str(ar.exception))
        # Shows the problem data
        self.assertIn("was 'int'", str(ar.exception))

    # TODO: Account for the warnings about get_absolute_url not being in the model.
    # @TracebaseTestCase.assertNotWarns()
    def test_init_column_setting(self):
        alv = AnimalBLV()

        alv.init_column_setting({"field_path": "field1"}, "field1")
        self.assertEqual({}, alv.column_settings["field1"])

        # dict with name, model and visible -> self.column_settings["field3"]: {"visible": False}
        alv.init_column_setting(
            {"name": "field2", "model": BSTBLVStudyTestModel, "visible": False},
            "field2",
        )
        self.assertEqual({"visible": False}, alv.column_settings["field2"])

        alv.init_column_setting(
            BSTColumn("name", BSTBLVStudyTestModel),
            "name",
        )
        self.assertEqual(
            BSTColumn("name", BSTBLVStudyTestModel), alv.column_settings["name"]
        )

        group = BSTColumnGroup(
            BSTManyRelatedColumn("animals__name", BSTBLVStudyTestModel),
            BSTManyRelatedColumn("animals__desc", BSTBLVStudyTestModel),
        )
        alv.init_column_setting(group, "animals_group")
        self.assertEqual(group, alv.column_settings["animals_group"])
        self.assertEqual(group.columns[0], alv.column_settings["animals__name"])
        self.assertEqual(group.columns[1], alv.column_settings["animals__desc"])

    def test_add_default_many_related_column_settings(self):
        slv = StudyBLV()
        slv.column_settings = {}
        slv.add_default_many_related_column_settings()
        self.assertDictEquivalent(
            {
                "animals_mm_count": BSTAnnotColumn(
                    "animals_mm_count",
                    Count("animals", output_field=IntegerField(), distinct=True),
                    model=BSTBLVStudyTestModel,
                    header="Animals Count",
                    filterer="strictFilterer",
                    sorter="numericSorter",
                    tooltip="Count of related BSTBLVAnimalTestModel records.",
                ),
            },
            slv.column_settings,
        )

        # Set self.column_ordering containing a MR relation that is not the first in the path and
        # check that count col is added to the settings, even if the FK field is excluded
        class SampleBLV(BSTBaseListView):
            model = BSTBLVSampleTestModel
            column_ordering = ["name", "animal__studies__name", "animal__studies__desc"]
            exclude = ["id", "animal__studies"]

        mlv = SampleBLV()
        mlv.add_default_many_related_column_settings()
        self.assertDictEquivalent(
            {
                "animal_studies_mm_count": BSTAnnotColumn(
                    "animal_studies_mm_count",
                    Count(
                        "animal__studies", output_field=IntegerField(), distinct=True
                    ),
                    model=BSTBLVSampleTestModel,
                    header="Studies Count",
                    filterer="strictFilterer",
                    sorter="numericSorter",
                    tooltip="Count of related BSTBLVStudyTestModel records.",
                ),
            },
            mlv.column_settings,
        )

        # Check that having count col manually entered as dict into column_settings class attribute without a setting
        # for converter still auto-creates count col in column_settings, preserving those settings, e.g. visible=False
        class CustomSampleBLV(BSTBaseListView):
            model = BSTBLVSampleTestModel
            column_ordering = ["name", "animal__studies__name", "animal__studies__desc"]
            exclude = ["id", "animal__studies"]
            column_settings = {"animal_studies_mm_count": {"visible": False}}

        clv = CustomSampleBLV()
        clv.add_default_many_related_column_settings()
        self.assertDictEquivalent(
            {
                "animal_studies_mm_count": BSTAnnotColumn(
                    "animal_studies_mm_count",
                    Count(
                        "animal__studies", output_field=IntegerField(), distinct=True
                    ),
                    model=BSTBLVSampleTestModel,
                    header="Studies Count",
                    filterer="strictFilterer",
                    sorter="numericSorter",
                    visible=False,
                    tooltip="Count of related BSTBLVStudyTestModel records.",
                ),
            },
            clv.column_settings,
        )

        # Test that a dict for an annot col in the annotations class attribute creates a BSTAnnotColumn in
        # self.columns[colname] using that converter
        class SampleWithAnnotBLV(BSTBaseListView):
            model = BSTBLVSampleTestModel
            column_ordering = ["name", "animal__studies__name", "animal__studies__desc"]
            exclude = ["id", "animal__studies"]
            # Custom count annotation (could be anything, but arbitrarily using Value, just to have something different
            # and confirm it is used)
            annotations = {"animal_studies_mm_count": Value(5)}

        clv = SampleWithAnnotBLV()
        clv.add_default_many_related_column_settings()
        self.assertDictEquivalent(
            {
                "animal_studies_mm_count": BSTAnnotColumn(
                    "animal_studies_mm_count",
                    Value(5),
                    model=BSTBLVSampleTestModel,
                    header="Studies Count",
                    filterer="strictFilterer",
                    sorter="numericSorter",
                    tooltip="Count of related BSTBLVStudyTestModel records.",
                ),
            },
            clv.column_settings,
        )

        # Test that a dict for an annot col with a converter in the column_settings class attribute creates a
        # BSTAnnotColumn in self.columns[colname] using that converter (same as above, only different attribute)
        class SampleWithAnnotSettingBLV(BSTBaseListView):
            model = BSTBLVSampleTestModel
            column_ordering = ["name", "animal__studies__name", "animal__studies__desc"]
            exclude = ["id", "animal__studies"]
            # Custom count annotation (could be anything, but arbitrarily using Value, just to have something different
            # and confirm it is used)
            column_settings = {"animal_studies_mm_count": {"converter": Value(5)}}

        clv = SampleWithAnnotSettingBLV()
        clv.add_default_many_related_column_settings()
        self.assertDictEquivalent(
            {
                "animal_studies_mm_count": BSTAnnotColumn(
                    "animal_studies_mm_count",
                    Value(5),
                    model=BSTBLVSampleTestModel,
                    header="Studies Count",
                    filterer="strictFilterer",
                    sorter="numericSorter",
                    tooltip="Count of related BSTBLVStudyTestModel records.",
                ),
            },
            clv.column_settings,
        )

        # Test that an excluded many-related FK, but included count in the column_ordering, gets an annotation added
        class AnimalWithStudiesExcCountIncOrderBLV(BSTBaseListView):
            model = BSTBLVAnimalTestModel
            column_ordering = ["name", "studies_mm_count"]
            exclude = ["id", "studies", "samples"]

        clv = AnimalWithStudiesExcCountIncOrderBLV()
        clv.add_default_many_related_column_settings()
        self.assertDictEquivalent(
            {
                "studies_mm_count": BSTAnnotColumn(
                    "studies_mm_count",
                    Count("studies", output_field=IntegerField(), distinct=True),
                    model=BSTBLVAnimalTestModel,
                    header="Studies Count",
                    filterer="strictFilterer",
                    sorter="numericSorter",
                    tooltip="Count of related BSTBLVStudyTestModel records.",
                ),
            },
            clv.column_settings,
        )

        # Test that an excluded many-related FK, but included count in the column_settings, gets an annotation added
        class AnimalWithStudiesExcCountIncSetBLV(BSTBaseListView):
            model = BSTBLVAnimalTestModel
            column_ordering = ["name"]
            exclude = ["id", "studies", "samples"]
            column_settings = {"studies_mm_count": {"header": "Studies Count"}}

        clv = AnimalWithStudiesExcCountIncSetBLV()
        clv.add_default_many_related_column_settings()
        self.assertDictEquivalent(
            {
                "studies_mm_count": BSTAnnotColumn(
                    "studies_mm_count",
                    Count("studies", output_field=IntegerField(), distinct=True),
                    model=BSTBLVAnimalTestModel,
                    header="Studies Count",
                    filterer="strictFilterer",
                    sorter="numericSorter",
                    tooltip="Count of related BSTBLVStudyTestModel records.",
                ),
            },
            clv.column_settings,
        )

    def test_many_related_columns_exist(self):
        alv1 = AnimalWithMultipleStudyColsBLV()
        self.assertTrue(alv1.many_related_columns_exist("studies"))
        alv2 = AnimalNoStudiesBLV()
        self.assertFalse(alv2.many_related_columns_exist("studies"))
        alv3 = AnimalBLV()
        self.assertFalse(alv3.many_related_columns_exist("studies"))

    @TracebaseTestCase.assertNotWarns()
    def test_get_context_data(self):
        request = HttpRequest()
        request.method = "GET"
        response = StudyBLV.as_view()(request)
        context = response.context_data
        self.assertEqual(
            set(
                [
                    # From parent class
                    "object_list",
                    "page_obj",
                    "cookie_prefix",
                    "clear_cookies",
                    "is_paginated",
                    "cookie_resets",
                    "paginator",
                    "model",
                    "view",
                    "scripts",
                    "bstblvstudytestmodel_list",  # Same as "object_list"
                    "table_id",
                    "table_name",
                    "sortcol",
                    "asc",
                    "search",
                    "limit",
                    "limit_default",
                    "warnings",
                    "raw_total",
                    "total",
                    "sort_cookie_name",
                    "search_cookie_name",
                    "filter_cookie_name",
                    "asc_cookie_name",
                    "limit_cookie_name",
                    "page_cookie_name",
                    "visible_cookie_name",
                    "collapsed",
                    "collapsed_cookie_name",
                    "above_template",
                    "below_template",
                    # columns is from this class
                    "columns",
                    "subtitles",
                ]
            ),
            set(context.keys()),
        )
        self.assertEqual("StudyBLV", context["table_id"])
        self.assertEqual("BSTBLV Study Test Models", context["table_name"])
        self.assertEqual(None, context["sortcol"])
        self.assertTrue(context["asc"])
        self.assertEqual(None, context["search"])

        # The table is empty (this class is just about column config), so limit is set to the default, which is 15
        self.assertEqual(15, context["limit"])
        self.assertEqual(15, context["limit_default"])

        self.assertEqual([], context["warnings"])
        self.assertDictEquivalent(
            {
                "name": BSTColumn("name", BSTBLVStudyTestModel, linked=True),
                "desc": BSTColumn("desc", BSTBLVStudyTestModel),
                "animals_mm_count": BSTAnnotColumn(
                    "animals_mm_count",
                    Count("animals", output_field=IntegerField(), distinct=True),
                    model=BSTBLVStudyTestModel,
                    header="Animals Count",
                    filterer="strictFilterer",
                    tooltip="Count of related BSTBLVAnimalTestModel records.",
                ),
                "animals": BSTManyRelatedColumn("animals", BSTBLVStudyTestModel),
            },
            context["columns"],
        )

    # TODO: Account for the warnings about get_absolute_url not being in the model.
    # @TracebaseTestCase.assertNotWarns()
    def test_add_check_groups(self):
        class AnimalWithAddedStudyColsBLV(BSTBaseListView):
            model = BSTBLVAnimalTestModel
            exclude = ["id", "studies"]

        awasc = AnimalWithAddedStudyColsBLV()

        # Now let's manually add a couple columns (avoiding the constructor so that add_check_groups isn't
        # automatically called and we can isolate it
        awasc.column_settings["studies__name"] = BSTManyRelatedColumn(
            "studies__name", BSTBLVAnimalTestModel
        )
        awasc.column_settings["studies__desc"] = BSTManyRelatedColumn(
            "studies__desc", BSTBLVAnimalTestModel
        )
        awasc.column_ordering.append("studies__name")
        awasc.column_ordering.append("studies__desc")
        awasc.init_column("studies__name")
        awasc.init_column("studies__desc")

        awasc.add_check_groups()

        self.assertIn("studies_group", awasc.groups.keys())
        self.assertEqual(2, len(awasc.groups["studies_group"].columns))
        self.assertEqual(
            set(["studies__name", "studies__desc"]),
            set([c.name for c in awasc.groups["studies_group"].columns]),
        )

        # Now test the warning
        class StudyWithAddedAnimalColsBLV(BSTBaseListView):
            model = BSTBLVStudyTestModel
            exclude = ["id", "animals"]

        swaac = StudyWithAddedAnimalColsBLV()

        # Now let's manually add a couple columns (avoiding the constructor so that add_check_groups isn't
        # automatically called and we can isolate it
        swaac.column_settings["animals_group"] = BSTColumnGroup(
            BSTManyRelatedColumn("animals__name", BSTBLVStudyTestModel),
            BSTManyRelatedColumn("animals__desc", BSTBLVStudyTestModel),
        )
        swaac.groups["animals_group"] = swaac.column_settings["animals_group"]
        swaac.column_settings["animals_group"] = BSTManyRelatedColumn(
            "animals__name", BSTBLVStudyTestModel
        )
        swaac.column_settings["animals__desc"] = BSTManyRelatedColumn(
            "animals__desc", BSTBLVStudyTestModel
        )
        # This added many-related column that's not in the group should cause a DeveloperWarning
        swaac.column_settings["animals__treatment__name"] = BSTManyRelatedColumn(
            "animals__treatment__name", BSTBLVStudyTestModel
        )
        swaac.column_ordering.append("animals__name")
        swaac.column_ordering.append("animals__desc")
        swaac.column_ordering.append("animals__treatment__name")
        swaac.init_column("animals__name")
        swaac.init_column("animals__desc")
        swaac.init_column("animals__treatment__name")

        with self.assertWarns(DeveloperWarning) as aw:
            swaac.add_check_groups()

        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Manually created column group 'animals_group'", str(aw.warnings[0].message)
        )
        self.assertIn("related model 'animals'", str(aw.warnings[0].message))
        self.assertIn(
            "1 column(s) that go through the same many-related model",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "not in the group: {'animals__treatment__name'}.",
            str(aw.warnings[0].message),
        )
        self.assertIn("add them", str(aw.warnings[0].message))

        self.assertIn("animals_group", swaac.groups.keys())
        # The custom group should not have been changed
        self.assertEqual(2, len(swaac.groups["animals_group"].columns))
        self.assertEqual(
            set(["animals__name", "animals__desc"]),
            set([c.name for c in swaac.groups["animals_group"].columns]),
        )

        # Now test that unique columns are excluded from the group:
        class AnimalWithAddedStudyColsBLV2(BSTBaseListView):
            model = BSTBLVAnimalTestModel
            exclude = ["id", "studies"]

        awasc2 = AnimalWithAddedStudyColsBLV2()

        # Now let's manually add a couple columns (avoiding the constructor so that add_check_groups isn't
        # automatically called and we can isolate it
        awasc2.column_settings["studies__name"] = BSTManyRelatedColumn(
            "studies__name", BSTBLVAnimalTestModel
        )
        awasc2.column_settings["studies__desc"] = BSTManyRelatedColumn(
            "studies__desc", BSTBLVAnimalTestModel, unique=True
        )
        awasc2.column_ordering.append("studies__name")
        awasc2.column_ordering.append("studies__desc")
        awasc2.init_column("studies__name")
        awasc2.init_column("studies__desc")

        awasc2.add_check_groups()

        self.assertNotIn("studies_group", awasc2.groups.keys())

    def test_init_no_representative(self):
        """Test that a linked "details" column is added when there is no clear representative field"""
        BSTBLVNoRepTestModel = create_test_model(
            "BSTBLVNoRepTestModel",
            {"name": CharField(), "desc": CharField()},
            attrs={"get_absolute_url": lambda self: f"/DataRepo/norep/{self.pk}/"},
        )

        class NoRepBLV(BSTBaseListView):
            model = BSTBLVNoRepTestModel

        nrblv = NoRepBLV()
        # An arbitrary representative is selected
        self.assertIsNotNone(nrblv.representative_column)
        self.assertIn("details", nrblv.columns.keys())
        self.assertIn("details", nrblv.column_ordering)
        self.assertTrue(nrblv.columns["details"].linked)

    def test_init_subquery(self):
        request = HttpRequest()
        request.GET.update(
            {
                "animals__name": "A1",
                "subquery": True,
            }
        )
        alv = StudyBLV(request=request)
        alv.subquery = None
        alv.subtitles = None
        alv.init_subquery()
        self.assertDictEquivalent(
            {"animals__name": "A1"},
            alv.subquery,
        )
        self.assertDictEquivalent(
            {"Animals Name": "A1"},
            alv.subtitles,
        )
