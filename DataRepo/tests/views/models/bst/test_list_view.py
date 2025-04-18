from django.db.models import CASCADE, CharField, ForeignKey, ManyToManyField, Q
from django.db.models.aggregates import Max
from django.db.models.functions import Lower, Upper
from django.http import HttpRequest
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.many_related_group import BSTColumnGroup
from DataRepo.views.models.bst.column.sorter.many_related_field import (
    BSTManyRelatedSorter,
)
from DataRepo.views.models.bst.list_view import BSTListView

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
            {"app_label": "loader", "ordering": [Lower("name").desc()]},
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


class StudyLV(BSTListView):
    model = BSTLVStudyTestModel
    annotations = {"description": Upper("desc")}
    exclude = ["id", "desc"]


class AnimalLV(BSTListView):
    model = BSTLVAnimalTestModel
    column_ordering = ["name", "desc", "treatment", "studies__name", "studies__desc"]
    exclude = ["id", "studies"]

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            columns=[
                BSTColumnGroup(
                    BSTManyRelatedColumn("studies__name", AnimalLV.model),
                    BSTManyRelatedColumn("studies__desc", AnimalLV.model),
                ),
            ],
            **kwargs,
        )


@override_settings(DEBUG=True)
class BSTListViewTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        t1 = BSTLVTreatmentTestModel.objects.create(name="T1", desc="t1")
        t2 = BSTLVTreatmentTestModel.objects.create(name="oddball", desc="t2")
        s1 = BSTLVStudyTestModel.objects.create(name="S1", desc="s1")
        s2 = BSTLVStudyTestModel.objects.create(name="S2", desc="s2")
        a1 = BSTLVAnimalTestModel.objects.create(name="A1", desc="a1", treatment=t1)
        a1.studies.add(s1)
        a2 = BSTLVAnimalTestModel.objects.create(name="A2", desc="a2", treatment=t2)
        a2.studies.add(s1)
        a2.studies.add(s2)
        print(f"COUNT OF ANIMALS: {BSTLVAnimalTestModel.objects.count()}")
        super().setUpTestData()

    @TracebaseTestCase.assertNotWarns()
    def test_init_no_cookies(self):
        slv = StudyLV()
        self.assertEqual(0, slv.raw_total)
        self.assertEqual(0, slv.total)
        self.assertEqual(["animals"], slv.prefetches)
        self.assertEqual(Q(), slv.filters)
        self.assertEqual(0, len(slv.prefilter_annots.keys()))
        self.assertDictEqual({"description": Upper("desc")}, slv.postfilter_annots)

    @TracebaseTestCase.assertNotWarns()
    def test_init_search_cookie(self):
        request = HttpRequest()
        request.COOKIES.update({f"StudyLV-{StudyLV.search_cookie_name}": "test"})
        slv = StudyLV(request=request)
        q = Q(**{"name__icontains": "test"})
        q |= Q(**{"animals__name__icontains": "test"})
        q |= Q(**{"description__icontains": "test"})
        self.assertEqual(q, slv.filters)
        self.assertDictEqual({"description": Upper("desc")}, slv.prefilter_annots)
        self.assertEqual(0, len(slv.postfilter_annots))

    @TracebaseTestCase.assertNotWarns()
    def test_init_filter_cookie(self):
        request = HttpRequest()
        request.COOKIES.update({f"StudyLV-{StudyLV.filter_cookie_name}-name": "test"})
        slv = StudyLV(request=request)
        q = Q(**{"name__icontains": "test"})
        self.assertEqual(q, slv.filters)
        self.assertEqual(0, len(slv.prefilter_annots.keys()))
        self.assertDictEqual({"description": Upper("desc")}, slv.postfilter_annots)

    @TracebaseTestCase.assertNotWarns()
    def test_init_sort_cookie(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                f"StudyLV-{StudyLV.sortcol_cookie_name}": "name",
                f"StudyLV-{StudyLV.asc_cookie_name}": "false",
            }
        )
        slv = StudyLV(request=request)
        self.assertEqual(Q(), slv.filters)
        self.assertEqual(0, len(slv.prefilter_annots.keys()))
        self.assertDictEqual({"description": Upper("desc")}, slv.postfilter_annots)
        self.assertEqual(
            Lower("name").desc(nulls_last=True),
            slv.columns[slv.sort_name].sorter.order_by,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_init_sort_group_cookie(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                f"AnimalLV-{AnimalLV.sortcol_cookie_name}": "studies__desc",
                f"AnimalLV-{AnimalLV.asc_cookie_name}": "false",
            }
        )
        alv = AnimalLV(request=request)
        self.assertEqual(Q(), alv.filters)
        self.assertEqual(
            Max(Lower("studies__desc")).desc(nulls_last=True),
            alv.columns[alv.sort_name].sorter.order_by,
        )

        # The user sorted based on the "studies__desc" column, which means that the sorter for both the "studies__desc"
        # and "studies__name" columns will be based on the "studies__desc" field.
        # Check that the "studies__desc" column's sorter is based on "studies__desc"
        study_desc_sorter: BSTManyRelatedSorter = alv.columns["studies__desc"].sorter
        self.assertEqual("studies__desc", study_desc_sorter.field_path)
        self.assertEqual(Lower, alv.columns["studies__desc"].sorter._server_sorter)
        self.assertFalse(alv.columns["studies__desc"].sorter.asc)

        # Check that the "studies__name" column's sorter is also based on "studies__desc"
        study_name_sorter: BSTManyRelatedSorter = alv.columns["studies__name"].sorter
        self.assertEqual("studies__desc", study_name_sorter.field_path)
        self.assertEqual(Lower, alv.columns["studies__name"].sorter._server_sorter)
        self.assertFalse(alv.columns["studies__name"].sorter.asc)

    @TracebaseTestCase.assertNotWarns()
    def test_get_queryset(self):
        alv = AnimalLV()
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.distinct(), alv.get_queryset(), ordered=False
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_user_queryset(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                f"AnimalLV-{AnimalLV.sortcol_cookie_name}": "name",
                f"AnimalLV-{AnimalLV.asc_cookie_name}": "false",
                f"AnimalLV-{AnimalLV.filter_cookie_name}-studies__name": "s",
            }
        )
        alv1 = AnimalLV(request=request)
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.filter(studies__name__icontains="s")
            .order_by("-name")
            .distinct(),
            alv1.get_queryset(),
        )

        request.COOKIES.update(
            {
                f"AnimalLV-{AnimalLV.filter_cookie_name}-studies__name": "2",
                f"AnimalLV-{AnimalLV.asc_cookie_name}": "true",
            }
        )
        alv2 = AnimalLV(request=request)
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.filter(studies__name__icontains="2")
            .order_by("name")
            .distinct(),
            alv2.get_queryset(),
        )

        request.COOKIES = {
            f"AnimalLV-{AnimalLV.filter_cookie_name}-treatment": "ball",
        }
        alv3 = AnimalLV(request=request)
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.filter(
                treatment__name__icontains="ball"
            ).distinct(),
            alv3.get_queryset(),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_prefetches(self):
        alv = AnimalLV()
        self.assertEqual(set(["studies", "treatment"]), set(alv.get_prefetches()))

    @TracebaseTestCase.assertNotWarns()
    def test_get_filters(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                f"AnimalLV-{AnimalLV.filter_cookie_name}-studies__name": "test1",
                f"AnimalLV-{AnimalLV.filter_cookie_name}-desc": "test2",
            }
        )
        alv = AnimalLV(request=request)
        self.assertEqual(
            "(AND: ('studies__name__icontains', 'test1'), ('desc__icontains', 'test2'))",
            str(alv.get_filters()),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_search(self):
        request = HttpRequest()
        request.COOKIES.update({"AnimalLV-search": "test1"})
        alv = AnimalLV(request=request)
        self.assertEqual(
            "(OR: ('name__icontains', 'test1'), "
            "('desc__icontains', 'test1'), "
            "('treatment__name__icontains', 'test1'), "
            "('studies__name__icontains', 'test1'), "
            "('studies__desc__icontains', 'test1'))",
            str(alv.search()),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_annotations(self):
        # No search or filter
        alv1 = StudyLV()
        before, after = alv1.get_annotations()
        self.assertDictEqual({}, before)
        self.assertDictEqual({"description": Upper("desc")}, after)

        # Search
        request = HttpRequest()
        request.COOKIES.update({"StudyLV-search": "test1"})
        alv2 = StudyLV(request=request)
        before, after = alv2.get_annotations()
        self.assertDictEqual({"description": Upper("desc")}, before)
        self.assertDictEqual({}, after)

        # Filter
        request.COOKIES = {"StudyLV-filter-description": "test1"}
        alv3 = StudyLV(request=request)
        before, after = alv3.get_annotations()
        self.assertDictEqual({"description": Upper("desc")}, before)
        self.assertDictEqual({}, after)

        # No search or filter (but cookies)
        request.COOKIES = {"StudyLV-asc": "false"}
        alv4 = StudyLV(request=request)
        before, after = alv4.get_annotations()
        self.assertDictEqual({}, before)
        self.assertDictEqual({"description": Upper("desc")}, after)

    @TracebaseTestCase.assertNotWarns()
    def test_apply_annotations(self):
        alv = StudyLV()
        qs = BSTLVStudyTestModel.objects.all()
        aqs = alv.apply_annotations(qs, alv.get_annotations()[1])
        self.assertEqual(
            set(["S1", "S2"]), set(list(aqs.values_list("description", flat=True)))
        )

    @TracebaseTestCase.assertNotWarns()
    def test_apply_filters_success(self):
        request = HttpRequest()
        request.COOKIES = {f"StudyLV-{StudyLV.filter_cookie_name}-name": "2"}
        alv = StudyLV(request=request)
        qs = BSTLVStudyTestModel.objects.all()
        fqs = alv.apply_filters(qs)
        self.assertQuerySetEqual(
            BSTLVStudyTestModel.objects.filter(name__icontains="2"),
            fqs,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_apply_filters_warns(self):
        request = HttpRequest()
        request.COOKIES = {f"StudyLV-{StudyLV.filter_cookie_name}-desc": "2"}
        qs = BSTLVStudyTestModel.objects.all()
        with self.assertWarns(DeveloperWarning) as aw:
            alv = StudyLV(request=request)
            fqs = alv.apply_filters(qs)
        self.assertEqual(1, len(aw.warnings))
        self.assertIn("Column 'desc' filter '2' failed", str(aw.warnings[0].message))
        self.assertIn("Column not found", str(aw.warnings[0].message))
        self.assertIn("Resetting filter cookie", str(aw.warnings[0].message))
        self.assertQuerySetEqual(
            BSTLVStudyTestModel.objects.all(),
            fqs,
        )
        self.assertEqual(
            [f"StudyLV-{StudyLV.filter_cookie_name}-desc"], alv.cookie_resets
        )
