from django.core.paginator import Page
from django.db.models import (
    CASCADE,
    CharField,
    F,
    ForeignKey,
    IntegerField,
    ManyToManyField,
    Q,
)
from django.db.models.aggregates import Count, Max
from django.db.models.functions import Lower, Upper
from django.http import HttpRequest
from django.test import RequestFactory, override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.field import BSTColumn
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.many_related_group import BSTColumnGroup
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn
from DataRepo.views.models.bst.column.sorter.many_related_field import (
    BSTManyRelatedSorter,
)
from DataRepo.views.models.bst.list_view import BSTListView
from DataRepo.views.utils import GracefulPaginator

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
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": ["-name"]},
        ),
    },
)

BSTLVTreatmentTestModel = create_test_model(
    "BSTLVTreatmentTestModel",
    {"name": CharField(unique=True), "desc": CharField()},
)


class StudyLV(BSTListView):
    model = BSTLVStudyTestModel
    annotations = {"description": Upper("desc", output_field=CharField())}
    exclude = ["id", "desc"]


class AnimalWithMultipleStudyColsLV(BSTListView):
    model = BSTLVAnimalTestModel
    column_ordering = ["name", "desc", "treatment", "studies__name", "studies__desc"]
    exclude = ["id", "studies"]

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            columns=[
                BSTColumnGroup(
                    BSTManyRelatedColumn(
                        "studies__name", AnimalWithMultipleStudyColsLV.model
                    ),
                    BSTManyRelatedColumn(
                        "studies__desc", AnimalWithMultipleStudyColsLV.model
                    ),
                ),
            ],
            **kwargs,
        )


class AnimalDefaultLV(BSTListView):
    model = BSTLVAnimalTestModel


@override_settings(DEBUG=True)
class BSTListViewTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        cls.t1 = BSTLVTreatmentTestModel.objects.create(name="T1", desc="t1")
        cls.t2 = BSTLVTreatmentTestModel.objects.create(name="oddball", desc="t2")
        cls.s1 = BSTLVStudyTestModel.objects.create(name="S1", desc="s1")
        cls.s2 = BSTLVStudyTestModel.objects.create(name="S2", desc="s2")
        cls.a1 = BSTLVAnimalTestModel.objects.create(
            name="A1", desc="a1", treatment=cls.t1
        )
        cls.a1.studies.add(cls.s1)
        cls.a2 = BSTLVAnimalTestModel.objects.create(
            name="A2", desc="a2", treatment=cls.t2
        )
        cls.a2.studies.add(cls.s1)
        cls.a2.studies.add(cls.s2)
        super().setUpTestData()

    @TracebaseTestCase.assertNotWarns()
    def test_init_no_cookies(self):
        slv = StudyLV()
        self.assertEqual(0, slv.raw_total)
        self.assertEqual(0, slv.total)
        self.assertEqual(["animals"], slv.prefetches)
        self.assertEqual(Q(), slv.filters)
        self.assertDictEquivalent(
            {
                "animals_mm_count": Count("animals", output_field=IntegerField()),
                "description": Upper("desc", output_field=CharField()),
            },
            slv.postfilter_annots,
        )
        self.assertDictEquivalent(
            {"name_bstrowsort": Lower("name")},
            slv.prefilter_annots,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_init_search_cookie(self):
        request = HttpRequest()
        request.COOKIES.update({f"StudyLV-{StudyLV.search_cookie_name}": "test"})
        slv = StudyLV(request=request)
        q = Q(**{"name__icontains": "test"})
        q |= Q(**{"animals_mm_count__iexact": "test"})
        q |= Q(**{"animals__name__icontains": "test"})
        q |= Q(**{"description__icontains": "test"})
        self.assertEqual(q, slv.filters)
        self.assertDictEquivalent(
            {
                "name_bstrowsort": Lower("name"),
                "animals_mm_count": Count("animals", output_field=IntegerField()),
                "description": Upper("desc", output_field=CharField()),
            },
            slv.prefilter_annots,
        )
        self.assertEqual(0, len(slv.postfilter_annots))

    @TracebaseTestCase.assertNotWarns()
    def test_init_filter_cookie(self):
        request = HttpRequest()
        request.COOKIES.update({f"StudyLV-{StudyLV.filter_cookie_name}-name": "test"})
        slv = StudyLV(request=request)
        q = Q(**{"name__icontains": "test"})
        self.assertDictEquivalent(
            {"name_bstrowsort": Lower("name")},
            slv.prefilter_annots,
        )
        self.assertEqual(q, slv.filters)
        self.assertDictEquivalent(
            {
                "animals_mm_count": Count("animals", output_field=IntegerField()),
                "description": Upper("desc", output_field=CharField()),
            },
            slv.postfilter_annots,
        )

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
        self.assertDictEquivalent(
            {"name_bstrowsort": Lower("name")},
            slv.prefilter_annots,
        )
        self.assertDictEquivalent(
            {
                "animals_mm_count": Count("animals", output_field=IntegerField()),
                "description": Upper("desc", output_field=CharField()),
            },
            slv.postfilter_annots,
        )
        self.assertEqual(
            F("name_bstrowsort").desc(nulls_last=True),
            slv.columns[slv.sort_name].sorter.order_by,
        )
        # Make sure the expression referred to in the annotation is correct
        self.assertEqual(
            Lower("name"),
            slv.columns[slv.sort_name].sorter.expression,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_init_sort_group_cookie(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                f"{AnimalWithMultipleStudyColsLV.__name__}-"
                f"{AnimalWithMultipleStudyColsLV.sortcol_cookie_name}": "studies__desc",
                f"{AnimalWithMultipleStudyColsLV.__name__}-"
                f"{AnimalWithMultipleStudyColsLV.asc_cookie_name}": "false",
            }
        )
        alv = AnimalWithMultipleStudyColsLV(request=request)
        self.assertEqual(Q(), alv.filters)
        self.assertEqual(
            F("studies__desc_bstrowsort").desc(nulls_last=True),
            alv.columns[alv.sort_name].sorter.order_by,
        )
        self.assertEqual(
            Max(Lower("studies__desc")),
            alv.columns[alv.sort_name].sorter.expression,
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
        alv = AnimalWithMultipleStudyColsLV()
        with self.assertNumQueries(2):
            # get_queryset doesn't return records, but it makes 2 queries:
            # 1. Count of model records (without any filtering/searching)
            # 2. Count of model records (with filtering/searching)
            qs = alv.get_queryset()
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.distinct(), qs, ordered=False
        )
        self.assertEqual(2, alv.raw_total)
        self.assertEqual(2, alv.total)

    @TracebaseTestCase.assertNotWarns()
    def test_get_user_queryset(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                f"{AnimalWithMultipleStudyColsLV.__name__}-"
                f"{AnimalWithMultipleStudyColsLV.sortcol_cookie_name}": "name",
                f"{AnimalWithMultipleStudyColsLV.__name__}-"
                f"{AnimalWithMultipleStudyColsLV.asc_cookie_name}": "false",
                f"{AnimalWithMultipleStudyColsLV.__name__}-"
                f"{AnimalWithMultipleStudyColsLV.filter_cookie_name}-studies__name": "s",
            }
        )
        alv1 = AnimalWithMultipleStudyColsLV(request=request)
        with self.assertNumQueries(2):
            qs1 = alv1.get_queryset()
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.filter(studies__name__icontains="s")
            .order_by("-name")
            .distinct(),
            qs1,
        )

        request.COOKIES.update(
            {
                f"{AnimalWithMultipleStudyColsLV.__name__}-"
                f"{AnimalWithMultipleStudyColsLV.filter_cookie_name}-studies__name": "2",
                f"{AnimalWithMultipleStudyColsLV.__name__}-"
                f"{AnimalWithMultipleStudyColsLV.asc_cookie_name}": "true",
            }
        )
        alv2 = AnimalWithMultipleStudyColsLV(request=request)
        with self.assertNumQueries(2):
            qs2 = alv2.get_queryset()
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.filter(studies__name__icontains="2")
            .order_by("name")
            .distinct(),
            qs2,
        )

        request.COOKIES = {
            f"{AnimalWithMultipleStudyColsLV.__name__}-"
            f"{AnimalWithMultipleStudyColsLV.filter_cookie_name}-treatment": "ball",
        }
        alv3 = AnimalWithMultipleStudyColsLV(request=request)
        with self.assertNumQueries(2):
            qs3 = alv3.get_queryset()
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.filter(
                treatment__name__icontains="ball"
            ).distinct(),
            qs3,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_prefetches(self):
        alv = AnimalWithMultipleStudyColsLV()
        self.assertEqual(set(["studies", "treatment"]), set(alv.get_prefetches()))

    @TracebaseTestCase.assertNotWarns()
    def test_get_filters(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                f"{AnimalWithMultipleStudyColsLV.__name__}-"
                f"{AnimalWithMultipleStudyColsLV.filter_cookie_name}-studies__name": "test1",
                f"{AnimalWithMultipleStudyColsLV.__name__}-"
                f"{AnimalWithMultipleStudyColsLV.filter_cookie_name}-desc": "test2",
            }
        )
        alv = AnimalWithMultipleStudyColsLV(request=request)
        self.assertEqual(
            "(AND: ('studies__name__icontains', 'test1'), ('desc__icontains', 'test2'))",
            str(alv.get_filters()),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_search(self):
        request = HttpRequest()
        request.COOKIES.update(
            {f"{AnimalWithMultipleStudyColsLV.__name__}-search": "test1"}
        )
        alv = AnimalWithMultipleStudyColsLV(request=request)
        self.assertEqual(
            "(OR: ('name__icontains', 'test1'), "
            "('desc__icontains', 'test1'), "
            "('treatment__name__icontains', 'test1'), "
            "('studies__name__icontains', 'test1'), "
            "('studies__desc__icontains', 'test1'), "
            "('studies_mm_count__iexact', 'test1'))",
            str(alv.search()),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_annotations(self):
        # No search or filter
        alv1 = StudyLV()
        before, after = alv1.get_annotations()
        self.assertDictEquivalent(
            {
                "animals_mm_count": Count("animals", output_field=IntegerField()),
                "description": Upper("desc", output_field=CharField()),
            },
            after,
        )
        self.assertDictEquivalent({"name_bstrowsort": Lower("name")}, before)

        # Search
        request = HttpRequest()
        request.COOKIES.update({"StudyLV-search": "test1"})
        alv2 = StudyLV(request=request)
        before, after = alv2.get_annotations()
        self.assertDictEquivalent(
            {
                "name_bstrowsort": Lower("name"),
                "animals_mm_count": Count("animals", output_field=IntegerField()),
                "description": Upper("desc", output_field=CharField()),
            },
            before,
        )
        self.assertDictEqual({}, after)

        # Filter
        request.COOKIES = {"StudyLV-filter-description": "test1"}
        alv3 = StudyLV(request=request)
        before, after = alv3.get_annotations()
        self.assertDictEquivalent(
            {
                "name_bstrowsort": Lower("name"),
                "description": Upper("desc", output_field=CharField()),
            },
            before,
        )
        self.assertDictEquivalent(
            {"animals_mm_count": Count("animals", output_field=IntegerField())}, after
        )

        # No search or filter (but cookies)
        request.COOKIES = {"StudyLV-asc": "false"}
        alv4 = StudyLV(request=request)
        before, after = alv4.get_annotations()
        self.assertDictEquivalent({"name_bstrowsort": Lower("name")}, before)
        self.assertDictEquivalent(
            {
                "animals_mm_count": Count("animals", output_field=IntegerField()),
                "description": Upper("desc", output_field=CharField()),
            },
            after,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_apply_annotations(self):
        slv = StudyLV()
        qs = BSTLVStudyTestModel.objects.all()
        aqs = slv.apply_annotations(qs, slv.get_annotations()[1])
        self.assertEqual(
            set(["S1", "S2"]), set(list(aqs.values_list("description", flat=True)))
        )

    @TracebaseTestCase.assertNotWarns()
    def test_apply_filters_success(self):
        request = HttpRequest()
        request.COOKIES = {f"StudyLV-{StudyLV.filter_cookie_name}-name": "2"}
        slv = StudyLV(request=request)
        qs = BSTLVStudyTestModel.objects.all()
        fqs = slv.apply_filters(qs)
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
            slv = StudyLV(request=request)
            fqs = slv.apply_filters(qs)
        self.assertEqual(1, len(aw.warnings))
        self.assertIn("Column 'desc' filter '2' failed", str(aw.warnings[0].message))
        self.assertIn("Column not found", str(aw.warnings[0].message))
        self.assertIn("Resetting filter cookie", str(aw.warnings[0].message))
        self.assertQuerySetEqual(
            BSTLVStudyTestModel.objects.all(),
            fqs,
        )
        self.assertEqual(
            [f"StudyLV-{StudyLV.filter_cookie_name}-desc"], slv.cookie_resets
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_paginate_by(self):
        for n in range(50):
            BSTLVStudyTestModel.objects.create(name=f"ts{n}")

        slv1 = StudyLV()
        qs = slv1.get_queryset()
        with self.assertNumQueries(0):
            self.assertEqual(slv1.paginate_by, slv1.get_paginate_by(qs))

        request = HttpRequest()

        # Sets to the cookie value
        request.COOKIES = {f"StudyLV-{StudyLV.limit_cookie_name}": "30"}
        slv2 = StudyLV(request=request)
        with self.assertNumQueries(1):
            # There is a count query if get_queryset hasn't been called, because slv2.total is 0
            self.assertEqual(30, slv2.get_paginate_by(qs))

        # Defaults to paginate_by if cookie limit is 0
        request.COOKIES = {f"StudyLV-{StudyLV.limit_cookie_name}": "0"}
        slv2 = StudyLV(request=request)
        qs = slv2.get_queryset()
        with self.assertNumQueries(0):
            # There is no count query if get_queryset has been called, because slv2.total is >0
            self.assertEqual(slv1.paginate_by, slv2.get_paginate_by(qs))

        # Sets to the param value
        request.GET = {"limit": "20"}
        slv3 = StudyLV(request=request)
        qs = slv3.get_queryset()
        with self.assertNumQueries(0):
            # There is no count query if get_queryset has been called, because slv2.total is >0
            self.assertEqual(20, slv3.get_paginate_by(qs))

        # Defaults to count if param limit is 0
        request.GET = {"limit": "0"}
        slv4 = StudyLV(request=request)
        qs = slv4.get_queryset()
        with self.assertNumQueries(0):
            # There is no count query if get_queryset has been called, because slv2.total is >0
            self.assertEqual(52, slv4.get_paginate_by(qs))

        # Defaults to count if limit is greater than count
        request.GET = {"limit": "60"}
        slv5 = StudyLV(request=request)
        qs = slv5.get_queryset()
        with self.assertNumQueries(0):
            # There is no count query if get_queryset has been called, because slv2.total is >0
            self.assertEqual(52, slv5.get_paginate_by(qs))

    @TracebaseTestCase.assertNotWarns()
    def test_paginate_queryset(self):
        request = HttpRequest()
        request.COOKIES = {f"StudyLV-{StudyLV.limit_cookie_name}": "1"}
        qs = BSTLVStudyTestModel.objects.all()
        slv = StudyLV(request=request)
        qs = slv.get_queryset()

        with self.assertNumQueries(3):
            # The 3 queries:
            # 1. SELECT COUNT(*) FROM (SELECT DISTINCT "loader_bstlvstudytestmodel"."name" AS "col1", ...
            #    This comes from the super().paginate_queryset call.
            #    The rest come from the get_column_val_by_iteration loop for the many-related record compilation
            # 2. SELECT DISTINCT "loader_bstlvstudytestmodel"."name", ...
            #    This comes from the single iteration of "for rec in object_list"
            # 3. SELECT ("loader_bstlvanimaltestmodel_studies"."bstlvstudytestmodel_id") AS "_prefetch_related_val_...
            #    This comes from the single iteration of "for rec in object_list" (when there is a many-related column)
            paginator, page, object_list, is_paginated = slv.paginate_queryset(qs, 1)

        self.assertEqual(2, slv.raw_total)
        self.assertEqual(2, slv.total)
        self.assertIsInstance(paginator, GracefulPaginator)
        self.assertIsInstance(page, Page)
        self.assertEqual("<Page 1 of 2>", str(page))
        self.assertQuerySetEqual(qs.all()[0:1], object_list)
        self.assertTrue(is_paginated)

    @TracebaseTestCase.assertNotWarns()
    def test_set_list_attr(self):
        # TODO: Move this test to the BSTManyRelatedColumn tests
        alv1 = AnimalDefaultLV()
        studycol: BSTManyRelatedColumn = alv1.columns["studies"]
        with self.assertNumQueries(0):
            studycol.set_list_attr(self.a2, [self.s1, self.s2])
        self.assertTrue(hasattr(self.a2, studycol.list_attr_name))
        self.assertEqual([self.s1, self.s2], getattr(self.a2, studycol.list_attr_name))

        # Now let's adjust the column's limit to the number of many-related values to display and test the ellipsis.
        # We are still sending in 2 study records, because that's what the code outside this does: it retrieves the
        # limit + 1.
        delattr(self.a2, studycol.list_attr_name)
        alv2 = AnimalDefaultLV()
        studycol: BSTManyRelatedColumn = alv2.columns["studies"]
        studycol.limit = 1
        with self.assertNumQueries(0):
            studycol.set_list_attr(self.a2, [self.s1, self.s2])
        # We also haven't added the count annotation, so just a plain ellipsis...
        self.assertEqual([self.s1, "..."], getattr(self.a2, studycol.list_attr_name))

        # Now let's simulate a count annotation having been added as an attribute/annotation
        delattr(self.a2, studycol.list_attr_name)
        setattr(self.a2, studycol.count_attr_name, 2)
        alv3 = AnimalDefaultLV()
        studycol: BSTManyRelatedColumn = alv3.columns["studies"]
        studycol.limit = 1
        with self.assertNumQueries(0):
            studycol.set_list_attr(self.a2, [self.s1, self.s2])
        # We also haven't added the count annotation, so just a plain ellipsis...
        self.assertEqual(
            [self.s1, "... (+1 more)"], getattr(self.a2, studycol.list_attr_name)
        )

        # Let's also try the multiple many-related column case...  We will call via paginate_queryset to do all in 1 go.
        request = HttpRequest()
        request.COOKIES = {
            f"{AnimalWithMultipleStudyColsLV.__name__}-{AnimalWithMultipleStudyColsLV.limit_cookie_name}": "1"
        }
        alv4 = AnimalWithMultipleStudyColsLV(request=request)
        studynamecol: BSTManyRelatedColumn = alv4.columns["studies__name"]
        studynamecol.limit = 1
        studydesccol: BSTManyRelatedColumn = alv4.columns["studies__desc"]
        studydesccol.limit = 1
        qs = alv4.get_queryset()
        with self.assertNumQueries(4):
            # 1. SELECT COUNT(*) FROM (SELECT DISTINCT "loader_bstlvanimaltestmodel"."name" AS "col1", ...
            #    From super().paginate_queryset
            #    All subsequent calls are from a single iteration of object_list
            # 2. SELECT DISTINCT "loader_bstlvanimaltestmodel"."name", ...
            #    From query to get the record from the root model
            # 3. SELECT "loader_bstlvtreatmenttestmodel"."name", ...
            #    From query to get the related treatment model record prefetch
            # 4. SELECT ("loader_bstlvanimaltestmodel_studies"."bstlvanimaltestmodel_id") AS "_prefetch_related_val_...
            #    From query to get the many-related animal model records prefetch
            _, _, object_list, _ = alv4.paginate_queryset(qs, 1)
        # The model's ordering is "-name"
        a2 = object_list[0]
        # The study *name* order is ascending by default (even though the excluded studies column is descending
        # according to the model's _meta.ordering, so we expect s1
        self.assertEqual(
            [self.s1.name, "... (+1 more)"], getattr(a2, studynamecol.list_attr_name)
        )
        self.assertEqual(
            [self.s1.desc, "... (+1 more)"], getattr(a2, studydesccol.list_attr_name)
        )

        # Now clean up so other tests do not fail
        delattr(self.a2, studycol.list_attr_name)
        delattr(self.a2, studycol.count_attr_name)

    @TracebaseTestCase.assertNotWarns()
    def test_get_column_val_by_iteration_with_annot(self):
        alv1 = AnimalDefaultLV()
        qs1 = BSTLVAnimalTestModel.objects.annotate(
            studies_mm_count=Count(
                "studies", distinct=True, output_field=IntegerField()
            )
        ).all()
        namecol = BSTColumn("name", BSTLVAnimalTestModel)
        trtdsccol = BSTRelatedColumn("treatment__desc", BSTLVAnimalTestModel)
        stdynmcol = BSTManyRelatedColumn("studies__name", BSTLVAnimalTestModel)
        stdycntcol = BSTAnnotColumn(
            "studies_mm_count",
            Count("studies", distinct=True, output_field=IntegerField()),
            header="Studies Count",
            filterer="strictFilterer",
            sorter="numericSorter",
        )
        rec1 = qs1.first()
        with self.assertNumQueries(0):
            # There's no need to query, because the field is in an attribute of the record
            self.assertEqual("A1", alv1.get_column_val_by_iteration(rec1, namecol))
        with self.assertNumQueries(1):
            # The related model record wasn't prefetched in qs1
            self.assertEqual("t1", alv1.get_column_val_by_iteration(rec1, trtdsccol))
        with self.assertNumQueries(0):
            # The annotation is in an attribute on the rec, so no query needed
            self.assertEqual(1, alv1.get_column_val_by_iteration(rec1, stdycntcol))
        with self.assertNumQueries(1):
            # 1. SELECT "loader_bstlvstudytestmodel"."name", ...
            #    From "for mr_rec in mr_qs.all()" in _recursive_many_rec_iterator
            self.assertEqual(["S1"], alv1.get_column_val_by_iteration(rec1, stdynmcol))

    @TracebaseTestCase.assertNotWarns()
    def test_get_column_val_by_iteration_annot_excluded(self):
        alv1 = AnimalDefaultLV()
        qs1 = BSTLVAnimalTestModel.objects.order_by("name")
        stdynmcol = BSTManyRelatedColumn("studies__name", BSTLVAnimalTestModel)
        rec1 = qs1.first()
        with self.assertWarns(DeveloperWarning) as aw:
            with self.assertNumQueries(1):
                self.assertEqual(
                    ["S1"], alv1.get_column_val_by_iteration(rec1, stdynmcol)
                )
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "The count annotation for column studies__name is absent",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Cannot guarantee the top 3 records will include the the min/max",
            str(aw.warnings[0].message),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_many_related_column_val_by_subquery(self):
        alv = AnimalWithMultipleStudyColsLV()
        qs = alv.get_queryset()
        rec = qs.first()
        studynamecol: BSTManyRelatedColumn = alv.columns["studies__name"]
        self.assertEqual(3, studynamecol.limit)
        with self.assertNumQueries(1):
            # V1: This was reduced from 3 to 2 queries by adding a prefetch_related
            # Of the 2 queries, one query is the many-related distinct query and the other is the prefetch
            # This could be a single query if the many-related model was the one being queried.
            # V2: This was reduced from 2 to 1 query by using .values_list with .distinct with no arguments (since
            # distinct with args causes an error about the annotation not existing)
            val = alv.get_many_related_column_val_by_subquery(rec, studynamecol)
        # NOTE: NONE OF THIS MATTERS: The animal model is sorted by descending animal name ("-name") [although, we're
        # explicitly supplying animal A2 as rec] and the study model is sorted by descending lower-cased study name.
        # THIS DOESN'T MATTER BECAUSE THE DEFAULT ASC FOR studynamecol IS TRUE, so the EXPECTED RESULT IS ["S1", "S2"]
        self.assertEqual(["S1", "S2"], val)

    @TracebaseTestCase.assertNotWarns()
    def test_get_context_data(self):
        # This creates a GET request.  The URL argument doesn't matter.  We just want the request object, with a little
        # bit of setup.
        request = RequestFactory().get("/")

        # Add the cookie we want.
        request.COOKIES = {f"StudyLV-{StudyLV.filter_cookie_name}-name": "2"}

        # The cookies are handled in the constructor.
        slv = StudyLV(request=request)

        # Perform some manual setup.  slv.object_list would otherwise be set when slv.get(request) is called, but we
        # want to call it directly (for this unit test).
        slv.object_list = slv.get_queryset()[:]

        # This is the method we are testing.
        context = slv.get_context_data()

        self.assertEqual(
            set(
                [
                    # From grandparent class
                    "object_list",
                    "page_obj",
                    "cookie_prefix",
                    "clear_cookies",
                    "is_paginated",
                    "cookie_resets",
                    "paginator",
                    "model",
                    "view",
                    # From ListView
                    "bstlvstudytestmodel_list",  # Same as "object_list"
                    # From parent class
                    "table_id",
                    "table_name",
                    # Query/pagination
                    "sortcol",
                    "asc",
                    "search",
                    "limit",
                    "limit_default",
                    # Problems encountered
                    "warnings",
                    # Column metadata (including column order)
                    "columns",
                    # The remainder are from this class
                    "raw_total",
                    "total",
                ]
            ),
            set(context.keys()),
        )
        # There are 2 study records possible to retrieve.
        self.assertEqual(2, context["raw_total"])

        # The cookie said to search for studies whose name contains "2".  There's only 1 of those.
        self.assertEqual(1, context["total"])

        # Finally, assure there were no warnings.
        self.assertEqual([], context["warnings"])
