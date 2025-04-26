from django.core.paginator import Page
from django.db.models import (
    CASCADE,
    CharField,
    ForeignKey,
    IntegerField,
    ManyToManyField,
    Q,
)
from django.db.models.aggregates import Count, Max
from django.db.models.functions import Lower, Upper
from django.http import HttpRequest
from django.test import override_settings

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
        self.assertEqual(0, len(slv.prefilter_annots.keys()))

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
        self.assertEqual(0, len(slv.prefilter_annots.keys()))
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
        self.assertEqual(0, len(slv.prefilter_annots.keys()))
        self.assertDictEquivalent(
            {
                "animals_mm_count": Count("animals", output_field=IntegerField()),
                "description": Upper("desc", output_field=CharField()),
            },
            slv.postfilter_annots,
        )
        self.assertEqual(
            Lower("name").desc(nulls_last=True),
            slv.columns[slv.sort_name].sorter.order_by,
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
        alv = AnimalWithMultipleStudyColsLV()
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.distinct(), alv.get_queryset(), ordered=False
        )

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
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.filter(studies__name__icontains="s")
            .order_by("-name")
            .distinct(),
            alv1.get_queryset(),
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
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.filter(studies__name__icontains="2")
            .order_by("name")
            .distinct(),
            alv2.get_queryset(),
        )

        request.COOKIES = {
            f"{AnimalWithMultipleStudyColsLV.__name__}-"
            f"{AnimalWithMultipleStudyColsLV.filter_cookie_name}-treatment": "ball",
        }
        alv3 = AnimalWithMultipleStudyColsLV(request=request)
        self.assertQuerySetEqual(
            BSTLVAnimalTestModel.objects.filter(
                treatment__name__icontains="ball"
            ).distinct(),
            alv3.get_queryset(),
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
        self.assertDictEqual({}, before)
        self.assertDictEquivalent(
            {
                "animals_mm_count": Count("animals", output_field=IntegerField()),
                "description": Upper("desc", output_field=CharField()),
            },
            after,
        )

        # Search
        request = HttpRequest()
        request.COOKIES.update({"StudyLV-search": "test1"})
        alv2 = StudyLV(request=request)
        before, after = alv2.get_annotations()
        self.assertDictEquivalent(
            {
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
            {"description": Upper("desc", output_field=CharField())}, before
        )
        self.assertDictEquivalent(
            {"animals_mm_count": Count("animals", output_field=IntegerField())}, after
        )

        # No search or filter (but cookies)
        request.COOKIES = {"StudyLV-asc": "false"}
        alv4 = StudyLV(request=request)
        before, after = alv4.get_annotations()
        self.assertDictEqual({}, before)
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
    def test__lower(self):
        self.assertEqual("test string", BSTListView._lower("Test String"))
        self.assertEqual(5, BSTListView._lower(5))
        self.assertIsNone(BSTListView._lower(None))

    @TracebaseTestCase.assertNotWarns()
    def test_get_paginate_by(self):
        for n in range(50):
            BSTLVStudyTestModel.objects.create(name=f"ts{n}")

        slv1 = StudyLV()
        qs = slv1.get_queryset()
        self.assertEqual(slv1.paginate_by, slv1.get_paginate_by(qs))

        request = HttpRequest()

        # Sets to the cookie value
        request.COOKIES = {f"StudyLV-{StudyLV.limit_cookie_name}": "30"}
        slv2 = StudyLV(request=request)
        self.assertEqual(30, slv2.get_paginate_by(qs))

        # Defaults to paginate_by if cookie limit is 0
        request.COOKIES = {f"StudyLV-{StudyLV.limit_cookie_name}": "0"}
        slv2 = StudyLV(request=request)
        qs = slv2.get_queryset()
        self.assertEqual(slv1.paginate_by, slv2.get_paginate_by(qs))

        # Sets to the param value
        request.GET = {"limit": "20"}
        slv3 = StudyLV(request=request)
        qs = slv3.get_queryset()
        self.assertEqual(20, slv3.get_paginate_by(qs))

        # Defaults to count if param limit is 0
        request.GET = {"limit": "0"}
        slv4 = StudyLV(request=request)
        qs = slv4.get_queryset()
        self.assertEqual(52, slv4.get_paginate_by(qs))

        # Defaults to count if limit is greater than count
        request.GET = {"limit": "60"}
        slv5 = StudyLV(request=request)
        qs = slv5.get_queryset()
        self.assertEqual(52, slv5.get_paginate_by(qs))

    @TracebaseTestCase.assertNotWarns()
    def test_paginate_queryset(self):
        request = HttpRequest()
        request.COOKIES = {f"StudyLV-{StudyLV.limit_cookie_name}": "1"}
        qs = BSTLVStudyTestModel.objects.all()
        slv = StudyLV(request=request)
        qs = slv.get_queryset()
        paginator, page, object_list, is_paginated = slv.paginate_queryset(qs, 1)
        self.assertIsInstance(paginator, GracefulPaginator)
        self.assertIsInstance(page, Page)
        self.assertEqual("<Page 1 of 2>", str(page))
        self.assertQuerySetEqual(qs.all()[0:1], object_list)
        self.assertTrue(is_paginated)

    @TracebaseTestCase.assertNotWarns()
    def test_set_many_related_records_list(self):
        alv1 = AnimalDefaultLV()
        studycol: BSTManyRelatedColumn = alv1.columns["studies"]
        alv1.set_many_related_records_list(self.a2, studycol, [self.s1, self.s2])
        self.assertTrue(hasattr(self.a2, studycol.list_attr_name))
        self.assertEqual([self.s1, self.s2], getattr(self.a2, studycol.list_attr_name))

        # Now let's adjust the column's limit to the number of many-related values to display and test the ellipsis.
        # We are still sending in 2 study records, because that's what the code outside this does: it retrieves the
        # limit + 1.
        delattr(self.a2, studycol.list_attr_name)
        alv2 = AnimalDefaultLV()
        studycol: BSTManyRelatedColumn = alv2.columns["studies"]
        studycol.limit = 1
        alv2.set_many_related_records_list(self.a2, studycol, [self.s1, self.s2])
        # We also haven't added the count annotation, so just a plain ellipsis...
        self.assertEqual([self.s1, "..."], getattr(self.a2, studycol.list_attr_name))

        # Now let's simulate a count annotation having been added as an attribute/annotation
        delattr(self.a2, studycol.list_attr_name)
        setattr(self.a2, studycol.count_attr_name, 2)
        alv3 = AnimalDefaultLV()
        studycol: BSTManyRelatedColumn = alv3.columns["studies"]
        studycol.limit = 1
        alv3.set_many_related_records_list(self.a2, studycol, [self.s1, self.s2])
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
    def test_get_rec_val_by_iteration_with_annot(self):
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
        self.assertEqual("A1", alv1.get_rec_val_by_iteration(rec1, namecol))
        self.assertEqual("t1", alv1.get_rec_val_by_iteration(rec1, trtdsccol))
        self.assertEqual(1, alv1.get_rec_val_by_iteration(rec1, stdycntcol))
        self.assertEqual(["S1"], alv1.get_rec_val_by_iteration(rec1, stdynmcol))

    @TracebaseTestCase.assertNotWarns()
    def test_get_rec_val_by_iteration_annot_excluded(self):
        alv1 = AnimalDefaultLV()
        qs1 = BSTLVAnimalTestModel.objects.order_by("name")
        stdynmcol = BSTManyRelatedColumn("studies__name", BSTLVAnimalTestModel)
        rec1 = qs1.first()
        with self.assertWarns(DeveloperWarning) as aw:
            self.assertEqual(["S1"], alv1.get_rec_val_by_iteration(rec1, stdynmcol))
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
    def test__get_rec_val_by_iteration_helper(self):
        alv1 = AnimalDefaultLV()
        val, sval, id = alv1._get_rec_val_by_iteration_helper(
            self.a1,
            ["treatment"],
            sort_field_path=["treatment", "name"],
        )
        self.assertEqual(self.t1, val)
        # Whether this is lower-cased or not, it does not matter.  The sort value and unique value are only present to
        # be compatible with the many-related companion recursive path.  It might not even be necessary, since I split
        # up the methods... so I should look into the possibility of remove it.
        self.assertEqual("T1", sval)
        self.assertIsInstance(id, int)

        alv2 = AnimalDefaultLV()
        vals = alv2._get_rec_val_by_iteration_helper(
            self.a2,
            ["studies"],
            related_limit=2,
            sort_field_path=["studies", "name"],
        )
        expected1 = set([self.s2, self.s1])
        expected2 = set(["s1", "s2"])
        vals1 = set([v[0] for v in vals])
        vals2 = set([v[1] for v in vals])
        vals3 = set([v[2] for v in vals])
        self.assertEqual(expected1, vals1)
        self.assertEqual(expected2, vals2)
        self.assertTrue(all(isinstance(v3, int) for v3 in vals3))

    @TracebaseTestCase.assertNotWarns()
    def test__get_rec_val_by_iteration_single_helper(self):
        alv = AnimalDefaultLV()
        val, sval, id = alv._get_rec_val_by_iteration_single_helper(
            self.a1,
            ["treatment"],
            sort_field_path=["treatment", "name"],
        )
        self.assertEqual(self.t1, val)
        # Whether this is lower-cased or not, it does not matter.  The sort value and unique value are only present to
        # be compatible with the many-related companion recursive path.  It might not even be necessary, since I split
        # up the methods... so I should look into the possibility of remove it.
        self.assertEqual("T1", sval)
        self.assertIsInstance(id, int)

    @TracebaseTestCase.assertNotWarns()
    def test__get_rec_val_by_iteration_many_helper(self):
        alv = AnimalDefaultLV()
        vals = alv._get_rec_val_by_iteration_many_helper(
            self.a2,
            ["studies"],
            related_limit=2,
            sort_field_path=["studies", "name"],
        )
        expected2 = set(["s1", "s2"])
        expected1 = set([self.s2, self.s1])
        vals1 = set([v[0] for v in vals])
        vals2 = set([v[1] for v in vals])
        vals3 = set([v[2] for v in vals])
        self.assertEqual(expected2, vals2)
        self.assertEqual(expected1, vals1)
        self.assertTrue(all(isinstance(v3, int) for v3 in vals3))

    @TracebaseTestCase.assertNotWarns()
    def test__last_many_rec_iterator(self):
        alv = AnimalDefaultLV()
        mr_qs = BSTLVStudyTestModel.objects.all()
        iterator = iter(alv._last_many_rec_iterator(mr_qs, ["name"]))
        expected1 = set([self.s2, self.s1])
        # The names are lower-cased
        expected2 = set(["s1", "s2"])
        val1 = next(iterator)
        val2 = next(iterator)
        vals1 = set([val1[0], val2[0]])
        vals2 = set([val1[1], val2[1]])
        vals3 = set([val1[2], val2[2]])
        self.assertEqual(expected1, vals1)
        self.assertEqual(expected2, vals2)
        self.assertTrue(all(isinstance(v3, int) for v3 in vals3))
        with self.assertRaises(StopIteration):
            next(iterator)

    @TracebaseTestCase.assertNotWarns()
    def test__recursive_many_rec_iterator(self):
        alv = AnimalWithMultipleStudyColsLV()
        mr_qs = BSTLVStudyTestModel.objects.all()
        iterator = iter(
            alv._recursive_many_rec_iterator(mr_qs, ["name"], ["name"], 2, None)
        )
        expected = set([("S2", "S2", "S2"), ("S1", "S1", "S1")])
        vals = set(
            [
                next(iterator),
                next(iterator),
            ]
        )
        self.assertEqual(expected, vals)
        with self.assertRaises(StopIteration):
            next(iterator)

    @TracebaseTestCase.assertNotWarns()
    def test_get_many_related_rec_val_by_subquery(self):
        # TODO: Implement test
        # BUG: CURRENTLY BROKEN
        pass

    @TracebaseTestCase.assertNotWarns()
    def test__get_many_related_rec_val_by_subquery_helper(self):
        # TODO: Implement test
        # BUG: CURRENTLY BROKEN
        pass
