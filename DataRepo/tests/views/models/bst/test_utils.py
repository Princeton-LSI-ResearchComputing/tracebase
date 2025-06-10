from django.db.models import CharField

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.views.models.bst.utils import SizedPaginator
from DataRepo.widgets.bst.rows_per_page_select import BSTRowsPerPageSelect

USPStudyTestModel = create_test_model(
    "USPStudyTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
    },
)


class SizedPaginatorTests(TracebaseTestCase):
    def test_constructor(self):
        # Test default options
        for n in range(10):
            USPStudyTestModel.objects.create(name=f"ts{n}")
        qs = USPStudyTestModel.objects.all()

        sp = SizedPaginator(
            10,  # total num records in queryset
            qs,  # queryset needed by superclass
            5,  # per_page needed by superclass
        )

        self.assertEqual(10, sp.total)
        self.assertEqual(10, sp.raw_total)
        self.assertIsInstance(sp.size_select_list, BSTRowsPerPageSelect)
        self.assertEqual("page", sp.page_name)
        self.assertEqual("limit", sp.limit_name)
        self.assertEqual(1, sp.first_row)
        self.assertEqual(5, sp.last_row)
        self.assertFalse(sp.show_first_shortcut)
        self.assertFalse(sp.show_last_shortcut)
        self.assertFalse(sp.show_left_ellipsis)
        self.assertFalse(sp.show_right_ellipsis)

        # Test with all options and enough pages to include first & last shortcuts, but no ellipses (9 pages, 45 recs)
        # when on the middle page (5)
        for n in range(10, 45):
            USPStudyTestModel.objects.create(name=f"ts{n}")
        qs = USPStudyTestModel.objects.all()

        sp = SizedPaginator(
            45,  # total num records in queryset
            qs,
            5,
            raw_total=50,
            page=5,
            page_name="p",
            limit_name="rpp",
        )

        self.assertEqual(45, sp.total)
        self.assertEqual(50, sp.raw_total)
        self.assertEqual("p", sp.page_name)
        self.assertEqual("rpp", sp.limit_name)
        self.assertEqual(21, sp.first_row)
        self.assertEqual(25, sp.last_row)
        self.assertTrue(sp.show_first_shortcut)
        self.assertTrue(sp.show_last_shortcut)
        self.assertFalse(sp.show_left_ellipsis)
        self.assertFalse(sp.show_right_ellipsis)

        # Test with enough pages to include ellipses (11 pages, 55 recs) when on the middle page (6)
        for n in range(45, 55):
            USPStudyTestModel.objects.create(name=f"ts{n}")
        qs = USPStudyTestModel.objects.all()

        sp = SizedPaginator(
            55,  # total num records in queryset
            qs,
            5,
            page=6,
        )

        self.assertTrue(sp.show_left_ellipsis)
        self.assertTrue(sp.show_right_ellipsis)
