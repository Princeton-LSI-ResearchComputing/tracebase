from django.http import HttpRequest

from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.utils import (
    GracefulPaginator,
    delete_cookie,
    get_cookie,
    get_cookie_dict,
)


class UtilsMainTests(TracebaseTestCase):

    def test_GracefulPaginator(self):
        Study.objects.create(name="1")
        Study.objects.create(name="2")
        # 2 records, 1 record/row per page, makes 2 pages
        gp = GracefulPaginator(Study.objects.all(), 1)
        self.assertEqual("<Page 1 of 2>", str(gp.get_page("x")))
        self.assertEqual("<Page 2 of 2>", str(gp.get_page(5)))

    def test_get_cookie_dict(self):
        class Request:
            COOKIES = {
                "abc": "1",
                "abd": "2",
                "abe": "",
                "ab": "3",
                "xyz": "3",
                "lmn": "",
            }

        request = Request()
        expected = {
            "abc": "1",
            "abd": "2",
            "abe": None,
            "ab": "3",
            "xyz": "3",
            "lmn": None,
        }
        self.assertDictEqual(expected, get_cookie_dict(request, exclude_empties=False))
        expected2 = expected.copy()
        expected2.pop("lmn")
        expected2.pop("abe")
        self.assertDictEqual(expected2, get_cookie_dict(request))
        expected3 = {"c": "1", "d": "2"}
        self.assertDictEqual(expected3, get_cookie_dict(request, prefix="ab"))
        expected4 = {"c": "1", "d": "2", "e": None}
        self.assertDictEqual(
            expected4, get_cookie_dict(request, prefix="ab", exclude_empties=False)
        )

    def test_get_cookie(self):
        request = HttpRequest()
        request.COOKIES = {"cname": "value", "blank": ""}
        self.assertEqual("value", get_cookie(request, "cname"))
        self.assertEqual("def", get_cookie(request, "notset", "def"))
        self.assertEqual("def", get_cookie(request, "blank", "def"))

    def test_delete_cookie(self):
        request = HttpRequest()
        request.COOKIES = {"cname": "value", "blank": ""}
        self.assertEqual("value", delete_cookie(request, "cname"))
        self.assertDictEqual({"blank": ""}, request.COOKIES)
        self.assertIsNone(delete_cookie(request, "notset"))
        self.assertDictEqual({"blank": ""}, request.COOKIES)
        self.assertIsNone(delete_cookie(request, "blank"))
        self.assertDictEqual({}, request.COOKIES)
