from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.utils import (
    GracefulPaginator,
    get_cookie_dict,
    reduceuntil,
)


class UtilsMainTests(TracebaseTestCase):

    def test_reduceuntil(self):
        input_list = [2, 2, 2, 2, 2, 3, 4, 5, 6, 7]
        max_unique_len = 2
        self.assertEqual(
            [2, 3],
            reduceuntil(
                lambda ulst, val: ulst + [val] if val not in ulst else ulst,
                lambda val: len(val) >= max_unique_len,
                input_list,
                [],
            ),
        )

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
