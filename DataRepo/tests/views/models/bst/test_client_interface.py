from django.db.models import CharField
from django.db.models.functions import Lower
from django.http import HttpRequest
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.client_interface import BSTClientInterface
from DataRepo.views.models.bst.utils import SizedPaginator

BCIStudyTestModel = create_test_model(
    "BCIStudyTestModel",
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


class StudyBCI(BSTClientInterface):
    model = BCIStudyTestModel


@override_settings(DEBUG=True)
class BSTClientInterfaceTests(TracebaseTestCase):
    @TracebaseTestCase.assertNotWarns()
    def test_init(self):
        c = BSTClientInterface()
        self.assertEqual("BSTClientInterface-", c.cookie_prefix)
        self.assertEqual([], c.warnings)
        self.assertEqual([], c.cookie_resets)
        self.assertFalse(c.clear_cookies)

        class MyBSTListView(BSTClientInterface):
            pass

        m = MyBSTListView()
        self.assertEqual("MyBSTListView-", m.cookie_prefix)

    @TracebaseTestCase.assertNotWarns()
    def test_get_cookie_name(self):
        c = BSTClientInterface()
        self.assertEqual("BSTClientInterface-cname", c.get_cookie_name("cname"))

    @TracebaseTestCase.assertNotWarns()
    def test_get_cookie(self):
        request = HttpRequest()
        view_cookie_name = "cname"
        request.COOKIES = {f"BSTClientInterface-{view_cookie_name}": "test_value"}
        c = BSTClientInterface(request=request)
        self.assertEqual("test_value", c.get_cookie(view_cookie_name))
        request.COOKIES = {f"BSTClientInterface-{view_cookie_name}": ""}
        self.assertIsNone(c.get_cookie(view_cookie_name))
        request.COOKIES = {}
        self.assertIsNone(c.get_cookie(view_cookie_name))
        request.COOKIES = {}
        self.assertEqual("mydef", c.get_cookie(view_cookie_name, default="mydef"))
        request.COOKIES = {f"BSTClientInterface-{view_cookie_name}": ""}
        self.assertEqual("mydef", c.get_cookie(view_cookie_name, default="mydef"))

    @TracebaseTestCase.assertNotWarns()
    def test_get_boolean_cookie_success(self):
        request = HttpRequest()
        view_cookie_name = "cname"
        request.COOKIES = {f"BSTClientInterface-{view_cookie_name}": "TRUE"}
        c = BSTClientInterface(request=request)
        self.assertTrue(c.get_boolean_cookie(view_cookie_name))
        request.COOKIES = {f"BSTClientInterface-{view_cookie_name}": "false"}
        self.assertFalse(c.get_boolean_cookie(view_cookie_name))
        # default is False
        request.COOKIES = {f"BSTClientInterface-{view_cookie_name}": ""}
        self.assertFalse(c.get_boolean_cookie(view_cookie_name))
        # explicit default
        request.COOKIES = {f"BSTClientInterface-{view_cookie_name}": ""}
        self.assertTrue(c.get_boolean_cookie(view_cookie_name, default=True))
        # cookie absent
        request.COOKIES = {}
        self.assertFalse(c.get_boolean_cookie(view_cookie_name))
        request.COOKIES = {}
        self.assertTrue(c.get_boolean_cookie(view_cookie_name, default=True))

    # NOTE: This only doesn't warn because the warning is captured by assertWarns, but this assures the second call to
    # get_boolean_cookie does not warn
    @TracebaseTestCase.assertNotWarns()
    def test_get_boolean_cookie_warn(self):
        request = HttpRequest()
        view_cookie_name = "cname"
        request.COOKIES = {f"BSTClientInterface-{view_cookie_name}": "wrong"}
        c = BSTClientInterface(request=request)
        with self.assertWarns(DeveloperWarning) as aw:
            val = c.get_boolean_cookie(view_cookie_name)
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            f"Invalid '{view_cookie_name}' value encountered: 'wrong'.",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            f"Clearing cookie 'BSTClientInterface-{view_cookie_name}'.",
            str(aw.warnings[0].message),
        )
        self.assertFalse(val)
        self.assertEqual(1, len(c.warnings))
        self.assertEqual(1, len(c.cookie_resets))
        self.assertEqual(str(aw.warnings[0].message), c.warnings[0])
        self.assertEqual(f"BSTClientInterface-{view_cookie_name}", c.cookie_resets[0])

        # second occurrence does not warn
        val = c.get_boolean_cookie(view_cookie_name, default=True)
        self.assertTrue(val)
        # The rest has not changed...
        self.assertEqual(1, len(c.warnings))
        self.assertEqual(str(aw.warnings[0].message), c.warnings[0])
        self.assertEqual(1, len(c.cookie_resets))
        self.assertEqual(f"BSTClientInterface-{view_cookie_name}", c.cookie_resets[0])

    @TracebaseTestCase.assertNotWarns()
    def test_get_column_cookie_name(self):
        c = BSTClientInterface()
        self.assertEqual(
            "BSTClientInterface-cname-column1",
            c.get_column_cookie_name("column1", "cname"),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_column_cookie_dict(self):
        request = HttpRequest()
        request.COOKIES = {
            "BSTClientInterface-visible-column1": "a",
            "BSTClientInterface-filter-column1": "b",
            "BSTClientInterface-visible-column2": "c",
            "BSTClientInterface-filter-column2": "d",
        }
        with self.assertWarns(DeveloperWarning) as aw:
            c = BSTClientInterface(request=request)
        self.assertIn(
            "Invalid 'visible' cookie value encountered for column 'column1': 'a'",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Invalid 'visible' cookie value encountered for column 'column2': 'c'",
            str(aw.warnings[1].message),
        )
        self.assertDictEqual(
            {"column1": "a", "column2": "c"},
            c.get_column_cookie_dict("visible"),
        )
        self.assertDictEqual(
            {"column1": "b", "column2": "d"},
            c.get_column_cookie_dict("filter"),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_boolean_column_cookie_dict_success(self):
        request = HttpRequest()
        request.COOKIES = {
            "BSTClientInterface-visible-column1": "T",
            "BSTClientInterface-visible-column2": "F",
        }
        c = BSTClientInterface(request=request)
        self.assertDictEqual(
            {"column1": True, "column2": False},
            c.get_boolean_column_cookie_dict("visible"),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_boolean_column_cookie_dict_warn(self):
        request = HttpRequest()
        request.COOKIES = {
            "BSTClientInterface-filter-column1": "b",
            "BSTClientInterface-filter-column2": "d",
        }
        c = BSTClientInterface(request=request)
        with self.assertWarns(DeveloperWarning) as aw:
            c.get_boolean_column_cookie_dict("filter")
        self.assertEqual(2, len(aw.warnings))
        self.assertEqual(2, len(c.warnings))
        self.assertEqual(2, len(c.cookie_resets))
        self.assertIn("BSTClientInterface-filter-column1", c.cookie_resets)
        self.assertIn("BSTClientInterface-filter-column2", c.cookie_resets)

    @TracebaseTestCase.assertNotWarns()
    def test_get_column_cookie(self):
        view_cookie_name = "cname"
        column_name = "column1"
        request = HttpRequest()
        request.COOKIES = {
            f"BSTClientInterface-{view_cookie_name}-{column_name}": "testval"
        }
        c = BSTClientInterface(request=request)
        self.assertIsNone(c.get_column_cookie("column2", view_cookie_name))
        self.assertEqual(
            "mydef", c.get_column_cookie("column2", view_cookie_name, default="mydef")
        )
        self.assertIsNone(c.get_column_cookie(column_name, "unset"))
        self.assertEqual(
            "mydef", c.get_column_cookie(column_name, "unset", default="mydef")
        )
        request.COOKIES = {}
        self.assertIsNone(c.get_column_cookie(column_name, view_cookie_name))
        self.assertEqual(
            "mydef", c.get_column_cookie(column_name, view_cookie_name, default="mydef")
        )

    def test_get_param(self):
        param_name = "cname"
        request = HttpRequest()
        request.GET = {param_name: "x"}
        c = BSTClientInterface(request=request)
        self.assertEqual("x", c.get_param(param_name))
        self.assertEqual("mydef", c.get_param("notthere", default="mydef"))
        c.request.GET.update({"cname": ""})
        self.assertIsNone(c.get_param(param_name))
        self.assertEqual("mydef", c.get_param("notthere", default="mydef"))

    @TracebaseTestCase.assertNotWarns()
    def test_reset_column_cookies(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                "BSTClientInterface-visible-name": "true",
                "BSTClientInterface-visible-desc": "false",
                "BSTClientInterface-search": "",
                "BSTClientInterface-filter-name": "",
                "BSTClientInterface-filter-desc": "description",
                "BSTClientInterface-sortcol": "name",
                "BSTClientInterface-asc": "false",
            }
        )
        request.GET.update({"limit": "20"})
        bci = BSTClientInterface(request=request)
        bci.reset_column_cookies(["name", "desc"], "visible")
        # Only deletes the ones that are "set" (and empty string is eval'ed as None)
        self.assertEqual(
            ["BSTClientInterface-visible-name", "BSTClientInterface-visible-desc"],
            bci.cookie_resets,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_reset_cookie(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                "BSTClientInterface-visible-name": "true",
                "BSTClientInterface-visible-desc": "false",
                "BSTClientInterface-filter-name": "",
                "BSTClientInterface-filter-desc": "description",
                "BSTClientInterface-search": "",
                "BSTClientInterface-sortcol": "name",
                "BSTClientInterface-asc": "false",
            }
        )
        request.GET.update({"limit": "20"})
        bci = BSTClientInterface(request=request)
        bci.reset_cookie("sortcol")
        # Only deletes the ones that are "set" (and empty string is eval'ed as None)
        self.assertEqual(["BSTClientInterface-sortcol"], bci.cookie_resets)

    def test_model_title_plural(self):
        self.assertEqual("BCI Study Test Models", StudyBCI.model_title_plural)

    def test_model_title(self):
        self.assertEqual("BCI Study Test Model", StudyBCI.model_title)

    @TracebaseTestCase.assertNotWarns()
    def test_reset_filter_cookies(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                "StudyBCI-visible-name": "true",
                "StudyBCI-visible-desc": "false",
                "StudyBCI-filter-name": "",
                "StudyBCI-filter-desc": "description",
                "StudyBCI-search": "",
                "StudyBCI-sortcol": "name",
                "StudyBCI-asc": "false",
            }
        )
        request.GET.update({"limit": "20"})
        slv = StudyBCI(request=request)
        slv.reset_filter_cookies()
        # Only deletes the ones that are "set" (and empty string is eval'ed as None)
        self.assertEqual(["StudyBCI-filter-desc"], slv.cookie_resets)

    @TracebaseTestCase.assertNotWarns()
    def test_reset_search_cookie(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                "StudyBCI-visible-name": "true",
                "StudyBCI-visible-desc": "false",
                "StudyBCI-filter-name": "",
                "StudyBCI-filter-desc": "description",
                "StudyBCI-search": "term",
                "StudyBCI-sortcol": "name",
                "StudyBCI-asc": "false",
            }
        )
        request.GET.update({"limit": "20"})
        slv = StudyBCI(request=request)
        slv.reset_search_cookie()
        self.assertEqual(["StudyBCI-search"], slv.cookie_resets)

    @TracebaseTestCase.assertNotWarns()
    def test_get_context_data(self):
        request = HttpRequest()
        bci = BSTClientInterface(request=request)
        bci.object_list = []
        context = bci.get_context_data()
        self.assertEqual(
            set(
                [
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
                    "search",
                    "table_id",
                    "warnings",
                    "asc",
                    "limit",
                    "sortcol",
                    "total",
                    "raw_total",
                    "limit_default",
                    "table_name",
                ]
            ),
            set(context.keys()),
        )
        self.assertEqual("BSTClientInterface-", context["cookie_prefix"])
        self.assertFalse(context["clear_cookies"])
        self.assertEqual([], context["cookie_resets"])
        self.assertIsNone(context["model"])

    @TracebaseTestCase.assertNotWarns()
    def test_get_paginate_by(self):
        for n in range(50):
            BCIStudyTestModel.objects.create(name=f"ts{n}")

        slv1 = StudyBCI()
        qs = slv1.get_queryset()
        with self.assertNumQueries(0):
            self.assertEqual(slv1.paginate_by, slv1.get_paginate_by(qs))

        request = HttpRequest()

        # Sets to the cookie value
        request.COOKIES = {f"StudyBCI-{StudyBCI.limit_cookie_name}": "30"}
        slv2 = StudyBCI(request=request)
        with self.assertNumQueries(1):
            # There is a count query if get_queryset hasn't been called, because slv2.total is 0
            self.assertEqual(30, slv2.get_paginate_by(qs))

        # Defaults to paginate_by if cookie limit is 0
        request.COOKIES = {f"StudyBCI-{StudyBCI.limit_cookie_name}": "0"}
        slv2 = StudyBCI(request=request)
        qs = slv2.get_queryset()
        with self.assertNumQueries(0):
            # There is no count query if get_queryset has been called, because slv2.total is >0
            self.assertEqual(slv1.paginate_by, slv2.get_paginate_by(qs))

        # Sets to the param value
        request.GET = {"limit": "20"}
        slv3 = StudyBCI(request=request)
        qs = slv3.get_queryset()
        with self.assertNumQueries(0):
            # There is no count query if get_queryset has been called, because slv2.total is >0
            self.assertEqual(20, slv3.get_paginate_by(qs))

        # Defaults to count if param limit is 0
        request.GET = {"limit": "0"}
        slv4 = StudyBCI(request=request)
        qs = slv4.get_queryset()
        with self.assertNumQueries(0):
            # There is no count query if get_queryset has been called, because slv2.total is >0
            self.assertEqual(50, slv4.get_paginate_by(qs))

        # Defaults to count if limit is greater than count
        request.GET = {"limit": "60"}
        slv5 = StudyBCI(request=request)
        qs = slv5.get_queryset()
        with self.assertNumQueries(0):
            # There is no count query if get_queryset has been called, because slv2.total is >0
            self.assertEqual(50, slv5.get_paginate_by(qs))

    @TracebaseTestCase.assertNotWarns()
    def test_get_queryset(self):
        for n in range(2):
            BCIStudyTestModel.objects.create(name=f"ts{n}")
        slv = StudyBCI()
        with self.assertNumQueries(1):
            # The count query
            qs = slv.get_queryset()
        self.assertQuerySetEqual(
            BCIStudyTestModel.objects.distinct(), qs, ordered=False
        )
        self.assertEqual(2, slv.raw_total)
        self.assertEqual(2, slv.total)

    @TracebaseTestCase.assertNotWarns()
    def test_get_paginator(self):
        for n in range(10):
            BCIStudyTestModel.objects.create(name=f"ts{n}")
        slv = StudyBCI()
        qs = slv.get_queryset()
        pgntr = slv.get_paginator(qs, 5)
        self.assertIsInstance(pgntr, SizedPaginator)
