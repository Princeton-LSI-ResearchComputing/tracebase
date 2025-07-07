from django.db.models import CharField
from django.db.models.functions import Lower
from django.http import HttpRequest
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.client_interface import (
    BSTClientInterface,
    BSTDetailViewClient,
    BSTListViewClient,
)
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


class BSTClientInterfaceTests(TracebaseTestCase):
    def test_BSTClientInterface(self):
        ci = BSTClientInterface()
        self.assertEqual("model", ci.model_var_name)
        self.assertEqual("table_id", ci.table_id_var_name)
        self.assertEqual("table_name", ci.title_var_name)
        self.assertEqual("columns", ci.columns_var_name)
        self.assertEqual("warnings", ci.warnings_var_name)
        self.assertEqual([], ci.warnings)


class StudyDetailBCI(BSTDetailViewClient):
    model = BCIStudyTestModel


class BSTDetailViewClientTests(TracebaseTestCase):
    def test_BSTDetailViewClient(self):
        dvc = BSTDetailViewClient()
        self.assertEqual("models/bst/detail_view.html", dvc.template_name)

    def test_model_title(self):
        self.assertEqual("BCI Study Test Model", StudyDetailBCI.model_title)

    def test_get_context_data(self):
        request = HttpRequest()
        dvc = BSTDetailViewClient(request=request)
        dvc.object = []
        context = dvc.get_context_data()
        self.assertEqual(
            set(
                [
                    "model",
                    "view",
                    "table_id",
                    "table_name",
                ]
            ),
            set(context.keys()),
        )
        # Not a standard Paginator.  Having is_paginated=None prevents the base.html template from rendering the vanilla
        # paginator under the SizedPaginator
        self.assertIsNone(context["model"])


class StudyBCI(BSTListViewClient):
    model = BCIStudyTestModel


@override_settings(DEBUG=True)
class BSTListViewClientTests(TracebaseTestCase):
    @TracebaseTestCase.assertNotWarns()
    def test_init(self):
        c = BSTListViewClient()
        self.assertEqual("BSTListViewClient-", c.cookie_prefix)
        self.assertEqual([], c.warnings)
        self.assertEqual([], c.cookie_resets)
        self.assertFalse(c.clear_cookies)

        class MyBSTListView(BSTListViewClient):
            pass

        m = MyBSTListView()
        self.assertEqual("MyBSTListView-", m.cookie_prefix)

    @TracebaseTestCase.assertNotWarns()
    def test_get_cookie_name(self):
        c = BSTListViewClient()
        self.assertEqual("BSTListViewClient-cname", c.get_cookie_name("cname"))

    @TracebaseTestCase.assertNotWarns()
    def test_get_cookie(self):
        request = HttpRequest()
        view_cookie_name = "cname"
        request.COOKIES = {f"BSTListViewClient-{view_cookie_name}": "test_value"}
        c = BSTListViewClient(request=request)
        c.init_interface()
        self.assertEqual("test_value", c.get_cookie(view_cookie_name))
        request.COOKIES = {f"BSTListViewClient-{view_cookie_name}": ""}
        self.assertIsNone(c.get_cookie(view_cookie_name))
        request.COOKIES = {}
        self.assertIsNone(c.get_cookie(view_cookie_name))
        request.COOKIES = {}
        self.assertEqual("mydef", c.get_cookie(view_cookie_name, default="mydef"))
        request.COOKIES = {f"BSTListViewClient-{view_cookie_name}": ""}
        self.assertEqual("mydef", c.get_cookie(view_cookie_name, default="mydef"))

    @TracebaseTestCase.assertNotWarns()
    def test_get_boolean_cookie_success(self):
        request = HttpRequest()
        view_cookie_name = "cname"
        request.COOKIES = {f"BSTListViewClient-{view_cookie_name}": "TRUE"}
        c = BSTListViewClient(request=request)
        c.init_interface()
        self.assertTrue(c.get_boolean_cookie(view_cookie_name))
        request.COOKIES = {f"BSTListViewClient-{view_cookie_name}": "false"}
        self.assertFalse(c.get_boolean_cookie(view_cookie_name))
        # default is False
        request.COOKIES = {f"BSTListViewClient-{view_cookie_name}": ""}
        self.assertFalse(c.get_boolean_cookie(view_cookie_name))
        # explicit default
        request.COOKIES = {f"BSTListViewClient-{view_cookie_name}": ""}
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
        request.COOKIES = {f"BSTListViewClient-{view_cookie_name}": "wrong"}
        c = BSTListViewClient(request=request)
        c.init_interface()
        with self.assertWarns(DeveloperWarning) as aw:
            val = c.get_boolean_cookie(view_cookie_name)
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            f"Invalid '{view_cookie_name}' value encountered: 'wrong'.",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "Resetting cookie",
            str(aw.warnings[0].message),
        )
        self.assertFalse(val)
        self.assertEqual(1, len(c.warnings))
        self.assertEqual(1, len(c.cookie_resets))
        # The warning message gives the full cookie name, which the user does not need to know.
        self.assertEqual(
            str(aw.warnings[0].message), c.warnings[0] + "  'BSTListViewClient-cname'"
        )
        self.assertEqual(view_cookie_name, c.cookie_resets[0])

        # second occurrence does not warn
        val = c.get_boolean_cookie(view_cookie_name, default=True)
        self.assertTrue(val)
        # The rest has not changed...
        self.assertEqual(
            1,
            len(c.warnings),
            msg=f"Cookie: {view_cookie_name} Resets: {c.cookie_resets} Warnings: {c.warnings}",
        )
        # The warning message gives the full cookie name, which the user does not need to know.
        self.assertEqual(
            str(aw.warnings[0].message), c.warnings[0] + "  'BSTListViewClient-cname'"
        )
        self.assertEqual(1, len(c.cookie_resets))
        self.assertEqual(view_cookie_name, c.cookie_resets[0])

    @TracebaseTestCase.assertNotWarns()
    def test_get_column_cookie_name(self):
        c = BSTListViewClient()
        self.assertEqual(
            "BSTListViewClient-cname-column1",
            c.get_column_cookie_name("column1", "cname"),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_column_cookie_dict(self):
        request = HttpRequest()
        request.COOKIES = {
            "BSTListViewClient-visible-column1": "a",
            "BSTListViewClient-filter-column1": "b",
            "BSTListViewClient-visible-column2": "c",
            "BSTListViewClient-filter-column2": "d",
        }
        with self.assertWarns(DeveloperWarning) as aw:
            c = BSTListViewClient(request=request)
            c.init_interface()
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
            "BSTListViewClient-visible-column1": "T",
            "BSTListViewClient-visible-column2": "F",
        }
        c = BSTListViewClient(request=request)
        c.init_interface()
        self.assertDictEqual(
            {"column1": True, "column2": False},
            c.get_boolean_column_cookie_dict("visible"),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_boolean_column_cookie_dict_warn(self):
        request = HttpRequest()
        request.COOKIES = {
            "BSTListViewClient-filter-column1": "b",
            "BSTListViewClient-filter-column2": "d",
        }
        c = BSTListViewClient(request=request)
        c.init_interface()
        with self.assertWarns(DeveloperWarning) as aw:
            c.get_boolean_column_cookie_dict("filter")
        self.assertEqual(2, len(aw.warnings))
        self.assertEqual(2, len(c.warnings))
        self.assertEqual(2, len(c.cookie_resets))
        self.assertIn("filter-column1", c.cookie_resets)
        self.assertIn("filter-column2", c.cookie_resets)

    @TracebaseTestCase.assertNotWarns()
    def test_get_column_cookie(self):
        view_cookie_name = "cname"
        column_name = "column1"
        request = HttpRequest()
        request.COOKIES = {
            f"BSTListViewClient-{view_cookie_name}-{column_name}": "testval"
        }
        c = BSTListViewClient(request=request)
        c.init_interface()
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
        c = BSTListViewClient(request=request)
        c.init_interface()
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
                "BSTListViewClient-visible-name": "true",
                "BSTListViewClient-visible-desc": "false",
                "BSTListViewClient-search": "",
                "BSTListViewClient-filter-name": "",
                "BSTListViewClient-filter-desc": "description",
                "BSTListViewClient-sortcol": "name",
                "BSTListViewClient-asc": "false",
            }
        )
        request.GET.update({"limit": "20"})
        bci = BSTListViewClient(request=request)
        bci.init_interface()
        bci.reset_column_cookies(["name", "desc"], "visible")
        # Only deletes the ones that are "set" (and empty string is eval'ed as None)
        self.assertEqual(
            ["visible-name", "visible-desc"],
            bci.cookie_resets,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_reset_cookie(self):
        request = HttpRequest()
        request.COOKIES.update(
            {
                "BSTListViewClient-visible-name": "true",
                "BSTListViewClient-visible-desc": "false",
                "BSTListViewClient-filter-name": "",
                "BSTListViewClient-filter-desc": "description",
                "BSTListViewClient-search": "",
                "BSTListViewClient-sortcol": "name",
                "BSTListViewClient-asc": "false",
            }
        )
        request.GET.update({"limit": "20"})
        bci = BSTListViewClient(request=request)
        bci.init_interface()
        bci.reset_cookie("sortcol")
        # Only deletes the ones that are "set" (and empty string is eval'ed as None)
        self.assertEqual(["sortcol"], bci.cookie_resets)

    def test_model_title_plural(self):
        self.assertEqual("BCI Study Test Models", StudyBCI.model_title_plural)

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
        slv.init_interface()
        slv.reset_filter_cookies()
        # Only deletes the ones that are "set" (and empty string is eval'ed as None)
        self.assertEqual(["filter-desc"], slv.cookie_resets)

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
        slv.init_interface()
        slv.reset_search_cookie()
        self.assertEqual(["search"], slv.cookie_resets)

    @TracebaseTestCase.assertNotWarns()
    def test_get_context_data(self):
        request = HttpRequest()
        bci = BSTListViewClient(request=request)
        bci.init_interface()
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
                    "sort_cookie_name",
                    "search_cookie_name",
                    "filter_cookie_name",
                    "asc_cookie_name",
                    "limit_cookie_name",
                    "page_cookie_name",
                    "visible_cookie_name",
                    "collapsed",
                    "collapsed_cookie_name",
                ]
            ),
            set(context.keys()),
        )
        # Not a standard Paginator.  Having is_paginated=None prevents the base.html template from rendering the vanilla
        # paginator under the SizedPaginator
        self.assertIsNone(context["is_paginated"])
        self.assertEqual("BSTListViewClient-", context["cookie_prefix"])
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
        slv2.init_interface()
        with self.assertNumQueries(1):
            self.assertEqual(30, slv2.get_paginate_by(qs))

        # Defaults to paginate_by if cookie limit is 0
        request.COOKIES = {f"StudyBCI-{StudyBCI.limit_cookie_name}": "0"}
        slv2 = StudyBCI(request=request)
        slv2.init_interface()
        qs = slv2.get_queryset()
        with self.assertNumQueries(0):
            self.assertEqual(slv1.paginate_by, slv2.get_paginate_by(qs))

        # Sets to the param value
        request.GET = {"limit": "20"}
        slv3 = StudyBCI(request=request)
        slv3.init_interface()
        qs = slv3.get_queryset()
        with self.assertNumQueries(0):
            self.assertEqual(20, slv3.get_paginate_by(qs))

        # Defaults to count if param limit is 0
        request.GET = {"limit": "0"}
        slv4 = StudyBCI(request=request)
        slv4.init_interface()
        qs = slv4.get_queryset()
        with self.assertNumQueries(0):
            self.assertEqual(50, slv4.get_paginate_by(qs))

        # Stays at user-selected rows per page, even if fewer results
        request.GET = {"limit": "60"}
        slv5 = StudyBCI(request=request)
        slv5.init_interface()
        qs = slv5.get_queryset()
        with self.assertNumQueries(0):
            self.assertEqual(60, slv5.get_paginate_by(qs))

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

    def test_reset_all_cookies(self):
        request = HttpRequest()
        view_cookie_name = "cname"
        request.COOKIES = {
            f"StudyBCI-{view_cookie_name}": "TRUE",
            "OtherView-cname": "value",
        }
        slv = StudyBCI(request=request)
        slv.reset_all_cookies()
        self.assertDictEqual({"OtherView-cname": "value"}, slv.request.COOKIES)
