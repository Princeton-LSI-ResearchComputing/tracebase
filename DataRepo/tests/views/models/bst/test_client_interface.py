from django.http import HttpRequest
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.client_interface import BSTClientInterface


@override_settings(DEBUG=True)
class BSTClientInterfaceTests(TracebaseTestCase):
    @TracebaseTestCase.assertNotWarns()
    def test_init(self):
        c = BSTClientInterface()
        self.assertEqual("BSTClientInterface-", c.cookie_prefix)
        self.assertEqual([], c.cookie_warnings)
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
        self.assertEqual(1, len(c.cookie_warnings))
        self.assertEqual(1, len(c.cookie_resets))
        self.assertEqual(str(aw.warnings[0].message), c.cookie_warnings[0])
        self.assertEqual(f"BSTClientInterface-{view_cookie_name}", c.cookie_resets[0])

        # second occurrence does not warn
        val = c.get_boolean_cookie(view_cookie_name, default=True)
        self.assertTrue(val)
        # The rest has not changed...
        self.assertEqual(1, len(c.cookie_warnings))
        self.assertEqual(str(aw.warnings[0].message), c.cookie_warnings[0])
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
        c = BSTClientInterface(request=request)
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
        self.assertEqual(2, len(c.cookie_warnings))
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

    @TracebaseTestCase.assertNotWarns()
    def test_get_context_data(self):
        bci = BSTClientInterface()
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
                ]
            ),
            set(context.keys()),
        )
        self.assertEqual("BSTClientInterface-", context["cookie_prefix"])
        self.assertFalse(context["clear_cookies"])
        self.assertEqual([], context["cookie_resets"])
        self.assertIsNone(context["model"])
