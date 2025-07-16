from django.templatetags.static import static
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.client_interface import BSTClientInterface


class CookieRequest:
    def __init__(self, **cookies):
        self.COOKIES = cookies if len(cookies) > 0 else {}
        self.GET = {}


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
    def test_script(self):
        c = BSTClientInterface()
        self.assertEqual(
            f"<script src='{static(BSTClientInterface.script_name)}'></script>",
            c.script,
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_cookie_name(self):
        c = BSTClientInterface()
        self.assertEqual("BSTClientInterface-cname", c.get_cookie_name("cname"))

    @TracebaseTestCase.assertNotWarns()
    def test_get_cookie(self):
        c = BSTClientInterface()
        view_cookie_name = "cname"
        c.request = CookieRequest(
            **{f"BSTClientInterface-{view_cookie_name}": "test_value"}
        )
        self.assertEqual("test_value", c.get_cookie(view_cookie_name))
        c.request = CookieRequest(**{f"BSTClientInterface-{view_cookie_name}": ""})
        self.assertIsNone(c.get_cookie(view_cookie_name))
        c.request = CookieRequest()
        self.assertIsNone(c.get_cookie(view_cookie_name))
        c.request = CookieRequest()
        self.assertEqual("mydef", c.get_cookie(view_cookie_name, default="mydef"))
        c.request = CookieRequest(**{f"BSTClientInterface-{view_cookie_name}": ""})
        self.assertEqual("mydef", c.get_cookie(view_cookie_name, default="mydef"))

    @TracebaseTestCase.assertNotWarns()
    def test_get_boolean_cookie_success(self):
        c = BSTClientInterface()
        view_cookie_name = "cname"
        c.request = CookieRequest(**{f"BSTClientInterface-{view_cookie_name}": "TRUE"})
        self.assertTrue(c.get_boolean_cookie(view_cookie_name))
        c.request = CookieRequest(**{f"BSTClientInterface-{view_cookie_name}": "false"})
        self.assertFalse(c.get_boolean_cookie(view_cookie_name))
        # default is False
        c.request = CookieRequest(**{f"BSTClientInterface-{view_cookie_name}": ""})
        self.assertFalse(c.get_boolean_cookie(view_cookie_name))
        # explicit default
        c.request = CookieRequest(**{f"BSTClientInterface-{view_cookie_name}": ""})
        self.assertTrue(c.get_boolean_cookie(view_cookie_name, default=True))
        # cookie absent
        c.request = CookieRequest()
        self.assertFalse(c.get_boolean_cookie(view_cookie_name))
        c.request = CookieRequest()
        self.assertTrue(c.get_boolean_cookie(view_cookie_name, default=True))

    # NOTE: This only doesn't warn because the warning is captured by assertWarns, but this assures the second call to
    # get_boolean_cookie does not warn
    @TracebaseTestCase.assertNotWarns()
    def test_get_boolean_cookie_warn(self):
        c = BSTClientInterface()
        view_cookie_name = "cname"
        c.request = CookieRequest(**{f"BSTClientInterface-{view_cookie_name}": "wrong"})
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
        c = BSTClientInterface()
        c.request = CookieRequest(
            **{
                "BSTClientInterface-visible-column1": "a",
                "BSTClientInterface-filter-column1": "b",
                "BSTClientInterface-visible-column2": "c",
                "BSTClientInterface-filter-column2": "d",
            }
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
        c = BSTClientInterface()
        c.request = CookieRequest(
            **{
                "BSTClientInterface-visible-column1": "T",
                "BSTClientInterface-visible-column2": "F",
            }
        )
        self.assertDictEqual(
            {"column1": True, "column2": False},
            c.get_boolean_column_cookie_dict("visible"),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_get_boolean_column_cookie_dict_warn(self):
        c = BSTClientInterface()
        c.request = CookieRequest(
            **{
                "BSTClientInterface-filter-column1": "b",
                "BSTClientInterface-filter-column2": "d",
            }
        )
        with self.assertWarns(DeveloperWarning) as aw:
            c.get_boolean_column_cookie_dict("filter")
        self.assertEqual(2, len(aw.warnings))
        self.assertEqual(2, len(c.cookie_warnings))
        self.assertEqual(2, len(c.cookie_resets))
        self.assertIn("BSTClientInterface-filter-column1", c.cookie_resets)
        self.assertIn("BSTClientInterface-filter-column2", c.cookie_resets)

    @TracebaseTestCase.assertNotWarns()
    def test_get_column_cookie(self):
        c = BSTClientInterface()
        view_cookie_name = "cname"
        column_name = "column1"
        c.request = CookieRequest(
            **{f"BSTClientInterface-{view_cookie_name}-{column_name}": "testval"}
        )
        self.assertIsNone(c.get_column_cookie("column2", view_cookie_name))
        self.assertEqual(
            "mydef", c.get_column_cookie("column2", view_cookie_name, default="mydef")
        )
        self.assertIsNone(c.get_column_cookie(column_name, "unset"))
        self.assertEqual(
            "mydef", c.get_column_cookie(column_name, "unset", default="mydef")
        )
        c.request = CookieRequest()
        self.assertIsNone(c.get_column_cookie(column_name, view_cookie_name))
        self.assertEqual(
            "mydef", c.get_column_cookie(column_name, view_cookie_name, default="mydef")
        )

    def test_get_param(self):
        c = BSTClientInterface()
        param_name = "cname"
        c.request = CookieRequest()
        c.request.GET.update({"cname": "x"})
        self.assertEqual("x", c.get_param(param_name))
        self.assertEqual("mydef", c.get_param("notthere", default="mydef"))
        c.request.GET.update({"cname": ""})
        self.assertIsNone(c.get_param(param_name))
        self.assertEqual("mydef", c.get_param("notthere", default="mydef"))
