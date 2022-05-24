from django.test import tag

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.infusate_name_parser import parse_infusate_name


@tag("parsing")
class InfusateParsingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        pass

    def test_infusate_parsing_with_name_1(self):
        name = "BCAAs {isoleucine-[13C6,15N1];leucine-[13C6,15N1];valine-[13C5,15N1]}"
        data = parse_infusate_name(name)
        self.assertEqual(data["infusate_name"], "BCAAs")
        self.assertEqual(len(data["tracer_names"]), 3)
        self.assertEqual(len(data["compound_names"]), 3)
        self.assertEqual(data["tracer_names"][1], "leucine-[13C6,15N1]")
        self.assertEqual(data["compound_names"][2], "valine")
        self.assertEqual(data["isotope_labels"][0], "13C6,15N1")

    def test_infusate_parsing_without_name_1(self):
        name = "L-Leucine-[1,2-13C2]"
        data = parse_infusate_name(name)
        self.assertEqual(data["infusate_name"], None)
        self.assertEqual(len(data["tracer_names"]), 1)
        self.assertEqual(len(data["compound_names"]), 1)
        self.assertEqual(data["tracer_names"][0], name)
        self.assertEqual(data["compound_names"][0], "L-Leucine")
        self.assertEqual(data["isotope_labels"][0], "1,2-13C2")

    def test_malformed_infusate_parsing_1(self):
        name = "not a {properly encoded tracer-[NAME1]}"
        with self.assertRaises(Exception) as context:
            data = parse_infusate_name(name)  # noqa: F841
        self.assertTrue("cannot be parsed" in str(context.exception))

    def test_malformed_infusate_parsing_2(self):
        name = "not a properly encoded tracer name"
        with self.assertRaises(Exception) as context:
            data = parse_infusate_name(name)  # noqa: F841
        self.assertTrue("cannot be parsed" in str(context.exception))
