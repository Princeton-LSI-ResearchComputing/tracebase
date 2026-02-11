from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.text_utils import (
    autowrap,
    camel_to_title,
    get_num_parts,
    get_plural,
    getsigfig,
    indent,
    iswhole,
    sigfig,
    sigfigceil,
    sigfigfilter,
    sigfigfloor,
    underscored_to_title,
)


class TextUtilsTests(TracebaseTestCase):
    def test_autowrap(self):
        self.assertEqual(
            (
                (
                    "This is a test of\n"
                    "the autowrap method\n"
                    "\n"
                    "with lines longer\n"
                    "than 20 characters.\n"
                    "and a line that's ex\n"
                    "-actly 20 characters\n\n\n"
                    "You betcha."
                ),
                10,
                20,
            ),
            autowrap(
                (
                    "This is a test of the autowrap method\n"
                    "\n"
                    "with lines longer than 20 characters.\n"
                    "and a line that's ex\n"
                    "-actly 20 characters\n\n\n"
                    "You betcha."
                ),
                default_width=20,
            ),
        )
        self.assertEqual(
            ("A", 1, 1),
            autowrap(
                "A",
                default_width=20,
            ),
        )

    def test_sigfig(self):
        self.assertEqual("1.23e+05", sigfig(123456.789))

    def test_get_num_parts(self):
        self.assertEqual(("+", ".122", "33"), get_num_parts("+.122e33"))
        self.assertEqual(("", "0.122", None), get_num_parts("0.122"))
        self.assertEqual(("-", "000.122", "+33"), get_num_parts("-000.122e+33"))
        self.assertEqual(("", "0", None), get_num_parts("0"))
        self.assertEqual(("", "66", None), get_num_parts("66"))
        self.assertEqual(("", "99", "33"), get_num_parts("99E33"))

        # * python applies its own rules to scientific notation
        self.assertEqual(("", "1.22", "+32"), get_num_parts(0.122e33))  # *
        self.assertEqual(("", "0.122", None), get_num_parts(0.122))
        self.assertEqual(("-", "1.22", "+32"), get_num_parts(-000.122e33))  # *
        self.assertEqual(("", "0", None), get_num_parts(0))
        self.assertEqual(("", "66", None), get_num_parts(66))
        self.assertEqual(("", "9.9", "+34"), get_num_parts(99e33))  # *

    def test_sigfigfloor(self):
        self.assertEqual(0.001234, sigfigfloor("0.0012345", 4))
        self.assertEqual(0.01234, sigfigfloor("0.012345", 4))
        self.assertEqual(0.1234, sigfigfloor("0.12345", 4))
        self.assertEqual(1.234, sigfigfloor("1.2345", 4))
        self.assertEqual(12.34, sigfigfloor("12.345", 4))
        self.assertEqual(123.4, sigfigfloor("123.45", 4))
        self.assertEqual(1234, sigfigfloor("1234.5", 4))
        self.assertEqual(12340, sigfigfloor("12345", 4))
        self.assertEqual(123400, sigfigfloor("123450", 4))
        self.assertEqual(1234000, sigfigfloor("1234500", 4))
        self.assertEqual(12340000, sigfigfloor("12345000", 4))
        self.assertEqual(123400000, sigfigfloor("123450000", 4))
        self.assertEqual(0, sigfigfloor("0", 4))
        self.assertEqual(1, sigfigfloor("1", 4))
        self.assertEqual(0, sigfigfloor("0.0", 4))
        self.assertEqual(1, sigfigfloor("1.0", 4))
        self.assertEqual(0.0012, sigfigfloor("0.0012", 4))
        self.assertEqual(0.012, sigfigfloor("0.012", 4))
        self.assertEqual(0.12, sigfigfloor("0.12", 4))
        self.assertEqual(1.2, sigfigfloor("1.2", 4))
        self.assertEqual(12, sigfigfloor("12", 4))
        self.assertEqual(120, sigfigfloor("120", 4))
        self.assertEqual(1200, sigfigfloor("1200", 4))
        # * python applies its own rules to scientific notation
        self.assertEqual(-1.235e-13, sigfigfloor("-0.0012345E-10", 4))

    def test_sigfigceil(self):
        self.assertEqual(0.001235, sigfigceil("0.0012345", 4))
        self.assertEqual(0.01235, sigfigceil("0.012345", 4))
        self.assertEqual(0.1235, sigfigceil("0.12345", 4))
        self.assertEqual(1.235, sigfigceil("1.2345", 4))
        self.assertEqual(12.35, sigfigceil("12.345", 4))
        self.assertEqual(123.5, sigfigceil("123.45", 4))
        self.assertEqual(1235, sigfigceil("1234.5", 4))
        self.assertEqual(12350, sigfigceil("12345", 4))
        self.assertEqual(123500, sigfigceil("123450", 4))
        self.assertEqual(1235000, sigfigceil("1234500", 4))
        self.assertEqual(12350000, sigfigceil("12345000", 4))
        self.assertEqual(123500000, sigfigceil("123450000", 4))
        self.assertEqual(0.001, sigfigceil("0", 4))
        self.assertEqual(1.001, sigfigceil("1", 4))
        self.assertEqual(0.001, sigfigceil("0.0", 4))
        self.assertEqual(1.001, sigfigceil("1.0", 4))
        self.assertEqual(0.001201, sigfigceil("0.0012", 4))
        self.assertEqual(0.01201, sigfigceil("0.012", 4))
        self.assertEqual(0.1201, sigfigceil("0.12", 4))
        self.assertEqual(1.201, sigfigceil("1.2", 4))
        self.assertEqual(12.01, sigfigceil("12", 4))
        self.assertEqual(120.1, sigfigceil("120", 4))
        self.assertEqual(1201, sigfigceil("1200", 4))
        self.assertEqual(-0.001234e-10, sigfigceil("-0.0012345E-10", 4))

        # Increments carry over
        self.assertEqual(0.01, sigfigceil("0.0099999", 4))
        self.assertEqual(0.1, sigfigceil("0.099999", 4))
        self.assertEqual(1, sigfigceil("0.99999", 4))
        self.assertEqual(10, sigfigceil("9.9999", 4))
        self.assertEqual(100, sigfigceil("99.999", 4))
        self.assertEqual(1000, sigfigceil("999.99", 4))
        self.assertEqual(10000, sigfigceil("9999.9", 4))
        self.assertEqual(100000, sigfigceil("99999.0", 4))
        self.assertEqual(1000000, sigfigceil("999990.0", 4))
        self.assertEqual(10000000, sigfigceil("9999900.0", 4))
        self.assertEqual(100000000, sigfigceil("99999000.0", 4))
        self.assertEqual(1000000000, sigfigceil("999990000.0", 4))

        # Special cases
        self.assertEqual(1, sigfigceil("0", 1))
        self.assertEqual(0.1, sigfigceil("0.099999", 4))

        # Invalid figures
        with self.assertRaises(ValueError):
            sigfigceil(1, 0)

    def test_iswhole(self):
        self.assertTrue(iswhole(0))
        self.assertFalse(iswhole(0.1))
        self.assertTrue(iswhole(1.0))
        self.assertTrue(iswhole(1.0e0))
        self.assertTrue(iswhole(1.0e1))
        self.assertFalse(iswhole(10e-2))
        self.assertFalse(iswhole("0001e-1"))
        self.assertTrue(iswhole(10))

    def test_getsigfig(self):
        self.assertEqual(5, getsigfig(0.00012345))
        self.assertEqual(6, getsigfig("0.000123450"))
        self.assertEqual(9, getsigfig(1.00012345))
        self.assertEqual(10, getsigfig("1.000123450"))
        self.assertEqual(4, getsigfig("10.00"))
        self.assertEqual(1, getsigfig("0010"))
        self.assertEqual(2, getsigfig("0010."))
        self.assertEqual(4, getsigfig("1.000e7"))
        self.assertEqual(1, getsigfig(1000000))
        self.assertEqual(1, getsigfig("1e7"))
        self.assertEqual(2, getsigfig("1.0e7"))

        # The following are technically wrong, because python doesn't preserve or respect significant zeroes
        self.assertEqual(
            1, getsigfig(1.000e-7)
        )  # Because python represents 1.000e-7 as 1e-7
        self.assertEqual(
            9, getsigfig(1e7)
        )  # Because python represents 1e7 as 10000000.0

    def test_sigfigfilter(self):
        self.assertDictEqual(
            {"field__gte": 1.44, "field__lt": 1.45},
            sigfigfilter(1.444, "field", figures=3),
        )
        self.assertDictEqual(
            {"field__gt": -1.45, "field__lte": -1.44},
            sigfigfilter(-1.444, "field", figures=3),
        )
        self.assertDictEqual(
            {"field__gte": 1.44, "field__lt": 1.45},
            sigfigfilter(1.445, "field", figures=3),
        )
        self.assertDictEqual(
            {"field__gt": -1.45, "field__lte": -1.44},
            sigfigfilter(-1.445, "field", figures=3),
        )

    def test_camel_to_title(self):
        self.assertEqual(camel_to_title("MSRunSample"), "MS Run Sample")

    def test_underscored_to_title(self):
        self.assertEqual(
            underscored_to_title("this_is_a__function_tEST"), "This is a Function tEST"
        )

    def test_get_plural(self):
        self.assertEqual("algae", get_plural("alga"))
        self.assertEqual("Samples", get_plural("Sample"))
        self.assertEqual("Samples", get_plural("Samples"))

    def test_indent(self):
        self.assertEqual(
            "\tThis\n\t\n\tis a\n\t\ttest",
            indent("This\n\nis a\n\ttest"),
        )
        self.assertEqual(
            "\t\tThis\n\t\t\n\t\tis a\n\t\t\ttest",
            indent("This\n\nis a\n\ttest", degree=2),
        )
        self.assertEqual(
            "  This\n  \n  is a\n  \ttest",
            indent("This\n\nis a\n\ttest", indent_str="  "),
        )
