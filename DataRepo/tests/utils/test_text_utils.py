from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.text_utils import autowrap, sigfig


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
