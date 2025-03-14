from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.utils import reduceuntil


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
