from DataRepo.models.researcher import UnknownResearcherError
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    UnexpectedIsotopes,
    summarize_int_list,
)


class ExceptionTests(TracebaseTestCase):
    def assert_aggregated_exception_states(
        self,
        aes,
        expected_should_raise,
        should_raise,
        expected_errors,
        expected_warnings,
    ):
        self.assertEqual(expected_should_raise, should_raise)
        self.assertEqual(
            expected_errors,
            aes.get_num_errors(),
            msg=f"There should be {expected_errors} errors",
        )
        self.assertEqual(
            expected_warnings,
            aes.get_num_warnings(),
            msg=f"There should be {expected_warnings} warnings",
        )

    def test_buffer_ure_validate_warning_raise(self):
        unknown = ["Dave"]
        new = ["Dave", "Dan"]
        known = ["Dan", "Rob", "Shaji", "Mike"]
        ure = UnknownResearcherError(unknown, new, known)

        validate_mode = True

        aes = AggregatedErrors()
        aes.buffer_exception(ure, is_error=not validate_mode, is_fatal=True)

        self.assert_aggregated_exception_states(aes, True, aes.should_raise(), 0, 1)
        self.assertTrue(isinstance(aes.exceptions[0], UnknownResearcherError))

    def test_buffer_ure_novalidate_error_raise(self):
        unknown = ["Dave"]
        new = ["Dave", "Dan"]
        known = ["Dan", "Rob", "Shaji", "Mike"]
        ure = UnknownResearcherError(unknown, new, known)

        aes = AggregatedErrors()
        aes.buffer_error(ure, is_fatal=True)

        self.assert_aggregated_exception_states(aes, True, aes.should_raise(), 1, 0)
        self.assertTrue(isinstance(aes.exceptions[0], UnknownResearcherError))

    def test_buffer_uie_validate_warning_raise(self):
        # The types of the contents of these arrays doesn't matter
        detected = ["C13", "N15"]
        labeled = ["C13"]
        compounds = ["Lysine"]
        uie = UnexpectedIsotopes(detected, labeled, compounds)

        validate_mode = True

        aes = AggregatedErrors()
        aes.buffer_warning(uie, is_fatal=validate_mode)

        self.assert_aggregated_exception_states(aes, True, aes.should_raise(), 0, 1)
        self.assertTrue(isinstance(aes.exceptions[0], UnexpectedIsotopes))

    def test_buffer_uie_novalidate_warning_raise(self):
        # The types of the contents of these arrays doesn't matter
        detected = ["C13", "N15"]
        labeled = ["C13"]
        compounds = ["Lysine"]
        uie = UnexpectedIsotopes(detected, labeled, compounds)

        validate_mode = False

        aes = AggregatedErrors()
        aes.buffer_exception(uie, is_error=False, is_fatal=validate_mode)

        self.assert_aggregated_exception_states(aes, False, aes.should_raise(), 0, 1)
        self.assertTrue(isinstance(aes.exceptions[0], UnexpectedIsotopes))

    def test_get_buffered_traceback_string(self):
        def frame_one():
            frame_two()

        def frame_two():
            frame_three()

        def frame_three():
            buffered_tb_str = AggregatedErrors.get_buffered_traceback_string()
            self.assertTrue("frame_one" in buffered_tb_str)
            self.assertTrue("frame_two" in buffered_tb_str)
            self.assertTrue("frame_three" in buffered_tb_str)
            self.assertTrue("test_get_buffered_traceback_string" in buffered_tb_str)

        frame_one()

    def test_aes_no_args(self):
        aes = None
        try:
            raise AggregatedErrors()
        except AggregatedErrors as e:
            aes = e
        expected_message = (
            "AggregatedErrors exception.  No exceptions have been buffered.  Use the return of self.should_raise() to "
            "determine if an exception should be raised before raising this exception."
        )
        self.assertEqual(expected_message, str(aes))

    def test_aes_construct_with_errors(self):
        aes = None
        try:
            raise AggregatedErrors(errors=[ValueError("Test")])
        except AggregatedErrors as e:
            aes = e
        expected_message = (
            "1 exceptions occurred, including type(s): [ValueError].\n"
            "AggregatedErrors Summary (1 errors / 0 warnings):\n"
            "\tEXCEPTION1(ERROR): ValueError: Test\n"
            "Scroll up to see tracebacks for these exceptions printed as they were encountered."
        )
        self.assertEqual(expected_message, str(aes))

    def test_aes_construct_with_warnings(self):
        aes = None
        try:
            raise AggregatedErrors(warnings=[ValueError("Test")])
        except AggregatedErrors as e:
            aes = e
        expected_message = (
            "1 exceptions occurred, including type(s): [ValueError].  This exception should not have been raised.  "
            "Use the return of self.should_raise() to determine if an exception should be raised before raising this "
            "exception.\n"
            "AggregatedErrors Summary (0 errors / 1 warnings):\n"
            "\tEXCEPTION1(WARNING): ValueError: Test\n"
            "Scroll up to see tracebacks for these exceptions printed as they were encountered."
        )
        self.assertEqual(expected_message, str(aes))

    def test_aes_construct_with_exceptions(self):
        aes = None
        try:
            raise AggregatedErrors(exceptions=[ValueError("Test")])
        except AggregatedErrors as e:
            aes = e
        expected_message = (
            "1 exceptions occurred, including type(s): [ValueError].\n"
            "AggregatedErrors Summary (1 errors / 0 warnings):\n"
            "\tEXCEPTION1(ERROR): ValueError: Test\n"
            "Scroll up to see tracebacks for these exceptions printed as they were encountered."
        )
        self.assertEqual(expected_message, str(aes))

    def test_aes_with_buffered_errors_no_should_raise(self):
        aes = None
        try:
            aes = AggregatedErrors()
            aes.buffer_error(ValueError("Test"))
            raise aes
        except AggregatedErrors as e:
            aes = e
        expected_message = (
            "1 exceptions occurred, including type(s): [ValueError].\n"
            "AggregatedErrors Summary (1 errors / 0 warnings):\n"
            "\tEXCEPTION1(ERROR): ValueError: Test\n"
            "Scroll up to see tracebacks for these exceptions printed as they were encountered."
        )
        self.assertEqual(
            expected_message,
            str(aes),
            msg="Not necessary to call should_raise.  Buffering updates the exception message.",
        )

    def test_aes_with_buffered_error_and_should_raise(self):
        aes = None
        try:
            aes = AggregatedErrors()
            aes.buffer_error(ValueError("Test"))
            aes.should_raise()
            raise aes
        except AggregatedErrors as e:
            aes = e
        expected_message = (
            "1 exceptions occurred, including type(s): [ValueError].\n"
            "AggregatedErrors Summary (1 errors / 0 warnings):\n"
            "\tEXCEPTION1(ERROR): ValueError: Test\n"
            "Scroll up to see tracebacks for these exceptions printed as they were encountered."
        )
        self.assertEqual(expected_message, str(aes))

    def test_aes_with_buffered_warning_and_should_raise(self):
        aes = None
        try:
            aes = AggregatedErrors()
            aes.buffer_warning(ValueError("Test"))
            aes.should_raise()
            raise aes
        except AggregatedErrors as e:
            aes = e
        expected_message = (
            "1 exceptions occurred, including type(s): [ValueError].  This exception should not have been raised.  "
            "Use the return of self.should_raise() to determine if an exception should be raised before raising this "
            "exception.\n"
            "AggregatedErrors Summary (0 errors / 1 warnings):\n"
            "\tEXCEPTION1(WARNING): ValueError: Test\n"
            "Scroll up to see tracebacks for these exceptions printed as they were encountered."
        )
        self.assertEqual(expected_message, str(aes))

    def test_summarize_int_list(self):
        il = [1, 7, 2, 3, 5, 8]
        esl = ["1-3", "5", "7-8"]
        sl = summarize_int_list(il)
        self.assertEqual(esl, sl)
