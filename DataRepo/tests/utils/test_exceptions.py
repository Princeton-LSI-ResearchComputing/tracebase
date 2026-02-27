from __future__ import annotations

from datetime import datetime

from DataRepo.models.researcher import UnknownResearcherError
from DataRepo.models.utilities import get_model_by_name
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AllUnskippedBlanks,
    AmbiguousMzxmlSampleMatch,
    AmbiguousMzxmlSampleMatches,
    AnimalsWithoutSamples,
    AnimalsWithoutSerumSamples,
    AnimalWithoutSamples,
    AnimalWithoutSerumSamples,
    ComplexPeakGroupDuplicate,
    ComplexPeakGroupDuplicates,
    CompoundDoesNotExist,
    DateParseError,
    DBFieldVsFileColDeveloperWarning,
    DBFieldVsFileColDeveloperWarnings,
    DefaultSequenceNotFound,
    DeveloperWarning,
    DuplicateCompoundIsotopes,
    DuplicatePeakGroup,
    DuplicatePeakGroups,
    DuplicateValueErrors,
    DuplicateValues,
    EmptyColumns,
    ExcelSheetNotFound,
    ExcelSheetsNotFound,
    InfileError,
    InvalidDtypeDict,
    InvalidDtypeKeys,
    InvalidHeaderCrossReferenceError,
    IsotopeStringDupe,
    MissingC12ParentPeak,
    MissingC12ParentPeaks,
    MissingColumnGroup,
    MissingCompounds,
    MissingDataAdded,
    MissingFCircCalculationValue,
    MissingFCircCalculationValues,
    MissingRecords,
    MissingSamples,
    MultiLoadStatus,
    MultipleConflictingValueMatches,
    MultipleConflictingValueMatchesSummary,
    MultipleDefaultSequencesFound,
    MutuallyExclusiveOptions,
    MzxmlColocatedWithMultipleAnnot,
    MzxmlColocatedWithMultipleAnnots,
    MzxmlNotColocatedWithAnnot,
    MzxmlNotColocatedWithAnnots,
    MzxmlSampleHeaderMismatch,
    NewResearcher,
    NewResearchers,
    NoLoadData,
    NoPeakAnnotationDetails,
    NoPeakAnnotationDetailsErrors,
    NoSamples,
    NoScans,
    NoTracerLabeledElements,
    ObservedIsotopeUnbalancedError,
    OptionsNotAvailable,
    PossibleDuplicateSample,
    PossibleDuplicateSamples,
    ProhibitedCompoundName,
    ProhibitedCompoundNames,
    ProhibitedStringValue,
    RecordDoesNotExist,
    RequiredArgument,
    RequiredColumnValue,
    RequiredColumnValues,
    RequiredHeadersError,
    RequiredOptions,
    RequiredValueError,
    RequiredValueErrors,
    SheetMergeError,
    SummarizableError,
    SummarizedInfileError,
    TechnicalPeakGroupDuplicate,
    TechnicalPeakGroupDuplicates,
    UnequalColumnGroups,
    UnexpectedLabel,
    UnexpectedSamples,
    UnknownHeader,
    UnskippedBlanks,
    generate_file_location_string,
    summarize_int_list,
    trace,
)


class MultiLoadStatusTests(TracebaseTestCase):

    def test_init_load(self):
        """
        Tests that init_load creates a key in MultiLoadStatus.statuses[string] with:
        {
            "aggregated_errors": None,
            "state": "PASSED",
            "num_errors": 0,
            "num_warnings": 0,
            "top": True,  # Passing files will appear first
        }
        """
        mls = MultiLoadStatus()
        self.assertEqual(0, len(mls.statuses.keys()))
        mls.init_load("mykey")
        self.assertEqual(1, len(mls.statuses.keys()))
        self.assertEqual(
            {
                "mykey": {
                    "aggregated_errors": None,
                    "state": "PASSED",
                    "top": True,  # Passing files will appear first
                },
            },
            mls.statuses,
        )
        self.assertTrue(mls.is_valid)
        self.assertEqual(("Load PASSED", "PASSED"), mls.get_status_message())

    def test_constructor_with_key_list(self):
        """
        Tests that constructor creates a key in MultiLoadStatus.statuses[every string in list] with:
        {
            "aggregated_errors": None,
            "state": "PASSED",
            "num_errors": 0,
            "num_warnings": 0,
            "top": True,  # Passing files will appear first
        }
        """
        mls = MultiLoadStatus(["mykey", "mykey2"])
        self.assertEqual(2, len(mls.statuses.keys()))
        self.assertEqual(
            {
                "mykey": {
                    "aggregated_errors": None,
                    "state": "PASSED",
                    "top": True,  # Passing files will appear first
                },
                "mykey2": {
                    "aggregated_errors": None,
                    "state": "PASSED",
                    "top": True,  # Passing files will appear first
                },
            },
            mls.statuses,
        )
        self.assertEqual(("Load PASSED", "PASSED"), mls.get_status_message())

    def test_set_load_exception_aggregated_errors_warning(self):
        """
        Test that set_load_exception(agg_errs_obj, load_key, top=False)
        adds AggregatedErrors exceptions to mls.statuses and updates:
            statuses[load_key]["num_errors"]
            statuses[load_key]["num_warnings"]
            statuses[load_key]["top"]
            statuses[load_key]["aggregated_errors"]
            statuses[load_key]["state"]
            state
            is_valid
        """
        mls = MultiLoadStatus()
        aes = AggregatedErrors()
        aes.buffer_warning(ValueError("Test warning"), is_fatal=True)
        mls.set_load_exception(aes, "mykey", top=True)
        self.assertEqual(1, len(mls.statuses.keys()))
        self.assertEqual(
            {
                "top": True,
                "aggregated_errors": aes,
                "state": "WARNING",
            },
            mls.statuses["mykey"],
        )
        self.assertFalse(mls.is_valid)
        self.assertEqual("WARNING", mls.state)
        self.assertFalse(mls.is_valid)
        self.assertEqual(
            ("Load WARNING 1 warnings", "WARNING"), mls.get_status_message()
        )

    def test_set_load_exception_aggregated_errors_error(self):
        """
        Test that set_load_exception(agg_errs_obj, load_key, top=False)
        adds AggregatedErrors exceptions to mls.statuses and updates:
            statuses[load_key]["num_errros"]
            statuses[load_key]["num_warnings"]
            statuses[load_key]["top"]
            statuses[load_key]["aggregated_errors"]
            statuses[load_key]["state"]
            state
            is_valid
        """
        mls = MultiLoadStatus()
        aes = AggregatedErrors()
        aes.buffer_error(ValueError("Test error"))
        mls.set_load_exception(aes, "mykey", top=False)
        self.assertEqual(1, len(mls.statuses.keys()))
        self.assertEqual(
            {
                "top": False,
                "aggregated_errors": aes,
                "state": "FAILED",
            },
            mls.statuses["mykey"],
        )
        self.assertFalse(mls.is_valid)
        self.assertEqual("FAILED", mls.state)
        self.assertFalse(mls.is_valid)
        self.assertEqual(("Load FAILED 1 errors", "FAILED"), mls.get_status_message())

    def test_set_load_exception_other_exceptions(self):
        """
        Test that set_load_exception(exception, load_key, top=False)
        adds non-AggregatedErrors exceptions as an AggregatedErrors exception to mls.statuses and updates:
            statuses[load_key]["num_errros"]
            statuses[load_key]["num_warnings"]
            statuses[load_key]["top"]
            statuses[load_key]["aggregated_errors"]
            statuses[load_key]["state"]
            state
            is_valid
        """
        mls = MultiLoadStatus()
        exc = ValueError("Test error")
        mls.set_load_exception(exc, "mykey")

        self.assertEqual(1, len(mls.statuses.keys()))

        # This is for the assertion below
        aes = AggregatedErrors()
        aes.buffer_error(exc)

        # Make sure all these attributes of the contained AggregatedErrors object are equal to the one created above
        # for comparison
        attrs = [
            "exceptions",
            "num_errors",
            "num_warnings",
            "is_fatal",
            "is_error",
            "custom_message",
            "quiet",
        ]
        for attr in attrs:
            self.assertEqual(
                getattr(aes, attr),
                getattr(mls.statuses["mykey"]["aggregated_errors"], attr),
            )
        # The buffered_tb_str attribute won't be the same, but we'll just assert that it's there.
        self.assertTrue(
            hasattr(mls.statuses["mykey"]["aggregated_errors"], "buffered_tb_str")
        )

        self.assertFalse(mls.statuses["mykey"]["top"])
        self.assertEqual("FAILED", mls.statuses["mykey"]["state"])
        self.assertEqual(1, mls.statuses["mykey"]["aggregated_errors"].num_errors)
        self.assertEqual(0, mls.statuses["mykey"]["aggregated_errors"].num_warnings)

        self.assertFalse(mls.is_valid)
        self.assertEqual("FAILED", mls.state)
        self.assertFalse(mls.is_valid)
        self.assertEqual(("Load FAILED 1 errors", "FAILED"), mls.get_status_message())

    def test_set_load_exception_key_exists(self):
        """Check that if you try to add 2 exceptions with the same load key, the errors are merged"""
        mls = MultiLoadStatus()
        aes = AggregatedErrors()
        aes.buffer_warning(ValueError("Test warning"))
        mls.set_load_exception(aes, "mykey", top=False)
        aes2 = AggregatedErrors()
        aes2.buffer_error(ValueError("Test error"))
        mls.set_load_exception(aes2, "mykey", top=True)
        self.assertEqual(1, mls.num_errors)
        self.assertEqual(1, mls.num_errors)
        self.assertTrue(mls.statuses["mykey"]["top"])
        self.assertEqual("FAILED", mls.statuses["mykey"]["state"])
        self.assertEqual("FAILED", mls.state)
        # The merge of the aggregated errors object is tested elsewhere
        self.assertEqual(2, len(mls.statuses["mykey"]["aggregated_errors"].exceptions))

    def test_get_ordered_status_keys(self):
        """Check that top=True puts grouped exceptions at the top by default"""
        mls = MultiLoadStatus()
        aes = AggregatedErrors()
        aes.buffer_error(ValueError("Test error 1"))
        mls.set_load_exception(aes, "mykey", top=False)
        aes2 = AggregatedErrors()
        aes2.buffer_error(ValueError("Test error 2"))
        mls.set_load_exception(aes2, "mykey2", top=True)
        self.assertEqual(["mykey2", "mykey"], mls.get_ordered_status_keys())
        self.assertEqual(["mykey", "mykey2"], mls.get_ordered_status_keys(reverse=True))

    def test_get_status_messages(self):
        """
        Makes sure that top errors are passing and those assigned top=Tue, then failing, and that all are formatted
        correctly.  Load key state messages are followed by AggregatedErrors summaries, if any.
        """
        mls = MultiLoadStatus(["mykey", "mykey2", "mykey3"])
        aes = AggregatedErrors()
        aes.buffer_error(ValueError("Test error"))
        mls.set_load_exception(aes, "mykey")
        aes2 = AggregatedErrors()
        aes2.buffer_error(ValueError("Test error 2"))
        mls.set_load_exception(aes2, "mykey2", top=True)
        messages = mls.get_status_messages()
        self.assertEqual(
            [
                {
                    "message": "mykey2: FAILED",
                    "state": "FAILED",
                },
                {
                    "message": (
                        "AggregatedErrors Summary (1 errors / 0 warnings):\n"
                        "\tEXCEPTION1(ERROR): ValueError: Test error 2"
                    ),
                    "state": "FAILED",
                },
                {
                    "message": "mykey3: PASSED",
                    "state": "PASSED",
                },
                {
                    "message": "mykey: FAILED",
                    "state": "FAILED",
                },
                {
                    "message": (
                        "AggregatedErrors Summary (1 errors / 0 warnings):\n"
                        "\tEXCEPTION1(ERROR): ValueError: Test error"
                    ),
                    "state": "FAILED",
                },
            ],
            messages,
        )

    def test_mls_copy_constructor(self):
        mls = MultiLoadStatus(["mykey", "mykey2"])

        aes = AggregatedErrors()
        aes.buffer_error(ValueError("Test error"))
        mls.set_load_exception(aes, "mykey")

        aes2 = AggregatedErrors()
        aes2.buffer_error(ValueError("Test error"))
        mls.set_load_exception(aes2, "mykey2")

        # Remove the exception (this makes the status data in mls stale)
        mls.statuses["mykey"]["aggregated_errors"].remove_exception_type(ValueError)

        # Create a new mls object
        mls2 = MultiLoadStatus()
        mls2.copy_constructor(mls)
        self.assertEqual("PASSED", mls2.statuses["mykey"]["state"])
        self.assertEqual("FAILED", mls2.statuses["mykey2"]["state"])

    def test_mls_remove_exception_type(self):
        """Indirectly also tests update_state()"""
        mls = MultiLoadStatus(["mykey"])

        aes = AggregatedErrors()
        aes.buffer_error(ValueError("Test error"))
        mls.set_load_exception(aes, "mykey")

        mls.remove_exception_type("mykey", ValueError)

        self.assertTrue(mls.is_valid)
        self.assertEqual("PASSED", mls.state)
        self.assertEqual(0, mls.num_errors)
        self.assertEqual(0, mls.num_warnings)
        self.assertIsNone(mls.statuses["mykey"]["aggregated_errors"])
        self.assertFalse(mls.statuses["mykey"]["top"])


class AggregatedErrorsTests(TracebaseTestCase):
    def test_merge_aggregated_errors_object(self):
        aes1 = AggregatedErrors(errors=[ValueError("Test error")])
        aes2 = AggregatedErrors(warnings=[ValueError("Test warning")])
        aes1.merge_aggregated_errors_object(aes2)
        self.assertEqual(2, len(aes1.exceptions))
        self.assertTrue(aes1.is_error)
        self.assertTrue(aes1.is_fatal)
        self.assertFalse(aes1.custom_message)
        self.assertEqual(1, aes1.num_errors)
        self.assertEqual(1, aes1.num_warnings)
        self.assertFalse(aes1.quiet)
        expected_message = (
            "2 exceptions occurred, including type(s): [ValueError].\n"
            "AggregatedErrors Summary (1 errors / 1 warnings):\n"
            "\tEXCEPTION1(ERROR): ValueError: Test error\n"
            "\tEXCEPTION2(WARNING): ValueError: Test warning\n"
            "Scroll up to see tracebacks for these exceptions printed as they were encountered."
        )
        self.assertEqual(expected_message, str(aes1))
        self.assertIn(
            "\nAn additional AggregatedErrors object was merged with this one.  The appended trace is:\n\n",
            aes1.buffered_tb_str,
        )

    def test_exception_type_exists(self):
        aes = AggregatedErrors(errors=[ValueError("Test error")])
        self.assertTrue(aes.exception_type_exists(ValueError))
        self.assertFalse(aes.exception_type_exists(AttributeError))

    def assert_2_value_errors(self, aes):
        self.assertEqual((2, 0), (aes.num_errors, aes.num_warnings))
        self.assertEqual(2, len(aes.exceptions))
        self.assertTrue(aes.is_fatal)
        self.assertTrue(aes.is_error)
        self.assertTrue(aes.exceptions[0].is_fatal)
        self.assertTrue(aes.exceptions[0].is_error)
        self.assertTrue(aes.exceptions[1].is_fatal)
        self.assertTrue(aes.exceptions[1].is_error)

    def test_modify_exception_type(self):
        aes = AggregatedErrors(
            errors=[ValueError("Test error 1"), ValueError("Test error 2")]
        )

        # Establish everything is correct BEFORE calling modify_exception_type
        self.assert_2_value_errors(aes)

        aes.modify_exception_type(ValueError, is_error=False, is_fatal=False)

        # Establish everything is changed AFTER calling modify_exception_type
        self.assertEqual((0, 2), (aes.num_errors, aes.num_warnings))
        self.assertEqual(2, len(aes.exceptions))
        self.assertFalse(aes.is_fatal)
        self.assertFalse(aes.is_error)
        self.assertFalse(aes.exceptions[0].is_fatal)
        self.assertFalse(aes.exceptions[0].is_error)
        self.assertFalse(aes.exceptions[1].is_fatal)
        self.assertFalse(aes.exceptions[1].is_error)

    def test_get_exception_type_sets_attributes_correctly_when_not_removing(self):
        aes = AggregatedErrors(
            errors=[ValueError("Test error 1"), ValueError("Test error 2")]
        )

        # Establish everything is correct BEFORE calling modify_exception_type
        self.assert_2_value_errors(aes)

        removed = aes.remove_exception_type(ValueError)

        # Establish everything is changed AFTER calling remove_exception_type
        self.assertEqual((0, 0), (aes.num_errors, aes.num_warnings))
        self.assertEqual(0, len(aes.exceptions))
        self.assertFalse(aes.is_fatal)
        self.assertFalse(aes.is_error)
        self.assertFalse(removed[0].is_fatal)
        self.assertFalse(removed[0].is_error)
        self.assertFalse(removed[1].is_fatal)
        self.assertFalse(removed[1].is_error)

    def test_get_exception_types(self):
        aes = AggregatedErrors(exceptions=[ValueError(), KeyError(), KeyError()])
        types = aes.get_exception_types()
        self.assertEqual([ValueError, KeyError], types)

    def test_exception_matches(self):
        ke = KeyError()
        ke.is_error = False
        aes = AggregatedErrors()
        self.assertTrue(aes.exception_matches(ke, KeyError, "is_error", False))
        self.assertFalse(aes.exception_matches(ke, KeyError, "is_error", True))

    def test_exception_exists(self):
        ke = KeyError()
        ke.is_error = False
        aes = AggregatedErrors(exceptions=[ke])
        self.assertTrue(aes.exception_exists(KeyError, "is_error", False))
        self.assertFalse(aes.exception_exists(KeyError, "is_error", True))

    def test_remove_matching_exceptions(self):
        ke = KeyError()
        ke.is_error = False
        aes = AggregatedErrors(exceptions=[ke])
        aes.remove_matching_exceptions(KeyError, "is_error", False)
        self.assertEqual(0, len(aes.exceptions))
        self.assertEqual(0, aes.num_warnings)


class SummarizableErrorTests(TracebaseTestCase):
    @staticmethod
    def summarizer_class_factory():
        # Create a summarizer Exception class for testing - code it however you want
        class MyExceptionSummarier(Exception):
            def __init__(self, _):
                pass

        return MyExceptionSummarier

    @staticmethod
    def summarizable_class_factory():
        # Create an exception class that inherits from SummarizableError - code it however you want, just define
        # SummarizerExceptionClass
        class MyException(SummarizableError):
            SummarizerExceptionClass = SummarizableErrorTests.summarizer_class_factory()

        return MyException

    def test_summarize_exceptions(self):
        test_exception_class = self.summarizable_class_factory()
        test_exception = test_exception_class()
        # Later, when you're handling multiple buffered exceptions (e.g. you have an AggregatedErrors object: aes),
        # you can check if it's summarizable, and replace them with the summarized version
        self.assertTrue(issubclass(test_exception.__class__, SummarizableError))
        summarized_exception = test_exception.SummarizerExceptionClass([test_exception])
        # NOTE: assertIsInstance() doesn't work due to the factory creating a distinct class each time, so we check by
        # qualname
        self.assertEqual(
            summarized_exception.__class__.__qualname__,
            self.summarizer_class_factory().__qualname__,
        )

    def test_SummarizedInfileError(self):
        """This basically checks that the SummarizedInfileError constructor creates a file_dict instance variable
        containing exception lists keys by the file string that the exception is based on.
        """

        class MySummarizedInfileException(SummarizedInfileError, Exception):
            def __init__(
                self,
                exceptions: list[MySummarizableInfileException],
            ):
                SummarizedInfileError.__init__(self, exceptions)
                Exception.__init__(self, "test")

        class MySummarizableInfileException(InfileError, SummarizableError):
            SummarizerExceptionClass = MySummarizedInfileException

        msbie = MySummarizableInfileException("there was a problem", file="test.txt")
        msdie = MySummarizedInfileException([msbie])

        self.assertTrue(hasattr(msdie, "file_dict"))
        self.assertDictEqual({"test.txt": [msbie]}, msdie.file_dict)


class ExceptionTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

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

    def test_generate_file_location_string(self):
        lstr = generate_file_location_string(
            column=2, rownum=3, sheet="Animals", file="animals.xlsx"
        )
        self.assertEqual(
            "column [2] on row [3] of sheet [Animals] in animals.xlsx", lstr
        )
        lstr = generate_file_location_string(column=2, rownum=3, sheet="Animals")
        self.assertEqual(
            "column [2] on row [3] of sheet [Animals] in the load file data", lstr
        )
        lstr = generate_file_location_string(
            rownum=3, sheet="Animals", file="animals.xlsx"
        )
        self.assertEqual("row [3] of sheet [Animals] in animals.xlsx", lstr)
        lstr = generate_file_location_string(
            column=2, sheet="Animals", file="animals.xlsx"
        )
        self.assertEqual("column [2] of sheet [Animals] in animals.xlsx", lstr)
        lstr = generate_file_location_string(column=2, rownum=3, file="animals.xlsx")
        self.assertEqual("column [2] on row [3] in animals.xlsx", lstr)

    def test_DuplicateValueErrors(self):
        """Test that DuplicateValueErrors correctly summarizes a series of DuplicateValues exceptions"""
        dvs = [
            DuplicateValues({"x": [0, 1]}, ["col2"], sheet=None, file="loadme.txt"),
            DuplicateValues({"2": [0, 1]}, ["col3"], sheet=None, file="loadme.txt"),
            DuplicateValues({"x": [0, 1]}, ["col2"], sheet=None, file="loadme2.txt"),
            DuplicateValues({"2": [0, 1]}, ["col3"], sheet=None, file="loadme2.txt"),
        ]
        dve = DuplicateValueErrors(dvs)
        expected = (
            "The following unique column(s) (or column combination(s)) were found to have duplicate occurrences on the "
            "indicated rows:\n"
            "\tloadme.txt\n"
            "\t\tColumn(s) ['col2']\n"
            "\t\t\tx (rows*: 2-3)\n"
            "\t\tColumn(s) ['col3']\n"
            "\t\t\t2 (rows*: 2-3)\n"
            "\tloadme2.txt\n"
            "\t\tColumn(s) ['col2']\n"
            "\t\t\tx (rows*: 2-3)\n"
            "\t\tColumn(s) ['col3']\n"
            "\t\t\t2 (rows*: 2-3)\n"
        )
        self.assertEqual(expected, str(dve))

    def test_RequiredColumnValues(self):
        rcvs = [
            RequiredColumnValue("col2", rownum=5, sheet="Tissues", file="loadme.tsv"),
            RequiredColumnValue("col2", rownum=6, sheet="Tissues", file="loadme.tsv"),
            RequiredColumnValue("col2", rownum=7, sheet="Tissues", file="loadme.tsv"),
            RequiredColumnValue("col2", rownum=8, sheet="Tissues", file="loadme.tsv"),
        ]
        rcv = RequiredColumnValues(rcvs)
        expected = (
            "Required column values missing on the indicated rows:\n"
            "\tsheet [Tissues] in loadme.tsv\n"
            "\t\tColumn: [col2] on rows: ['5-8']\n"
        )
        self.assertEqual(expected, str(rcv))
        rcv2 = RequiredColumnValues(rcvs, suggestion="SUGGEST")
        self.assertIn("SUGGEST", str(rcv2))

    def test_RequiredValueErrors(self):
        rves = [
            RequiredValueError(
                "Tissue Name",
                3,
                "Tissue",
                "name",
                rec_dict={
                    "name": None,
                    "description": "This is the armpit",
                    "type": "epidermal",
                },
                sheet="tissues",
                file="tissues.tsv",
            ),
            RequiredValueError(
                "Tissue Name",
                4,
                "Tissue",
                "name",
                rec_dict={
                    "name": None,
                    "description": "This is the sphincter",
                    "type": "epidermal",
                },
                sheet="tissues",
                file="tissues.tsv",
            ),
            RequiredValueError(
                "Tissue Name",
                4,
                "Tissue",
                "name",
                rec_dict={
                    "name": None,
                    "description": "This is the elbowpit",
                    "type": "epidermal",
                },
                sheet="tissues",
                file="tissues.tsv",
            ),
        ]
        rve = RequiredValueErrors(rves)
        self.assertIn("Required values found missing", str(rve))
        self.assertIn("sheet [tissues] in tissues.tsv", str(rve))
        self.assertIn("Column: [Tissue Name] on row(s): 3-4", str(rve))
        self.assertIn(
            "Errors like this only happen when related data failed to load", str(rve)
        )
        self.assertIn(
            "evidenced by the fact that the indicated column/rows have values", str(rve)
        )
        self.assertIn("Fixing errors above this will fix this error.", str(rve))

    def test_ExcelSheetsNotFound(self):
        esnf = ExcelSheetsNotFound(
            unknowns={"x": [2, 3, 5]},
            all_sheets=["a", "b"],
            source_file="test.xlsx",
            source_sheet="defs",
            source_column="Sheet Name",
        )
        self.assertEqual(
            (
                "The following excel sheet(s) parsed from column [Sheet Name] of sheet [defs] in test.xlsx on "
                "the indicated rows were not found.\n"
                "\t[x] on rows: ['2-3', '5']\n"
                "The available sheets are: [['a', 'b']]."
            ),
            str(esnf),
        )

    def test_InvalidHeaderCrossReferenceError(self):
        ihcre = InvalidHeaderCrossReferenceError(
            source_file="test.xlsx",
            source_sheet="Defaults",
            column="Column Header",
            unknown_headers={"X": [2, 5, 6, 7]},
            target_file="test.xlsx",
            target_sheet="Data",
            target_headers=["A", "B"],
        )
        self.assertEqual(
            (
                "The following column-references parsed from column [Column Header] of sheet [Defaults] in "
                "test.xlsx:\n"
                "\t[X] on row(s): ['2', '5-7']\n"
                "were not found in sheet [Data] in test.xlsx, which has the following columns:\n"
                "\tA, B."
            ),
            str(ihcre),
        )

    def test_OptionsNotAvailable(self):
        ona = OptionsNotAvailable()
        self.assertEqual(
            "Cannot get command line option values until handle() has been called.",
            str(ona),
        )

    def test_MutuallyExclusiveOptions(self):
        meo = MutuallyExclusiveOptions("My message")
        self.assertEqual("My message", str(meo))

    def test_NoLoadData(self):
        nld = NoLoadData("My message")
        self.assertEqual("My message", str(nld))

    def test_InvalidDtypeDict(self):
        idd = InvalidDtypeDict(
            {"Wrong": str, "WrongAgain": int},
            file="afile.xlsx",
            sheet="SheetName",
            columns=["A", "B"],
        )
        self.assertEqual(
            (
                "Invalid dtype dict supplied for parsing sheet [SheetName] in afile.xlsx.  None of its keys "
                "['Wrong', 'WrongAgain'] are present in the dataframe, whose columns are ['A', 'B']."
            ),
            str(idd),
        )

    def test_InvalidDtypeKeys(self):
        idk = InvalidDtypeKeys(
            ["Wrong"],
            file="afile.xlsx",
            sheet="SheetName",
            columns=["A", "Right"],
        )
        self.assertEqual(
            (
                "Missing dtype dict keys supplied for parsing sheet [SheetName] in afile.xlsx.  These keys "
                "['Wrong'] are not present in the resulting dataframe, whose available columns are ['A', 'Right']."
            ),
            str(idk),
        )

    def test_DateParseError(self):
        ve = ValueError("unconverted data remains:  00:00:00")
        dpe = DateParseError("a date", ve, "a format")
        self.assertEqual(
            "The date string 'a date' found in the load file data did not match the pattern 'a format'.",
            str(dpe),
        )

    def test_InfileError_placeholder(self):
        ie = InfileError(
            "You did something weird here: %s. You shouldn't do that.",
            rownum=2,
            sheet="Test Sheet",
            file="test.xlsx",
            column="Col1",
        )
        self.assertEqual(
            (
                "You did something weird here: column [Col1] on row [2] of sheet [Test Sheet] in test.xlsx. You "
                "shouldn't do that."
            ),
            str(ie),
        )

    def test_InfileError_no_placeholder(self):
        ie = InfileError(
            "You did something weird. You shouldn't do that.",
            rownum=2,
            sheet="Test Sheet",
            file="test.xlsx",
            column="Col1",
        )
        self.assertEqual(
            (
                "You did something weird. You shouldn't do that.  Location: column [Col1] on row [2] of sheet [Test "
                "Sheet] in test.xlsx."
            ),
            str(ie),
        )

    def test_InfileError_string_rownum(self):
        ie = InfileError(
            "Tests that rownum can be a string.",
            rownum="record name",
            sheet="Test Sheet 1",
            file="testrowname.xlsx",
            column="Col5",
        )
        self.assertEqual(
            (
                "Tests that rownum can be a string.  Location: column [Col5] on row [record name] of sheet [Test Sheet "
                "1] in testrowname.xlsx."
            ),
            str(ie),
        )

    def test_InfileError_set_formatted_message(self):
        ie = InfileError("Test that location can be added to %s later.")
        self.assertEqual(
            "Test that location can be added to the load file data later.", str(ie)
        )
        ie.set_formatted_message(
            rownum="record name",
            sheet="Test Sheet 1",
            file="testrowname.xlsx",
            column="Col5",
        )
        self.assertEqual(
            (
                "Test that location can be added to column [Col5] on row [record name] of sheet [Test Sheet 1] in "
                "testrowname.xlsx later."
            ),
            str(ie),
        )

    def test_CompoundDoesNotExist(self):
        cdne = CompoundDoesNotExist(
            "compound x",
            rownum=2,
            sheet="Test Sheet",
            file="test.xlsx",
            column="Col1",
        )
        self.assertEqual(
            (
                "Compound [compound x] from column [Col1] on row [2] of sheet [Test Sheet] in test.xlsx does "
                "not exist as either a primary compound name or synonym."
            ),
            str(cdne),
        )

    def test_ExcelSheetNotFound(self):
        esnf = ExcelSheetNotFound(
            sheet="Not Present", file="an_excel_file.xlsx", all_sheets=["A", "B"]
        )
        self.assertIn("[Not Present] not found", str(esnf))
        self.assertIn("in an_excel_file.xlsx", str(esnf))
        self.assertIn("Available sheets: ['A', 'B']", str(esnf))

    def test_MissingDataAdded(self):
        mda = MissingDataAdded(["5 sample names"], file="Study doc.xlsx")
        self.assertEqual(
            "Missing data ['5 sample names'] was added to Study doc.xlsx.",
            str(mda),
        )

    def test_RecordDoesNotExist(self):
        rdne = RecordDoesNotExist(
            model=get_model_by_name("Tissue"),
            query_obj={"name": "invalid"},
        )
        self.assertEqual(
            "Tissue record matching {'name': 'invalid'} from the load file data does not exist.",
            str(rdne),
        )

    def test_RequiredOptions(self):
        ro = RequiredOptions(["infile"])
        self.assertEqual("Missing required options: ['infile'].", str(ro))

    def test_MissingColumnGroup(self):
        mcg = MissingColumnGroup("Sample")
        self.assertIn("No Sample columns found", str(mcg))

    def test_UnequalColumnGroups(self):
        exc = UnequalColumnGroups("Sample", {"orig": ["A", "B"], "corr": ["A", "C"]})
        self.assertIn("sheets ['orig', 'corr'] differ", str(exc))
        self.assertIn(
            "'orig' sheet has 2 out of 3 total unique Sample columns, and is missing:\n\tC",
            str(exc),
        )
        self.assertIn(
            "'corr' sheet has 2 out of 3 total unique Sample columns, and is missing:\n\tB",
            str(exc),
        )

    def test_UnknownHeaderError(self):
        exc = UnknownHeader("C", ["A", "B"])
        self.assertEqual(
            "Unknown header encountered: [C] in the load file data.  Must be one of ['A', 'B'].",
            str(exc),
        )

    def test_NewResearchers(self):
        nrs = [NewResearcher("George"), NewResearcher("Patty")]
        exc = NewResearchers(nrs)
        self.assertIn("New researchers encountered:", str(exc))
        self.assertIn("George", str(exc))
        self.assertIn("Patty", str(exc))

    def test_NewResearcher(self):
        exc = NewResearcher("Thelma")
        self.assertIn("new researcher [Thelma] is being added", str(exc))

    def test_RequiredArgument(self):
        exc = RequiredArgument("val", methodname="do_stuff")
        self.assertEqual(
            "do_stuff requires a non-None value for argument 'val'.", str(exc)
        )

    def test_EmptyColumns(self):
        exc = EmptyColumns(
            "Sample",
            ["A", "B"],
            ["Unnamed: jwbc", "Unnamed: wale"],
            ["A", "B", "sample1", "sample2", "Unnamed: jwbc", "Unnamed: wale"],
            addendum="They will be skipped.",
        )
        self.assertIn("[Sample] columns are expected", str(exc))
        self.assertIn("2 expected constant columns", str(exc))
        self.assertIn("6 columns total", str(exc))
        self.assertIn("4 potential Sample columns", str(exc))
        self.assertIn("2 were unnamed.", str(exc))
        self.assertIn("They will be skipped.", str(exc))

    def test_DuplicateCompoundIsotope(self):
        dvs = [
            DuplicateValues({"1": [1, 2]}, ["A", "B", "C"]),
            DuplicateValues({"2": [6, 9]}, ["A", "B", "C"]),
        ]
        exc = DuplicateCompoundIsotopes(dvs, ["A", "B"])
        self.assertIn("Column(s) ['A', 'B']", str(exc))
        self.assertIn("1 (rows*: 3-4)", str(exc))
        self.assertIn("2 (rows*: 8, 11)", str(exc))

    def test_SheetMergeError(self):
        exc = SheetMergeError([100, 102])
        self.assertIn("missing an Animal Name", str(exc))
        self.assertIn("empty rows: [100, 102]", str(exc))

    def test_IsotopeStringDupe(self):
        exc = IsotopeStringDupe("C13N15C13-label-2-1-1", "C")
        self.assertIn(
            " match tracer labeled element (C) in the measured labeled element string: [C13N15C13-label-2-1-1]",
            str(exc),
        )

    def test_ObservedIsotopeUnbalancedError(self):
        exc = ObservedIsotopeUnbalancedError(
            ["C", "N"], [13, 15], [1, 2, 1], "13C15N-1-2-1"
        )
        self.assertIn(
            "elements (2), mass numbers (2), and counts (3) from isotope label: [13C15N-1-2-1]",
            str(exc),
        )

    def test_UnexpectedLabels(self):
        exc = UnexpectedLabel(["D"], ["C", "N"])
        self.assertIn(
            "One or more observed peak labels were not among the label(s) in the tracer(s)",
            str(exc),
        )
        self.assertIn("Observed: ['D']", str(exc))
        self.assertIn("Expected: ['C', 'N']", str(exc))
        self.assertIn("There may be contamination", str(exc))

    def test_MzxmlSampleHeaderMismatch(self):
        exc = MzxmlSampleHeaderMismatch("sample", "location/sample_neg.mzXML")
        self.assertIn("mzXML file [location/sample_neg.mzXML]", str(exc))
        self.assertIn("Sample header:       [sample]", str(exc))
        self.assertIn("mzXML Base Filename: [sample_neg]", str(exc))

    # NOTE: MultiplePeakGroupRepresentations is tested in the peak group tests, because it needs records

    def test_RequiredHeadersError(self):
        exc = RequiredHeadersError(["A"])
        self.assertIn("header(s) missing: ['A']", str(exc))

    def test_NoTracerLabeledElements(self):
        exc = NoTracerLabeledElements()
        self.assertIn("No tracer_labeled_elements.", str(exc))

    def test_MissingCompounds(self):
        from DataRepo.models import Compound

        excs = [
            RecordDoesNotExist(
                Compound,
                Compound.get_name_query_expression("lysine"),
                column="compound",
                file="accucor.xlsx",
                sheet="Corrected",
                rownum=5,
            ),
            RecordDoesNotExist(
                Compound,
                Compound.get_name_query_expression("vibranium"),
                column="compound",
                file="accucor.xlsx",
                sheet="Corrected",
                rownum=19,
            ),
        ]
        mcs = MissingCompounds(excs)
        self.assertIn("2 Compound records", str(mcs))
        self.assertIn(
            "in column [compound] of sheet [Corrected] in accucor.xlsx", str(mcs)
        )
        self.assertIn("'lysine' from row(s): [5]", str(mcs))
        self.assertIn("'vibranium' from row(s): [19]", str(mcs))

    def test_MissingRecords(self):
        from DataRepo.models import Compound, MSRunSample

        excs = [
            RecordDoesNotExist(
                Compound,
                Compound.get_name_query_expression("lysine"),
                column="compound",
                file="accucor.xlsx",
                sheet="Corrected",
                rownum=5,
            ),
            RecordDoesNotExist(
                MSRunSample,
                {"name": "wish this existed"},
                column="MSRun Name",
                file="accucor.xlsx",
                sheet="Corrected",
                rownum=19,
            ),
        ]
        mcs = MissingRecords(excs)
        self.assertIn("1 Compound records", str(mcs))
        self.assertIn(
            "using search field(s): (OR: name__iexact, synonyms__name__iexact)",
            str(mcs),
        )
        self.assertIn("lysine from row(s): ['5']", str(mcs))
        self.assertIn("1 MSRunSample records", str(mcs))
        self.assertIn("wish this existed from row(s): ['19']", str(mcs))
        self.assertIn(
            "in column [compound] of sheet [Corrected] in accucor.xlsx", str(mcs)
        )
        self.assertIn(
            "in column [MSRun Name] of sheet [Corrected] in accucor.xlsx", str(mcs)
        )

    def get_sample_dnes(self):
        from DataRepo.models import Sample

        return [
            RecordDoesNotExist(
                Sample,
                {"name": "sample1"},
                column="Sample",
                file="accucor.xlsx",
                sheet="Corrected",
                rownum=5,
            ),
            RecordDoesNotExist(
                Sample,
                {"name": "sample2"},
                column="Sample",
                file="accucor.xlsx",
                sheet="Corrected",
                rownum=19,
            ),
        ]

    def test_MissingSamples(self):
        mss = MissingSamples(self.get_sample_dnes())
        self.assertIn("2 Sample records", str(mss))
        self.assertIn("'sample1' from row(s): [5]", str(mss))
        self.assertIn("'sample2' from row(s): [19]", str(mss))
        self.assertIn("column [Sample] of sheet [Corrected] in accucor.xlsx", str(mss))

    def test_UnskippedBlanks(self):
        usbs = UnskippedBlanks(self.get_sample_dnes())
        self.assertIn(
            "2 sample(s) from the load file data, that appear to possibly be blanks",
            str(usbs),
        )

    def test_NoSamples(self):
        nss = NoSamples(self.get_sample_dnes())
        self.assertIn("None of the 2 samples", str(nss))

    def test_UnexpectedSamples(self):
        sample_names = ["sample1", "sample2"]
        uess = UnexpectedSamples(
            sample_names,
            "study.xlsx",
            "Peak Annotation Details",
            "Sample Header",
            file="accucor.xlsx",
            sheet="Corrected",
        )
        self.assertIn("study.xlsx", str(uess))
        self.assertIn("Peak Annotation Details", str(uess))
        self.assertIn("Sample Header", str(uess))
        self.assertIn("sheet [Corrected] in accucor.xlsx", str(uess))
        self.assertIn("['sample1', 'sample2']", str(uess))

    def test_RecordDoesNotExist_get_failed_searches_dict(self):
        kwargs, stub, dct = RecordDoesNotExist.get_failed_searches_dict(
            self.get_sample_dnes()
        )
        self.assertDictEqual(
            {"column": "Sample", "file": "accucor.xlsx", "sheet": "Corrected"}, kwargs
        )
        self.assertEqual("name", stub)
        self.assertDictEqual({"sample1": [5], "sample2": [19]}, dct)

    def test_RecordDoesNotExist_get_query_stub(self):
        sdnes = self.get_sample_dnes()
        stub = sdnes[0]._get_query_stub()
        self.assertEqual("name", stub)

    def test_RecordDoesNotExist_get_query_values_str(self):
        sdnes = self.get_sample_dnes()
        valstr = sdnes[0]._get_query_values_str()
        self.assertEqual("sample1", valstr)

    def test_MzxmlNotColocatedWithAnnot(self):
        mncwa = MzxmlNotColocatedWithAnnot(
            file="/abs/path/to/file.mzXML",
            suggestion="Move a peak annot file to a point along the path.",
        )
        self.assertIn(
            "'/abs/path/to/file.mzXML' does not share a common path with a peak annotation file",
            str(mncwa),
        )
        self.assertIn("Move a peak annot file to a point along the path.", str(mncwa))

    def test_MzxmlNotColocatedWithAnnots(self):
        """Tests that the summary exception includes all the mzXMLs not colocated with a peak annotation file, that the
        exception describes this, and contains a suggestion of how to fix it."""
        exc1 = MzxmlNotColocatedWithAnnot(
            file="/abs/path/to/file.mzXML",
        )
        exc2 = MzxmlNotColocatedWithAnnot(
            file="/second/abs/path/to/otherfile.mzXML",
        )
        mncwas = MzxmlNotColocatedWithAnnots([exc1, exc2])
        self.assertIn(
            "/abs/path/to/file.mzXML",
            str(mncwas),
        )
        self.assertIn(
            "/second/abs/path/to/otherfile.mzXML",
            str(mncwas),
        )
        self.assertIn(
            "do not have a peak annotation file existing along their paths",
            str(mncwas),
        )
        self.assertIn(
            "add the related peak annotation file to the directory containing the mzXML files",
            str(mncwas),
        )

    def test_MzxmlColocatedWithMultipleAnnot(self):
        mcwma = MzxmlColocatedWithMultipleAnnot(
            ["name1", "name2"],
            "/abs/path/",
            file="/abs/path/to/file.mzXML",
        )
        self.assertIn(
            "associated with different sequences:\n\tname1\n\tname2\n", str(mcwma)
        )

    def test_MzxmlColocatedWithMultipleAnnots(self):
        """Tests that the summary exception includes:

        1. An explanation of the cause of the exception
        2. All mzXML files
        3. A suggestion how to fix the data to resolve the exception
        4. The directory containing the peak annotation files associated with sequence defaults
        5. The sequence names associated with the directory

        Example:
            The following directories have multiple peak annotation files (associated with different 'Default
            Sequence's, assigned in the 'Peak Annotation Files' sheet), meaning that the listed mzXML files cannot be
            unambiguously assigned an MSRunSequence record.

                Directory '/abs/path/' contains multiple peak annotation files associated with sequences ['seqname1',
                'seqname2']:
                    /abs/path/to/file.mzXML
                Directory '/abs/path2/' contains multiple peak annotation files associated with sequences ['seqnameA',
                'seqnameB']:
                    /abs/path/to/file.mzXML

            Explanation: When a sequence is not provided in the 'Peak Annotation Details' sheet for an mzXML file, the
            association between an mzXML and the MSRunSequence it belongs to is inferred by its colocation with (or its
            location under a parent directory containing) a peak annotation file, based on the 'Default Sequence'
            assigned in the 'Peak Annotation Files' sheet.

            Suggestion: Either provide values in the 'Sequence' column in the 'Peak Annotation Files' sheet or
            re-arrange the multiple colocated peak annotation files to ensure that they are in the directory containing
            the mzXML files that were used to generate them.  (If a peak annotation file was generated using a mix of
            mzXML files from different sequences, the 'Sequence' column in the 'Oeak Annotation Details' sheet must be
            filled in and it is recommended that mzXML files are grouped into directories defined by the sequence that
            generated them.)
        """
        exc1 = MzxmlColocatedWithMultipleAnnot(
            ["seqname1", "seqname2"],
            "/abs/path/",
            file="/abs/path/to/file.mzXML",
        )
        exc2 = MzxmlColocatedWithMultipleAnnot(
            ["seqnameA", "seqnameB"],
            "/abs/path2/",
            file="/abs/path/to/file.mzXML",
        )
        mcwmas = MzxmlColocatedWithMultipleAnnots([exc1, exc2])
        # Explanation
        self.assertIn(
            "mzXML and the MSRunSequence it belongs to is inferred by its colocation",
            str(mcwmas),
        )
        # mzXML files
        self.assertIn("/abs/path/to/file.mzXML", str(mcwmas))
        self.assertIn("/abs/path/to/file.mzXML", str(mcwmas))
        # Suggestion
        self.assertIn("provide values in the 'Sequence' column", str(mcwmas))
        self.assertIn(
            "re-arrange the multiple colocated peak annotation files", str(mcwmas)
        )
        # Directories
        self.assertIn("'/abs/path/'", str(mcwmas))
        self.assertIn("'/abs/path2/'", str(mcwmas))
        # Sequences
        self.assertIn("['seqname1', 'seqname2']", str(mcwmas))
        self.assertIn("['seqnameA', 'seqnameB']", str(mcwmas))

    def test_NoScans(self):
        ns = NoScans("/abs/path/to/file.mzXML")
        self.assertIn("'/abs/path/to/file.mzXML' contains no scans", str(ns))

    def test_DefaultSequenceNotFound(self):
        dsnf = DefaultSequenceNotFound(
            "Rob", "1972-11-24", "HILIC", "polar-HILIC-25-min"
        )
        self.assertIn(
            "operator: Rob\n\tprotocol: polar-HILIC-25-min\n\tinstrument: HILIC\n\tdate: 1972-11-24",
            str(dsnf),
        )

    def test_MultipleDefaultSequencesFound(self):
        mdsf = MultipleDefaultSequencesFound("Rob", "1972-11-24", "QE", None)
        self.assertIn(
            "operator: Rob\n\tprotocol: None\n\tinstrument: QE\n\tdate: 1972-11-24",
            str(mdsf),
        )

    def test_MissingC12ParentPeakErrors(self):
        mcpp = MissingC12ParentPeak("lysine")
        mcppe = MissingC12ParentPeaks([mcpp])
        # Check problem described
        self.assertIn("C12 PARENT peak row is missing", str(mcppe))
        # Check data included
        self.assertIn("lysine", str(mcppe))
        # Check suggestions exists
        self.assertIn("neglect to include the C12 PARENT peak", str(mcppe))
        self.assertIn(
            "ignore this error if the peak is below the detection threshold", str(mcppe)
        )

    def test_MissingC12ParentPeak(self):
        mcpp = MissingC12ParentPeak("lysine", file="accucor.xlsx")
        # Check problem described
        self.assertIn(
            "C12 PARENT peak row missing for compound 'lysine' in 'accucor.xlsx'.",
            str(mcpp),
        )
        # Check suggestions exists
        self.assertIn(
            "ignore this error if the peak is below the detection threshold",
            str(mcpp),
        )
        self.assertIn("neglect to include the C12 PARENT peak", str(mcpp))

    def test_PossibleDuplicateSamplesError(self):
        pds = PossibleDuplicateSample("s1", ["s1_pos", "s1_neg"])
        pdse = PossibleDuplicateSamples([pds])
        # Check problem described
        self.assertIn(
            "same name that are associated with different database samples", str(pdse)
        )
        # Check data included
        self.assertIn("header 's1' maps to samples: ['s1_pos', 's1_neg']", str(pdse))
        # Check suggestion exists
        self.assertIn("associated with the same tracebase sample", str(pdse))

    def test_DBFieldVsFileColDeveloperWarning(self):
        """Tests developer warnings about database/file value type issues.

        Requirements tested (from GitHub issue #1662):
            2. Repeated warnings must be summarized.
            3. The warning about comparing a value from the DB with with a value from a file when their types differ
               must be clear as to what the problem with it is and whether it needs to be addressed (and how)
            4. The warning must mention whether ConflictingValueError exceptions will be created, in the context of it's
               correctness being a determinant as to whether the type for the field should be added to the loader.
            6. Customized ProgrammerErrors' names must be clearly applicable to only programmers.
        """
        tissue_class = get_model_by_name("Tissue")
        rec = tissue_class.objects.create(name="elbow", description="knobby")
        exc = DBFieldVsFileColDeveloperWarning(
            rec,
            "description",
            "knobby",
            5,
            "TissuesLoader",
            rownum=22,
            sheet="Tissues",
            file="mystudy.xlsx",
        )
        # Test general requirement: All relevant data included to identify the problem
        self.assertIn("Tissue.description", str(exc))
        self.assertIn(
            "unmapped column in row [22] of sheet [Tissues] in mystudy.xlsx", str(exc)
        )
        self.assertIn("'knobby', a 'str'", str(exc))
        self.assertIn("'5', a 'int'", str(exc))
        # Test general requirement: Message includes suggestion
        self.assertIn(
            "If that conclusion is wrong, the loader (TissuesLoader) must be updated",
            str(exc),
        )
        self.assertIn(
            "Consult the docstring of the DBFieldVsFileColDeveloperWarning class for details.",
            str(exc),
        )
        # Test general requirement: Message includes explanation
        self.assertIn(
            "intended to help debug the case where that ConflictingValueError appears wrong",
            str(exc),
        )
        # Test Req 2. See docstring
        self.assertIsInstance(exc, SummarizableError)
        # Test Req 3. See docstring
        self.assertIn("type of the value", str(exc))
        self.assertIn("differs", str(exc))
        # Test Req 4. See docstring
        self.assertIn(
            "this exception will be followed by a ConflictingValueError", str(exc)
        )
        # Test Req 6. See docstring
        self.assertIsInstance(exc, DeveloperWarning)
        self.assertIn("DeveloperWarning", type(exc).__name__)

    def test_DBFieldVsFileColDeveloperWarnings(self):
        """Tests summary developer warnings about database/file value type issues.

        Requirements tested (from GitHub issue #1662):
            3. The warning about comparing a value from the DB with with a value from a file when their types differ
               must be clear as to what the problem with it is and whether it needs to be addressed (and how)
            4. The warning must mention whether ConflictingValueError exceptions will be created, in the context of it's
               correctness being a determinant as to whether the type for the field should be added to the loader.
            6. Customized ProgrammerErrors' names must be clearly applicable to only programmers.
        """
        tissue_class = get_model_by_name("Tissue")
        rec1 = tissue_class.objects.create(name="elbow", description="knobby")
        exc1 = DBFieldVsFileColDeveloperWarning(
            rec1,
            "description",
            "knobby",
            5,
            "TissuesLoader",
            rownum=22,
            sheet="Tissues",
            file="mystudy.xlsx",
        )
        rec2 = tissue_class.objects.create(name="knee", description="gnarly")
        exc2 = DBFieldVsFileColDeveloperWarning(
            rec2,
            "description",
            "gnarly",
            66,
            "TissuesLoader",
            rownum=55,
            sheet="Tissues",
            file="mystudy.xlsx",
        )
        exc = DBFieldVsFileColDeveloperWarnings([exc1, exc2])

        # Test general requirement: Message includes explanation
        self.assertIn(
            (
                "Model field values from existing records in the database were compared to values from the input file, "
                "but the types differed"
            ),
            str(exc),
        )
        self.assertIn(
            (
                "This warning is intended to help debug the case where any of those ConflictingValueError exceptions "
                "appear wrong"
            ),
            str(exc),
        )
        # Test general requirement: Message includes suggestion
        self.assertIn(
            "If any conclusions are wrong, the corresponding loader class must be updated",
            str(exc),
        )
        self.assertIn(
            "Consult the docstring of the DBFieldVsFileColDeveloperWarning class for details",
            str(exc),
        )
        # Test general requirement: All relevant data included to identify the problem
        self.assertIn("Loader: TissuesLoader, Field: Tissue.description", str(exc))
        self.assertIn("row [22] of sheet [Tissues] in mystudy.xlsx", str(exc))
        self.assertIn("'knobby' (type: str)", str(exc))
        self.assertIn("'5' (type: int)", str(exc))
        self.assertIn("row [55] of sheet [Tissues] in mystudy.xlsx", str(exc))
        self.assertIn("'gnarly' (type: str)", str(exc))
        self.assertIn("'66' (type: int)", str(exc))
        # Test Req 3. See docstring
        self.assertIn("types differed", str(exc))
        # Test Req 4. See docstring
        self.assertIn(
            "exceptions will be followed by ConflictingValueError exceptions", str(exc)
        )
        # Test Req 6. See docstring
        self.assertIsInstance(exc, DeveloperWarning)
        self.assertIn("DeveloperWarning", type(exc).__name__)

    def test_MissingFCircCalculationValues(self):
        mfcv = MissingFCircCalculationValues(
            [
                MissingFCircCalculationValue(
                    file="myfile",
                    sheet="Animals",
                    column="Infusion Rate",
                    rownum=5,
                ),
                MissingFCircCalculationValue(
                    file="myfile",
                    sheet="Animals",
                    column="Weight",
                    rownum=5,
                ),
                MissingFCircCalculationValue(
                    file="myfile",
                    sheet="Samples",
                    column="Collection Time",
                    rownum=20,
                ),
            ]
        )
        self.assertIn("FCirc calculations on TraceBase are done using", str(mfcv))
        self.assertIn("tracer peak group(s) from the last serum sample", str(mfcv))
        self.assertIn("infusion rate, and the animal weight", str(mfcv))
        self.assertIn("values are missing", str(mfcv))
        self.assertIn("sheet [Animals] in myfile", str(mfcv))
        self.assertIn("'Infusion Rate' on row(s): ['5']", str(mfcv))
        self.assertIn("'Weight' on row(s): ['5']", str(mfcv))
        self.assertIn("sheet [Samples] in myfile", str(mfcv))
        self.assertIn("'Collection Time' on row(s): ['20']", str(mfcv))

    def test_MissingFCircCalculationValue(self):
        mfcv = MissingFCircCalculationValue(
            file="myfile",
            sheet="mysheet",
            column="Infusion Rate",
            rownum=5,
        )
        self.assertIn("FCirc calculations on TraceBase are done using", str(mfcv))
        self.assertIn("tracer peak group(s) from the last serum sample", str(mfcv))
        self.assertIn("infusion rate, and the animal weight", str(mfcv))
        self.assertIn("This value is missing", str(mfcv))
        self.assertIn(
            "column [Infusion Rate] on row [5] of sheet [mysheet] in myfile", str(mfcv)
        )
        self.assertIn(
            "You can load data into tracebase without these values", str(mfcv)
        )
        self.assertIn(
            "FCirc values will either be missing (when there is no animal weight or infusion rate",
            str(mfcv),
        )
        self.assertIn("inaccurate (if the sample collection time is missing", str(mfcv))

    def test_ProhibitedCompoundNames(self):
        pcn1 = ProhibitedCompoundName(
            [";", "/"], file="file.txt", column="Compound", rownum=2
        )
        pcn2 = ProhibitedCompoundName(
            [";"], file="file.txt", column="Compound", rownum=5
        )
        pcn3 = ProhibitedCompoundName(
            ["/"], file="file.txt", column="Compound", rownum=16
        )
        e = ProhibitedCompoundNames(exceptions=[pcn1, pcn2, pcn3])
        self.assertIn("Prohibited substrings encountered", str(e))
        self.assertIn("column [Compound] in file.txt", str(e))
        self.assertIn("'/' on row(s): ['2', '16']", str(e))
        self.assertIn("';' on row(s): ['2', '5']", str(e))

    def test_ProhibitedStringValue(self):
        e = ProhibitedStringValue([";"], disallowed=[";", "/"], value="test;1")
        self.assertIn("Prohibited character(s) [';'] encountered", str(e))
        self.assertIn("(in 'test;1')", str(e))
        self.assertIn(
            "None of the following reserved substrings are allowed: [';', '/']", str(e)
        )

    def test_ProhibitedCompoundName(self):
        with self.assertRaises(RequiredArgument):
            ProhibitedCompoundName(
                [";", "/"], value="compound;test/name;/", fixed="compound_test_name__"
            )
        e = ProhibitedCompoundName(
            [";", "/"],
            value="compound;test/name;/",
            fixed="compound_test_name__",
            column="Compound",
            sheet="Compounds",
        )
        self.assertIn(
            "Prohibited compound name substring(s) [';', '/'] encountered", str(e)
        )
        self.assertIn("(in compound name 'compound;test/name;/')", str(e))
        self.assertIn(
            "in column [Compound] of sheet [Compounds] in the load file data", str(e)
        )
        self.assertIn(
            "Column 'Compound' values may not have any of the following reserved substrings: [';', '/'].",
            str(e),
        )
        self.assertIn(
            "The compound name was automatically repaired to be 'compound_test_name__'",
            str(e),
        )

    def test_AnimalWithoutSamples(self):
        e = AnimalWithoutSamples(
            "George", file="doc.xlsx", sheet="Animals", column="Name", rownum=5
        )
        # Contains all pertinent data to solve the problem
        self.assertIn("George", str(e))
        self.assertIn("column [Name] on row [5] of sheet [Animals] in doc.xlsx", str(e))
        # Explanation
        self.assertIn("Animal ", str(e))
        self.assertIn("does not have any samples", str(e))
        # Suggestion
        self.assertIn(
            "You can ignore this for now and submit samples for this animal in the future",
            str(e),
        )
        self.assertIn(
            "you can address the issue now by adding overlooked samples", str(e)
        )
        self.assertIn("or remove the animal from the Animals sheet", str(e))

    def test_AnimalsWithoutSamples(self):
        e1 = AnimalWithoutSamples(
            "George", file="doc.xlsx", sheet="Animals", column="Name", rownum=5
        )
        e2 = AnimalWithoutSamples(
            "Henrietta", file="doc.xlsx", sheet="Animals", column="Name", rownum=19
        )
        e = AnimalsWithoutSamples([e1, e2])
        # Contains all pertinent data to solve the problem
        self.assertIn("column [Name] of sheet [Animals] in doc.xlsx", str(e))
        self.assertIn("'George' on row 5", str(e))
        self.assertIn("'Henrietta' on row 19", str(e))
        # Explanation
        self.assertIn("Animals ", str(e))
        self.assertIn("do not have any samples", str(e))
        # Suggestion
        self.assertIn(
            "You can ignore this for now and submit samples for these animals in the future",
            str(e),
        )
        self.assertIn(
            "you can address the issue now by adding overlooked samples", str(e)
        )
        self.assertIn("or remove the animals from the Animals sheet", str(e))

    def test_AnimalWithoutSerumSamples(self):
        e = AnimalWithoutSerumSamples(
            "Kramer", file="doc.xlsx", sheet="Animals", column="Name", rownum=10
        )
        # Contains all pertinent data to solve the problem
        self.assertIn("Kramer", str(e))
        self.assertIn(
            "column [Name] on row [10] of sheet [Animals] in doc.xlsx", str(e)
        )
        # Explanation
        self.assertIn("Animal ", str(e))
        self.assertIn(
            "does not have the necessary serum samples to perform FCirc calc", str(e)
        )
        self.assertIn("FCirc calculations on TraceBase are done using", str(e))
        self.assertIn("the last serum sample", str(e))
        # Suggestion
        self.assertIn(
            "You can ignore this for now and submit serum samples for this animal in the future",
            str(e),
        )
        self.assertIn(
            "you can address the issue now by adding overlooked serum samples", str(e)
        )
        self.assertIn("or remove the animal from the Animals sheet", str(e))

    def test_AnimalsWithoutSerumSamples(self):
        e1 = AnimalWithoutSerumSamples(
            "Kramer", file="doc.xlsx", sheet="Animals", column="Name", rownum=10
        )
        e2 = AnimalWithoutSerumSamples(
            "Molly", file="doc.xlsx", sheet="Animals", column="Name", rownum=11
        )
        e = AnimalsWithoutSerumSamples([e1, e2])
        # Contains all pertinent data to solve the problem
        self.assertIn("column [Name] of sheet [Animals] in doc.xlsx", str(e))
        self.assertIn("'Kramer' on row 10", str(e))
        self.assertIn("'Molly' on row 11", str(e))
        # Explanation
        self.assertIn("animals ", str(e))
        self.assertIn(
            "do not have the necessary serum samples to perform FCirc calculations",
            str(e),
        )
        self.assertIn("FCirc calculations on TraceBase are done using", str(e))
        self.assertIn("the last serum sample", str(e))
        # Suggestion
        self.assertIn(
            "You can ignore this for now and submit serum samples for these animals in the future",
            str(e),
        )
        self.assertIn(
            "you can address the issue now by adding overlooked serum samples", str(e)
        )
        self.assertIn("or remove the animals from the Animals sheet", str(e))

    def test_trace(self):
        trc = trace()
        self.assertIn("trc = trace()", trc, msg=f"trace() output:\n{trc}")
        self.assertIn("test_exceptions.py", trc, msg=f"trace() output:\n{trc}")
        self.assertNotIn("site-packages", trc, msg=f"trace() output:\n{trc}")

    def test_MultipleConflictingValueMatchesSummary(self):
        from DataRepo.models import Study

        # These will not cause an integrity error, but that's not important for this test
        s1 = Study.objects.create(name="s1")
        s2 = Study.objects.create(name="s2")
        rec_dict1 = {"name": "s3"}

        recs_diffs1 = [
            (s1, {"name": {"orig": "s1", "new": "s3"}}),
            (s2, {"name": {"orig": "s2", "new": "s3"}}),
        ]

        mcvm1 = MultipleConflictingValueMatches(
            recs_diffs1,
            rec_dict=rec_dict1,
        )

        rec_dict2 = {"name": "s4"}

        recs_diffs2 = [
            (s1, {"name": {"orig": "s1", "new": "s4"}}),
            (s2, {"name": {"orig": "s2", "new": "s4"}}),
        ]

        mcvm2 = MultipleConflictingValueMatches(
            recs_diffs2,
            rec_dict=rec_dict2,
        )

        mcvms = MultipleConflictingValueMatchesSummary([mcvm1, mcvm2])

        # Number of sheets reported
        self.assertIn("Data from 1 file sheets has conflicts", str(mcvms))
        # Model and file reported
        self.assertIn("Model Study in the load file data", str(mcvms))
        # Record reported
        self.assertIn("file record: {'name': 's3'}", str(mcvms))
        # Differences indented and summarized
        self.assertIn("{'name': 's1', 'description': None}", str(mcvms))
        self.assertIn("name\n\t\t\t\tdatabase: [s1]\n\t\t\t\tfile: [s3]", str(mcvms))
        self.assertIn("{'name': 's2', 'description': None}", str(mcvms))
        self.assertIn("name\n\t\t\t\tdatabase: [s2]\n\t\t\t\tfile: [s3]", str(mcvms))
        self.assertIn("database: [s1]\n\t\t\t\tfile: [s4]", str(mcvms))
        self.assertIn("name\n\t\t\t\tdatabase: [s2]\n\t\t\t\tfile: [s4]", str(mcvms))

    def test_MultipleConflictingValueMatches(self):
        from DataRepo.models import Study

        # These will not cause an integrity error, but that's not important for this test
        s1 = Study.objects.create(name="s1")
        s2 = Study.objects.create(name="s2")
        rec_dict = {"name": "s3"}

        recs_diffs = [
            (s1, {"name": {"orig": "s1", "new": "s3"}}),
            (s2, {"name": {"orig": "s2", "new": "s3"}}),
        ]

        mcvm = MultipleConflictingValueMatches(
            recs_diffs,
            rec_dict=rec_dict,
            rownum=2,
            sheet="Study",
            file="study.xlsx",
        )

        self.assertIn("row [2] of sheet [Study] in study.xlsx", str(mcvm))
        self.assertIn("conflicts with 2 existing database records", str(mcvm))
        self.assertIn("database: [s1]\n\t\t\tfile: [s3]", str(mcvm))
        self.assertIn("database: [s2]\n\t\t\tfile: [s3]", str(mcvm))

    def test_AmbiguousMzxmlSampleMatch(self):
        amsm = AmbiguousMzxmlSampleMatch(
            ["sample1", "sample1other"],
            "sample1.mzXML",
            file="study.xlsx",
            sheet="Peak Annotation Details",
        )
        # Defines the problem
        self.assertIn(
            "mzXML file 'sample1.mzXML' could not be mapped to a single sample.",
            str(amsm),
        )
        # Explains the reason
        self.assertIn(
            "Each mzXML must be associated with an MSRunSample, which links to a Sample",
            str(amsm),
        )
        # Supplies resolution/fix suggestion
        self.assertIn(
            "add a row for every mzXML file with this name, including its path",
            str(amsm),
        )
        self.assertIn("to sheet [Peak Annotation Details] in study.xlsx", str(amsm))
        # Supplies the data necessary to implement the suggestion
        self.assertIn("sample matches include: ['sample1', 'sample1other']", str(amsm))

    def test_AmbiguousMzxmlSampleMatches(self):
        amsm1 = AmbiguousMzxmlSampleMatch(
            ["sample1", "sample1other"],
            "sample1.mzXML",
        )
        amsm2 = AmbiguousMzxmlSampleMatch(
            ["sample2", "sample2other"],
            "sample2.mzXML",
        )
        amsm_summary = AmbiguousMzxmlSampleMatches([amsm1, amsm2])
        # Defines the problem
        self.assertIn(
            "The following mzXML files could not be mapped to a single sample",
            str(amsm_summary),
        )
        # Explains the reason
        self.assertIn(
            "Each mzXML must be associated with an MSRunSample, which links to a Sample",
            str(amsm_summary),
        )
        # Supplies resolution/fix suggestion
        self.assertIn(
            "add a row for every mzXML file with the indicated name, including their path",
            str(amsm_summary),
        )
        self.assertIn("to the Peak Annotation Details sheet", str(amsm_summary))
        # Supplies the data necessary to implement the suggestion
        self.assertIn(
            "'sample1.mzXML' matches samples: ['sample1', 'sample1other']",
            str(amsm_summary),
        )
        self.assertIn(
            "'sample2.mzXML' matches samples: ['sample2', 'sample2other']",
            str(amsm_summary),
        )

    def create_quick_msrunsample(self):
        from DataRepo.models import (
            Animal,
            LCMethod,
            MSRunSample,
            MSRunSequence,
            Sample,
            Tissue,
        )

        # Some quick data setup.  We only need an MSRunSample record, but to create it, we need this stuff...
        animal = Animal.objects.create(name="test1", genotype="WT")
        tissue = Tissue.objects.create(name="liver")
        sample = Sample.objects.create(
            name="test1",
            date=datetime.now(),
            researcher="Paul",
            tissue=tissue,
            animal=animal,
        )
        lcm = LCMethod.objects.first()
        msrun_sequence = MSRunSequence.objects.create(
            researcher="John Doe",
            date=datetime.now(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
            lc_method=lcm,
        )
        return MSRunSample.objects.create(sample=sample, msrun_sequence=msrun_sequence)

    def test_ComplexPeakGroupDuplicate(self):
        """This exception is for PeakGroups that are duplicated due to an edited peak annotation file (and the data in
        the PeakGroup DID change).  Also tests the summary exception.
        """
        from DataRepo.models import (
            ArchiveFile,
            DataFormat,
            DataType,
            PeakGroup,
        )

        msrun_sample = self.create_quick_msrunsample()

        # Prepare to create an "edited" file (loaded and being loaded)
        ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
        accucor_format = DataFormat.objects.get(code="accucor")

        # This is the technical duplicate component (an edited file). The differing record content is the formula change
        orig_peak_annotation_file = ArchiveFile.objects.create(
            filename="test_data_file",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
            data_type=ms_peak_annotation,
            data_format=accucor_format,
        )
        new_peak_annotation_file = ArchiveFile.objects.create(
            filename="test_data_file",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c2",
            data_type=ms_peak_annotation,
            data_format=accucor_format,
        )

        pg = PeakGroup(
            name="testname",
            formula="C2H4O1",
            msrun_sample=msrun_sample,
            peak_annotation_file=orig_peak_annotation_file,
        )

        exc = ComplexPeakGroupDuplicate(
            pg,
            {
                "formula": {"orig": "C2H6", "new": "C2H4O1"},
                "peak_annotation_file": {
                    "orig": orig_peak_annotation_file,
                    "new": new_peak_annotation_file,
                },
            },
            rec_dict={
                "name": "testname",
                "formula": "C2H4O1",
                "msrun_sample": msrun_sample,
                "peak_annotation_file": new_peak_annotation_file,
            },
        )
        self.assertIn("Conflicting field values encountered", str(exc))
        self.assertIn("formula in\n\t\tdatabase: [C2H6]\n\t\tfile: [C2H4O1]", str(exc))
        self.assertIn(
            (
                "peak_annotation_file in\n\t\tdatabase: [test_data_file (558ea654d7f2914ca4527580edf4fac11bd151c3)]\n"
                "\t\tfile: [test_data_file (558ea654d7f2914ca4527580edf4fac11bd151c2)]"
            ),
            str(exc),
        )
        self.assertIn("There are 3 likely cases causing this error", str(exc))

        cpgds = ComplexPeakGroupDuplicates([exc])
        self.assertIn("C2H6", str(cpgds))
        self.assertIn("C2H4O1", str(cpgds))
        self.assertIn(
            "test_data_file (558ea654d7f2914ca4527580edf4fac11bd151c3)", str(cpgds)
        )
        self.assertIn(
            "test_data_file (558ea654d7f2914ca4527580edf4fac11bd151c2)", str(cpgds)
        )

    def test_TechnicalPeakGroupDuplicate(self):
        """This exception is for PeakGroups that are duplicated due to an edited peak annotation file (and the data in
        the PeakGroup DID NOT change).  Also tests the summary exception.
        """
        from DataRepo.models import (
            ArchiveFile,
            DataFormat,
            DataType,
            PeakGroup,
        )

        msrun_sample = self.create_quick_msrunsample()

        # Prepare to create an "edited" file (loaded and being loaded)
        ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
        accucor_format = DataFormat.objects.get(code="accucor")

        # This is the technical duplicate component (an edited file). The differing record content is the formula change
        orig_peak_annotation_file = ArchiveFile.objects.create(
            filename="test_data_file",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
            data_type=ms_peak_annotation,
            data_format=accucor_format,
        )
        new_peak_annotation_file = ArchiveFile.objects.create(
            filename="test_data_file",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c2",
            data_type=ms_peak_annotation,
            data_format=accucor_format,
        )

        new_pg = PeakGroup(
            name="testname",
            formula="C2H4O1",
            msrun_sample=msrun_sample,
            peak_annotation_file=new_peak_annotation_file,
        )
        PeakGroup(
            name="testname",
            formula="C2H4O1",
            msrun_sample=msrun_sample,
            peak_annotation_file=orig_peak_annotation_file,
        ).save()

        exc = TechnicalPeakGroupDuplicate(
            new_pg,
            PeakGroup.objects.filter(
                peak_annotation_file__checksum="558ea654d7f2914ca4527580edf4fac11bd151c3"
            ),
        )
        self.assertIn("Duplicate PeakGroup record created", str(exc))
        self.assertIn("edit of the peak annotation file", str(exc))
        self.assertIn(
            (
                "Compound: testname\n\tSample: test1\nEdited Peak Annotation Files:\n\tNew: test_data_file "
                "(558ea654d7f2914ca4527580edf4fac11bd151c2)"
            ),
            str(exc),
        )
        self.assertIn(
            "Existing: test_data_file (558ea654d7f2914ca4527580edf4fac11bd151c3)",
            str(exc),
        )

        tpgds = TechnicalPeakGroupDuplicates([exc])
        self.assertIn("test_data_file (1 peak groups)", str(tpgds))

    def test_DuplicatePeakGroup(self):
        """This exception is for PeakGroups that are duplicated due to a change in business rules relating to whether
        PeakGroup records link to concrete MSRunSample records or placeholder records.  Also tests the summary
        exception.
        """
        from DataRepo.models import (
            ArchiveFile,
            DataFormat,
            DataType,
            MSRunSample,
            PeakGroup,
        )

        msrun_sample = self.create_quick_msrunsample()

        # Prepare to create an "edited" file (loaded and being loaded)
        ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
        accucor_format = DataFormat.objects.get(code="accucor")

        # This is the technical duplicate component (an edited file). The differing record content is the formula change
        peak_annotation_file = ArchiveFile.objects.create(
            filename="test_data_file",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
            data_type=ms_peak_annotation,
            data_format=accucor_format,
        )

        mz_file = ArchiveFile.objects.create(
            filename="test_mz_file",
            file_location=None,
            checksum="2345678901",
            data_type=DataType.objects.get(code="ms_data"),
            data_format=DataFormat.objects.get(code="mzxml"),
        )

        concrete_msrun_sample = MSRunSample.objects.create(
            sample=msrun_sample.sample,
            msrun_sequence=msrun_sample.msrun_sequence,
            ms_data_file=mz_file,
        )

        PeakGroup(
            name="testname",
            formula="C2H4O1",
            msrun_sample=concrete_msrun_sample,
            peak_annotation_file=peak_annotation_file,
        ).save()

        new_pg = PeakGroup(
            name="testname",
            formula="C2H4O1",
            msrun_sample=msrun_sample,
            peak_annotation_file=peak_annotation_file,
        )
        exc = DuplicatePeakGroup(
            new_pg, PeakGroup.objects.filter(msrun_sample=concrete_msrun_sample)
        )
        self.assertIn("Duplicate PeakGroup record created", str(exc))
        self.assertIn(
            "Compound: testname\n\tPeak Annotation File: test_data_file", str(exc)
        )
        self.assertIn("MSRunSamples:\n\tNew: test1 run by John Doe", str(exc))
        self.assertIn("Existing: [<MSRunSample: test1 run by John Doe on ", str(exc))
        self.assertIn(
            "duplicate PeakGroup records are linked to different MSRunSample records",
            str(exc),
        )

        dpgs = DuplicatePeakGroups([exc])
        self.assertIn("test_data_file\n\t\ttestname", str(dpgs))

    def test_NoPeakAnnotationDetails(self):
        no_pk_ann_deets = NoPeakAnnotationDetails(
            "annot.xlsx",
            file="study.xlsx",
            sheet="Peak Annotation Details",
            column="Peak Annot File",
        )
        self.assertIn(
            "No sample headers for peak annotation file 'annot.xlsx'",
            str(no_pk_ann_deets),
        )
        self.assertIn(
            "column [Peak Annot File] of sheet [Peak Annotation Details] in study.xlsx",
            str(no_pk_ann_deets),
        )
        self.assertIn(
            "populating the Peak Annotation Details sheet", str(no_pk_ann_deets)
        )
        self.assertIn(
            "use the submission start page to generate this data", str(no_pk_ann_deets)
        )

        # Test its summary exception
        no_pk_ann_deets_ers = NoPeakAnnotationDetailsErrors([no_pk_ann_deets])
        self.assertIn(
            "No sample headers for the following peak annotation files",
            str(no_pk_ann_deets_ers),
        )
        self.assertIn(
            "found in the Peak Annotation Details sheet", str(no_pk_ann_deets_ers)
        )
        self.assertIn("annot.xlsx", str(no_pk_ann_deets_ers))
        self.assertIn(
            "associate them by populating the Peak Annotation Details sheet",
            str(no_pk_ann_deets_ers),
        )
        self.assertIn(
            "use the submission start page to generate this data",
            str(no_pk_ann_deets_ers),
        )

    def test_AllUnskippedBlanks(self):
        from DataRepo.models import Sample

        exceptions = [RecordDoesNotExist(Sample, {"name": "blank"})]
        exc = AllUnskippedBlanks(
            exceptions, succinct=False, suggestion="Test suggestion"
        )
        self.assertIn(
            "1 Sample records matching the following values were not found", str(exc)
        )
        self.assertIn("blank", str(exc))
        self.assertIn("Test suggestion", str(exc))
