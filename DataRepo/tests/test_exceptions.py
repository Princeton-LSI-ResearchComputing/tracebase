from DataRepo.models.researcher import UnknownResearcherError
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    DuplicateValueErrors,
    DuplicateValues,
    MultiLoadStatus,
    RequiredColumnValue,
    RequiredColumnValues,
    RequiredValueError,
    RequiredValueErrors,
    ResearcherNotNew,
    UnexpectedIsotopes,
    UnitsWrong,
    generate_file_location_string,
    summarize_int_list,
)

# TODO: Move this file into the utils subdirectory


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

    def test_units_wrong(self):
        units_dict = {
            "Infusion Rate": {
                "example_val": "3.3 non/sense",
                "expected": "ul/m/g",
                "rows": [5, 6],
                "units": "non/sense",
            },
        }
        uw = UnitsWrong(units_dict)
        self.assertEqual(
            (
                "Unexpected units were found in 1 columns:\n"
                "\tInfusion Rate (example: [3.3 non/sense] does not match units: [ul/m/g] on row(s): [5, 6])\n"
                "Units are not allowed, but these also appear to be the wrong units."
            ),
            str(uw),
        )
        self.assertTrue(hasattr(uw, "units_dict"))
        self.assertEqual(uw.units_dict, units_dict)

    def test_researcher_not_new_takes_list(self):
        """
        Issue #712
        Requirement: 6. ResearcherNotNew must take a list of researchers (see TODO in accucor_data_loader.py)
        """
        existing = ["paul", "bob"]
        all = ["paul", "bob", "george"]
        ResearcherNotNew(existing, "--new-researcher", all)
        # No exception = successful test

    def test_generate_file_location_string(self):
        lstr = generate_file_location_string(
            column=2, rownum=3, sheet="Animals", file="animals.xlsx"
        )
        self.assertEqual(
            "column [2] on row [3] of sheet [Animals] in file [animals.xlsx]", lstr
        )
        lstr = generate_file_location_string(column=2, rownum=3, sheet="Animals")
        self.assertEqual(
            "column [2] on row [3] of sheet [Animals] in the load file data", lstr
        )
        lstr = generate_file_location_string(
            rownum=3, sheet="Animals", file="animals.xlsx"
        )
        self.assertEqual("row [3] of sheet [Animals] in file [animals.xlsx]", lstr)
        lstr = generate_file_location_string(
            column=2, sheet="Animals", file="animals.xlsx"
        )
        self.assertEqual("column [2] of sheet [Animals] in file [animals.xlsx]", lstr)
        lstr = generate_file_location_string(column=2, rownum=3, file="animals.xlsx")
        self.assertEqual("column [2] on row [3] in file [animals.xlsx]", lstr)

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
            "\tfile [loadme.txt]\n"
            "\t\tColumn(s) ['col2']\n"
            "\t\t\tx (rows*: 2-3)\n"
            "\t\tColumn(s) ['col3']\n"
            "\t\t\t2 (rows*: 2-3)\n"
            "\tfile [loadme2.txt]\n"
            "\t\tColumn(s) ['col2']\n"
            "\t\t\tx (rows*: 2-3)\n"
            "\t\tColumn(s) ['col3']\n"
            "\t\t\t2 (rows*: 2-3)\n"
        )
        self.assertEqual(expected, str(dve))

    def test_RequiredColumnValues(self):
        rcvs = [
            RequiredColumnValue("col2", 5, sheet="Tissues", file="loadme.tsv"),
            RequiredColumnValue("col2", 6, sheet="Tissues", file="loadme.tsv"),
            RequiredColumnValue("col2", 7, sheet="Tissues", file="loadme.tsv"),
            RequiredColumnValue("col2", 8, sheet="Tissues", file="loadme.tsv"),
        ]
        rcv = RequiredColumnValues(rcvs)
        expected = (
            "Required column values missing on the indicated rows:\n"
            "\tsheet [Tissues] in file [loadme.tsv]\n"
            "\t\tColumn: [col2] on rows: ['5-8']\n"
        )
        self.assertEqual(expected, str(rcv))

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
        expected = (
            "Required values found missing during loading:\n"
            "\tsheet [tissues] in file [tissues.tsv]:\n"
            "\t\tField: [Tissue.name] Column: [Tissue Name] on row(s): 3-4\n"
        )
        self.assertEqual(expected, str(rve))


class MultiLoadStatusTests(TracebaseTestCase):
    maxDiff = None

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
                    "num_errors": 0,
                    "num_warnings": 0,
                    "top": True,  # Passing files will appear first
                },
            },
            mls.statuses,
        )
        self.assertTrue(mls.get_success_status())
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
                    "num_errors": 0,
                    "num_warnings": 0,
                    "top": True,  # Passing files will appear first
                },
                "mykey2": {
                    "aggregated_errors": None,
                    "state": "PASSED",
                    "num_errors": 0,
                    "num_warnings": 0,
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
        aes.buffer_warning(ValueError("Test warning"))
        mls.set_load_exception(aes, "mykey", top=True)
        self.assertEqual(1, len(mls.statuses.keys()))
        self.assertEqual(
            {
                "num_errors": 0,
                "num_warnings": 1,
                "top": True,
                "aggregated_errors": aes,
                "state": "WARNING",
            },
            mls.statuses["mykey"],
        )
        self.assertFalse(mls.is_valid)
        self.assertEqual("WARNING", mls.state)
        self.assertFalse(mls.get_success_status())
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
                "num_errors": 1,
                "num_warnings": 0,
                "top": False,
                "aggregated_errors": aes,
                "state": "FAILED",
            },
            mls.statuses["mykey"],
        )
        self.assertFalse(mls.is_valid)
        self.assertEqual("FAILED", mls.state)
        self.assertFalse(mls.get_success_status())
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

        # We will assert that the 1 status recorded has these values (excluding the AggregatedErrors object, which will
        # differ)
        status_vals = {
            "num_errors": 1,
            "num_warnings": 0,
            "top": False,
            "state": "FAILED",
        }
        for key in status_vals.keys():
            self.assertEqual(
                status_vals[key],
                mls.statuses["mykey"][key],
            )

        self.assertFalse(mls.is_valid)
        self.assertEqual("FAILED", mls.state)
        self.assertFalse(mls.get_success_status())
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
        self.assertEqual(1, mls.num_warnings)
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
        print(f"MESSAGES:\n{messages}")
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

    def test_RequiredColumnValueWhenNovel(self):
        pass

    def test_RequiredColumnValuesWhenNovel(self):
        pass

    def test_ExcelSheetsNotFound(self):
        pass

    def test_InvalidHeaderCrossReferenceError(self):
        pass

    def test_OptionsNotAvailable(self):
        pass

    def test_MutuallyExclusiveOptions(self):
        pass

    def test_NoLoadData(self):
        pass
