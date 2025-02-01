from django.core.management import call_command

from DataRepo.models.maintained_model import (
    MaintainedModel,
    MaintainedModelCoordinator,
    UncleanBufferError,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


def assert_coordinator_state_is_initialized():
    # Obtain all coordinators that exist
    all_coordinators = [MaintainedModel._get_default_coordinator()]
    all_coordinators.extend(MaintainedModel._get_coordinator_stack())
    if 1 != len(all_coordinators):
        raise ValueError(
            f"Before setting up test data, there are {len(all_coordinators)} (not 1) MaintainedModelCoordinators."
        )
    if all_coordinators[0].auto_update_mode != "always":
        raise ValueError(
            "Before setting up test data, the default coordinator is not in always autoupdate mode."
        )
    if 0 != all_coordinators[0].buffer_size():
        raise UncleanBufferError()


class ModelViewTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_formats.yaml", "data_types.yaml"]

    @classmethod
    def setUpTestData(cls, disabled_coordinator=False):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_no_newsample.xlsx",
        )
        cls.ALL_TISSUES_COUNT = 35
        cls.ALL_COMPOUNDS_COUNT = 51

        if not disabled_coordinator:
            # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after
            # itself
            assert_coordinator_state_is_initialized()

        # 15 placeholders and 2 concrete from mzXML files
        cls.ALL_SAMPLES_COUNT = 15
        cls.ALL_MSRUN_SAMPLES_COUNT = 17
        # not counting the header and the BLANK animal
        cls.ALL_ANIMALS_COUNT = 1

        cls.INF_COMPOUNDS_COUNT = 2
        cls.INF_SAMPLES_COUNT = 14
        cls.INF_PEAKDATA_ROWS = 11
        cls.INF_PEAKGROUP_COUNT = cls.INF_COMPOUNDS_COUNT * cls.INF_SAMPLES_COUNT

        cls.SERUM_COMPOUNDS_COUNT = 3
        cls.SERUM_SAMPLES_COUNT = 1
        cls.SERUM_PEAKDATA_ROWS = 13
        cls.SERUM_PEAKGROUP_COUNT = cls.SERUM_COMPOUNDS_COUNT * cls.SERUM_SAMPLES_COUNT

        cls.ALL_SEQUENCES_COUNT = 1

        super().setUpTestData()

    def setUp(self):
        # Load data and buffer autoupdates before each test
        self.assert_coordinator_state_is_initialized()
        super().setUp()

    def tearDown(self):
        self.assert_coordinator_state_is_initialized()
        super().tearDown()

    def assert_coordinator_state_is_initialized(
        self, msg="MaintainedModelCoordinators are in the default state."
    ):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        # Make sure there is only the default coordinator
        self.assertEqual(
            1,
            len(all_coordinators),
            msg=msg + "  The coordinator_stack should be empty.",
        )
        # Make sure that its mode is "always"
        self.assertEqual(
            "always",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode should be 'always'.",
        )
        # Make sure that the buffer is empty to start
        for coordinator in all_coordinators:
            self.assertEqual(
                0, coordinator.buffer_size(), msg=msg + "  The buffer should be empty."
            )


def create_null_tolerance_test_class(base_class):
    """
    Class creation factory where the base class is an argument.  Note, it must receive a TestCase-compatible class.

    Supply a test class where you want to redo every test with auto-updates disabled.

    Example:
    CompoundViewNullToleranceTests: TestCase = create_null_tolerance_test_class(CompoundViewTests)
    """

    class ModelViewNullToleranceTests(base_class):
        """
        This class overrides the setUpTestData method to load without auto-updates.  All super tests are executed.
        Any that are broken are overridden here to have something to apply the broken tags to.
        """

        @classmethod
        def setUpTestData(cls):
            super().setUpTestData(disabled_coordinator=True)

        @classmethod
        def setUpClass(self):
            # Silently dis-allow auto-updates by adding a disabled coordinator
            disabled_coordinator = MaintainedModelCoordinator("disabled")
            MaintainedModel._add_coordinator(disabled_coordinator)
            super().setUpClass()

        def setUp(self):
            # Load data and buffer autoupdates before each test
            self.assert_coordinator_state_is_initialized()
            super().setUp()

        @classmethod
        def tearDownClass(cls):
            super().tearDownClass()
            MaintainedModel._reset_coordinators()

        def assert_coordinator_state_is_initialized(
            self, msg="MaintainedModelCoordinators are in the default state."
        ):
            # Obtain all coordinators that exist
            all_coordinators = [MaintainedModel._get_default_coordinator()]
            all_coordinators.extend(MaintainedModel._get_coordinator_stack())
            # Make sure there is only the default coordinator
            self.assertEqual(
                2,
                len(all_coordinators),
                msg=msg
                + "  The coordinator_stack should have the disabled coordinator.",
            )
            # Make sure that its mode is "always"
            self.assertEqual(
                "always",
                all_coordinators[0].auto_update_mode,
                msg=msg + "  Mode should be 'always'.",
            )
            # Make sure that the buffer is empty to start
            for coordinator in all_coordinators:
                self.assertEqual(
                    0,
                    coordinator.buffer_size(),
                    msg=msg + "  The buffer should be empty.",
                )

    return ModelViewNullToleranceTests
