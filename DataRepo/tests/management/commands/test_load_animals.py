from django.conf import settings
from django.core.management import call_command
from django.db.models.deletion import RestrictedError
from django.test import override_settings

from DataRepo.models.animal import Animal
from DataRepo.models.compound import Compound
from DataRepo.models.element_label import ElementLabel
from DataRepo.models.infusate import Infusate
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.protocol import Protocol
from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    InfileDatabaseError,
    RequiredColumnValues,
)
from DataRepo.utils.infusate_name_parser import (
    parse_infusate_name,
    parse_infusate_name_with_concs,
)


@override_settings(CACHES=settings.TEST_CACHES)
class LoadAnimalsSmallObob2Tests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        Study.objects.create(name="obob_fasted")
        Study.objects.create(name="exp024_michael lactate timecourse")
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/small_obob2/protocols.tsv",
        )
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/small_obob2/compounds_for_animals.tsv",
        )
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name("C16:0-[13C16]", [1])
        )
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name("lysine-[13C6]", [2])
        )
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name("lactate-[13C3]", [148.88])
        )
        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob2/animals_table.tsv",
            headers="DataRepo/data/tests/small_obob2/animal_headers.yaml",
        )
        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob2/serum_lactate_animal_table.tsv",
            headers="DataRepo/data/tests/small_obob2/animal_headers.yaml",
        )

    def test_animals_loaded(self):
        self.assertEqual(8, Animal.objects.count())
        Study.objects.get(name="obob_fasted")
        self.assertEqual(7, Animal.objects.filter(studies__name="obob_fasted").count())
        self.assertEqual(8, Animal.objects.filter(labels__element="C").count())
        self.assertEqual(
            ["C"], list(Animal.objects.first().labels.values_list("element", flat=True))
        )

    def test_animal_tracers(self):
        a = Animal.objects.get(name="969")
        c = Compound.objects.get(name="C16:0")
        self.assertEqual(a.infusate.tracers.first().compound, c)
        self.assertEqual(
            a.infusate.tracers.first().labels.first().element, ElementLabel.CARBON
        )
        self.assertEqual(a.infusate.tracers.count(), 1)
        self.assertEqual(a.infusate.tracers.first().labels.count(), 1)
        self.assertEqual(a.sex, None)

    def test_animal_treatments_loaded(self):
        a = Animal.objects.get(name="969")
        self.assertEqual(a.treatment, None)
        a = Animal.objects.get(name="exp024f_M2")
        self.assertEqual(a.treatment.name, "T3")
        self.assertEqual(
            a.treatment.description,
            "For protocol's full text, please consult Michael Neinast.",
        )

    def test_restricted_animal_treatment_deletion(self):
        treatment = Animal.objects.get(name="exp024f_M2").treatment
        with self.assertRaises(RestrictedError):
            # test a restricted deletion
            treatment.delete()


@override_settings(CACHES=settings.TEST_CACHES)
class LoadAnimalsSmallObobTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        Study.objects.create(name="Small OBOB")
        Study.objects.create(name="test_labeled_elements")

        # TODO: This will need to change once the submission process refactor is done
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/small_obob/small_obob_study_prerequisites.yaml",
        )
        super().setUpTestData()

    @MaintainedModel.no_autoupdates()
    def test_animals_load_xlsx(self):
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )

        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob/study.xlsx",
        )
        self.assertEqual(1, Animal.objects.all().count())

    @MaintainedModel.no_autoupdates()
    def test_animals_labeled_element_parsing_invalid(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_animals",
                infile="DataRepo/data/tests/small_obob/study_labeled_elements_invalid.xlsx",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertIsInstance(aes.exceptions[0], InfileDatabaseError)
        self.assertIn("IsotopeParsingError", str(aes.exceptions[0]))


# TODO: Move MaintainedModel-specific tests to its own test file that doesn't use tracebase models
@override_settings(CACHES=settings.TEST_CACHES)
class LoadAnimalsAutoupdateTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        if 1 != len(all_coordinators):
            raise ValueError(
                f"Before setting up test data, there are {len(all_coordinators)} MaintainedModelCoordinators."
            )
        if all_coordinators[0].auto_update_mode != "always":
            raise ValueError(
                "Before setting up test data, the default coordinator is not in immediate autoupdate mode."
            )
        if 0 != all_coordinators[0].buffer_size():
            raise ValueError(
                f"Before setting up test data, there are {all_coordinators[0].buffer_size()} items in the buffer."
            )

        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/small_multitracer/compounds.tsv",
        )
        bcaa = "BCAAs (VLI) {valine-[13C5,15N1][20]; leucine-[13C6,15N1][24]; isoleucine-[13C6,15N1][12]}"
        Infusate.objects.get_or_create_infusate(parse_infusate_name_with_concs(bcaa))
        eaa6 = (
            "6EAAs (MFWKHT) {methionine-[13C5][14]; phenylalanine-[13C9][18]; tryptophan-[13C11][5]; "
            "lysine-[13C6][23]; histidine-[13C6][10]; threonine-[13C4][15]}"
        )
        Infusate.objects.get_or_create_infusate(parse_infusate_name_with_concs(eaa6))
        Protocol.objects.create(name="no treatment", category=Protocol.ANIMAL_TREATMENT)
        Protocol.objects.create(name="obob_fasted", category=Protocol.ANIMAL_TREATMENT)
        Study.objects.create(name="ob/ob Fasted")
        Study.objects.create(name="obob_fasted")
        Study.objects.create(name="Small OBOB")

        if 0 != all_coordinators[0].buffer_size():
            raise ValueError(
                f"legacy_load_study left {all_coordinators[0].buffer_size()} items in the buffer."
            )

        super().setUpTestData()

    def setUp(self):
        # Load data and buffer autoupdates before each test
        MaintainedModel._reset_coordinators()
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
            1, len(all_coordinators), msg=msg + "  The coordinator_stack is empty."
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
                0, coordinator.buffer_size(), msg=msg + "  The buffer is empty."
            )

    def test_animal_load_in_dry_run(self):
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )
        # Load some data to ensure that none of it changes during the actual test
        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_multitracer/study.xlsx",
        )

        pre_load_counts = self.get_record_counts()
        pre_load_maintained_values = MaintainedModel.get_all_maintained_field_values(
            "DataRepo.models"
        )
        self.assertGreater(
            len(pre_load_maintained_values.keys()),
            0,
            msg="Ensure there is data in the database before the test",
        )
        self.assert_coordinator_state_is_initialized()

        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob/study.xlsx",
            dry_run=True,
        )

        post_load_maintained_values = MaintainedModel.get_all_maintained_field_values(
            "DataRepo.models"
        )
        post_load_counts = self.get_record_counts()

        self.assertEqual(
            pre_load_counts,
            post_load_counts,
            msg="DryRun mode doesn't change any table's record count.",
        )
        self.assertEqual(
            pre_load_maintained_values,
            post_load_maintained_values,
            msg="DryRun mode doesn't autoupdate.",
        )

    @MaintainedModel.no_autoupdates()
    def test_animals_loader_check_required_values(self):
        """
        Check that missing required vals are raised as errors
        """
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_animals",
                infile="DataRepo/data/tests/small_obob/study_missing_rqd_vals.xlsx",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(isinstance(aes.exceptions[0], RequiredColumnValues))
        self.assertEqual(
            1,
            len(aes.exceptions[0].required_column_values),
            msg="1 row with missing required values",
        )
        self.assertIn(
            "[Genotype, Infusate, Study] on rows: ['3']",
            str(aes.exceptions[0]),
        )
