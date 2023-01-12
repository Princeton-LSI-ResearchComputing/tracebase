from DataRepo.models.researcher import UnknownResearcherError
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import AggregatedErrors, UnexpectedIsotopes


class ExceptionTests(TracebaseTestCase):
    def test_cull_warnings_ure_validate_warning_raise(self):
        unknown = ["Dave"]
        new = ["Dave", "Dan"]
        known = ["Dan", "Rob", "Shaji", "Mike"]
        ure = UnknownResearcherError(unknown, new, known)
        aes = AggregatedErrors([ure])
        validate = True
        should_raise = aes.cull_warnings(validate)
        self.assertTrue(should_raise)
        self.assertEqual(0, len(aes.errors))
        self.assertEqual(1, len(aes.warnings))
        self.assertTrue(isinstance(aes.warnings[0], UnknownResearcherError))

    def test_cull_warnings_ure_novalidate_warning_raise(self):
        unknown = ["Dave"]
        new = ["Dave", "Dan"]
        known = ["Dan", "Rob", "Shaji", "Mike"]
        ure = UnknownResearcherError(unknown, new, known)
        aes = AggregatedErrors([ure])
        validate = False
        should_raise = aes.cull_warnings(validate)
        self.assertTrue(should_raise)
        self.assertEqual(1, len(aes.errors))
        self.assertEqual(0, len(aes.warnings))
        self.assertTrue(isinstance(aes.errors[0], UnknownResearcherError))

    def test_cull_warnings_uie_validate_warning_raise(self):
        # The types of the contents of these arrays doesn't matter
        detected = ["C13", "N15"]
        labeled = ["C13"]
        compounds = ["Lysine"]
        uie = UnexpectedIsotopes(detected, labeled, compounds)
        aes = AggregatedErrors([uie])
        validate = True
        should_raise = aes.cull_warnings(validate)
        self.assertTrue(should_raise)
        self.assertEqual(0, len(aes.errors))
        self.assertEqual(1, len(aes.warnings))
        self.assertTrue(isinstance(aes.warnings[0], UnexpectedIsotopes))

    def test_cull_warnings_uie_novalidate_warning_raise(self):
        # The types of the contents of these arrays doesn't matter
        detected = ["C13", "N15"]
        labeled = ["C13"]
        compounds = ["Lysine"]
        uie = UnexpectedIsotopes(detected, labeled, compounds)
        aes = AggregatedErrors([uie])
        validate = False
        should_raise = aes.cull_warnings(validate)
        self.assertFalse(should_raise)
        self.assertEqual(0, len(aes.errors))
        self.assertEqual(1, len(aes.warnings))
        self.assertTrue(isinstance(aes.warnings[0], UnexpectedIsotopes))
