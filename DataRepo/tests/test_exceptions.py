from DataRepo.models.researcher import UnknownResearcherError
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import AggregatedErrors, UnexpectedIsotopes


class ExceptionTests(TracebaseTestCase):
    def do_validate_level_raise_assertions(
        self, exception, validate, exp_should_raise, exp_errlen, exp_warnlen
    ):
        aes = AggregatedErrors([exception])
        should_raise = aes.cull_warnings(validate)
        self.assertEqual(exp_should_raise, should_raise)
        self.assertEqual(exp_errlen, len(aes.errors))
        self.assertEqual(exp_warnlen, len(aes.warnings))
        return aes

    def test_cull_warnings_ure_validate_warning_raise(self):
        unknown = ["Dave"]
        new = ["Dave", "Dan"]
        known = ["Dan", "Rob", "Shaji", "Mike"]
        ure = UnknownResearcherError(unknown, new, known)
        aes = self.do_validate_level_raise_assertions(ure, True, True, 0, 1)
        self.assertTrue(isinstance(aes.warnings[0], UnknownResearcherError))

    def test_cull_warnings_ure_novalidate_error_raise(self):
        unknown = ["Dave"]
        new = ["Dave", "Dan"]
        known = ["Dan", "Rob", "Shaji", "Mike"]
        ure = UnknownResearcherError(unknown, new, known)
        aes = self.do_validate_level_raise_assertions(ure, False, True, 1, 0)
        self.assertTrue(isinstance(aes.errors[0], UnknownResearcherError))

    def test_cull_warnings_uie_validate_warning_raise(self):
        # The types of the contents of these arrays doesn't matter
        detected = ["C13", "N15"]
        labeled = ["C13"]
        compounds = ["Lysine"]
        uie = UnexpectedIsotopes(detected, labeled, compounds)
        aes = self.do_validate_level_raise_assertions(uie, True, True, 0, 1)
        self.assertTrue(isinstance(aes.warnings[0], UnexpectedIsotopes))

    def test_cull_warnings_uie_novalidate_warning_raise(self):
        # The types of the contents of these arrays doesn't matter
        detected = ["C13", "N15"]
        labeled = ["C13"]
        compounds = ["Lysine"]
        uie = UnexpectedIsotopes(detected, labeled, compounds)
        aes = self.do_validate_level_raise_assertions(uie, False, False, 0, 1)
        self.assertTrue(isinstance(aes.warnings[0], UnexpectedIsotopes))
