import unittest

from .utils import get_setting_values


class TestProdSettings(unittest.TestCase):
    def test_debug_vars_false(self):
        """Ensures that the debug variables cannot be turned on in production."""
        settings = get_setting_values(
            "TraceBase.settings.prod",
            "DEBUG",
            "SQL_LOGGING",
            "DEBUG_TOOLBAR_ENABLED",
            "DEBUG_TOOLBAR",
            "TESTING",
            env_overrides={
                "DEBUG": "true",
                "SQL_LOGGING": "true",
                "DEBUG_TOOLBAR_ENABLED": "true",
                "DEBUG_TOOLBAR": "true",
                "TESTING": "true",
            },
        )

        self.assertFalse(settings["DEBUG"])
        self.assertFalse(settings["SQL_LOGGING"])
        self.assertFalse(settings["DEBUG_TOOLBAR_ENABLED"])
        self.assertFalse(settings["DEBUG_TOOLBAR"])
        self.assertFalse(settings["TESTING"])
