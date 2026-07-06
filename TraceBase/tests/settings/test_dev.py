import unittest

from .utils import get_setting_values


class TestDevSettings(unittest.TestCase):
    def test_debug_is_true(self):
        settings = get_setting_values(
            "TraceBase.settings.dev",
            "DEBUG",
            "SQL_LOGGING",
            "DEBUG_TOOLBAR_ENABLED",
            "DEBUG_TOOLBAR",
            "TESTING",
            env_overrides={
                "DEBUG": "True",
                "SQL_LOGGING": "True",
                "DEBUG_TOOLBAR_ENABLED": "True",
                "DEBUG_TOOLBAR": "True",
                "TESTING": "True",  # Cannot override
            },
        )

        self.assertTrue(settings["DEBUG"])
        self.assertTrue(settings["SQL_LOGGING"])
        self.assertTrue(settings["DEBUG_TOOLBAR_ENABLED"])
        self.assertTrue(settings["DEBUG_TOOLBAR"])
        self.assertFalse(settings["TESTING"])
