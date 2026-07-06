import importlib
import unittest


class TestBaseSettings(unittest.TestCase):
    def test_import(self):
        """Tests that base can be imported."""
        importlib.import_module("TraceBase.settings.base")

    def test_secret_key_not_defined(self):
        """Tests that using TraceBase.settings.base will fail because it does not have a secret key."""
        base = importlib.import_module("TraceBase.settings.base")
        self.assertFalse(hasattr(base, "SECRET_KEY"))
