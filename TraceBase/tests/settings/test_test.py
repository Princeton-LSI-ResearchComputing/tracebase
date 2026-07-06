import unittest

from .utils import get_setting_values


class TestTestSettings(unittest.TestCase):
    def test_installed_apps(self):
        """Tests that "DataRepo.tests.apps.test_apps.LoaderTestConfig" was added to the installed apps."""
        settings = get_setting_values(
            "TraceBase.settings.test",
            "INSTALLED_APPS",
        )

        self.assertIn(
            "DataRepo.tests.apps.test_apps.LoaderTestConfig",
            settings["INSTALLED_APPS"],
        )

    def test_caches_setting(self):
        settings = get_setting_values(
            "TraceBase.settings.test",
            "CACHES_SETTING",
            env_overrides={"CACHES_SETTING": "PROD_CACHES"},
        )

        self.assertEqual(
            "PROD_CACHES",
            settings["CACHES_SETTING"],
        )

    def test_test_env_variables_present(self):
        settings = get_setting_values(
            "TraceBase.settings.test",
            "TEST_MEDIA_ROOT",
            "TEST_STORAGES",
            "TEST_FILE_STORAGES",
            "TEST_CACHES",
            "TEST_RUNNER",
            "TESTING",
            env_overrides={"TESTING": "false"},  # Cannot override
        )

        self.assertIn("TEST_MEDIA_ROOT", settings)
        self.assertIn("TEST_STORAGES", settings)
        self.assertIn("TEST_FILE_STORAGES", settings)
        self.assertIn("TEST_CACHES", settings)
        self.assertIn("TEST_RUNNER", settings)
        self.assertTrue(settings["TESTING"])
