from pathlib import Path

from django.test import SimpleTestCase


class TestManage(SimpleTestCase):
    def test_uses_dev_settings(self):
        manage = Path(__file__).resolve().parents[2] / "manage.py"

        source = manage.read_text()

        self.assertIn(
            '"TraceBase.settings.dev"',
            source,
        )

        self.assertNotIn(
            '"TraceBase.settings"',
            source,
        )
