from pathlib import Path

from django.test import SimpleTestCase


class TestASGI(SimpleTestCase):
    def test_uses_prod_settings(self):
        asgi = Path(__file__).resolve().parents[2] / "TraceBase" / "asgi.py"

        source = asgi.read_text()

        self.assertIn(
            '"TraceBase.settings.prod"',
            source,
        )

        self.assertNotIn(
            '"TraceBase.settings"',
            source,
        )
