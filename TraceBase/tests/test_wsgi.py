from pathlib import Path

from django.test import SimpleTestCase


class TestWSGI(SimpleTestCase):
    def test_uses_prod_settings(self):
        wsgi = Path(__file__).resolve().parents[2] / "TraceBase" / "wsgi.py"

        source = wsgi.read_text()

        self.assertIn(
            '"TraceBase.settings.prod"',
            source,
        )

        self.assertNotIn(
            '"TraceBase.settings"',
            source,
        )
