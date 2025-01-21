from DataRepo.formats.fluxcirc_dataformat import FluxCircFormat
from DataRepo.tests.formats.formats_test_base import FormatsTestCase


class FluxCircFormatTests(FormatsTestCase):
    def test_FluxCircFormat(self):
        """Test FluxCircFormat.FluxCircFormat - no exception = successful test"""
        FluxCircFormat()

    def test_getRootQuerySet(self):
        """Test FluxCircFormat.getRootQuerySet"""
        fcf = FluxCircFormat()
        fcf.getRootQuerySet()
