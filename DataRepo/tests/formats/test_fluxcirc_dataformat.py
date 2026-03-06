from DataRepo.formats.fluxcirc_dataformat import FluxCircFormat
from DataRepo.tests.formats.formats_test_base import FormatsTestCase


class FluxCircFormatTests(FormatsTestCase):
    def test_flux_circ_format(self):
        """Test FluxCircFormat.FluxCircFormat - no exception = successful test"""
        FluxCircFormat()

    def test_get_root_query_set(self):
        """Test FluxCircFormat.get_root_query_set"""
        fcf = FluxCircFormat()
        fcf.get_root_query_set()
