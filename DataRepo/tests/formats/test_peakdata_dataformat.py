from parameterized import parameterized

from DataRepo.formats.peakdata_dataformat import PeakDataFormat
from DataRepo.models.utilities import get_model_by_name
from DataRepo.tests.formats.formats_test_base import FormatsTestCase


class PeakdataDataformatMainTests(FormatsTestCase):

    def test_PeakDataFormat(self):
        """Test __main__.PeakDataFormat - no exception = successful test"""
        PeakDataFormat()


class PeakDataFormatTests(FormatsTestCase):

    @parameterized.expand(FormatsTestCase.archive_file_instances)
    def test_PeakDataFormat_getModelFromInstance(self, _, instance, model):
        pgsv = PeakDataFormat()
        res = pgsv.getModelFromInstance(instance)
        self.assertEqual(res, model)

    def test_getOrderByFields_instance(self):
        pdf = PeakDataFormat()
        mdl_inst = "PeakData"
        mdl = get_model_by_name(mdl_inst)

        # Retreive any custom ordering
        self.assertEqual(
            ["peak_group", "-corrected_abundance"],
            mdl._meta.__dict__["ordering"],
            msg=(
                "Ensure that the peak_group field is present (because it's a foreign key that should be converted to "
                "the default field) and that the corrected abundance field has a negative sign so that the following "
                "test is meaningful."
            ),
        )
        order_bys = pdf.getOrderByFields(mdl_inst_nm=mdl_inst)
        expected_order_bys = ["peak_group__name", "corrected_abundance"]
        self.assertEqual(expected_order_bys, order_bys)
