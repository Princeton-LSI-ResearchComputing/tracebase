from django.db.utils import IntegrityError

from DataRepo.models import PeakData, PeakDataLabel
from DataRepo.tests.models.test_peak_data import PeakDataData


class PeakDataLabelTests(PeakDataData):
    def setUp(self):
        super().setUp()
        pd = PeakData.objects.get(raw_abundance=1000.0)
        PeakDataLabel.objects.create(
            peak_data=pd,
            element="C",
            count=5,
            mass_number=13,
        )

    def test_record(self):
        rec = PeakDataLabel.objects.get(element="C")
        rec.full_clean()

    def test_multiple_labels_with_same_elem(self):
        """Test creating a second PeakDataLabel with the same element"""
        pd = PeakData.objects.get(raw_abundance=1000.0)
        with self.assertRaisesRegex(IntegrityError, r"\(\d+, C\)"):
            PeakDataLabel.objects.create(
                peak_data=pd,
                element="C",
                count=1,
                mass_number=13,
            )
