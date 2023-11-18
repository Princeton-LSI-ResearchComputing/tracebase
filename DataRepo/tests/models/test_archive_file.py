from pathlib import Path

from django.conf import settings
from django.core.files import File
from django.test import override_settings

from DataRepo.models import ArchiveFile, DataFormat, DataType
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
class ArchiveFileTests(TracebaseTestCase):
    record_id = None

    fixtures = ["data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        path = Path("DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx")
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)

            cls.ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
            cls.accucor_format = DataFormat.objects.get(code="accucor")
            cls.accucor_file = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
                data_type=cls.ms_peak_annotation,
                data_format=cls.accucor_format,
            )
            cls.accucor_file.save()
            cls.record_id = cls.accucor_file.pk

    def test_data_type(self):
        ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
        self.assertEqual(ms_peak_annotation.code, "ms_peak_annotation")

    def test_archive_file_filename(self):
        """ArchiveFile lookup by id"""
        accucor_file = ArchiveFile.objects.get(id=self.accucor_file.id)
        self.assertEqual(accucor_file.filename, self.accucor_file.filename)

    def test_archive_file_checksum(self):
        """ArchiveFile lookup by checksum"""
        accucor_file = ArchiveFile.objects.get(checksum=self.accucor_file.checksum)
        self.assertEqual(accucor_file.filename, self.accucor_file.filename)
