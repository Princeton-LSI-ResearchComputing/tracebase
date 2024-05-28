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

    def test_hash_file(self):
        fn = "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        expected_hash = "c95f714d690bdd2ad069a7a0345dee9cb7cc1e23"
        self.assertEqual(expected_hash, ArchiveFile.hash_file(Path(fn)))

    def test_file_is_binary_true(self):
        fn = "DataRepo/data/tests/small_obob/small_obob_study.xlsx"
        self.assertTrue(ArchiveFile.file_is_binary(fn))

    def test_file_is_binary_false(self):
        fn = "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        self.assertFalse(ArchiveFile.file_is_binary(fn))

    def test_get_or_create_override(self):
        """This tests the essential functionality of the get_or_create method override, which is to ignore the
        randomized hash string appended to file_location.  but it also tests the conveniences (takes a path string,
        extracts the file name, generates a hash, and takes the codes for DataType and DataFormat).
        """
        fn = "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        rec_dict = {
            # "filename": xxx,  # Gets automatically filled in by the override of get_or_create
            # "checksum": xxx,  # Gets automatically filled in by the override of get_or_create
            # "imported_timestamp": xxx,  # Gets automatically filled in by the model
            "file_location": fn,  # Intentionally a string and not a File object
            "data_type": "ms_data",
            "data_format": "mzxml",
        }
        # Called the first time to create
        created_rec, created = ArchiveFile.objects.get_or_create(**rec_dict)
        self.assertTrue(created)
        self.assertEqual(
            "c95f714d690bdd2ad069a7a0345dee9cb7cc1e23", created_rec.checksum
        )
        self.assertEqual("mzxml", created_rec.data_format.code)
        self.assertEqual("ms_data", created_rec.data_type.code)
        self.assertEqual("BAT-xz971.mzXML", created_rec.filename)

        # Called a second time to "get"
        gotten_rec, second_created = ArchiveFile.objects.get_or_create(**rec_dict)
        self.assertFalse(second_created)
        self.assertEqual(created_rec.id, gotten_rec.id)

        # TODO: Figure out how to test that the file_location is an actual stored file
