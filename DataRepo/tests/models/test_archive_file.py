import os
from pathlib import Path

from django.conf import settings
from django.core.files import File
from django.db import transaction
from django.test import override_settings

from DataRepo.models import ArchiveFile, DataFormat, DataType
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.utilities import exists_in_db
from DataRepo.tests.tracebase_test_case import (
    TracebaseArchiveTestCase,
    TracebaseTestCase,
)


@override_settings(CACHES=settings.TEST_CACHES)
class ArchiveFileTests(TracebaseTestCase):
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
        fn = "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        expected_hash = "c95f714d690bdd2ad069a7a0345dee9cb7cc1e23"
        self.assertEqual(expected_hash, ArchiveFile.hash_file(Path(fn)))

    def test_file_is_binary_true(self):
        fn = "DataRepo/data/tests/small_obob/small_obob_study.xlsx"
        self.assertTrue(ArchiveFile.file_is_binary(fn))

    def test_file_is_binary_false(self):
        fn = "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        self.assertFalse(ArchiveFile.file_is_binary(fn))

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_archive_file_allow_missing_no_checksum_or_existing_file(
        self,
    ):
        """
        If a file does not exist and no checksum is provided, a ValueError should be raised.
        """
        fn = "does not exist"
        with self.assertRaises(ValueError) as ar:
            ArchiveFile.objects.get_or_create(
                filename=fn,
                data_type="ms_data",
                data_format="ms_raw",
            )
        ve = ar.exception
        self.assertIn(
            "A checksum is required if the supplied file path is not an existing file.",
            str(ve),
        )

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_archive_file_with_checksum(self):
        """
        If a checksum is supplied and the file doesn't exist, a record is created
        """
        fn = "does not exist"
        afrec, created = ArchiveFile.objects.get_or_create(
            filename=fn,
            data_type="ms_data",
            data_format="mzxml",
            checksum="somesuppliedvalue",
        )
        afrec.full_clean()
        afrec.save()
        self.assertTrue(created)
        self.assertTrue(exists_in_db(afrec))
        self.assertEqual("somesuppliedvalue", afrec.checksum)

    @MaintainedModel.no_autoupdates()
    def test_get_or_create_archive_file_allow_missing_file_exists(self):
        """
        If a file exists and a checksum is provided, an exception should be raised when that checksum does not match.
        """
        fn = "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        with self.assertRaises(ValueError) as ar:
            ArchiveFile.objects.get_or_create(
                file_location=Path(fn),
                data_type="ms_data",
                data_format="mzxml",
                checksum="somesuppliedvalue",
            )
        ve = ar.exception
        self.assertIn("somesuppliedvalue", str(ve))
        expected_hash = ArchiveFile.hash_file(Path(fn))
        self.assertIn(expected_hash, str(ve))


class ArchiveFileArchiveTests(TracebaseArchiveTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
        accucor_format = DataFormat.objects.get(code="accucor")
        path = Path("DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx")
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)
        cls.rec_dict = {
            "filename": "small_obob_maven_6eaas_inf.xlsx",
            "file_location": myfile,
            "checksum": "558ea654d7f2914ca4527580edf4fac11bd151c3",
            "data_type": ms_peak_annotation,
            "data_format": accucor_format,
        }
        super().setUpTestData()

    def setUp(self):
        super().setUp()
        self.rec = None

    def create_archive_file(self):
        path = Path("DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx")
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)
            ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
            accucor_format = DataFormat.objects.get(code="accucor")
            rec_dict = {
                "filename": "small_obob_maven_6eaas_inf.xlsx",
                "file_location": myfile,
                "checksum": "558ea654d7f2914ca4527580edf4fac11bd151c3",
                "data_type": ms_peak_annotation,
                "data_format": accucor_format,
            }
            self.rec = ArchiveFile.objects.create(**rec_dict)
            self.rec.full_clean()
        self.assertTrue(
            os.path.isfile(self.rec.file_location.path),
            msg="Asserts created file was saved to disk.",
        )

    @transaction.atomic
    def delete_during_transaction_failure(self):
        self.create_archive_file()
        self.rec.delete()
        self.assertTrue(
            os.path.isfile(self.rec.file_location.path),
            msg="Asserts created file does not delete during transaction.",
        )
        raise ValueError("TEST")

    @transaction.atomic
    def delete_during_transaction_success(self):
        self.create_archive_file()
        self.rec.delete()
        self.assertTrue(
            os.path.isfile(self.rec.file_location.path),
            msg="Asserts created file does not delete during transaction.",
        )

    def test_post_delete_commit_failure(self):
        """Tests that archive files are cleaned up after rollback."""
        with self.assertRaises(ValueError):
            self.delete_during_transaction_failure()
        self.assertIsNotNone(self.rec)
        self.assertIsNotNone(self.rec.file_location)
        self.assertIn(self.ARCHIVE_DIR, str(self.rec.file_location.path))
        self.assertTrue(
            os.path.isfile(self.rec.file_location.path),
            msg="Asserts created file was not deleted due to transaction rollback.",
        )
        self.assertFalse(exists_in_db(self.rec))

    def test_post_delete_commit_success(self):
        """Tests that archive files are cleaned up after rollback."""
        self.delete_during_transaction_success()
        self.assertIsNotNone(self.rec)
        self.assertIsNotNone(self.rec.file_location)
        self.assertIn(self.ARCHIVE_DIR, str(self.rec.file_location.path))
        self.assertFalse(
            os.path.isfile(self.rec.file_location.path),
            msg="Asserts created file was deleted after transaction commit due to call to record delete.",
        )
        self.assertFalse(exists_in_db(self.rec))

    def test_get_or_create_override(self):
        """This tests the essential functionality of the get_or_create method override, which is to ignore the
        randomized hash string appended to file_location.  but it also tests the conveniences (takes a path string,
        extracts the file name, generates a hash, and takes the codes for DataType and DataFormat).
        """
        fn = "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
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
        self.assertTrue(
            os.path.isfile(created_rec.file_location.path),
            msg="Asserts mzXML file was created in the archive.",
        )

        # Called a second time to "get"
        gotten_rec, second_created = ArchiveFile.objects.get_or_create(**rec_dict)
        self.assertFalse(second_created)
        self.assertEqual(created_rec.id, gotten_rec.id)
        self.assertTrue(
            os.path.isfile(gotten_rec.file_location.path),
            msg="Asserts mzXML file still exists.",
        )
