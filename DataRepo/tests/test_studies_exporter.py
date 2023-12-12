import os
import tempfile

from django.core.exceptions import ObjectDoesNotExist
from django.core.management import call_command

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.studies_exporter import BadQueryTerm


class StudiesExporterTestBase(TracebaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir_obj = tempfile.TemporaryDirectory()
        cls.tmpdir = cls.tmpdir_obj.name
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        cls.tmpdir_obj.cleanup()
        super().tearDownClass()

    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename="DataRepo/example_data/small_dataset/"
            "small_obob_animal_and_sample_table.xlsx",
        )


class StudiesExporterTests(StudiesExporterTestBase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_serum.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
        )

    def test_all_studies_all_types(self):
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "test_all_studies_all_types"),
        )

    def test_all_studies_one_type(self):
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "test_all_studies_one_type"),
            data_type=["Fcirc"],
        )

    def test_str_data_type_changed_to_list(self):
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "test_str_data_type_changed_to_list"),
            data_type="Fcirc",
        )

    def test_one_study_all_types(self):
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "test_one_study_all_types"),
            studies=["Small OBOB"],
        )

    def test_bad_query_term(self):
        with self.assertRaises(BadQueryTerm) as ar:
            call_command(
                "export_studies",
                outdir=os.path.join(self.tmpdir, "test_one_studies_all_types"),
                studies=["these are not the droids", "youre looking for"],
            )
        exc = ar.exception
        self.assertIn("these are not the droids", str(exc))
        self.assertIn("youre looking for", str(exc))
        self.assertIn("DoesNotExist", str(exc))

    def test_dir_exists(self):
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "test_dir_exists"),
            data_type=["Fcirc"],
        )
        with self.assertRaises(FileExistsError):
            call_command(
                "export_studies",
                outdir=os.path.join(self.tmpdir, "test_dir_exists"),
                data_type=["Fcirc"],
            )


class MissingDataTests(StudiesExporterTestBase):
    def test_no_data_study_exists(self):
        """
        Should not raise exception when no data is available and study exists
        """
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "test_no_data_study_exists"),
            studies=["Small OBOB"],
        )

    def test_study_does_not_exist(self):
        """
        Should not raise exception when no data is available and study exists
        """
        with self.assertRaises(ObjectDoesNotExist):
            call_command(
                "export_studies",
                outdir=os.path.join(self.tmpdir, "test_study_does_not_exist"),
                studies=["Small OBOB"],
            )
