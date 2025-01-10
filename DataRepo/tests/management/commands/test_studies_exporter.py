import os
import tempfile

from django.core.management import call_command

from DataRepo.models.infusate import Infusate
from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs
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
        call_command("loaddata", "lc_methods")
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_study_prerequisites.xlsx",
        )
        Study.objects.create(name="Small OBOB")
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )
        call_command(
            "load_animals",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_samples",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )


class StudiesExporterTests(StudiesExporterTestBase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        call_command(
            "load_study",
            infile=(
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_serum/"
                "small_obob_sample_table_serum_only.xlsx"
            ),
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
