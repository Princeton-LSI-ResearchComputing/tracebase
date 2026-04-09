import os
import re
import shutil
import tempfile
from pathlib import Path

from django.core.management import call_command

from DataRepo.models.infusate import Infusate
from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs
from DataRepo.utils.studies_exporter import BadQueryTerm


class ExportStudiesTestBase(TracebaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir_obj = tempfile.TemporaryDirectory()
        cls.tmpdir = cls.tmpdir_obj.name
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):  # pylint: disable=invalid-name
        cls.tmpdir_obj.cleanup()
        super().tearDownClass()

    def tearDown(self):
        for filename in os.listdir(self.tmpdir):
            file_path = os.path.join(self.tmpdir, filename)
            shutil.rmtree(file_path, ignore_errors=True)
        super().tearDown()

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


class ExportStudiesTests(ExportStudiesTestBase):
    fixtures = ["data_formats", "data_types"]

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
        outdir = os.path.join(self.tmpdir, "test_dir_exists")
        os.mkdir(outdir)
        # No FileExistsError
        call_command(
            "export_studies",
            outdir=outdir,
            data_type=["Fcirc"],
        )

    def test_file_exists_no_overwrite(self):
        outdir = os.path.join(self.tmpdir, "test_dir")
        call_command(
            "export_studies",
            outdir=outdir,
            data_type=["Fcirc"],
        )
        with self.assertRaises(FileExistsError):
            call_command(
                "export_studies",
                outdir=outdir,
                data_type=["Fcirc"],
            )

    def test_file_exists_overwrite(self):
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "test_dir"),
            data_type=["Fcirc"],
        )
        # No FileExistsError
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "test_dir"),
            data_type=["Fcirc"],
            overwrite=True,
        )

    def test_mzxml_zip_export(self):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/full_tiny_study/study.xlsx",
        )
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "mzxml_study_all_types"),
            studies=["test v3 study"],
        )
        self.assertEqual(
            set(
                [
                    "study_0003/study_0003-peakgroups.tsv",
                    "study_0003/study_0003-peakdata.tsv",
                    "study_0003/study_0003-fcirc.tsv",
                    "study_0003/study_0003-mzxml.zip",
                ]
            ),
            set(
                [
                    # The study ID is random/arbitrary.  Change them all to be the same.  I chose 0003 arbitrarily,
                    # because that's how they appear when I run the test without modifying them.
                    re.sub(
                        r"study_\d+",
                        "study_0003",
                        str(
                            os.path.relpath(
                                p, os.path.join(self.tmpdir, "mzxml_study_all_types")
                            )
                        ),
                        count=2,
                    )
                    for p in Path(
                        os.path.join(self.tmpdir, "mzxml_study_all_types")
                    ).rglob("*/*/")
                ]
            ),
        )


class ExportStudiesMissingDataTests(ExportStudiesTestBase):
    def test_no_data_study_exists(self):
        """
        Should not raise exception when no data is available and study exists
        """
        call_command(
            "export_studies",
            outdir=os.path.join(self.tmpdir, "test_no_data_study_exists"),
            studies=["Small OBOB"],
        )
