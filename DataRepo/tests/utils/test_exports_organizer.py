import os
from pathlib import Path
import shutil
import tempfile

from django.test import TestCase, override_settings

from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exports_organizer import ExportsOrganizer


class ExportsOrganizerTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        Study.objects.create(
            id=0,
            name="Test Study-1",
            description="Test Description",
        )
        Study.objects.create(
            id=1,
            name="Test Study 2",
            description="Test Description",
        )

    def test_exportsorganizer(self):
        exports_organizer = ExportsOrganizer()
        self.assertEqual(2, len(exports_organizer.slugified_study_names))
        self.assertEqual("allstudies", exports_organizer.allstudies_str)
        self.assertEqual("alldatatypes", exports_organizer.alldatatypes_str)

    def test_get_slugified_study_names_dict(self):
        exports_organizer = ExportsOrganizer()
        self.assertEqual({0: "Test_Study_1", 1: "Test_Study_2"}, exports_organizer.slugified_study_names)

    def test_organize(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "one_export")
                shutil.copytree("DataRepo/data/tests/exports_organizer/one_export", export_dir)
                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base))
                    for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.organize(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base))
                    for p in base.rglob("*")
                )
                self.assertEqual(["YY"], export_dir_contents)

    def test_organize_exports_by_study_and_datatype(self):
        # TODO: Implement test
        pass

    def test_parse_export_filename(self):
        # TODO: Implement test
        pass

    def test_remove_unchanged_exports(self):
        # TODO: Implement test
        pass

    def test_assemble_export_packages(self):
        # TODO: Implement test
        pass

    def test_files_differ(self):
        # TODO: Implement test
        pass

    def test_mzxml_zips_differ(self):
        # TODO: Implement test
        pass

    def test_tsv_files_differ(self):
        # TODO: Implement test
        pass

    def test_tsv_file_objs_differ(self):
        # TODO: Implement test
        pass

    def test_get_mzxml_zip_checksums(self):
        # TODO: Implement test
        pass

    def test_compute_all_package_filename(self):
        # TODO: Implement test
        pass

    def test_compute_study_package_filename(self):
        # TODO: Implement test
        pass

    def test_compute_datatype_package_filename(self):
        # TODO: Implement test
        pass

    def test_zip_export_combos(self):
        # TODO: Implement test
        pass

    def test_zip_study_packages(self):
        # TODO: Implement test
        pass

    def test_zip_datatype_packages(self):
        # TODO: Implement test
        pass

    def test_zip_everything_packages(self):
        # TODO: Implement test
        pass

    def test_filepaths_to_zip(self):
        # TODO: Implement test
        pass

class ExportParseErrorTests(TracebaseTestCase):
    def test_exportparseerror(self):
        # TODO: Implement test
        pass

class NotOneMzxmlMetadataFileTests(TracebaseTestCase):
    def test_notonemzxmlmetadatafile(self):
        # TODO: Implement test
        pass
