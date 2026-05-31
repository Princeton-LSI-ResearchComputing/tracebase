import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import List
from unittest.mock import patch

from django.test import override_settings

from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exports_organizer import (
    ExportParseError,
    ExportsOrganizer,
    NotOneMzxmlMetadataFile,
)


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
        self.assertEqual(
            {0: "Test_Study_1", 1: "Test_Study_2"},
            exports_organizer.slugified_study_names,
        )

    def assert_zip_file_contents(self, zip_path: str, expected_root_files: List[str]):
        with zipfile.ZipFile(zip_path) as zf:
            root_files = [name for name in zf.namelist()]
        self.assertEqual(expected_root_files, root_files)

    def test_organize_one_study_one_date(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "one_export")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/one_export", export_dir
                )
                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
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
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-Fcirc.zip",
                    "tb9-2026.04.17-allstudies-PeakData.zip",
                    "tb9-2026.04.17-allstudies-PeakGroups.zip",
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-mzXML.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

                self.assert_zip_file_contents(
                    os.path.join(
                        export_dir, "tb9-2026.04.17-Test_Study_2-alldatatypes.zip"
                    ),
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    ],
                )
                self.assert_zip_file_contents(
                    os.path.join(export_dir, "tb9-2026.04.17-allstudies-Fcirc.zip"),
                    ["tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"],
                )
                self.assert_zip_file_contents(
                    os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakData.zip"),
                    ["tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"],
                )
                self.assert_zip_file_contents(
                    os.path.join(
                        export_dir, "tb9-2026.04.17-allstudies-PeakGroups.zip"
                    ),
                    ["tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv"],
                )
                self.assert_zip_file_contents(
                    os.path.join(
                        export_dir,
                        "tb9-2026.04.17-allstudies-alldatatypes.zip",
                    ),
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    ],
                )
                self.assert_zip_file_contents(
                    os.path.join(export_dir, "tb9-2026.04.17-allstudies-mzXML.zip"),
                    ["tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"],
                )

    def test_organize_one_study_two_dates_no_change(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_no_change")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_no_change",
                    export_dir,
                )
                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.organize(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    # Since there are no changes in the 4/24/26 files, they are removed
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-Fcirc.zip",
                    "tb9-2026.04.17-allstudies-PeakData.zip",
                    "tb9-2026.04.17-allstudies-PeakGroups.zip",
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-mzXML.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.17-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-Fcirc.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakData.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakGroups.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-mzXML.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"],
            )

    def test_organize_one_study_two_dates_one_mzxml_change(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_one_mzxml_change")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_one_mzxml_change",
                    export_dir,
                )
                print(f"L {export_dir}")
                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.organize(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    # Since there are no changes in the 4/24/26 files, they are removed
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-Fcirc.zip",
                    "tb9-2026.04.17-allstudies-PeakData.zip",
                    "tb9-2026.04.17-allstudies-PeakGroups.zip",
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-mzXML.zip",
                    # The zip had a changed mzXML, so it is retained and the other file types from that date removed
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    # All of the zip packages are made for the new date since one file changes
                    "tb9-2026.04.24-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.24-allstudies-Fcirc.zip",
                    "tb9-2026.04.24-allstudies-PeakData.zip",
                    "tb9-2026.04.24-allstudies-PeakGroups.zip",
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                    "tb9-2026.04.24-allstudies-mzXML.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.17-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-Fcirc.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakData.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakGroups.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-mzXML.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"],
            )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.24-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    # This is the one file that changed
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-Fcirc.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-PeakData.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-PeakGroups.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    # This is the one file that changed
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-mzXML.zip"),
                # This is the one file that changed
                ["tb9-2026.04.24-Test_Study_2-0001-mzXML.zip"],
            )

    def test_organize_one_study_two_dates_one_mzxmltsv_change(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_one_mzxmltsv_change")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_one_mzxmltsv_change",
                    export_dir,
                )
                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.organize(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    # Since there are no changes in the 4/24/26 files, they are removed
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-Fcirc.zip",
                    "tb9-2026.04.17-allstudies-PeakData.zip",
                    "tb9-2026.04.17-allstudies-PeakGroups.zip",
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-mzXML.zip",
                    # The zip had a changed tsv, so it is retained and the other file types from that date removed
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    # All of the zip packages are made for the new date since one file changes
                    "tb9-2026.04.24-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.24-allstudies-Fcirc.zip",
                    "tb9-2026.04.24-allstudies-PeakData.zip",
                    "tb9-2026.04.24-allstudies-PeakGroups.zip",
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                    "tb9-2026.04.24-allstudies-mzXML.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.17-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-Fcirc.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakData.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakGroups.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-mzXML.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"],
            )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.24-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    # This is the one file that changed
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-Fcirc.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-PeakData.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-PeakGroups.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    # This is the one file that changed
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-mzXML.zip"),
                # This is the one file that changed
                ["tb9-2026.04.24-Test_Study_2-0001-mzXML.zip"],
            )

    def test_organize_one_study_two_dates_one_tsv_change(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_one_tsv_change")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_one_tsv_change",
                    export_dir,
                )
                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.organize(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    # Since there are no changes in the 4/24/26 files, they are removed
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-Fcirc.zip",
                    "tb9-2026.04.17-allstudies-PeakData.zip",
                    "tb9-2026.04.17-allstudies-PeakGroups.zip",
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-mzXML.zip",
                    # The zip had a changed tsv, so it is retained and the other file types from that date removed
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    # All of the zip packages are made for the new date since one file changes
                    "tb9-2026.04.24-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.24-allstudies-Fcirc.zip",
                    "tb9-2026.04.24-allstudies-PeakData.zip",
                    "tb9-2026.04.24-allstudies-PeakGroups.zip",
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                    "tb9-2026.04.24-allstudies-mzXML.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.17-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-Fcirc.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakData.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakGroups.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-mzXML.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"],
            )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.24-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    # This is the one file that changed
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-Fcirc.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-PeakData.zip"),
                ["tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-PeakGroups.zip"),
                ["tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    # This is the one file that changed
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-mzXML.zip"),
                # This is the one file that changed
                ["tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"],
            )

    def test_organize_two_studies_two_dates_no_change(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_two_studies_no_change")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_two_studies_no_change",
                    export_dir,
                )
                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.organize(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    # Since there are no changes in the 4/24/26 files, they are removed
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_1-alldatatypes.zip",
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-Fcirc.zip",
                    "tb9-2026.04.17-allstudies-PeakData.zip",
                    "tb9-2026.04.17-allstudies-PeakGroups.zip",
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-mzXML.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.17-Test_Study_1-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.17-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-Fcirc.zip"),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakData.zip"),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakGroups.zip"),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-mzXML.zip"),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                ],
            )

    def test_organize_one_study_two_dates_study_added(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_one_study_add_study")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_one_study_add_study",
                    export_dir,
                )
                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.organize(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    # Since there are no changes in the 4/24/26 files, they are removed
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.17-Test_Study_1-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-Fcirc.zip",
                    "tb9-2026.04.17-allstudies-PeakData.zip",
                    "tb9-2026.04.17-allstudies-PeakGroups.zip",
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                    "tb9-2026.04.17-allstudies-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_1-alldatatypes.zip",
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.24-allstudies-Fcirc.zip",
                    "tb9-2026.04.24-allstudies-PeakData.zip",
                    "tb9-2026.04.24-allstudies-PeakGroups.zip",
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                    "tb9-2026.04.24-allstudies-mzXML.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.17-Test_Study_1-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.24-Test_Study_1-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.24-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-Fcirc.zip"),
                ["tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-Fcirc.zip"),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakData.zip"),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-PeakData.zip"),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-PeakGroups.zip"),
                ["tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-PeakGroups.zip"),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                ],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.17-allstudies-mzXML.zip"),
                ["tb9-2026.04.17-Test_Study_1-0000-mzXML.zip"],
            )
            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-mzXML.zip"),
                [
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                ],
            )

    def test_organize_exports_by_study_and_datatype(self):
        export_files = [
            # Since there are no changes in the 4/24/26 files, they are removed
            "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
            "tb9-2026.04.17-Test_Study_1-alldatatypes.zip",
            "tb9-2026.04.17-allstudies-Fcirc.zip",
            "tb9-2026.04.17-allstudies-PeakData.zip",
            "tb9-2026.04.17-allstudies-PeakGroups.zip",
            "tb9-2026.04.17-allstudies-alldatatypes.zip",
            "tb9-2026.04.17-allstudies-mzXML.zip",
            "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
            "tb9-2026.04.24-Test_Study_1-alldatatypes.zip",
            "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
            "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
            "tb9-2026.04.24-Test_Study_2-alldatatypes.zip",
            "tb9-2026.04.24-allstudies-Fcirc.zip",
            "tb9-2026.04.24-allstudies-PeakData.zip",
            "tb9-2026.04.24-allstudies-PeakGroups.zip",
            "tb9-2026.04.24-allstudies-alldatatypes.zip",
            "tb9-2026.04.24-allstudies-mzXML.zip",
        ]
        exports_organizer = ExportsOrganizer()
        exports_by_study = exports_organizer.organize_exports_by_study_and_datatype(
            export_files
        )
        self.assertEquivalent(
            {
                "tb9": {
                    "0000": {
                        "Fcirc": [
                            {
                                "date": "2026.04.17",
                                # We passed in relative paths, so we get relative paths
                                "file": "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                                "id": 0,
                                "name": "Test Study 1",
                                "slug": "Test_Study_1",
                                "ext": "tsv",
                                "data_type": "Fcirc",
                            },
                            {
                                "date": "2026.04.24",
                                # We passed in relative paths, so we get relative paths
                                "file": "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                                "id": 0,
                                "name": "Test Study 1",
                                "slug": "Test_Study_1",
                                "ext": "tsv",
                                "data_type": "Fcirc",
                            },
                        ],
                    },
                    "0001": {
                        "Fcirc": [
                            {
                                "date": "2026.04.24",
                                # We passed in relative paths, so we get relative paths
                                "file": "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "Fcirc",
                            },
                        ],
                        "mzXML": [
                            {
                                "date": "2026.04.24",
                                # We passed in relative paths, so we get relative paths
                                "file": "tb9-2026.04.24-Test_Study_2-0001-mzXML.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "mzXML",
                            },
                        ],
                    },
                }
            },
            exports_by_study,
        )
        print(f"M {exports_by_study}")

    def test_parse_export_filename(self):
        self.assertEqual(
            ("tracebase", "2026.04.17", "acute_stress", "0035", "FCirc", ".tsv"),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-acute_stress-0035-FCirc.tsv"
            ),
        )
        self.assertEqual(
            ("tracebase", "2026.04.17", "acute_stress", "0035", "mzXML", ".zip"),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-acute_stress-0035-mzXML.zip"
            ),
        )
        self.assertEqual(
            ("tracebase", "2026.04.17", "acute_stress", "0035", "alldatatypes", ".zip"),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-acute_stress-0035-alldatatypes.zip"
            ),
        )
        self.assertEqual(
            ("tracebase", "2026.04.17", "allstudies", None, "alldatatypes", ".zip"),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-allstudies-alldatatypes.zip"
            ),
        )
        self.assertEqual(
            ("tracebase", "2026.04.17", "allstudies", None, "PeakData", ".zip"),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-allstudies-PeakData.zip"
            ),
        )

    def test_remove_unchanged_exports(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                test_dir = (
                    "DataRepo/data/tests/exports_organizer/two_exports_one_tsv_change"
                )

                # Copy in files that don't change and ones that do
                for src in [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",  # unchanged
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",  # changed
                ]:
                    shutil.copy2(os.path.join(test_dir, src), tmpdir)

                exports_organizer = ExportsOrganizer()
                exports_organizer.export_dir = tmpdir
                exports_by_study = {
                    "tb9": {
                        "0001": {
                            "Fcirc": [
                                {
                                    "date": "2026.04.17",
                                    "file": os.path.join(
                                        # The export dir is prepended in ExportsOrganizer.organize()
                                        tmpdir,
                                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "Fcirc",
                                },
                                {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        # The export dir is prepended in ExportsOrganizer.organize()
                                        tmpdir,
                                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "Fcirc",
                                },
                            ],
                            "PeakData": [
                                {
                                    "date": "2026.04.17",
                                    "file": os.path.join(
                                        # The export dir is prepended in ExportsOrganizer.organize()
                                        tmpdir,
                                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "PeakData",
                                },
                                {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        # The export dir is prepended in ExportsOrganizer.organize()
                                        tmpdir,
                                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "PeakData",
                                },
                            ],
                        },
                    },
                }
                package_dates_by_host = exports_organizer.remove_unchanged_exports(
                    exports_by_study
                )

                # Check that only tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv was removed
                base = Path(tmpdir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )

                # Check that the returned dict of sets is correct
                self.assertEqual(
                    {"tb9": set(["2026.04.17", "2026.04.24"])}, package_dates_by_host
                )

    def test_assemble_export_packages(self):
        package_dates_by_host = {"tb9": set(["2026.04.17", "2026.04.24"])}
        exports_by_study = {
            "tb9": {
                "0001": {
                    "Fcirc": [
                        {
                            "date": "2026.04.17",
                            "file": "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                            "id": 1,
                            "name": "Test Study 2",
                            "slug": "Test_Study_2",
                            "ext": "tsv",
                            "data_type": "Fcirc",
                        },
                    ],
                    "PeakData": [
                        {
                            "date": "2026.04.17",
                            "file": "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                            "id": 1,
                            "name": "Test Study 2",
                            "slug": "Test_Study_2",
                            "ext": "tsv",
                            "data_type": "PeakData",
                        },
                        {
                            "date": "2026.04.24",
                            "file": "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                            "id": 1,
                            "name": "Test Study 2",
                            "slug": "Test_Study_2",
                            "ext": "tsv",
                            "data_type": "PeakData",
                        },
                    ],
                },
            },
        }
        exports_organizer = ExportsOrganizer()
        (
            study_packages_by_differing_dates,
            datatype_packages_by_differing_dates,
            all_packages_by_differing_dates,
        ) = exports_organizer.assemble_export_packages(
            package_dates_by_host, exports_by_study
        )
        self.assertEqual(
            {
                "tb9": {
                    "0001": {
                        "2026.04.17": {
                            "Fcirc": {
                                "date": "2026.04.17",
                                "file": "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "Fcirc",
                            },
                            "PeakData": {
                                "date": "2026.04.17",
                                "file": "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "PeakData",
                            },
                        },
                        "2026.04.24": {
                            "Fcirc": {
                                "date": "2026.04.17",
                                "file": "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "Fcirc",
                            },
                            "PeakData": {
                                "date": "2026.04.24",
                                "file": "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "PeakData",
                            },
                        },
                    },
                },
            },
            study_packages_by_differing_dates,
        )
        self.assertEqual(
            {
                "tb9": {
                    "Fcirc": {
                        "2026.04.17": {
                            "0001": {
                                "date": "2026.04.17",
                                "file": "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "Fcirc",
                            },
                        },
                        "2026.04.24": {
                            "0001": {
                                "date": "2026.04.17",
                                "file": "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "Fcirc",
                            },
                        },
                    },
                    "PeakData": {
                        "2026.04.17": {
                            "0001": {
                                "date": "2026.04.17",
                                "file": "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "PeakData",
                            },
                        },
                        "2026.04.24": {
                            "0001": {
                                "date": "2026.04.24",
                                "file": "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "PeakData",
                            },
                        },
                    },
                },
            },
            datatype_packages_by_differing_dates,
        )
        self.assertEqual(
            {
                "tb9": {
                    "2026.04.17": {
                        "0001": {
                            "Fcirc": {
                                "date": "2026.04.17",
                                "file": "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "Fcirc",
                            },
                            "PeakData": {
                                "date": "2026.04.17",
                                "file": "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "PeakData",
                            },
                        },
                    },
                    "2026.04.24": {
                        "0001": {
                            "Fcirc": {
                                "date": "2026.04.17",
                                "file": "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "Fcirc",
                            },
                            "PeakData": {
                                "date": "2026.04.24",
                                "file": "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                                "id": 1,
                                "name": "Test Study 2",
                                "slug": "Test_Study_2",
                                "ext": "tsv",
                                "data_type": "PeakData",
                            },
                        },
                    },
                },
            },
            all_packages_by_differing_dates,
        )

    def test_files_differ(self):
        export_dir = "DataRepo/data/tests/exports_organizer/two_exports_one_tsv_change"

        same_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"
        )
        same_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv"
        )
        self.assertFalse(ExportsOrganizer.files_differ(same_file1, same_file2))

        same_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"
        )
        same_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip"
        )
        self.assertFalse(ExportsOrganizer.files_differ(same_file1, same_file2))

        diff_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"
        )
        diff_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv"
        )
        self.assertTrue(ExportsOrganizer.files_differ(diff_file1, diff_file2))

        # Check the mzXML zip files that differ
        export_dir = (
            "DataRepo/data/tests/exports_organizer/two_exports_one_mzxml_change"
        )
        diff_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"
        )
        diff_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip"
        )
        self.assertTrue(ExportsOrganizer.files_differ(diff_file1, diff_file2))

        export_dir = (
            "DataRepo/data/tests/exports_organizer/two_exports_one_mzxmltsv_change"
        )
        diff_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"
        )
        diff_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip"
        )
        self.assertTrue(ExportsOrganizer.files_differ(diff_file1, diff_file2))

    def test_mzxml_zips_differ(self):
        export_dir = "DataRepo/data/tests/exports_organizer/two_exports_one_tsv_change"
        same_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"
        )
        same_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip"
        )
        self.assertFalse(ExportsOrganizer.mzxml_zips_differ(same_file1, same_file2))

        export_dir = (
            "DataRepo/data/tests/exports_organizer/two_exports_one_mzxml_change"
        )
        diff_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"
        )
        diff_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip"
        )
        self.assertTrue(ExportsOrganizer.mzxml_zips_differ(diff_file1, diff_file2))

        export_dir = (
            "DataRepo/data/tests/exports_organizer/two_exports_one_mzxmltsv_change"
        )
        diff_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"
        )
        diff_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip"
        )
        self.assertTrue(ExportsOrganizer.mzxml_zips_differ(diff_file1, diff_file2))

    def test_tsv_files_differ(self):
        export_dir = "DataRepo/data/tests/exports_organizer/two_exports_one_tsv_change"

        same_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"
        )
        same_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv"
        )
        self.assertFalse(ExportsOrganizer.tsv_files_differ(same_file1, same_file2))

        diff_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"
        )
        diff_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv"
        )
        self.assertTrue(ExportsOrganizer.tsv_files_differ(diff_file1, diff_file2))

    def test_tsv_file_objs_differ(self):
        export_dir = "DataRepo/data/tests/exports_organizer/two_exports_one_tsv_change"

        same_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv"
        )
        same_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv"
        )
        with open(same_file1, "r") as f1, open(same_file2, "r") as f2:
            self.assertFalse(ExportsOrganizer.tsv_file_objs_differ(f1, f2))

        diff_file1 = os.path.join(
            export_dir, "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv"
        )
        diff_file2 = os.path.join(
            export_dir, "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv"
        )
        with open(diff_file1, "r") as f1, open(diff_file2, "r") as f2:
            self.assertTrue(ExportsOrganizer.tsv_file_objs_differ(f1, f2))

    def test_get_mzxml_zip_checksums(self):
        checksum_dict = ExportsOrganizer.get_mzxml_zip_checksums(
            (
                "DataRepo/data/tests/exports_organizer/two_exports_one_mzxmltsv_change/"
                "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip"
            )
        )
        self.assertDictEqual(
            {
                (
                    "mzXML_mzxmls_28.05.2026.15.50.31/2024-11-10/Edmundo Leiva/QEPlus/polar-HILIC-25-min/negative/"
                    "70-900/1NP24.mzXML"
                ): "a24e2886e05d8e004495174959640f68044a955d7af90809fdba525b0e3fd268",
                (
                    "mzXML_mzxmls_28.05.2026.15.50.31/2024-11-10/Edmundo Leiva/QEPlus/polar-HILIC-25-min/positive/"
                    "118.5-600/1NP24.mzXML"
                ): "adcd5337f7747fef9dc3576c5367c57640c42d62f3bc04f9f9649d0b15df9326",
                (
                    "mzXML_mzxmls_28.05.2026.15.50.31/2024-11-10/Edmundo Leiva/QEPlus/polar-HILIC-25-min/positive/"
                    "56-67.05/1NP24.mzXML"
                ): "de03f822a7f2abc026bbaa3054757b0062f557063d9d31bea94300f9bcddebe5",
            },
            checksum_dict,
        )

    def test_compute_all_package_filename(self):
        exports_organizer = ExportsOrganizer()
        filename = exports_organizer.compute_all_package_filename(
            "tb9",
            "2026.04.17",
        )
        self.assertEqual("tb9-2026.04.17-allstudies-alldatatypes.zip", filename)

    def test_compute_study_package_filename(self):
        exports_organizer = ExportsOrganizer()
        filename = exports_organizer.compute_study_package_filename(
            "tb9", "2026.04.17", "0001"
        )
        self.assertEqual("tb9-2026.04.17-Test_Study_2-alldatatypes.zip", filename)

    def test_compute_datatype_package_filename(self):
        exports_organizer = ExportsOrganizer()
        filename = exports_organizer.compute_datatype_package_filename(
            "tb9", "2026.04.17", "FCirc"
        )
        self.assertEqual("tb9-2026.04.17-allstudies-FCirc.zip", filename)

    def test_zip_export_combos(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_one_study_add_study")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_one_study_add_study",
                    export_dir,
                )

                study_packages = {
                    "tb9": {
                        "0001": {
                            "2026.04.24": {
                                "Fcirc": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "Fcirc",
                                },
                                "PeakData": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "PeakData",
                                },
                            },
                        },
                    },
                }
                datatype_packages = {
                    "tb9": {
                        "Fcirc": {
                            "2026.04.24": {
                                "0001": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "Fcirc",
                                },
                            },
                        },
                    },
                }
                all_packages = {
                    "tb9": {
                        "2026.04.24": {
                            "0001": {
                                "Fcirc": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "Fcirc",
                                },
                                "PeakData": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "PeakData",
                                },
                            },
                        },
                    },
                }

                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.export_dir = export_dir
                exports_organizer.zip_export_combos(
                    study_packages,
                    datatype_packages,
                    all_packages,
                )
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    # These are the zip archives created by the dictionaries:
                    "tb9-2026.04.24-Test_Study_2-alldatatypes.zip",
                    "tb9-2026.04.24-allstudies-Fcirc.zip",
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.24-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                ],
            )

            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-Fcirc.zip"),
                ["tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv"],
            )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                ],
            )

    def test_zip_study_packages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_one_study_add_study")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_one_study_add_study",
                    export_dir,
                )

                study_packages = {
                    "tb9": {
                        "0001": {
                            "2026.04.24": {
                                "Fcirc": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "Fcirc",
                                },
                                "PeakData": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "PeakData",
                                },
                            },
                        },
                    },
                }

                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.export_dir = export_dir
                exports_organizer.zip_study_packages(study_packages)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    # This is the zip archive created by the dictionary:
                    "tb9-2026.04.24-Test_Study_2-alldatatypes.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir, "tb9-2026.04.24-Test_Study_2-alldatatypes.zip"
                ),
                [
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                ],
            )

            # Assert that the file is not re-zipped if it exists by patching the filepaths_to_zip method and checking if
            # it was called
            with patch.object(
                ExportsOrganizer, "filepaths_to_zip"
            ) as mock_filepaths_to_zip:
                exports_organizer.zip_study_packages(study_packages)
            mock_filepaths_to_zip.assert_not_called()

    def test_zip_datatype_packages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_one_study_add_study")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_one_study_add_study",
                    export_dir,
                )

                datatype_packages = {
                    "tb9": {
                        "Fcirc": {
                            "2026.04.24": {
                                "0001": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "Fcirc",
                                },
                            },
                        },
                    },
                }

                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.export_dir = export_dir
                exports_organizer.zip_datatype_packages(
                    datatype_packages,
                )
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    # This is the zip archive created by the dictionary:
                    "tb9-2026.04.24-allstudies-Fcirc.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(export_dir, "tb9-2026.04.24-allstudies-Fcirc.zip"),
                ["tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv"],
            )

            # Assert that the file is not re-zipped if it exists by patching the filepaths_to_zip method and checking if
            # it was called
            with patch.object(
                ExportsOrganizer, "filepaths_to_zip"
            ) as mock_filepaths_to_zip:
                exports_organizer.zip_datatype_packages(datatype_packages)
            mock_filepaths_to_zip.assert_not_called()

    def test_zip_everything_packages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "two_exports_one_study_add_study")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/two_exports_one_study_add_study",
                    export_dir,
                )

                all_packages = {
                    "tb9": {
                        "2026.04.24": {
                            "0001": {
                                "Fcirc": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "Fcirc",
                                },
                                "PeakData": {
                                    "date": "2026.04.24",
                                    "file": os.path.join(
                                        export_dir,
                                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                                    ),
                                    "id": 1,
                                    "name": "Test Study 2",
                                    "slug": "Test_Study_2",
                                    "ext": "tsv",
                                    "data_type": "PeakData",
                                },
                            },
                        },
                    },
                }

                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    ],
                    export_dir_contents,
                )
                exports_organizer = ExportsOrganizer()
                exports_organizer.export_dir = export_dir
                exports_organizer.zip_everything_packages(all_packages)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    "tb9-2026.04.17-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_1-0000-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_1-0000-mzXML.zip",
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                    # This is the zip archive created by the dictionary:
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                ),
                [
                    "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                ],
            )

            # Assert that the file is not re-zipped if it exists by patching the filepaths_to_zip method and checking if
            # it was called
            with patch.object(
                ExportsOrganizer, "filepaths_to_zip"
            ) as mock_filepaths_to_zip:
                exports_organizer.zip_everything_packages(all_packages)
            mock_filepaths_to_zip.assert_not_called()

    def test_filepaths_to_zip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                export_dir = os.path.join(tmpdir, "one_export")
                shutil.copytree(
                    "DataRepo/data/tests/exports_organizer/one_export", export_dir
                )

                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
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
                exports_organizer.export_dir = export_dir
                exports_organizer.filepaths_to_zip(
                    [
                        os.path.join(
                            export_dir,
                            "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        ),
                        os.path.join(
                            export_dir,
                            "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        ),
                    ],
                    os.path.join(
                        export_dir,
                        "test.zip",
                    ),
                )
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.rglob("*")
                )
                expected_files = [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakGroups.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    "test.zip",
                ]
                self.assertEqual(
                    expected_files,
                    export_dir_contents,
                )
                self.assertTrue(
                    all(
                        [
                            os.path.getsize(os.path.join(export_dir, fp)) > 0
                            for fp in expected_files
                        ]
                    )
                )

            self.assert_zip_file_contents(
                os.path.join(
                    export_dir,
                    "test.zip",
                ),
                [
                    "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                    "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                ],
            )


class ExportParseErrorTests(TracebaseTestCase):
    def test_exportparseerror(self):
        exportparseerror = ExportParseError("file.txt")
        self.assertEqual(
            "Unable to parse export filename: 'file.txt'.", str(exportparseerror)
        )


class NotOneMzxmlMetadataFileTests(TracebaseTestCase):
    def test_notonemzxmlmetadatafile(self):
        notonemzxmlmetadatafile = NotOneMzxmlMetadataFile(
            "file.zip", ["metadata1.tsv", "metadata2.tsv"]
        )
        self.assertIn("Zip archive 'file.zip'", str(notonemzxmlmetadatafile))
        self.assertIn("expected to have 1 TSV file", str(notonemzxmlmetadatafile))
        self.assertIn("found to have 2:", str(notonemzxmlmetadatafile))
        self.assertIn(
            "['metadata1.tsv', 'metadata2.tsv']", str(notonemzxmlmetadatafile)
        )
