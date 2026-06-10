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
from DataRepo.utils.exports_organizer import ExportParseError, ExportsOrganizer


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

    def test_parse_export_filename(self):
        self.assertEqual(
            ("tracebase", "2026.04.17", "acute_stress", "0035", "FCirc", ".tsv", False),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-acute_stress-0035-FCirc.tsv"
            ),
        )
        self.assertEqual(
            ("tracebase", "2026.04.17", "acute_stress", "0035", "mzXML", ".zip", False),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-acute_stress-0035-mzXML.zip"
            ),
        )
        self.assertEqual(
            (
                "tracebase",
                "2026.04.17",
                "acute_stress",
                "0035",
                "alldatatypes",
                ".zip",
                False,
            ),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-acute_stress-0035-alldatatypes.zip"
            ),
        )
        self.assertEqual(
            (
                "tracebase",
                "2026.04.17",
                "allstudies",
                None,
                "alldatatypes",
                ".zip",
                False,
            ),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-allstudies-alldatatypes.zip"
            ),
        )
        self.assertEqual(
            ("tracebase", "2026.04.17", "allstudies", None, "PeakData", ".zip", False),
            ExportsOrganizer.parse_export_filename(
                "tracebase-2026.04.17-allstudies-PeakData.zip"
            ),
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
                                    "staged": False,
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
                                    "staged": False,
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
                                    "staged": False,
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
                                    "staged": False,
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
                                    "staged": False,
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
                                    "staged": False,
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
                                    "staged": False,
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
                                    "staged": False,
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
                                    "staged": False,
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
                                    "staged": False,
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

    def test_package_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                test_dir = "DataRepo/data/tests/exports_organizer/one_export"

                existing_package1 = os.path.join(
                    tmpdir,
                    "tb9-2026.04.17-allstudies-alldatatypes.zip",
                )
                existing_package2 = os.path.join(
                    tmpdir,
                    "tb9-2026.04.24-allstudies-alldatatypes.zip",
                )
                nonexistent_package3 = "tb9-2026.04.10-allstudies-alldatatypes.zip"

                shutil.copy2(
                    os.path.join(
                        test_dir,
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    ),
                    # We're going to pretend this zip archive is an unstaged package
                    existing_package1,
                )
                shutil.copy2(
                    os.path.join(
                        test_dir,
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                    ),
                    # We're going to pretend this zip archive is a staged package
                    f"{existing_package2}{ExportsOrganizer.staged_ext}",
                )

                self.assertTrue(ExportsOrganizer.package_exists(existing_package1))
                self.assertTrue(
                    ExportsOrganizer.package_exists(
                        f"{existing_package1}{ExportsOrganizer.staged_ext}"
                    )
                )
                self.assertTrue(ExportsOrganizer.package_exists(existing_package2))
                self.assertTrue(
                    ExportsOrganizer.package_exists(
                        f"{existing_package2}{ExportsOrganizer.staged_ext}"
                    )
                )
                self.assertFalse(ExportsOrganizer.package_exists(nonexistent_package3))
                self.assertFalse(
                    ExportsOrganizer.package_exists(
                        f"{nonexistent_package3}{ExportsOrganizer.staged_ext}"
                    )
                )

    def test_unstage_all_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with override_settings(
                MEDIA_ROOT=tmpdir,
                FILE_UPLOAD_TEMP_DIR=tmpdir,
            ):
                test_dir = (
                    "DataRepo/data/tests/exports_organizer/two_exports_one_tsv_change"
                )
                export_dir = os.path.join(tmpdir, "two_exports_one_tsv_change")
                os.makedirs(export_dir, exist_ok=True)

                # Copy in files that don't change and ones that do
                for src, trg in [
                    (
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv.staged",
                    ),
                    (
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv.staged",
                    ),
                    (
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv.staged",
                    ),
                    (
                        # Null test - assert remains unchanged
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                    ),
                    (
                        # We will pretend this one is a zip archive package
                        "tb9-2026.04.24-Test_Study_2-0001-mzXML.zip",
                        "tb9-2026.04.24-allstudies-alldatatypes.zip.staged",
                    ),
                    (
                        # Null test - assert remains unchanged
                        "tb9-2026.04.17-Test_Study_2-0001-mzXML.zip",
                        "tb9-2026.04.17-allstudies-alldatatypes.zip",
                    ),
                ]:
                    shutil.copy2(
                        os.path.join(test_dir, src), os.path.join(export_dir, trg)
                    )

                base = Path(export_dir)
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.glob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv.staged",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv.staged",
                        "tb9-2026.04.17-allstudies-alldatatypes.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv.staged",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-allstudies-alldatatypes.zip.staged",
                    ],
                    export_dir_contents,
                )

                organize_exports = ExportsOrganizer()
                organize_exports.export_dir = export_dir

                # Confirm that when not in staging mode (the default), nothing is unstaged
                organize_exports.unstage_all_files()
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.glob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv.staged",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv.staged",
                        "tb9-2026.04.17-allstudies-alldatatypes.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv.staged",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-allstudies-alldatatypes.zip.staged",
                    ],
                    export_dir_contents,
                )

                # Confirm that when in staging mode, everything is unstaged
                organize_exports.staging_mode = True
                organize_exports.unstage_all_files()
                export_dir_contents = sorted(
                    str(p.relative_to(base)) for p in base.glob("*")
                )
                self.assertEqual(
                    [
                        "tb9-2026.04.17-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.17-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.17-allstudies-alldatatypes.zip",
                        "tb9-2026.04.24-Test_Study_2-0001-Fcirc.tsv",
                        "tb9-2026.04.24-Test_Study_2-0001-PeakData.tsv",
                        "tb9-2026.04.24-allstudies-alldatatypes.zip",
                    ],
                    export_dir_contents,
                )


class ExportParseErrorTests(TracebaseTestCase):
    def test_exportparseerror(self):
        exportparseerror = ExportParseError("file.txt")
        self.assertEqual(
            "Unable to parse export filename: 'file.txt'.", str(exportparseerror)
        )
