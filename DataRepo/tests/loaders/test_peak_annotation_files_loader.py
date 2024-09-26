from datetime import datetime, timedelta

import pandas as pd

from DataRepo.loaders.peak_annotation_files_loader import (
    PeakAnnotationFilesLoader,
)
from DataRepo.loaders.study_loader import StudyV3Loader
from DataRepo.models import (
    Animal,
    ArchiveFile,
    Compound,
    Infusate,
    LCMethod,
    MSRunSample,
    MSRunSequence,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupLabel,
    Sample,
    Tissue,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import AggregatedErrorsSet, InfileError
from DataRepo.utils.file_utils import read_from_file
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs

PeakGroupCompound = PeakGroup.compounds.through


class PeakAnnotationFilesLoaderTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    def test_pafl_get_file_and_format_success(self):
        pafl = PeakAnnotationFilesLoader()
        exp_file = "DataRepo/data/tests/small_multitracer/6eaafasted1_cor.xlsx"
        exp_fmt = "isocorr"
        row = pd.Series(
            {
                PeakAnnotationFilesLoader.DataHeaders.FILE: exp_file,
                PeakAnnotationFilesLoader.DataHeaders.FORMAT: exp_fmt,
            }
        )
        name, file, fmt = pafl.get_file_and_format(row)
        self.assertEqual("6eaafasted1_cor.xlsx", name)
        self.assertEqual(exp_fmt, fmt)
        self.assertEqual(exp_file, file)
        self.assertEqual(0, len(pafl.aggregated_errors_object.exceptions))

    def test_pafl_get_file_and_format_warning(self):
        pafl = PeakAnnotationFilesLoader()
        exp_file = "DataRepo/data/tests/small_multitracer/6eaafasted1_cor.xlsx"
        exp_fmt = "accucor"
        row = pd.Series(
            {
                PeakAnnotationFilesLoader.DataHeaders.FILE: exp_file,
                PeakAnnotationFilesLoader.DataHeaders.FORMAT: exp_fmt,
            }
        )
        name, file, fmt = pafl.get_file_and_format(row)
        self.assertEqual("6eaafasted1_cor.xlsx", name)
        self.assertEqual(exp_file, file)
        self.assertEqual(exp_fmt, fmt)
        self.assertEqual(1, len(pafl.aggregated_errors_object.exceptions))
        self.assertEqual(1, pafl.aggregated_errors_object.num_warnings)

    def test_pafl_get_file_and_format_auto(self):
        pafl = PeakAnnotationFilesLoader()
        exp_file = "DataRepo/data/tests/small_multitracer/6eaafasted1_cor.xlsx"
        exp_fmt = "isocorr"
        row = pd.Series(
            {
                PeakAnnotationFilesLoader.DataHeaders.FILE: exp_file,
                PeakAnnotationFilesLoader.DataHeaders.FORMAT: None,
            }
        )
        name, file, fmt = pafl.get_file_and_format(row)
        self.assertEqual("6eaafasted1_cor.xlsx", name)
        self.assertEqual(exp_file, file)
        self.assertEqual(exp_fmt, fmt)
        self.assertEqual(0, len(pafl.aggregated_errors_object.exceptions))

    def test_pafl_get_file_and_format_none(self):
        pafl = PeakAnnotationFilesLoader()
        exp_file = "DataRepo/data/tests/small_multitracer/study.xlsx"
        row = pd.Series(
            {
                PeakAnnotationFilesLoader.DataHeaders.FILE: exp_file,
                PeakAnnotationFilesLoader.DataHeaders.FORMAT: None,
            }
        )
        name, file, fmt = pafl.get_file_and_format(row)
        self.assertIsNone(fmt)
        self.assertEqual("study.xlsx", name)
        self.assertEqual(exp_file, file)
        self.assertEqual(1, len(pafl.aggregated_errors_object.exceptions))
        self.assertIsInstance(pafl.aggregated_errors_object.exceptions[0], InfileError)
        self.assertIn(
            "No matching formats.", str(pafl.aggregated_errors_object.exceptions[0])
        )

    def test_pafl_get_file_and_format_multiple(self):
        pafl = PeakAnnotationFilesLoader()
        exp_file = "DataRepo/data/tests/singly_labeled_isocorr/small_cor.csv"
        row = pd.Series(
            {
                PeakAnnotationFilesLoader.DataHeaders.FILE: exp_file,
                PeakAnnotationFilesLoader.DataHeaders.FORMAT: None,
            }
        )
        name, file, fmt = pafl.get_file_and_format(row)
        self.assertEqual("small_cor.csv", name)
        self.assertEqual(exp_file, file)
        self.assertIsNone(fmt)
        self.assertEqual(1, len(pafl.aggregated_errors_object.exceptions))
        self.assertIsInstance(pafl.aggregated_errors_object.exceptions[0], InfileError)
        self.assertIn(
            "Multiple matching formats",
            str(pafl.aggregated_errors_object.exceptions[0]),
        )

    def test_pafl_get_or_create_annot_file(self):
        pafl = PeakAnnotationFilesLoader()
        file = "DataRepo/data/tests/small_multitracer/6eaafasted1_cor.xlsx"
        fmt = "isocorr"
        rec, created = pafl.get_or_create_annot_file(file, fmt)
        self.assertIsInstance(rec, ArchiveFile)
        self.assertTrue(created)
        self.assertEqual(0, len(pafl.aggregated_errors_object.exceptions))

    def load_test_prereqs(self):
        # We need: glucose, lactate, pyruvate, citrate/isocitrate, succinate, malate, a-ketoglutarate
        Compound.objects.create(name="Lactate", formula="C3H6O3", hmdb_id="HMDB0000190")
        Compound.objects.create(
            name="Pyruvate", formula="C3H4O3", hmdb_id="HMDB0000243"
        )
        Compound.objects.create(name="Citrate", formula="C6H8O7", hmdb_id="HMDB0000094")
        Compound.objects.create(
            name="Isocitrate", formula="C6H8O7", hmdb_id="HMDB0000193"
        )
        Compound.objects.create(
            name="Succinate", formula="C4H6O4", hmdb_id="HMDB0000254"
        )
        Compound.objects.create(name="Malate", formula="C4H6O5", hmdb_id="HMDB0000156")
        Compound.objects.create(
            name="a-ketoglutarate", formula="C5H6O5", hmdb_id="HMDB0000208"
        )
        Compound.objects.create(
            name="glucose", formula="C6H12O6", hmdb_id="HMDB0000122"
        )
        # We need an infusate...
        ido = parse_infusate_name_with_concs("glucose-[13C6][200]")
        inf, _ = Infusate.objects.get_or_create_infusate(ido)
        inf.save()
        anml = Animal.objects.create(
            name="test_animal",
            age=timedelta(weeks=int(13)),
            sex="M",
            genotype="WT",
            body_weight=200,
            diet="normal",
            feeding_status="fed",
            infusate=inf,
        )
        tsu = Tissue.objects.create(name="Brain")
        # Create a sequence for the load to retrieve
        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")
        seq = MSRunSequence.objects.create(
            researcher="Dick",
            date=datetime.strptime("1991-5-7", "%Y-%m-%d"),
            instrument="QE2",
            lc_method=lcm,
        )
        # Create sample for the load to retrieve
        xz969 = Sample.objects.create(
            name="bat-xz969",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        MSRunSample.objects.create(
            msrun_sequence=seq,
            sample=xz969,
            polarity=None,  # Placeholder
            ms_raw_file=None,  # Placeholder
            ms_data_file=None,  # Placeholder
        )

    def assert_test_peak_annotations_loaded(self):
        # There are 7 PeakGroups because 1 sample and each has 7 peak group names
        self.assertEqual(
            7,
            PeakGroup.objects.filter(
                peak_annotation_file__filename="obob_maven_c160_inf.xlsx"
            ).count(),
        )
        # We only created 1 label per
        self.assertEqual(7, PeakGroupLabel.objects.count())
        # and 1 compound per plus the isocitrate
        self.assertEqual(8, PeakGroupCompound.objects.count())
        # and 38 total peakdata rows
        self.assertEqual(38, PeakData.objects.count())
        # and 7 records have no labels (parent records)
        self.assertEqual(31, PeakDataLabel.objects.count())
        # Assert the peak groups all belong to an msrunsample belonging to the same sequence (Dick as opposed to Roger)
        self.assertEqual(
            7,
            PeakGroup.objects.filter(
                msrun_sample__msrun_sequence__researcher="Dick"
            ).count(),
        )

    def test_pafl_load_peak_annotations(self):
        self.load_test_prereqs()

        pafl = PeakAnnotationFilesLoader(
            # This file is unrelated to the test, but it doesn't matter
            file="DataRepo/data/tests/small_obob/study.xlsx"
        )
        file = "DataRepo/data/tests/small_obob2/obob_maven_c160_inf.xlsx"
        fmt = "accucor"
        pafl.load_peak_annotations(
            file,
            fmt,
            operator="Dick",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="QE2",
            date="1991-5-7",
        )

        self.assert_test_peak_annotations_loaded()

    def test_pafl_load_data_no_details(self):
        self.load_test_prereqs()

        file = "DataRepo/data/tests/small_obob2/obob_maven_c160_inf.xlsx"
        fmt = "accucor"
        df = pd.DataFrame.from_dict(
            {
                PeakAnnotationFilesLoader.DataHeaders.FILE: [file],
                PeakAnnotationFilesLoader.DataHeaders.FORMAT: [fmt],
                PeakAnnotationFilesLoader.DataHeaders.SEQNAME: [
                    "Dick, polar-HILIC-25-min, QE2, 1991-5-7"
                ],
            }
        )
        pafl = PeakAnnotationFilesLoader(
            df=df,
            # This file is unrelated to the test, but it doesn't matter
            file="DataRepo/data/tests/small_obob/study.xlsx",
        )
        pafl.load_data()

        self.assert_test_peak_annotations_loaded()

    def test_pafl_load_data_details(self):
        self.load_test_prereqs()
        MSRunSequence.objects.create(
            researcher="Roger Wrong",
            date=datetime.strptime("1999-12-31", "%Y-%m-%d"),
            instrument="QE",
            lc_method=LCMethod.objects.get(name__exact="polar-HILIC-25-min"),
        )

        peak_annotation_details_df = pd.DataFrame.from_dict(
            {
                "Sample Name": ["bat-xz969"],
                "Sample Data Header": ["bat-xz969"],
                "mzXML File Name": [None],
                "Peak Annotation File Name": ["obob_maven_c160_inf.xlsx"],
                "Sequence Name": ["Dick, polar-HILIC-25-min, QE2, 1991-5-7"],
                "Skip": [None],
            },
        )
        peak_annotation_details_df = peak_annotation_details_df
        file = "DataRepo/data/tests/small_obob2/obob_maven_c160_inf.xlsx"
        fmt = "accucor"
        df = pd.DataFrame.from_dict(
            {
                PeakAnnotationFilesLoader.DataHeaders.FILE: [file],
                PeakAnnotationFilesLoader.DataHeaders.FORMAT: [fmt],
                PeakAnnotationFilesLoader.DataHeaders.SEQNAME: [
                    "Roger Wrong, polar-HILIC-25-min, QE, 1999-12-31"
                ],
            }
        )
        pafl = PeakAnnotationFilesLoader(
            df=df,
            peak_annotation_details_df=peak_annotation_details_df,
            # This file is unrelated to the test, but it doesn't matter
            file="DataRepo/data/tests/small_obob/study.xlsx",
        )
        pafl.load_data()

        self.assert_test_peak_annotations_loaded()

    def test_PeakAnnotationFilesLoader_conflicting_peak_group_resolutions(self):
        # Load all the prerequisites (everything but the Peak Annotation Files and Peak Group Conflicts)
        dfdict = read_from_file(
            "DataRepo/data/tests/multiple_representations/resolution_handling/prereqs.xlsx",
            None,
        )
        sl = StudyV3Loader(
            file="DataRepo/data/tests/multiple_representations/resolution_handling/prereqs.xlsx",
            df=dfdict,
        )
        sl.load_data()

        pafl = PeakAnnotationFilesLoader(
            df=read_from_file(
                "DataRepo/data/tests/multiple_representations/resolution_handling/peak_annotation_files.tsv",
            ),
            file="DataRepo/data/tests/multiple_representations/resolution_handling/peak_annotation_files.tsv",
            peak_group_conflicts_file=(
                "DataRepo/data/tests/multiple_representations/"
                "resolution_handling/conflicting_resolutions.tsv"
            ),
            peak_group_conflicts_df=read_from_file(
                "DataRepo/data/tests/multiple_representations/resolution_handling/conflicting_resolutions.tsv",
            ),
            peak_annotation_details_file=(
                "DataRepo/data/tests/multiple_representations/"
                "resolution_handling/prereqs.xlsx"
            ),
            peak_annotation_details_df=dfdict["Peak Annotation Details"],
        )
        with self.assertRaises(AggregatedErrorsSet):
            pafl.load_data()
        self.assertEqual(
            (1, 0),
            (
                pafl.aggregated_errors_dict["negative_cor.xlsx"].num_errors,
                pafl.aggregated_errors_dict["negative_cor.xlsx"].num_warnings,
            ),
        )
        self.assertEqual(
            (1, 0),
            (
                pafl.aggregated_errors_dict["poshigh_cor.xlsx"].num_errors,
                pafl.aggregated_errors_dict["poshigh_cor.xlsx"].num_warnings,
            ),
        )
        # negative_cor.xlsx is tested in DataRepo/tests/loaders/test_peak_annotations_loader.py
        dpgr = pafl.aggregated_errors_dict["poshigh_cor.xlsx"].exceptions[0]
        self.assertTrue(dpgr.conflicting)
        self.assertEqual("3-methylglutaconic acid", dpgr.pgname)
        self.assertEqual(["negative_cor.xlsx", "poshigh_cor.xlsx"], dpgr.selected_files)
        expected = {
            "ArchiveFile": {
                "created": 2,
                "deleted": 0,
                "errored": 0,
                "existed": 2,  # Both the PeakAnnotationFilesLoader and PeakAnnotationsLoader attempt this load
                "skipped": 0,
                "updated": 0,
                "warned": 0,
            },
            "PeakData": {
                "created": 0,
                "deleted": 0,
                "errored": 0,
                "existed": 0,
                "skipped": 6,  # 4 (neg) + 2 (poshigh)
                "updated": 0,
                "warned": 0,
            },
            "PeakDataLabel": {
                "created": 0,
                "deleted": 0,
                "errored": 0,
                "existed": 0,
                "skipped": 4,  # 3 (neg) + 1 (poshigh)
                "updated": 0,
                "warned": 0,
            },
            "PeakGroup": {
                "created": 0,
                "deleted": 0,
                "errored": 6,  # 4 (neg) + 2 (poshigh)
                "existed": 0,
                "skipped": 0,
                "updated": 0,
                "warned": 0,
            },
            "PeakGroupLabel": {
                "created": 0,
                "deleted": 0,
                "errored": 0,
                "existed": 0,
                "skipped": 4,  # 3 (neg) + 1 (poshigh)
                "updated": 0,
                "warned": 0,
            },
            "PeakGroup_compounds": {
                "created": 0,
                "deleted": 0,
                "errored": 0,
                "existed": 0,
                "skipped": 6,  # 4 (neg) + 2 (poshigh)
                "updated": 0,
                "warned": 0,
            },
        }
        self.assertDictEqual(expected, pafl.get_load_stats())

    def test_get_default_sequence_details(self):
        # TODO: Implement test
        pass
