from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from django.core.files import File

from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.models.animal import Animal
from DataRepo.models.archive_file import ArchiveFile, DataFormat, DataType
from DataRepo.models.infusate import Infusate
from DataRepo.models.lc_method import LCMethod
from DataRepo.models.msrun_sample import MSRunSample
from DataRepo.models.msrun_sequence import MSRunSequence
from DataRepo.models.peak_data import PeakData
from DataRepo.models.peak_group import PeakGroup
from DataRepo.models.sample import Sample
from DataRepo.models.tissue import Tissue
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class MSRunsLoaderTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    MOCK_MZXML_DICT = {
        "mysample1_edited_filename": {
            "/path/to/first/file": [
                {
                    "added": True,
                    "raw_file_name": "mysample1_edited_filename.raw",
                    "raw_file_sha1": "uniquerawhash1",
                    "polarity": "positive",
                    "mz_min": 1.0,
                    "mz_max": 8.0,
                    "mzaf_record": "ignore this invalid value",
                    "rawaf_record": "ignore this invalid value",
                    "mzxml_filename": "mysample1_edited_filename.mzXML",
                    "mzxml_dir": "/path/to/first/file",
                }
            ],
            "/path/to/second/file": [
                {
                    "added": True,
                    "raw_file_name": "mysample1_edited_filename.raw",
                    "raw_file_sha1": "uniquerawhash2",
                    "polarity": "positive",
                    "mz_min": 1.0,
                    "mz_max": 8.0,
                    "mzaf_record": "ignore this invalid value",
                    "rawaf_record": "ignore this invalid value",
                    "mzxml_filename": "mysample1_edited_filename.mzXML",
                    "mzxml_dir": "/path/to/second/file",
                }
            ],
        },
        "mysample_neg_pos_scan1": {
            "/path/to/unique/file/name/set": [
                {
                    "added": True,
                    "raw_file_name": "mysample_neg_pos_scan1.raw",
                    "raw_file_sha1": "uniquerawhash3",
                    "polarity": "positive",
                    "mz_min": 1.0,
                    "mz_max": 8.0,
                    "mzaf_record": "ignore this invalid value",
                    "rawaf_record": "ignore this invalid value",
                    "mzxml_filename": "mysample_neg_pos_scan1.mzXML",
                    "mzxml_dir": "/path/to/unique/file/name/set",
                }
            ],
        },
        "mysample_neg_pos_scan2": {
            "/path/to/unique/file/name/set": [
                {
                    "added": False,
                    "raw_file_name": "mysample_neg_pos_scan2.raw",
                    "raw_file_sha1": "uniquerawhash4",
                    "polarity": "positive",
                    "mz_min": 1.0,
                    "mz_max": 8.0,
                    "mzaf_record": "ignore this invalid value",
                    "rawaf_record": "ignore this invalid value",
                    "mzxml_filename": "mysample_neg_pos_scan2.mzXML",
                    "mzxml_dir": "/path/to/unique/file/name/set",
                }
            ],
        },
    }

    @classmethod
    def setUpTestData(cls):
        inf = Infusate()
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
        smpl = Sample.objects.create(
            name="Sample Name",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")

        seq = MSRunSequence.objects.create(
            researcher="John Doe",
            date=datetime.strptime("1991-5-7".strip(), "%Y-%m-%d"),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
            lc_method=lcm,
        )
        seq.full_clean()
        cls.msr = MSRunSample.objects.create(
            msrun_sequence=seq,
            sample=smpl,
            polarity=None,  # Placeholder
            ms_raw_file=None,  # Placeholder
            ms_data_file=None,  # Placeholder
        )
        cls.msr.full_clean()

        # Create a peak group
        path = Path("DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx")
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)
            ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
            accucor_format = DataFormat.objects.get(code="accucor")
            accucor_file = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
                data_type=ms_peak_annotation,
                data_format=accucor_format,
            )
            accucor_file.save()
        pg = PeakGroup.objects.create(
            name="gluc",
            formula="C6H12O6",
            msrun_sample=cls.msr,
            peak_annotation_file=accucor_file,
        )
        PeakData.objects.create(
            raw_abundance=1000.0,
            corrected_abundance=1000.0,
            peak_group=pg,
            med_mz=4.0,
            med_rt=1.0,
        )
        PeakData.objects.create(
            raw_abundance=2000.0,
            corrected_abundance=2000.0,
            peak_group=pg,
            med_mz=2.0,
            med_rt=2.0,
        )

        # Create a second peak group from the same sample/sequence, but a different accucor and from a different mzXML
        # (though in the placeholder, we don't know that)
        # This may not have the same samples, but it doesn't matter for this test
        path = Path(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate.xlsx"
        )
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)
            ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
            accucor_format = DataFormat.objects.get(code="accucor")
            accucor_file2 = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf_lactate.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c4",
                data_type=ms_peak_annotation,
                data_format=accucor_format,
            )
            accucor_file2.save()
        pg2 = PeakGroup.objects.create(
            name="lact",
            formula="C6H12O5",
            msrun_sample=cls.msr,
            peak_annotation_file=accucor_file2,
        )
        PeakData.objects.create(
            raw_abundance=500.0,
            corrected_abundance=500.0,
            peak_group=pg2,
            med_mz=4.0,
            med_rt=1.0,
        )
        PeakData.objects.create(
            raw_abundance=800.0,
            corrected_abundance=800.0,
            peak_group=pg2,
            med_mz=2.0,
            med_rt=2.0,
        )

        super().setUpTestData()

    def deletecreate_msrun_sample_placeholder_rec(self):
        inf = Infusate()
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
        smpl = Sample.objects.create(
            name="Sample Name",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")

        seq = MSRunSequence(
            researcher="John Doe",
            date=datetime.now(),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
            lc_method=lcm,
        )
        seq.full_clean()
        seq.save()
        mstype = DataType.objects.get(code="ms_data")
        rawfmt = DataFormat.objects.get(code="ms_raw")
        mzxfmt = DataFormat.objects.get(code="mzxml")
        rawrec = ArchiveFile.objects.create(
            filename="test.raw",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c5",
            data_type=mstype,
            data_format=rawfmt,
        )
        mzxrec = ArchiveFile.objects.create(
            filename="test.mzxml",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151c4",
            data_type=mstype,
            data_format=mzxfmt,
        )
        msr = MSRunSample.objects.create(
            msrun_sequence=seq,
            sample=smpl,
            polarity="unknown",
            ms_raw_file=rawrec,
            ms_data_file=mzxrec,
        )
        msr.full_clean()
        path = Path("DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx")
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)
            ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
            accucor_format = DataFormat.objects.get(code="accucor")
            accucor_file = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
                data_type=ms_peak_annotation,
                data_format=accucor_format,
            )
            accucor_file.save()
        pg = PeakGroup.objects.create(
            name="gluc",
            formula="C6H12O6",
            msrun_sample=msr,
            peak_annotation_file=accucor_file,
        )
        PeakData.objects.create(
            raw_abundance=1000.0,
            corrected_abundance=1000.0,
            peak_group=pg,
            med_mz=4.0,
            med_rt=1.0,
        )
        PeakData.objects.create(
            raw_abundance=2000.0,
            corrected_abundance=2000.0,
            peak_group=pg,
            med_mz=2.0,
            med_rt=2.0,
        )

        # This may not have the same samples, but it doesn't matter for this test
        path = Path(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate.xlsx"
        )
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)
            ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
            accucor_format = DataFormat.objects.get(code="accucor")
            accucor_file2 = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf_lactate.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
                data_type=ms_peak_annotation,
                data_format=accucor_format,
            )
            accucor_file2.save()
        pg2 = PeakGroup.objects.create(
            name="lact",
            formula="C6H12O5",
            msrun_sample=msr,
            peak_annotation_file=accucor_file2,
        )
        PeakData.objects.create(
            raw_abundance=500.0,
            corrected_abundance=500.0,
            peak_group=pg2,
            med_mz=None,  # Allow match from separate_placeholder_peak_groups based on annot file only
            med_rt=1.0,
        )
        PeakData.objects.create(
            raw_abundance=800.0,
            corrected_abundance=800.0,
            peak_group=pg2,
            med_mz=None,  # Allow match from separate_placeholder_peak_groups based on annot file only
            med_rt=2.0,
        )

    def test_guess_sample_name_default(self):
        samplename = MSRunsLoader.guess_sample_name("mysample_neg_pos_scan2")
        self.assertEqual("mysample", samplename)

    def test_guess_sample_name_add_custom(self):
        samplename = MSRunsLoader.guess_sample_name(
            "mysample_pos_blah_scan1",
            suffix_patterns=[r"_blah"],
        )
        self.assertEqual("mysample", samplename)

    def test_guess_sample_name_just_custom(self):
        samplename = MSRunsLoader.guess_sample_name(
            "mysample_pos_blah",
            suffix_patterns=[r"_blah"],
            add_patterns=False,
        )
        self.assertEqual("mysample_pos", samplename)

    def test_leftover_mzxml_files_exist_true(self):
        """Tests that leftover_mzxml_files_exist finds the existence of un-added mzXML files (i.e. those that were not
        described in the infile because they weren't used in the production of a peak annotation file).
        """
        msrl = MSRunsLoader()
        msrl.mzxml_dict = self.MOCK_MZXML_DICT
        self.assertTrue(msrl.leftover_mzxml_files_exist())

    def test_leftover_mzxml_files_exist_false(self):
        """Tests that leftover_mzxml_files_exist finds the existence of un-added mzXML files (i.e. those that were not
        described in the infile because they weren't used in the production of a peak annotation file).
        """
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
        msrl.mzxml_dict["mysample_neg_pos_scan2"]["/path/to/unique/file/name/set"][0][
            "added"
        ] = True
        self.assertFalse(msrl.leftover_mzxml_files_exist())

    def test_parse_mzxml(self):
        """
        Issue #712
        Requirement: 7.2. Parse polarity from the mzXML
        """
        expected = {
            "raw_file_name": "BAT-xz971.raw",
            "raw_file_sha1": "31bc554534cf9f1e568529d110caa85f1fd0a8c8",
            "polarity": MSRunSample.POSITIVE_POLARITY,
            "mz_max": 502.9,
            "mz_min": 1.0,
        }
        mz_dict, errs = MSRunsLoader.parse_mzxml(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
        )
        self.assertEqual(expected, mz_dict)
        self.assertEqual(0, len(errs.exceptions))

    def test_separate_placeholder_peak_groups_match_all(self):
        msrs = self.msr
        msrl = MSRunsLoader()
        # Although this is only ever called when adding an mzXML, we're not setting ms_data_file or ms_raw_file in the
        # rec_dict below, because the method doesn't use it.  It only uses the peak groups and their annotation files to
        # separate the peak grouos belonging to the existing MSRunSample placeholder record.
        matching_peakgroups_qs, unmatching_peakgroups_qs = (
            msrl.separate_placeholder_peak_groups(
                {
                    "msrun_sequence": msrs.msrun_sequence,
                    "sample": msrs.sample,
                    "polarity": "positive",
                    "mz_min": 0.0,  # Encompasses 4.0
                    "mz_max": 10.0,  # Encompasses 4.0
                    "ms_raw_file": None,  # See comment above
                    "ms_data_file": None,  # See comment above
                },
                "small_obob_maven_6eaas_inf.xlsx",
                msrs,
            )
        )
        self.assertEqual(1, matching_peakgroups_qs.count())
        self.assertEqual("gluc", matching_peakgroups_qs.first().name)
        self.assertEqual(1, unmatching_peakgroups_qs.count())
        self.assertEqual("lact", unmatching_peakgroups_qs.first().name)

    def test_separate_placeholder_peak_groups_match_annot(self):
        msrs = self.msr
        msrl = MSRunsLoader()
        # Although this is only ever called when adding an mzXML, we're not setting ms_data_file or ms_raw_file in the
        # rec_dict below, because the method doesn't use it.  It only uses the peak groups and their annotation files to
        # separate the peak grouos belonging to the existing MSRunSample placeholder record.
        matching_peakgroups_qs, unmatching_peakgroups_qs = (
            msrl.separate_placeholder_peak_groups(
                {
                    "msrun_sequence": msrs.msrun_sequence,
                    "sample": msrs.sample,
                    "polarity": "positive",
                    "mz_min": 0.0,  # Match's med_mz (in small_obob_maven_6eaas_inf_lactate.xlsx) is None
                    "mz_max": 10.0,  # Match's med_mz (in small_obob_maven_6eaas_inf_lactate.xlsx) is None
                    "ms_raw_file": None,  # See comment above
                    "ms_data_file": None,  # See comment above
                },
                "small_obob_maven_6eaas_inf_lactate.xlsx",
                msrs,
            )
        )
        self.assertEqual(1, matching_peakgroups_qs.count())
        self.assertEqual("lact", matching_peakgroups_qs.first().name)
        self.assertEqual(1, unmatching_peakgroups_qs.count())
        self.assertEqual("gluc", unmatching_peakgroups_qs.first().name)

    def test_separate_placeholder_peak_groups_diff_scan_range(self):
        msrs = self.msr
        msrl = MSRunsLoader()
        matching_peakgroups_qs, unmatching_peakgroups_qs = (
            msrl.separate_placeholder_peak_groups(
                {
                    "msrun_sequence": msrs.msrun_sequence,
                    "sample": msrs.sample,
                    "polarity": "positive",
                    "mz_min": 50.0,  # Does not encompass 4.0
                    "mz_max": 100.0,  # Does not encompass 4.0
                    "ms_raw_file": None,
                    "ms_data_file": None,
                },
                "small_obob_maven_6eaas_inf.xlsx",
                msrs,
            )
        )
        self.assertEqual(0, matching_peakgroups_qs.count())
        self.assertEqual(2, unmatching_peakgroups_qs.count())

    def test_get_matching_mzxml_metadata_error(self):
        """Tests the case where there are multiple mzXML files with the same name and the user will be prompted to add a
        path to the infile."""
        msrl = MSRunsLoader()
        msrl.set_row_index(2)
        msrl.mzxml_dict = self.MOCK_MZXML_DICT
        mzxml_metadata = msrl.get_matching_mzxml_metadata(
            "mysample1",  # Sample name - does not match
            "mysample1_neg_pos_scan2",  # Sample header - does not match (because of the "1")
            "mysample1_edited_filename.mzXML",  # file name without path will match multiple
        )
        self.assertIsNone(mzxml_metadata)
        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertIn(
            "Multiple mzXML files", str(msrl.aggregated_errors_object.exceptions[0])
        )
        self.assertIn(
            "/path/to/first/file/mysample1_edited_filename.mzXML",
            str(msrl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "/path/to/second/file/mysample1_edited_filename.mzXML",
            str(msrl.aggregated_errors_object.exceptions[0]),
        )

    def test_get_matching_mzxml_metadata_adding_path_fix(self):
        """Tests the case where there are multiple mzXML files with the same name and the user fixes the issue by adding
        a path to the mzXML Name column in the --infile."""
        msrl = MSRunsLoader()
        msrl.set_row_index(2)
        msrl.mzxml_dict = self.MOCK_MZXML_DICT
        mzxml_metadata = msrl.get_matching_mzxml_metadata(
            "mysample1",  # Sample name - does not match
            "mysample1_neg_pos_scan2",  # Sample header - does not match (because of the "1")
            "/path/to/second/file/mysample1_edited_filename.mzXML",  # file name with path will match 1
        )
        expected = self.MOCK_MZXML_DICT["mysample1_edited_filename"][
            "/path/to/second/file"
        ][0]
        self.assertDictEqual(expected, mzxml_metadata)
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))

    def test_get_matching_mzxml_metadata_header_matches_uniquely(self):
        msrl = MSRunsLoader()
        msrl.set_row_index(2)
        msrl.mzxml_dict = self.MOCK_MZXML_DICT
        expected = self.MOCK_MZXML_DICT["mysample_neg_pos_scan1"][
            "/path/to/unique/file/name/set"
        ][0]
        mzxml_metadata = msrl.get_matching_mzxml_metadata(
            "mysample",  # Sample name - does not match
            "mysample_neg_pos_scan1",  # Sample header - does match
            "mysample_neg_pos_scan1.mzXML",  # file name or path
        )
        self.assertDictEqual(expected, mzxml_metadata)
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))

    def test_get_msrun_sequence_supplied(self):
        msrl = MSRunsLoader()
        msrl.set_row_index(2)
        operator = "John Doe"
        lcprotname = "polar-HILIC-25-min"
        instrument = MSRunSequence.INSTRUMENT_CHOICES[0][0]
        date_str = "1991-05-7"
        seqname = ", ".join([operator, lcprotname, instrument, date_str])
        seq = msrl.get_msrun_sequence(seqname)
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))
        self.assertEqual(self.msr.msrun_sequence, seq)

    def test_get_msrun_sequence_custom_default(self):
        msrl = MSRunsLoader(
            lc_protocol_name="polar-HILIC-25-min",
            operator="John Doe",
            date="1991-05-07",
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
        )
        seq = msrl.get_msrun_sequence()
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))
        self.assertEqual(self.msr.msrun_sequence, seq)

    def test_get_msrun_sequence_defaults_file(self):
        inst = MSRunSequence.INSTRUMENT_CHOICES[0][0]
        msrl = MSRunsLoader(
            defaults_df=pd.DataFrame.from_dict(
                {
                    MSRunsLoader.DefaultsHeaders.SHEET_NAME: [
                        "Sequences",
                        "Sequences",
                        "Sequences",
                        "Sequences",
                    ],
                    MSRunsLoader.DefaultsHeaders.COLUMN_NAME: [
                        "Date",
                        "Operator",
                        "Instrument",
                        "LC Protocol Name",
                    ],
                    MSRunsLoader.DefaultsHeaders.DEFAULT_VALUE: [
                        "1991-5-7",
                        "John Doe",
                        inst,
                        "polar-HILIC-25-min",
                    ],
                }
            ),
        )
        seq = msrl.get_msrun_sequence()
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))
        self.assertEqual(self.msr.msrun_sequence, seq)

    def test_get_create_or_update_msrun_sample_from_leftover_mzxml(self):
        # TODO: Implement test
        pass

    def test_get_sample_by_name(self):
        # TODO: Implement test
        pass

    def test_get_create_or_update_msrun_sample_from_row(self):
        # TODO: Implement test
        pass

    def test_get_or_create_mzxml_and_raw_archive_files(self):
        # TODO: Implement test
        pass

    def test_load_data(self):
        # TODO: Implement test
        pass

    def test_constructor_custom_defaults(self):
        # TODO: Implement test
        pass
