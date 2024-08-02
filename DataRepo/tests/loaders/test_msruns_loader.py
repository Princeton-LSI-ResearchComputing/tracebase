import os
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from django.core.files import File

from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.models import (
    Animal,
    ArchiveFile,
    DataFormat,
    DataType,
    Infusate,
    LCMethod,
    MSRunSample,
    MSRunSequence,
    PeakData,
    PeakGroup,
    Sample,
    Tissue,
)
from DataRepo.tests.tracebase_test_case import (
    TracebaseArchiveTestCase,
    TracebaseTestCase,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    InfileError,
    MutuallyExclusiveArgs,
)


def create_animal_and_tissue_records():
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
    return anml, tsu


class MSRunsLoaderTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    @classmethod
    def setUpTestData(cls):
        cls.anml, cls.tsu = create_animal_and_tissue_records()
        smpl = Sample.objects.create(
            name="Sample Name",
            tissue=cls.tsu,
            animal=cls.anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")

        seq = MSRunSequence.objects.create(
            researcher="John Doe",
            date=datetime.strptime("1991-5-7", "%Y-%m-%d"),
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
            cls.accucor_file1 = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c3",
                data_type=ms_peak_annotation,
                data_format=accucor_format,
            )
            cls.accucor_file1.save()
        cls.pg1 = PeakGroup.objects.create(
            name="gluc",
            formula="C6H12O6",
            msrun_sample=cls.msr,
            peak_annotation_file=cls.accucor_file1,
        )
        PeakData.objects.create(
            raw_abundance=1000.0,
            corrected_abundance=1000.0,
            peak_group=cls.pg1,
            med_mz=200.0,
            med_rt=1.0,
        )
        PeakData.objects.create(
            raw_abundance=2000.0,
            corrected_abundance=2000.0,
            peak_group=cls.pg1,
            med_mz=201.0,
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
            cls.accucor_file2 = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf_lactate.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c4",
                data_type=ms_peak_annotation,
                data_format=accucor_format,
            )
            cls.accucor_file2.save()
        cls.pg2 = PeakGroup.objects.create(
            name="lact",
            formula="C6H12O5",
            msrun_sample=cls.msr,
            peak_annotation_file=cls.accucor_file2,
        )
        PeakData.objects.create(
            raw_abundance=500.0,
            corrected_abundance=500.0,
            peak_group=cls.pg2,
            med_mz=300.0,
            med_rt=1.0,
        )
        PeakData.objects.create(
            raw_abundance=800.0,
            corrected_abundance=800.0,
            peak_group=cls.pg2,
            med_mz=301.0,
            med_rt=2.0,
        )

        bat_mzxml_file = "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
        with path.open(mode="rb") as f:
            BAT_xz971_mz_af = ArchiveFile.objects.create(
                filename="BAT-xz971.mzXML",
                checksum=ArchiveFile.hash_file(Path(bat_mzxml_file)),
                file_location=File(f, name=path.name),
                data_type=DataType.objects.get(code="ms_data"),
                data_format=DataFormat.objects.get(code="mzxml"),
            )
        BAT_xz971_raw_af = ArchiveFile.objects.create(
            filename="BAT-xz971.raw",
            checksum="uniquerawhash4",
            data_type=DataType.objects.get(code="ms_data"),
            data_format=DataFormat.objects.get(code="ms_raw"),
        )

        cls.sample_with_no_msr = Sample.objects.create(
            name="sample_with_no_msr",
            tissue=cls.tsu,
            animal=cls.anml,
            researcher="John Doe",
            date=datetime.now(),
        )

        cls.seqname = ", ".join(
            [
                cls.msr.msrun_sequence.researcher,
                cls.msr.msrun_sequence.lc_method.name,
                cls.msr.msrun_sequence.instrument,
                "1991-5-7",  # Simpler to just supply as string here
            ]
        )

        cls.MOCK_MZXML_DICT = {
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
            "Br_xz971": {
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls": [
                    {
                        "added": True,
                        "raw_file_name": "Br-xz971.raw",
                        "raw_file_sha1": "uniquerawhash3",
                        "polarity": "positive",
                        "mz_min": 1.0,
                        "mz_max": 8.0,
                        "mzaf_record": "ignore this invalid value",
                        "rawaf_record": "ignore this invalid value",
                        "mzxml_filename": "Br-xz971.mzXML",
                        "mzxml_dir": "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls",
                    }
                ],
            },
            "BAT_xz971": {
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls": [
                    {
                        "added": False,
                        "raw_file_name": "BAT-xz971.raw",
                        "raw_file_sha1": "uniquerawhash4",
                        "polarity": "positive",
                        "mz_min": 100.9,
                        "mz_max": 502.9,
                        "mzaf_record": BAT_xz971_mz_af,
                        "rawaf_record": BAT_xz971_raw_af,
                        "mzxml_filename": "BAT-xz971.mzXML",
                        "mzxml_dir": "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls",
                    }
                ],
            },
        }

        super().setUpTestData()

    def test_guess_sample_name_default_end(self):
        samplename = MSRunsLoader.guess_sample_name("mysample_neg_pos_scan2")
        self.assertEqual("mysample", samplename)

    def test_guess_sample_name_default_middle(self):
        samplename = MSRunsLoader.guess_sample_name("His-pos-M3-T08-small-intes")
        self.assertEqual("His-M3-T08-small-intes", samplename)

    def test_guess_sample_name_add_custom(self):
        samplename = MSRunsLoader.guess_sample_name(
            "mysample_pos_blah_scan1",
            scan_patterns=[r"_blah"],
        )
        self.assertEqual("mysample", samplename)

    def test_guess_sample_name_just_custom(self):
        samplename = MSRunsLoader.guess_sample_name(
            "mysample_pos_blah",
            scan_patterns=[r"_blah"],
            add_patterns=False,
        )
        self.assertEqual("mysample_pos", samplename)

    def test_leftover_mzxml_files_exist_true(self):
        """Tests that leftover_mzxml_files_exist finds the existence of un-added mzXML files (i.e. those that were not
        described in the infile because they weren't used in the production of a peak annotation file).
        """
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
        self.assertTrue(msrl.leftover_mzxml_files_exist())

    def test_leftover_mzxml_files_exist_false(self):
        """Tests that leftover_mzxml_files_exist finds the existence of un-added mzXML files (i.e. those that were not
        described in the infile because they weren't used in the production of a peak annotation file).
        """
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
        msrl.mzxml_dict["BAT_xz971"][
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
        ][0]["added"] = True
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

    def test_separate_placeholder_peak_groups_match_med_mz_none(self):
        PeakData.objects.update(med_mz=None)
        self.assertEqual(4, PeakData.objects.filter(med_mz__isnull=True).count())
        msrs = self.msr
        msrl = MSRunsLoader()
        # Although this is only ever called when adding an mzXML, we're not setting ms_data_file or ms_raw_file in the
        # rec_dict below, because the method doesn't use it.  It only uses the peak groups and their annotation files to
        # separate the peak groups belonging to the existing MSRunSample placeholder record.
        matching_peakgroups_qs, unmatching_peakgroups_qs = (
            msrl.separate_placeholder_peak_groups(
                {
                    "msrun_sequence": msrs.msrun_sequence,
                    "sample": msrs.sample,
                    "polarity": MSRunSample.POSITIVE_POLARITY,
                    "mz_min": 100.0,  # Encompasses 200.0, 201.0, 300.0, and 301.0
                    "mz_max": 400.0,  # Encompasses 200.0, 201.0, 300.0, and 301.0
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
        msrl = MSRunsLoader()
        # Although this is only ever called when adding an mzXML, we're not setting ms_data_file or ms_raw_file in the
        # rec_dict below, because the method doesn't use it.  It only uses the peak groups and their annotation files to
        # separate the peak groups belonging to the existing MSRunSample placeholder record.
        matching_peakgroups_qs, unmatching_peakgroups_qs = (
            msrl.separate_placeholder_peak_groups(
                {
                    "msrun_sequence": self.msr.msrun_sequence,
                    "sample": self.msr.sample,
                    "polarity": MSRunSample.POSITIVE_POLARITY,
                    "mz_min": 100.0,  # Encompasses 200.0, 201.0, 300.0, and 301.0
                    "mz_max": 400.0,  # Encompasses 200.0, 201.0, 300.0, and 301.0
                    "ms_raw_file": None,  # See comment above
                    "ms_data_file": None,  # See comment above
                },
                "small_obob_maven_6eaas_inf_lactate.xlsx",
                self.msr,
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
                    "polarity": MSRunSample.POSITIVE_POLARITY,
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
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
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
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
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
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
        expected = self.MOCK_MZXML_DICT["Br_xz971"][
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
        ][0]
        mzxml_metadata = msrl.get_matching_mzxml_metadata(
            "mysample",  # Sample name - does not match
            "Br_xz971",  # Sample header - does match
            "Br-xz971.mzXML",  # file name or path
        )
        self.assertDictEqual(expected, mzxml_metadata)
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))

    def test_get_matching_mzxml_metadata_header_with_underscore_matches_dash(self):
        msrl = MSRunsLoader()
        msrl.set_row_index(2)
        # Have the object setup the mzxml_dict, so we see what would really happen
        msrl.get_or_create_mzxml_and_raw_archive_files(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        )
        mzxml_metadata = msrl.get_matching_mzxml_metadata(
            "mysample",  # Sample name - does not match
            "BAT_xz971",  # Sample header - does match
            None,  # no mzxml filename from sheet
        )
        self.assertFalse(mzxml_metadata["added"])
        self.assertEqual(502.9, mzxml_metadata["mz_max"])
        self.assertEqual(1.0, mzxml_metadata["mz_min"])
        self.assertIsInstance(mzxml_metadata["mzaf_record"], ArchiveFile)
        self.assertEqual(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_mzxmls",
            mzxml_metadata["mzxml_dir"],
        )
        self.assertEqual("BAT-xz971.mzXML", mzxml_metadata["mzxml_filename"])
        self.assertEqual("negative", mzxml_metadata["polarity"])
        self.assertEqual("BAT-xz971_neg.raw", mzxml_metadata["raw_file_name"])
        self.assertEqual(
            "31bc554534cf9f1e568529d110caa85f1fd0a8c9", mzxml_metadata["raw_file_sha1"]
        )
        self.assertIsInstance(mzxml_metadata["rawaf_record"], ArchiveFile)
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

    def test_get_sample_by_name(self):
        msrl = MSRunsLoader()
        sample = msrl.get_sample_by_name("Sample Name")
        self.assertEqual(Sample.objects.get(name="Sample Name"), sample)

    def test_get_or_create_msrun_sample_from_mzxml_success(self):
        msrl = MSRunsLoader()
        sample = self.msr.sample
        msrun_sequence = self.msr.msrun_sequence

        # Copy the metadata, because the method will modify it
        mzxml_metadata = deepcopy(
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]
        )

        # Test create
        rec, created = msrl.get_or_create_msrun_sample_from_mzxml(
            sample, msrun_sequence, mzxml_metadata
        )
        self.assertTrue(created)
        self.assertEqual(rec.sample, sample)
        self.assertEqual(rec.msrun_sequence, msrun_sequence)
        self.assertEqual(mzxml_metadata["mzaf_record"], rec.ms_data_file)

        # Test get
        # Copy the metadata again
        mzxml_metadata2 = deepcopy(
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]
        )
        rec2, created2 = msrl.get_or_create_msrun_sample_from_mzxml(
            sample, msrun_sequence, mzxml_metadata2
        )
        self.assertFalse(created2)
        self.assertEqual(rec, rec2)

    def test_get_or_create_msrun_sample_from_mzxml_error(self):
        msrl = MSRunsLoader()
        sample = self.msr.sample
        msrun_sequence = self.msr.msrun_sequence
        # Copy the metadata, because the method will modify it
        mzxml_metadata = deepcopy(
            self.MOCK_MZXML_DICT["Br_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]
        )
        rec, created = msrl.get_or_create_msrun_sample_from_mzxml(
            sample, msrun_sequence, mzxml_metadata
        )
        self.assertFalse(created)
        self.assertIsNone(rec)

    def test_get_or_create_mzxml_and_raw_archive_files_str(self):
        """Tests: 1. Accepts a path in string form.  Both creates and gets."""
        msrl = MSRunsLoader()
        (
            mzaf_rec,
            mzaf_created,
            rawaf_rec,
            rawaf_created,
        ) = msrl.get_or_create_mzxml_and_raw_archive_files(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        )
        self.assertEqual(ArchiveFile, type(mzaf_rec))
        self.assertTrue(mzaf_created)
        self.assertEqual(ArchiveFile, type(rawaf_rec))
        self.assertTrue(rawaf_created)

        # Now test that the raw record is not created when loading a different mzxml from the same raw
        (
            mzaf2_rec,
            mzaf2_created,
            rawaf2_rec,
            rawaf2_created,
        ) = msrl.get_or_create_mzxml_and_raw_archive_files(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_pos_mzxmls/BAT-xz971_pos.mzXML"
        )
        self.assertEqual(ArchiveFile, type(mzaf2_rec))
        self.assertTrue(mzaf2_created)
        self.assertEqual(rawaf_rec, rawaf2_rec)
        self.assertFalse(rawaf2_created)

        # Now test that the mzxml record is not created when loading the same file
        (
            mzaf3_rec,
            mzaf3_created,
            rawaf3_rec,
            rawaf3_created,
        ) = msrl.get_or_create_mzxml_and_raw_archive_files(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_pos_mzxmls/BAT-xz971_pos.mzXML"
        )
        self.assertEqual(mzaf2_rec, mzaf3_rec)
        self.assertFalse(mzaf3_created)
        self.assertEqual(rawaf2_rec, rawaf3_rec)
        self.assertFalse(rawaf3_created)

    def test_get_or_create_mzxml_and_raw_archive_files_path(self):
        """Tests that the argument can be a Path object"""
        msrl = MSRunsLoader()
        (
            mzaf_rec,
            mzaf_created,
            rawaf_rec,
            rawaf_created,
        ) = msrl.get_or_create_mzxml_and_raw_archive_files(
            Path(
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
            )
        )
        self.assertEqual(ArchiveFile, type(mzaf_rec))
        self.assertTrue(mzaf_created)
        self.assertEqual(ArchiveFile, type(rawaf_rec))
        self.assertTrue(rawaf_created)

    def test_get_create_or_update_msrun_sample_from_row_creating_placeholder_no_placeholder_exists(
        self,
    ):
        """Input: Data for a placeholder record
        State: No placeholder record exists, no peakgroups exist
        Result: Placeholder created
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        #     if existing_placeholder_qs.count() == 0:

        # Set up the loader object
        msrl = MSRunsLoader()

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.sample_with_no_msr.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.sample_with_no_msr.name}_pos",
                MSRunsLoader.DataHeaders.MZXMLNAME: "",  # Creating placeholder
                MSRunsLoader.DataHeaders.ANNOTNAME: "accucor_file.xlsx",
            }
        )

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.sample_with_no_msr, rec.sample)

        # Check that the record does not have an mzXML
        self.assertIsNone(rec.ms_data_file)
        self.assertTrue(created)
        self.assertFalse(updated)

    def test_get_create_or_update_msrun_sample_from_row_creating_placeholder_placeholder_exists(
        self,
    ):
        """Input: Data for a placeholder record
        State: Placeholder record exists, peak groups may or may not exist for it
        Result: Placeholder gotten
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        #     if NOT existing_placeholder_qs.count() == 0:

        # Set up the loader object
        msrl = MSRunsLoader()

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.msr.sample.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.msr.sample.name}_pos",
                MSRunsLoader.DataHeaders.MZXMLNAME: "",  # Getting placeholder
                MSRunsLoader.DataHeaders.ANNOTNAME: "accucor_file.xlsx",
            }
        )

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record does not have an mzXML
        self.assertIsNone(rec.ms_data_file)
        self.assertFalse(created)
        self.assertFalse(updated)

    def test_get_create_or_update_msrun_sample_from_row_no_concrete_no_placeholder(
        self,
    ):
        """Input: Data for a concrete record
        State: No placeholder record exists, no concrete record exists, no peak groups for them exist
        Result: Concrete created
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        # else:
        #     if existing_placeholder_qs.count() == 0:

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.msr.sample.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.msr.sample.name}_neg",
                MSRunsLoader.DataHeaders.MZXMLNAME: "BAT-xz971.mzXML",  # Creating concrete
                MSRunsLoader.DataHeaders.ANNOTNAME: "accucor_file.xlsx",
            }
        )

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the existing placeholder now has the mzXML
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertTrue(created)
        self.assertFalse(updated)

    def test_get_create_or_update_msrun_sample_from_row_concrete_exists_no_placeholder(
        self,
    ):
        """Input: Data for a concrete record
        State: No placeholder record exists, concrete record exists, peak groups may or may not exist for them
        Result: Concrete gotten
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        # else:
        #     if existing_placeholder_qs.count() == 0:

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # Assure no placeholder exists (i.e. make sure the test will be valid)
        self.assertEqual(
            0,
            MSRunSample.objects.filter(
                msrun_sequence=self.msr.msrun_sequence,
                sample=self.sample_with_no_msr,
                ms_data_file__isnull=True,
            ).count(),
        )

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.sample_with_no_msr.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.sample_with_no_msr.name}_neg",
                MSRunsLoader.DataHeaders.MZXMLNAME: "BAT-xz971.mzXML",  # Creating concrete
                MSRunsLoader.DataHeaders.ANNOTNAME: "accucor_file.xlsx",
            }
        )

        # Create a record to be "gotten" (this is setup, not the test method call)
        msrl.get_create_or_update_msrun_sample_from_row(row)

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.sample_with_no_msr, rec.sample)

        # Check that the record has the mzXML
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)
        self.assertFalse(updated)

    def test_get_create_or_update_msrun_sample_from_row_no_concrete_placeholder_all_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with all matching peak groups exists and no concrete record exists
        Result: Placeholder updated to concrete record
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        # else:
        #     if existing_placeholder_qs.count() == 0:
        #     else:
        #         if matching_peakgroups_qs.count() > 0 and unmatching_peakgroups_qs.count() == 0:
        #             if existing_concrete_rec is None:

        # Change the accucor file in the second peak group to be the same as the first so that all peak groups in the
        # existing placeholder record (created in setUpTestData) will match the added mzXML file / concrete msrun_sample
        # record
        self.pg2.peak_annotation_file = self.accucor_file1
        self.pg2.save()

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.msr.sample.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.msr.sample.name}_neg",
                MSRunsLoader.DataHeaders.MZXMLNAME: "BAT-xz971.mzXML",  # Creating concrete
                MSRunsLoader.DataHeaders.ANNOTNAME: self.accucor_file1.filename,
            }
        )

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the existing placeholder now has the mzXML
        self.assertEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)
        self.assertTrue(updated)

    def test_get_create_or_update_msrun_sample_from_row_concrete_exists_no_pgs_placeholder_all_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with all matching peak groups exists and concrete record with no peak groups exists
        Result: Placeholder updated to concrete and existing concrete record is deleted
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        # else:
        #     if existing_placeholder_qs.count() == 0:
        #     else:
        #         if matching_peakgroups_qs.count() > 0 and unmatching_peakgroups_qs.count() == 0:
        #             if existing_concrete_rec is None:
        #             else:
        #                 if existing_concrete_rec.peak_groups.count() == 0:

        # Change the accucor file in the second peak group to be the same as the first so that all peak groups in the
        # existing placeholder record (created in setUpTestData) will match the added mzXML file / concrete msrun_sample
        # record
        self.pg2.peak_annotation_file = self.accucor_file1
        self.pg2.save()

        # Create an empty concrete MSRunSample record (i.e. it has an mzXML file and no peak groups link to it)
        concrete_mzxml_dict = self.MOCK_MZXML_DICT["BAT_xz971"][
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
        ][0]
        empty_concrete_rec_dict = {
            "msrun_sequence": self.msr.msrun_sequence,
            "sample": self.msr.sample,
            "polarity": concrete_mzxml_dict["polarity"],
            "mz_min": concrete_mzxml_dict["mz_min"],
            "mz_max": concrete_mzxml_dict["mz_max"],
            "ms_data_file": concrete_mzxml_dict["mzaf_record"],
            "ms_raw_file": concrete_mzxml_dict["rawaf_record"],
        }
        empty_concrete_rec = MSRunSample.objects.create(**empty_concrete_rec_dict)
        empty_concrete_rec.full_clean()

        # Save the ID to check that this record is deleted
        empty_concrete_rec_id = empty_concrete_rec.id

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.msr.sample.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.msr.sample.name}_neg",
                MSRunsLoader.DataHeaders.MZXMLNAME: "BAT-xz971.mzXML",  # Creating concrete
                MSRunsLoader.DataHeaders.ANNOTNAME: self.accucor_file1.filename,
            }
        )

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)
        self.assertTrue(updated)

        # Check that the existing concrete record was deleted
        self.assertEqual(
            0, MSRunSample.objects.filter(id=empty_concrete_rec_id).count()
        )

    def test_get_create_or_update_msrun_sample_from_row_concrete_exists_with_pgs_placeholder_all_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder with all matching peak groups exists and concrete record with peak groups exists
        Result: Link placeholder's peakgroups to the existing concrete record and delete placeholder
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        # else:
        #     if existing_placeholder_qs.count() == 0:
        #     else:
        #         if matching_peakgroups_qs.count() > 0 and unmatching_peakgroups_qs.count() == 0:
        #             if existing_concrete_rec is None:
        #             else:
        #                 if existing_concrete_rec.peak_groups.count() == 0:
        #                 else:

        # Create an empty concrete MSRunSample record (i.e. it has an mzXML file and no peak groups link to it)
        concrete_mzxml_dict = self.MOCK_MZXML_DICT["BAT_xz971"][
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
        ][0]
        concrete_rec_dict = {
            "msrun_sequence": self.msr.msrun_sequence,
            "sample": self.msr.sample,
            "polarity": concrete_mzxml_dict["polarity"],
            "mz_min": concrete_mzxml_dict["mz_min"],
            "mz_max": concrete_mzxml_dict["mz_max"],
            "ms_data_file": concrete_mzxml_dict["mzaf_record"],
            "ms_raw_file": concrete_mzxml_dict["rawaf_record"],
        }

        # Create a concrete record, to which we will add 1 peakgroup using a preparatory call to
        # get_create_or_update_msrun_sample_from_row
        concrete_rec = MSRunSample.objects.create(**concrete_rec_dict)
        concrete_rec.full_clean()

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.msr.sample.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.msr.sample.name}_neg",
                MSRunsLoader.DataHeaders.MZXMLNAME: "BAT-xz971.mzXML",  # Creating concrete
                MSRunsLoader.DataHeaders.ANNOTNAME: self.accucor_file1.filename,
            }
        )

        # This is an initial call to assign 1 peak group (from the existing placeholder) to the concrete record we
        # created above
        msrl.get_create_or_update_msrun_sample_from_row(row)

        # Now we have both a placeholder and concrete record, each with 1 peak group.  Let's change the placeholder's
        # peak group to now match the existing concrete record, to simulate a newly made association...

        # Change the accucor file in the second peak group to be the same as the first so that all peak groups in the
        # existing placeholder record (created in setUpTestData) will match the mzXML file / concrete msrun_sample
        # record that is now associated with a peak annotation file
        self.pg2.peak_annotation_file = self.accucor_file1
        self.pg2.save()

        # Keep track of the placeholder record's ID so we can check it gets deleted
        placeholder_rec_id = self.msr.id

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertEqual(concrete_rec.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)
        self.assertTrue(updated)

        # Check that the existing concrete record now has 2 peak groups
        self.assertEqual(2, rec.peak_groups.count())

        # Check that the existing placeholder record was deleted
        self.assertEqual(0, MSRunSample.objects.filter(id=placeholder_rec_id).count())

    def test_get_create_or_update_msrun_sample_from_row_no_concrete_placeholder_exists_but_no_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with no matching peak groups exists and no concrete record exists
        Result: Concrete record created, placeholder record kept, and do not change the peak group links
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        # else:
        #     if existing_placeholder_qs.count() == 0:
        #     else:
        #         if matching_peakgroups_qs.count() > 0 and unmatching_peakgroups_qs.count() == 0:
        #         elif matching_peakgroups_qs.count() == 0:
        #             if existing_concrete_rec is None:

        # Change the accucor file in the second peak group to be the same as the first so that all peak groups in the
        # existing placeholder record (created in setUpTestData) will **NOT** match the added mzXML file / concrete
        # msrun_sample record
        self.pg2.peak_annotation_file = self.accucor_file1
        self.pg2.save()

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.msr.sample.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.msr.sample.name}_neg",
                MSRunsLoader.DataHeaders.MZXMLNAME: "BAT-xz971.mzXML",  # Creating concrete
                MSRunsLoader.DataHeaders.ANNOTNAME: self.accucor_file2.filename,
            }
        )

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertNotEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertTrue(created)
        self.assertFalse(updated)

        # Check that the existing placeholder record still exists
        self.assertEqual(1, MSRunSample.objects.filter(id=self.msr.id).count())

        # And that it still has both peak groups
        self.assertEqual(2, self.msr.peak_groups.count())

    def test_get_create_or_update_msrun_sample_from_row_concrete_exists_placeholder_exists_but_no_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with no matching peak groups exists and concrete record exists
        Result: Get existing concrete record
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        # else:
        #     if existing_placeholder_qs.count() == 0:
        #     else:
        #         if matching_peakgroups_qs.count() > 0 and unmatching_peakgroups_qs.count() == 0:
        #         elif matching_peakgroups_qs.count() == 0:
        #             if existing_concrete_rec is None:
        #             else:

        # Change the accucor file in the second peak group to be the same as the first so that all peak groups in the
        # existing placeholder record (created in setUpTestData) will **NOT** match the added mzXML file / concrete
        # msrun_sample record
        self.pg2.peak_annotation_file = self.accucor_file1
        self.pg2.save()

        # Create a concrete MSRunSample record (i.e. it has an mzXML file and no peak groups link to it)
        concrete_mzxml_dict = self.MOCK_MZXML_DICT["BAT_xz971"][
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
        ][0]
        concrete_rec_dict = {
            "msrun_sequence": self.msr.msrun_sequence,
            "sample": self.msr.sample,
            "polarity": concrete_mzxml_dict["polarity"],
            "mz_min": concrete_mzxml_dict["mz_min"],
            "mz_max": concrete_mzxml_dict["mz_max"],
            "ms_data_file": concrete_mzxml_dict["mzaf_record"],
            "ms_raw_file": concrete_mzxml_dict["rawaf_record"],
        }

        # Create a concrete record, to which no peak groups will be added
        concrete_rec = MSRunSample.objects.create(**concrete_rec_dict)
        concrete_rec.full_clean()

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.msr.sample.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.msr.sample.name}_neg",
                MSRunsLoader.DataHeaders.MZXMLNAME: "BAT-xz971.mzXML",  # Creating concrete
                MSRunsLoader.DataHeaders.ANNOTNAME: self.accucor_file2.filename,
            }
        )

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertNotEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)
        self.assertFalse(updated)

        # Check that the existing placeholder record still exists
        self.assertEqual(1, MSRunSample.objects.filter(id=self.msr.id).count())

        # And that it still has both peak groups
        self.assertEqual(2, self.msr.peak_groups.count())

    def test_get_create_or_update_msrun_sample_from_row_no_concrete_placeholder_exists_some_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with some matching peak groups exists and no concrete record exists
        Result: Create a concrete record and link some of the peak groups belonging to the placeholder to it
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        # else:
        #     if existing_placeholder_qs.count() == 0:
        #     else:
        #         if matching_peakgroups_qs.count() > 0 and unmatching_peakgroups_qs.count() == 0:
        #         elif matching_peakgroups_qs.count() == 0:
        #         elif matching_peakgroups_qs.count() > 0 and unmatching_peakgroups_qs.count() > 0:
        #             if existing_concrete_rec is None:

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.msr.sample.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.msr.sample.name}_neg",
                MSRunsLoader.DataHeaders.MZXMLNAME: "BAT-xz971.mzXML",  # Creating concrete
                MSRunsLoader.DataHeaders.ANNOTNAME: self.accucor_file1.filename,
            }
        )

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertNotEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertTrue(created)
        self.assertFalse(
            updated
        )  # Peakgroups are updated, but not the MSRunSequence placeholder record itself

        # Check that the existing placeholder record still exists
        self.assertEqual(1, MSRunSample.objects.filter(id=self.msr.id).count())

        # And that it still has 1 peak group
        self.assertEqual(1, self.msr.peak_groups.count())
        # And that the concrete record was given 1 peak group
        self.assertEqual(1, rec.peak_groups.count())

    def test_get_create_or_update_msrun_sample_from_row_concrete_exists_placeholder_exists_some_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with some matching peak groups exists and concrete record exists
        Result: Link some of the placeholder's peak groups to the existing concrete record
        """

        # This is the logic/place in the msruns_loader code we are testing works
        # if mzxml_metadata["mzaf_record"] is None:
        # else:
        #     if existing_placeholder_qs.count() == 0:
        #     else:
        #         if matching_peakgroups_qs.count() > 0 and unmatching_peakgroups_qs.count() == 0:
        #         elif matching_peakgroups_qs.count() == 0:
        #         elif matching_peakgroups_qs.count() > 0 and unmatching_peakgroups_qs.count() > 0:
        #             if existing_concrete_rec is None:
        #             else:

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # Create a concrete MSRunSample record (i.e. it has an mzXML file and no peak groups link to it)
        concrete_mzxml_dict = self.MOCK_MZXML_DICT["BAT_xz971"][
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
        ][0]
        concrete_rec_dict = {
            "msrun_sequence": self.msr.msrun_sequence,
            "sample": self.msr.sample,
            "polarity": concrete_mzxml_dict["polarity"],
            "mz_min": concrete_mzxml_dict["mz_min"],
            "mz_max": concrete_mzxml_dict["mz_max"],
            "ms_data_file": concrete_mzxml_dict["mzaf_record"],
            "ms_raw_file": concrete_mzxml_dict["rawaf_record"],
        }

        # Create a concrete record, to which no peak groups will be added
        concrete_rec = MSRunSample.objects.create(**concrete_rec_dict)
        concrete_rec.full_clean()

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: self.msr.sample.name,
                MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.msr.sample.name}_neg",
                MSRunsLoader.DataHeaders.MZXMLNAME: "BAT-xz971.mzXML",  # Creating concrete
                MSRunsLoader.DataHeaders.ANNOTNAME: self.accucor_file1.filename,
            }
        )

        # This is the method we're testing
        rec, created, updated = msrl.get_create_or_update_msrun_sample_from_row(row)

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertNotEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)
        # Peakgroups are updated, but not the MSRunSample placeholder record itself
        self.assertFalse(updated)

        # Check that the existing placeholder record still exists
        self.assertEqual(1, MSRunSample.objects.filter(id=self.msr.id).count())
        # Check that the existing concrete record still exists
        self.assertEqual(1, MSRunSample.objects.filter(id=concrete_rec.id).count())

        # And that it still has 1 peak group
        self.assertEqual(1, self.msr.peak_groups.count())
        # And that the concrete record was given 1 peak group
        self.assertEqual(1, rec.peak_groups.count())

    def test_constructor_conflicting_defaults(self):
        inst = MSRunSequence.INSTRUMENT_CHOICES[0][0]
        with self.assertRaises(AggregatedErrors) as ar:
            MSRunsLoader(
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
                operator="L.C. McMethod",
                date="2024-05-06",
                instrument="QE2",
                lc_protocol_name="polar-HILIC-20-min",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(MutuallyExclusiveArgs, type(aes.exceptions[0]))
        self.assertIn(
            "['Operator', 'Date', 'LC Protocol Name', 'Instrument']",
            str(aes.exceptions[0]),
        )

    def test_constructor_sequences_loader_error(self):
        """Trigger an error from the SequencesLoader class, which exists as an instance inside the MSRunsLoader, to
        ensure that the errors it buffers are extracted and incorporated into the MSRunsLoader object.
        """
        with self.assertRaises(AggregatedErrors) as ar:
            MSRunsLoader(
                defaults_df=pd.DataFrame.from_dict(
                    {
                        # These are defaults for the Sequences sheet, used by the SequencesLoader instance that is a
                        # member of an MSRunsLoader instance
                        MSRunsLoader.DefaultsHeaders.SHEET_NAME: ["Sequences"],
                        MSRunsLoader.DefaultsHeaders.COLUMN_NAME: [
                            "Invalid Column Name"
                        ],
                        MSRunsLoader.DefaultsHeaders.DEFAULT_VALUE: [
                            "Any irrelevant value"
                        ],
                    }
                ),
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(InfileError, type(aes.exceptions[0]))
        self.assertIn(
            "Expected: ['Sequence Name', 'Operator', 'LC Protocol Name', ",
            str(aes.exceptions[0]),
        )

    def test_get_sample_header_from_mzxml_name(self):
        msrl1 = MSRunsLoader()
        name1 = msrl1.get_sample_header_from_mzxml_name(
            "path/file-with-dashes_and_underscrore.mzXML"
        )
        self.assertEqual("file_with_dashes_and_underscrore", name1)
        name2 = msrl1.get_sample_header_from_mzxml_name(
            "file-with-dashes_and_underscrore.mzXML"
        )
        self.assertEqual("file_with_dashes_and_underscrore", name2)
        name3 = msrl1.get_sample_header_from_mzxml_name(
            "file-with-dashes_and_underscrore"
        )
        self.assertEqual("file_with_dashes_and_underscrore", name3)

        msrl2 = MSRunsLoader(exact_mode=True)
        name4 = msrl2.get_sample_header_from_mzxml_name(
            "path/file-with-dashes_and_underscrore.mzXML"
        )
        self.assertEqual("file-with-dashes_and_underscrore", name4)
        name5 = msrl2.get_sample_header_from_mzxml_name(
            "file-with-dashes_and_underscrore.mzXML"
        )
        self.assertEqual("file-with-dashes_and_underscrore", name5)
        name6 = msrl2.get_sample_header_from_mzxml_name(
            "file-with-dashes_and_underscrore"
        )
        self.assertEqual("file-with-dashes_and_underscrore", name6)


class MSRunsLoaderArchiveTests(TracebaseArchiveTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    def test_load_data_no_infile(self):
        """This tests loading JUST the mzxml files with just the files themselves and defaults arguments (/command line
        options).  I.e. no dataframe (/infile)."""

        # For some reason, doing this in a setUpTestData method caused errors on lines below that referenced self.anml
        # and self.tsu, so I put the call directly in the test.
        # TODO: Figure out how to put this call in classmethod: setUpTestData
        anml, tsu = create_animal_and_tissue_records()

        seq = MSRunSequence.objects.create(
            researcher="L.C. McMethod",
            date=datetime.strptime("2024-05-06", "%Y-%m-%d"),
            instrument="QE2",
            lc_method=LCMethod.objects.get(name__exact="polar-HILIC-25-min"),
        )
        smpl = Sample.objects.create(
            name="BAT-xz971",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )

        # Set up the loader object
        msrl = MSRunsLoader(
            mzxml_files=[
                "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
            ],
            operator="L.C. McMethod",
            date="2024-05-06",
            instrument="QE2",
            lc_protocol_name="polar-HILIC-25-min",
        )

        # Ensure record does not exist at first (so we know this test is meaningful)
        self.assertEqual(
            0,
            MSRunSample.objects.filter(
                sample=smpl,
                msrun_sequence=seq,
            ).count(),
        )

        # This is the method we're testing
        msrl.load_data()

        msrs = MSRunSample.objects.get(
            sample=smpl,
            msrun_sequence=seq,
            ms_data_file__filename="BAT-xz971.mzXML",
        )

        self.assertIsNotNone(msrs.ms_data_file.file_location)
        self.assertTrue(
            os.path.isfile(msrs.ms_data_file.file_location.path),
            msg="Asserts mzXML file was created in the archive.",
        )
        # No exception = successful test

    def setup_load(self):
        anml, tsu = create_animal_and_tissue_records()
        # Create a sequence for the load to retrieve
        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")
        inst = MSRunSequence.INSTRUMENT_CHOICES[0][0]
        seq = MSRunSequence.objects.create(
            researcher="Dick",
            date=datetime.strptime("1991-5-7", "%Y-%m-%d"),
            instrument=inst,
            lc_method=lcm,
        )
        # Create sample for the load to retrieve
        s1 = Sample.objects.create(
            name="s1",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        return inst, seq, s1

    def test_load_data_infile(self):
        inst, seq, s1 = self.setup_load()

        # Create a dataframe to use to retrieve the records
        # Including a sample (s2) from a non-matching file (which should not be retrieved)
        df = pd.DataFrame.from_dict(
            {
                "Sample Name": ["s1"],
                "Sample Data Header": ["s1_pos"],
                "mzXML File Name": [None],
                "Peak Annotation File Name": ["accucor.xlsx"],
                "Sequence Name": [f"Dick, polar-HILIC-25-min, {inst}, 1991-5-7"],
            },
        )
        msrl = MSRunsLoader(df=df)
        msrl.load_data()
        # No exception = successful test
        MSRunSample.objects.get(
            msrun_sequence=seq,
            sample=s1,
            ms_data_file__isnull=True,
        )

    def test_get_loaded_msrun_sample_dict(self):
        inst, seq, s1 = self.setup_load()

        # Create MSRunSample record for the load to retrieve
        msrs1 = MSRunSample.objects.create(
            msrun_sequence=seq,
            sample=s1,
            polarity=None,
            ms_raw_file=None,
            ms_data_file=None,
        )

        # Create a dataframe to use to retrieve the records
        # Including a sample (s2) from a non-matching file (which should not be retrieved)
        df = pd.DataFrame.from_dict(
            {
                "Sample Name": ["s1", "s2"],
                "Sample Data Header": ["s1_pos", "s2_pos"],
                "mzXML File Name": ["s1_pos.mzXML", "s2_pos.mzXML"],
                "Peak Annotation File Name": ["accucor.xlsx", "accucor2.xlsx"],
                "Sequence Name": [
                    f"Dick, polar-HILIC-25-min, {inst}, 1991-5-7",
                    f"Dick, polar-HILIC-25-min, {inst}, 1991-5-7",
                ],
            },
        )

        msrl = MSRunsLoader(df=df)
        msrsd = msrl.get_loaded_msrun_sample_dict("accucor.xlsx")

        expected = {
            "s1_pos": {
                "MSRunSample": msrs1,
                "Peak Annotation File Name": "accucor.xlsx",
                "Sample Data Header": "s1_pos",
                "Sample Name": "s1",
                "Sequence Name": "Dick, polar-HILIC-25-min, QE, 1991-5-7",
                "Skip": False,
                "mzXML File Name": "s1_pos.mzXML",
            },
        }

        self.assertDictEqual(expected, msrsd)
