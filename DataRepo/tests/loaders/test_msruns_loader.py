import os
import re
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
from DataRepo.models.compound import Compound
from DataRepo.tests.tracebase_test_case import (
    TracebaseArchiveTestCase,
    TracebaseTestCase,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AllMzxmlSequenceUnknown,
    InfileError,
    MissingSamples,
    MutuallyExclusiveArgs,
    MzxmlColocatedWithMultipleAnnot,
    MzxmlNotColocatedWithAnnot,
    NoSamples,
    PossibleDuplicateSample,
    RecordDoesNotExist,
    RequiredColumnValue,
    RollbackException,
    UnmatchedBlankMzXML,
    UnmatchedMzXML,
    UnskippedBlanks,
)
from DataRepo.utils.file_utils import read_from_file
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs


def create_animal_and_tissue_records():
    Compound.objects.create(
        name="gluc",
        formula="C6H12O6",
        hmdb_id="HMDB0000122",
    )
    infobj = parse_infusate_name_with_concs("gluc-[13C6][10]")
    inf, _ = Infusate.objects.get_or_create_infusate(infobj)
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
        cls.smpl = Sample.objects.create(
            name="Sample Name",
            tissue=cls.tsu,
            animal=cls.anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        cls.lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")

        cls.seq = MSRunSequence.objects.create(
            researcher="John Doe",
            date=datetime.strptime("1991-5-7", "%Y-%m-%d"),
            instrument=MSRunSequence.INSTRUMENT_CHOICES[0][0],
            lc_method=cls.lcm,
        )
        cls.seq.full_clean()
        cls.msr = MSRunSample.objects.create(
            msrun_sequence=cls.seq,
            sample=cls.smpl,
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

        bat_mzxml_file = (
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/"
            "BAT-xz971.mzXML"
        )
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
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls": [
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
                        "mzxml_dir": "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls",
                    }
                ],
            },
            "BAT_xz971": {
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls": [
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
                        "mzxml_dir": "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls",
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
            scan_patterns=[r"blah"],
        )
        self.assertEqual("mysample", samplename)

    def test_guess_sample_name_just_custom(self):
        samplename = MSRunsLoader.guess_sample_name(
            "mysample_pos_blah",
            scan_patterns=[r"blah"],
            add_patterns=False,
        )
        self.assertEqual("mysample_pos", samplename)

    def test_leftover_mzxml_files_exist_true(self):
        """Tests that leftover_mzxml_files_exist finds the existence of un-added mzXML files (i.e. those that were not
        described in the infile because they weren't used in the production of a peak annotation file).
        """
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
        self.assertTrue(msrl.unpaired_mzxml_files_exist())

    def test_leftover_mzxml_files_exist_false(self):
        """Tests that leftover_mzxml_files_exist finds the existence of un-added mzXML files (i.e. those that were not
        described in the infile because they weren't used in the production of a peak annotation file).
        """
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
        msrl.mzxml_dict["BAT_xz971"][
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
        ][0]["added"] = True
        self.assertFalse(msrl.unpaired_mzxml_files_exist())

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
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
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
        """Tests the case where there are multiple mzXML files with the same name and the record is created with a
        warning."""
        msrl = MSRunsLoader()
        msrl.set_row_index(2)
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
        mzxml_metadata, mult_matches = msrl.get_matching_mzxml_metadata(
            "mysample1",  # Sample name - does not match
            "mysample1_neg_pos_scan2",  # Sample header - does not match (because of the "1")
            "mysample1_edited_filename.mzXML",  # file name without path will match multiple
        )
        self.assertDictEqual(
            {
                "polarity": None,
                "mz_min": None,
                "mz_max": None,
                "raw_file_name": None,
                "raw_file_sha1": None,
                "mzaf_record": None,
                "rawaf_record": None,
                "mzxml_dir": None,
                "mzxml_filename": None,
                "added": False,
            },
            mzxml_metadata,
        )
        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertEqual(1, msrl.aggregated_errors_object.num_warnings)
        self.assertEqual(0, msrl.aggregated_errors_object.num_errors)
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

        self.assertTrue(mult_matches)

    def test_get_matching_mzxml_metadata_adding_path_fix(self):
        """Tests the case where there are multiple mzXML files with the same name and the user fixes the issue by adding
        a path to the mzXML Name column in the --infile."""
        msrl = MSRunsLoader()
        msrl.set_row_index(2)
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
        mzxml_metadata, mult_matches = msrl.get_matching_mzxml_metadata(
            "mysample1",  # Sample name - does not match
            "mysample1_neg_pos_scan2",  # Sample header - does not match (because of the "1")
            "/path/to/second/file/mysample1_edited_filename.mzXML",  # file name with path will match 1
        )
        expected = self.MOCK_MZXML_DICT["mysample1_edited_filename"][
            "/path/to/second/file"
        ][0]
        self.assertDictEqual(expected, mzxml_metadata)
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))

        self.assertFalse(mult_matches)

    def test_get_matching_mzxml_metadata_header_matches_uniquely(self):
        msrl = MSRunsLoader()
        msrl.set_row_index(2)
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)
        expected = self.MOCK_MZXML_DICT["Br_xz971"][
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
        ][0]
        mzxml_metadata, mult_matches = msrl.get_matching_mzxml_metadata(
            "mysample",  # Sample name - does not match
            "Br_xz971",  # Sample header - does match
            "Br-xz971.mzXML",  # file name or path
        )
        self.assertDictEqual(expected, mzxml_metadata)
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))

        self.assertFalse(mult_matches)

    def test_get_matching_mzxml_metadata_header_with_underscore_matches_dash(self):
        msrl = MSRunsLoader()
        msrl.set_row_index(2)
        # Have the object setup the mzxml_dict, so we see what would really happen
        msrl.get_or_create_mzxml_and_raw_archive_files(
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
        )
        mzxml_metadata, _ = msrl.get_matching_mzxml_metadata(
            "mysample",  # Sample name - does not match
            "BAT_xz971",  # Sample header - does match
            None,  # no mzxml filename from sheet
        )
        self.assertFalse(mzxml_metadata["added"])
        self.assertEqual(502.9, mzxml_metadata["mz_max"])
        self.assertEqual(1.0, mzxml_metadata["mz_min"])
        self.assertIsInstance(mzxml_metadata["mzaf_record"], ArchiveFile)
        self.assertEqual(
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls",
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
        # NOTE: See test_check_mzxml_files for testing the handling of unmatched mzXML exceptions added by this method

    def test_get_or_create_msrun_sample_from_mzxml_success(self):
        msrl = MSRunsLoader()
        sample = self.msr.sample
        msrun_sequence = self.msr.msrun_sequence

        # Copy the metadata, because the method will modify it
        mzxml_metadata = deepcopy(
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]
        )

        msrl.mzxml_dict = self.MOCK_MZXML_DICT

        # Test create
        rec, created = msrl.get_or_create_msrun_sample_from_mzxml(
            sample,
            "BAT_xz971",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls",
            mzxml_metadata,
            msrun_sequence,
        )
        self.assertTrue(created)
        self.assertEqual(rec.sample, sample)
        self.assertEqual(rec.msrun_sequence, msrun_sequence)
        self.assertEqual(mzxml_metadata["mzaf_record"], rec.ms_data_file)

        # Test get
        # Copy the metadata again
        mzxml_metadata2 = deepcopy(
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]
        )
        rec2, created2 = msrl.get_or_create_msrun_sample_from_mzxml(
            sample,
            "BAT_xz971",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls",
            mzxml_metadata2,
            msrun_sequence,
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
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]
        )

        msrl.mzxml_dict = self.MOCK_MZXML_DICT

        rec, created = msrl.get_or_create_msrun_sample_from_mzxml(
            sample,  # Make sure this isn't necessary in edge cases
            "Br_xz971",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls",
            mzxml_metadata,
            msrun_sequence,
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
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
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
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_pos_mzxmls/BAT-xz971_pos.mzXML"
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
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_pos_mzxmls/BAT-xz971_pos.mzXML"
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
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
            )
        )
        self.assertEqual(ArchiveFile, type(mzaf_rec))
        self.assertTrue(mzaf_created)
        self.assertEqual(ArchiveFile, type(rawaf_rec))
        self.assertTrue(rawaf_created)

    def test_get_or_create_msrun_sample_from_row_creating_placeholder_no_placeholder_exists(
        self,
    ):
        """Input: Data for a placeholder record
        State: No placeholder record exists, no peakgroups exist
        Result: Placeholder created
        """

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
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.sample_with_no_msr, rec.sample)

        # Check that the record does not have an mzXML
        self.assertIsNone(rec.ms_data_file)
        self.assertTrue(created)

    def test_get_or_create_msrun_sample_from_row_creating_placeholder_placeholder_exists(
        self,
    ):
        """Input: Data for a placeholder record
        State: Placeholder record exists, peak groups may or may not exist for it
        Result: Placeholder gotten
        """

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
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record does not have an mzXML
        self.assertIsNone(rec.ms_data_file)
        self.assertFalse(created)

    def test_get_or_create_msrun_sample_from_row_no_concrete_no_placeholder(
        self,
    ):
        """Input: Data for a concrete record
        State: No placeholder record exists, no concrete record exists, no peak groups for them exist
        Result: Concrete created
        """

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
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the existing placeholder now has the mzXML
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertTrue(created)

    def test_get_or_create_msrun_sample_from_row_concrete_exists_no_placeholder(
        self,
    ):
        """Input: Data for a concrete record
        State: No placeholder record exists, concrete record exists, peak groups may or may not exist for them
        Result: Concrete gotten and placeholder not created
        """

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
        msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # This is the method we're testing
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.sample_with_no_msr, rec.sample)

        # Check that the record has the mzXML
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)

    def test_get_or_create_msrun_sample_from_row_no_concrete_placeholder_all_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with all matching peak groups exists and no concrete record exists
        Result: Placeholder deleted, concrete record created, and PeakGroups updated
        """

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

        # Save the placeholder id
        ph_id = self.msr.id

        # This is the method we're testing
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the existing placeholder now has the mzXML
        self.assertNotEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertTrue(created)
        self.assertGreater(rec.peak_groups.count(), 0)
        self.assertEqual(0, MSRunSample.objects.filter(id=ph_id).count())

    def test_get_create_or_update_msrun_sample_from_row_concrete_exists_no_pgs_placeholder_all_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with all matching peak groups exists and concrete record with no peak groups exists
        Result: PeakGroups moved to the existing concrete record and Placeholder deleted
        """

        # Change the accucor file in the second peak group to be the same as the first so that all peak groups in the
        # existing placeholder record (created in setUpTestData) will match the added mzXML file / concrete msrun_sample
        # record
        self.pg2.peak_annotation_file = self.accucor_file1
        self.pg2.save()

        # Create an empty concrete MSRunSample record (i.e. it has an mzXML file and no peak groups link to it)
        concrete_mzxml_dict = self.MOCK_MZXML_DICT["BAT_xz971"][
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
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
        placeholder_id = self.msr.id

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
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertEqual(empty_concrete_rec.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)

        # Check that the existing placeholder record was deleted
        self.assertEqual(0, MSRunSample.objects.filter(id=placeholder_id).count())

        # Check that the concrete record received the peakgroups
        self.assertGreater(empty_concrete_rec.peak_groups.count(), 0)
        self.assertEqual(0, self.msr.peak_groups.count())

    def test_get_or_create_msrun_sample_from_row_concrete_exists_with_pgs_placeholder_all_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder does not exist and concrete record with peak groups exists
        Result: Existing concrete record retrieved and still has peak groups and no placeholder created
        """

        # Create an empty concrete MSRunSample record (i.e. it has an mzXML file and no peak groups link to it)
        concrete_mzxml_dict = self.MOCK_MZXML_DICT["BAT_xz971"][
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
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

        # Keep track of the placeholder record's ID so we can check it gets deleted
        placeholder_rec_id = self.msr.id
        msrun_sequence = self.msr.msrun_sequence
        sample = self.msr.sample

        # This is an initial call to assign peak groups (from the existing placeholder) to the concrete record we
        # created above and delete the placeholder record
        start_rec, start_created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure we're in the expected starting state
        self.assertEqual(0, MSRunSample.objects.filter(id=placeholder_rec_id).count())
        self.assertEqual(2, start_rec.peak_groups.count())
        self.assertFalse(start_created)

        # Now we have both a placeholder and concrete record, with peak groups belonging to the concrete record.  Let's
        # change a peak group to now belong to a different annotation file, to simulate a newly made association...

        # Change the accucor file in the second peak group to be the same as the first so that all peak groups in the
        # existing placeholder record (created in setUpTestData) will match the mzXML file / concrete msrun_sample
        # record that is now associated with a peak annotation file
        pg2 = start_rec.peak_groups.filter(name="lact").first()
        pg2.peak_annotation_file = self.accucor_file1
        pg2.save()

        # This is the method we're testing
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object
        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(msrun_sequence, rec.msrun_sequence)
        self.assertEqual(sample, rec.sample)

        # Check that the record has the mzXML
        self.assertEqual(concrete_rec.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)

        # Check that the existing concrete record now has 2 peak groups
        self.assertEqual(2, rec.peak_groups.count())

        # Check that the existing placeholder record was deleted
        self.assertEqual(0, MSRunSample.objects.filter(id=placeholder_rec_id).count())

    def test_get_or_create_msrun_sample_from_row_no_concrete_placeholder_exists_but_no_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with matching peak groups exists and no concrete record exists
        Result: Concrete record created, placeholder deleted, and peak groups moved to concrete record
        """

        path = Path(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose.xlsx"
        )
        with path.open(mode="rb") as f:
            myfile = File(f, name=path.name)
            tmp_accucor_file = ArchiveFile.objects.create(
                filename="small_obob_maven_6eaas_inf_lactate.xlsx",
                file_location=myfile,
                checksum="558ea654d7f2914ca4527580edf4fac11bd151c9",
                data_type=DataType.objects.get(code="ms_peak_annotation"),
                data_format=DataFormat.objects.get(code="accucor"),
            )
            tmp_accucor_file.save()

        # Change the accucor file in the second peak group to be the same as the first so that all peak groups in the
        # existing placeholder record (created in setUpTestData) will **NOT** match the added mzXML file / concrete
        # msrun_sample record
        self.pg2.peak_annotation_file = tmp_accucor_file
        self.pg2.save()

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        ph_id = self.msr.id

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
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertNotEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertTrue(created)

        # Check that the existing placeholder record was deleted
        self.assertEqual(0, MSRunSample.objects.filter(id=ph_id).count())

        # And that the concrete record got both peak groups
        self.assertEqual(2, rec.peak_groups.count())

    def test_get_or_create_msrun_sample_from_row_concrete_exists_placeholder_exists_but_no_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with matching peak groups exists and concrete record exists
        Result: Get existing concrete record, peak groups re-linked to the concrete record, and placeholder deleted
        """

        # Change the accucor file in the second peak group to be the same as the first so that all peak groups in the
        # existing placeholder record (created in setUpTestData) will **NOT** match the added mzXML file / concrete
        # msrun_sample record
        self.pg2.peak_annotation_file = self.accucor_file1
        self.pg2.save()

        # Create a concrete MSRunSample record (i.e. it has an mzXML file and no peak groups link to it)
        concrete_mzxml_dict = self.MOCK_MZXML_DICT["BAT_xz971"][
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
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

        ph_id = self.msr.id

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
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertNotEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)

        # Check that the existing placeholder record still exists
        self.assertEqual(0, MSRunSample.objects.filter(id=ph_id).count())

        # And that it still has both peak groups
        self.assertEqual(2, concrete_rec.peak_groups.count())

    def test_get_or_create_msrun_sample_from_row_no_concrete_placeholder_exists_some_pgs_match(
        self,
    ):
        """Input: Data for a concrete record
        State: Placeholder record with matching peak groups exists and no concrete record exists
        Result: Create a concrete record and peak groups are re-linked to the concrete record, and placeholder deleted
        """

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        ph_id = self.msr.id

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
        try:
            rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        except RollbackException:
            # If rollback was raised, raise the real exception that occurred and was buffered
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertNotEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertTrue(created)

        # Check that the existing placeholder record is deleted
        self.assertEqual(0, MSRunSample.objects.filter(id=ph_id).count())

        # And that the concrete record was given the peak groups
        self.assertEqual(2, rec.peak_groups.count())

    def test_get_or_create_msrun_sample_from_row_concrete_exists_placeholder_exists_some_pgs_match(
        self,
    ):
        """Input: Data for a concrete record (same as existing)
        State: Placeholder record with matching peak groups exists and concrete record exists
        Result: Re-link the placeholder's peak groups to the existing concrete record and delete the placeholder
        """

        # Set up the loader object
        msrl = MSRunsLoader()
        msrl.mzxml_dict = deepcopy(self.MOCK_MZXML_DICT)

        # Create a concrete MSRunSample record (i.e. it has an mzXML file and no peak groups link to it)
        concrete_mzxml_dict = self.MOCK_MZXML_DICT["BAT_xz971"][
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
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

        ph_id = self.msr.id

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
        rec, created = msrl.get_or_create_msrun_sample_from_row(row)
        if msrl.aggregated_errors_object.should_raise():
            raise msrl.aggregated_errors_object

        # Make sure that the record returned belongs to the expected sample and sequence
        self.assertEqual(self.msr.msrun_sequence, rec.msrun_sequence)
        self.assertEqual(self.msr.sample, rec.sample)

        # Check that the record has the mzXML
        self.assertNotEqual(self.msr.id, rec.id)
        self.assertEqual(
            rec.ms_data_file,
            self.MOCK_MZXML_DICT["BAT_xz971"][
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
            ][0]["mzaf_record"],
        )
        self.assertFalse(created)

        # Check that the existing placeholder record was deleted
        self.assertEqual(0, MSRunSample.objects.filter(id=ph_id).count())
        # Check that the existing concrete record still exists
        self.assertEqual(1, MSRunSample.objects.filter(id=concrete_rec.id).count())

        # And that the concrete record was given all of the peak groups
        self.assertEqual(2, rec.peak_groups.count())

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

    def test_get_mzxml_files_none(self):
        mzxml_files = MSRunsLoader.get_mzxml_files()
        self.assertEqual([], mzxml_files)

    def test_get_mzxml_files_files(self):
        files = [
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/Br-xz971.mzXML",
        ]
        mzxml_files = MSRunsLoader.get_mzxml_files(
            files=files,
            dir="DataRepo/data/tests/small_obob",
        )
        self.assertEqual(files, mzxml_files)

    def test_get_mzxml_files_dir(self):
        mzxml_files = MSRunsLoader.get_mzxml_files(
            dir="DataRepo/data/tests/small_obob_mzxmls"
        )
        expected = [
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_pos_mzxmls/Br-xz971_pos.mzXML",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_pos_mzxmls/BAT-xz971_pos.mzXML",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_neg_mzxmls/Br-xz971_neg.mzXML",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_neg_mzxmls/BAT-xz971_neg.mzXML",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/Br-xz971.mzXML",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/Br-xz971.mzXML",
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML",
        ]
        self.assertEqual(set(expected), set(mzxml_files))

    def test_sequence_or_skip_required(self):
        """This tests that a sequence name is not required if the row is being skipped."""
        msrl = MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    MSRunsLoader.DataHeaders.SEQNAME: [
                        None,  # No error
                        None,  # No error
                        None,  # Should produce RequiredColumnValue exception since SKIP is also None
                    ],
                    MSRunsLoader.DataHeaders.SAMPLENAME: [
                        "s1",
                        "s2",
                        "s3",
                    ],
                    MSRunsLoader.DataHeaders.SAMPLEHEADER: [
                        "s1_pos",
                        None,
                        "s3_pos",
                    ],
                    MSRunsLoader.DataHeaders.MZXMLNAME: [
                        None,
                        "s2_pos.mzXML",
                        None,
                    ],
                    MSRunsLoader.DataHeaders.ANNOTNAME: [
                        None,
                        None,
                        None,
                    ],
                    MSRunsLoader.DataHeaders.SKIP: [
                        "skip",  # No error
                        "skip",  # No error
                        None,  # Should produce RequiredColumnValue exception since SEQNAME is also None
                    ],
                }
            ),
        )
        # This should buffer RequiredColumnValue exceptions if there are any missing required values, one for each
        # offending row - and we only expect 1 for the last row (identified as row 4 in an excel sheet)
        msrl.check_dataframe_values()
        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertEqual(
            1,
            len(msrl.aggregated_errors_object.get_exception_type(RequiredColumnValue)),
        )
        self.assertEqual("4", str(msrl.aggregated_errors_object.exceptions[0].rownum))
        self.assertEqual(
            "(Sequence, Skip)^ (^ = Any Required)",
            str(msrl.aggregated_errors_object.exceptions[0].column),
            msg=f"'(Sequence, Skip)^ (^ = Any Required)' != {msrl.aggregated_errors_object.exceptions[0].column}",
        )

    def test_msrunsamples_created_for_mzxmls_with_same_name_using_default_seq(self):
        """This tests that MSRunSample records are created using the default sequence arguments and sample records
        matching the mzXML filenames."""
        # Create samples based on the file names (the one(s) created in setUpTestData have different names)
        Sample.objects.create(
            name="BAT_xz971",
            tissue=self.tsu,
            animal=self.anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        msrl = MSRunsLoader(
            mzxml_files=[
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML",
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML",
            ],
            operator="John Doe",  # From setUpTestData
            lc_protocol_name="polar-HILIC-25-min",
            instrument="QE",
            date="1991-5-7",
        )
        af_before = ArchiveFile.objects.count()
        msrs_before = MSRunSample.objects.count()
        msrl.load_data()
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))
        self.assertDictEqual(
            {
                "ArchiveFile": {
                    "created": 3,  # Both files have the same raw file
                    "existed": 1,
                    "skipped": 0,
                    "updated": 0,
                    "deleted": 0,
                    "errored": 0,
                    "warned": 0,
                },
                "MSRunSample": {
                    "created": 2,
                    "existed": 0,
                    "skipped": 0,
                    "updated": 0,
                    "deleted": 0,
                    "errored": 0,
                    "warned": 0,
                },
                "PeakGroup": {
                    "created": 0,
                    "existed": 0,
                    "skipped": 0,
                    "updated": 0,
                    "deleted": 0,
                    "errored": 0,
                    "warned": 0,
                },
            },
            msrl.record_counts,
        )
        self.assertEqual(af_before + 3, ArchiveFile.objects.count())
        self.assertEqual(msrs_before + 2, MSRunSample.objects.count())

    def test_msrunsamples_created_for_mzxmls_with_same_name_using_dir_dict_and_sample_from_infile(
        self,
    ):
        """This tests that MSRunSample records are created using the sequence records associated with a peak
        anotation file on the path of the mzXML and sample records matching the sample column that is associated with a
        header that matches the mzXML filenames."""
        df = read_from_file(
            "DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",
            sheet=MSRunsLoader.DataSheetName,
        )
        msrl = MSRunsLoader(
            df=df,
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",
            mzxml_files=[
                "DataRepo/data/tests/same_name_mzxmls/mzxmls/BAT-xz971.mzXML",
                "DataRepo/data/tests/same_name_mzxmls/mzxmls/pos/BAT-xz971.mzXML",
            ],
        )
        af_before = ArchiveFile.objects.count()
        msrs_before = MSRunSample.objects.count()

        msrl.load_data()

        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertEqual(1, msrl.aggregated_errors_object.num_warnings)
        self.assertIsInstance(
            msrl.aggregated_errors_object.exceptions[0], AllMzxmlSequenceUnknown
        )
        self.assertIn(
            "BAT_xz971 found on row(s): ['2-3']",
            str(msrl.aggregated_errors_object.exceptions[0]),
        )

        self.assertEqual(3, ArchiveFile.objects.count() - af_before)
        self.assertEqual(2, MSRunSample.objects.count() - msrs_before)

        self.assertEqual(3, msrl.record_counts["ArchiveFile"]["created"])
        self.assertEqual(1, msrl.record_counts["ArchiveFile"]["existed"])
        self.assertEqual(2, msrl.record_counts["MSRunSample"]["created"])
        self.assertEqual(0, msrl.record_counts["MSRunSample"]["errored"])

    def test_msrunsamples_created_for_mzxmls_with_same_name_using_dir_dict_from_infile(
        self,
    ):
        """This tests that MSRunSample records are created using the sequence records associated with a peak
        anotation file on the path of the mzXML and sample records matching the mzXML filenames (no matching row in the
        peak annotation details sheet).

        NOTE: This differs frm test_msrunsamples_created_for_mzxmls_with_same_name_using_dir_dict_and_sample_from_infile
        in that the mzXML filename in that one matched multiple rows in the Peak Annotation Details sheet, where a
        sample name differed from the mzXML filename.  Here, the mzXML isn't in the file at all, but we're still using
        the sequence associated with a peak annotation file found on the mzXML dir path
        """
        Sample.objects.create(
            name="BAT_xz971",
            tissue=self.tsu,
            animal=self.anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        msrl = MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    "Sample Name": [],
                    "Sample Data Header": [],
                    "mzXML File Name": [],
                    "Peak Annotation File Name": [],
                    "Sequence": [],
                    "Skip": [],
                }
            ),
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",  # Pk Annotation Details not used
            mzxml_files=[
                "DataRepo/data/tests/same_name_mzxmls/mzxmls/BAT-xz971.mzXML",
                "DataRepo/data/tests/same_name_mzxmls/mzxmls/pos/BAT-xz971.mzXML",
            ],
        )
        af_before = ArchiveFile.objects.count()
        msrs_before = MSRunSample.objects.count()

        msrl.load_data()

        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))

        self.assertEqual(3, ArchiveFile.objects.count() - af_before)
        self.assertEqual(2, MSRunSample.objects.count() - msrs_before)

        self.assertEqual(3, msrl.record_counts["ArchiveFile"]["created"])
        self.assertEqual(1, msrl.record_counts["ArchiveFile"]["existed"])
        self.assertEqual(2, msrl.record_counts["MSRunSample"]["created"])
        self.assertEqual(0, msrl.record_counts["MSRunSample"]["errored"])

    def test_get_msrun_sequence_from_dir_success(self):
        msrl = MSRunsLoader()
        msrl.annotdir_to_seq_dict = {
            "path/to": [
                "John Doe, polar-HILIC-25-min, QE, 1991-5-7",
            ],
            "path/to/scan2": [
                "John Doe, polar-HILIC-25-min, QE, 1991-5-7",
            ],
        }
        seq = msrl.get_msrun_sequence_from_dir(
            "sample.mzXML",
            "path/to/scan2/pos",
            None,
        )
        self.assertEqual(self.seq, seq)
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))

    def test_get_msrun_sequence_from_dir_ambiguous(self):
        msrl = MSRunsLoader()
        msrl.annotdir_to_seq_dict = {
            "path/to": [
                "Rob, polar-HILIC-25-min, QE, 1972-11-24",
                "Zoe, polar-HILIC-25-min, QE, 1985-4-18",
            ],
            "path/to/scan2": [
                "Rob, polar-HILIC-25-min, QE, 1972-11-24",
            ],
        }
        seq = msrl.get_msrun_sequence_from_dir(
            "sample.mzXML",
            "path/to",
            None,
        )
        self.assertIsNone(seq)
        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertTrue(
            msrl.aggregated_errors_object.exception_type_exists(
                MzxmlColocatedWithMultipleAnnot
            )
        )

    def test_get_msrun_sequence_from_dir_none(self):
        msrl = MSRunsLoader()
        msrl.annotdir_to_seq_dict = {
            "path/to": [
                "Rob, polar-HILIC-25-min, QE, 1972-11-24",
            ],
            "path/to/scan2": [
                "Rob, polar-HILIC-25-min, QE, 1972-11-24",
            ],
        }
        seq = msrl.get_msrun_sequence_from_dir(
            "sample.mzXML",
            "alternate/path/to",
            None,
        )
        self.assertIsNone(seq)
        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            msrl.aggregated_errors_object.exceptions[0], MzxmlNotColocatedWithAnnot
        )

    def test_get_msrun_sequence_from_dir_default(self):
        msrl = MSRunsLoader()
        msrl.annotdir_to_seq_dict = {
            "path/to": [
                "Rob, polar-HILIC-25-min, QE, 1972-11-24",
            ],
            "path/to/scan2": [
                "Rob, polar-HILIC-25-min, QE, 1972-11-24",
            ],
        }
        seq = msrl.get_msrun_sequence_from_dir(
            "sample.mzXML",
            "alternate/path/to",
            self.seq,
        )
        self.assertEqual(self.seq, seq)
        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertEqual(1, msrl.aggregated_errors_object.num_warnings)
        self.assertTrue(
            msrl.aggregated_errors_object.exception_type_exists(
                MzxmlNotColocatedWithAnnot
            )
        )

    def test_get_scan_pattern(self):
        sp1 = MSRunsLoader.get_scan_pattern()
        self.assertEqual(
            re.compile(
                "([\\-_]pos(?=[\\-_]|$)|[\\-_]neg(?=[\\-_]|$)|[\\-_]scan[0-9]+(?=[\\-_]|$))+"
            ),
            sp1,
        )
        sp2 = MSRunsLoader.get_scan_pattern(scan_patterns=["positive"])
        self.assertEqual(
            re.compile(
                "([\\-_]pos(?=[\\-_]|$)|[\\-_]neg(?=[\\-_]|$)|[\\-_]scan[0-9]+(?=[\\-_]|$)|[\\-_]positive(?=[\\-_]|"
                "$))+"
            ),
            sp2,
        )
        sp3 = MSRunsLoader.get_scan_pattern(
            scan_patterns=["positive", "negative"], add_patterns=False
        )
        self.assertEqual(
            re.compile("([\\-_]positive(?=[\\-_]|$)|[\\-_]negative(?=[\\-_]|$))+"), sp3
        )

    # NOTE: check_reassign_peak_groups is tested indirectly by the test_get_or_create_msrun_sample_from_row_* tests

    def test_report_discrepant_headers_nosamples(self):
        """Tests that report_discrepant_headers removes RecordDoesNotExist exceptions for the Sample model and
        summarizes them in NoSamples and UnskippedBlanks exceptions."""
        msrl = MSRunsLoader()
        msrl.header_to_sample_name = {
            "a_blank": {"a_blank": 0},
            "s1_pos": {"s1": 0},
            "s1_neg": {"s1": 0},
            "s2": {"s2": 0},
        }
        msrl.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "a_blank"},
            )
        )
        msrl.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s1"},
            )
        )
        msrl.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s2"},
            )
        )
        msrl.report_discrepant_headers()
        self.assertEqual(2, len(msrl.aggregated_errors_object.exceptions))
        self.assertIsInstance(msrl.aggregated_errors_object.exceptions[0], NoSamples)
        self.assertEqual(
            ["s1", "s2"], msrl.aggregated_errors_object.exceptions[0].search_terms
        )
        self.assertIsInstance(
            msrl.aggregated_errors_object.exceptions[1], UnskippedBlanks
        )
        self.assertEqual(
            ["a_blank"], msrl.aggregated_errors_object.exceptions[1].search_terms
        )

    def test_report_discrepant_headers_missingsamples(self):
        """Tests that report_discrepant_headers removes RecordDoesNotExist exceptions for the Sample model and
        summarizes them in a MissingSamples exception."""
        msrl = MSRunsLoader()
        msrl.header_to_sample_name = {
            "s1_pos": {"s1": 0},
            "s1_neg": {"s1": 0},
            "s2": {"s2": 0},
            "s3": {"s3": 0},
        }
        msrl.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s1"},
            )
        )
        msrl.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s2"},
            )
        )
        msrl.report_discrepant_headers()
        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            msrl.aggregated_errors_object.exceptions[0], MissingSamples
        )
        self.assertEqual(
            ["s1", "s2"], msrl.aggregated_errors_object.exceptions[0].search_terms
        )

    def test_check_sample_headers(self):
        """Tests that check_sample_headers catches cases where the same sample headers in different peak annotation
        files link to different samples, which suggests duplicate sample records."""
        msrl = MSRunsLoader()
        msrl.header_to_sample_name = {
            "s1": {
                "s1_pos": [2],
                "s1_neg": [3],
            },
        }
        msrl.check_sample_headers()
        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            msrl.aggregated_errors_object.exceptions[0], PossibleDuplicateSample
        )
        self.assertFalse(msrl.aggregated_errors_object.exceptions[0].is_error)
        self.assertFalse(msrl.aggregated_errors_object.exceptions[0].is_fatal)

    def test_check_mzxml_files_buffers_exc_for_every_unmatched_file(self):
        """This test ensures that every unmatched mzXML gets its own exception."""
        mrl = MSRunsLoader(
            df=pd.DataFrame.from_dict({}),
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",  # Peak Annotation Dtls not used
            mzxml_files=[
                "unknown_sample.mzXML",
                "scan2/unknown_sample.mzXML",
                "unknown_blank.mzXML",
                "scan2/unknown_blank.mzXML",
            ],
        )
        with self.assertRaises(AggregatedErrors) as ar:
            mrl.check_mzxml_files()
        aes = ar.exception
        # Assert that aes contains 2 errors for "unknown_sample.mzXML" and "scan2/unknown_sample.mzXML"
        self.assertEqual(2, aes.num_errors)
        self.assertIsInstance(aes.exceptions[0], UnmatchedMzXML)
        self.assertEqual("unknown_sample.mzXML", aes.exceptions[0].mzxml_file)
        self.assertIsInstance(aes.exceptions[1], UnmatchedMzXML)
        self.assertEqual("scan2/unknown_sample.mzXML", aes.exceptions[1].mzxml_file)
        # Assert that aes contains 2 warnings for "unknown_blank.mzXML" and "scan2/unknown_blank.mzXML"
        self.assertEqual(2, aes.num_warnings)
        self.assertIsInstance(aes.exceptions[2], UnmatchedBlankMzXML)
        self.assertEqual("unknown_blank.mzXML", aes.exceptions[2].mzxml_file)
        self.assertIsInstance(aes.exceptions[3], UnmatchedBlankMzXML)
        self.assertEqual("scan2/unknown_blank.mzXML", aes.exceptions[3].mzxml_file)

    def test_check_mzxml_files_does_not_error_when_skipped(self):
        """Test that there is no error about sample 'some_unknown_sample' not existing."""
        # Create an MSRunsLoader object named mrl
        # Set its mrl.df to a dataframe with skipped lines and an mzXML file whose name (some_unknown_sample.mzXML) does
        # not match any sample
        # Set mrl.mzxml_files to ["some_unknown_sample.mzXML"]
        msruns_loader_instance = MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    MSRunsLoader.DataHeaders.SAMPLENAME: [
                        "s1",
                        "s2",
                        "some_unknown_sample",
                    ],
                    MSRunsLoader.DataHeaders.SAMPLEHEADER: [
                        "s1_pos",
                        None,
                        "some_unknown_sample",
                    ],
                    MSRunsLoader.DataHeaders.MZXMLNAME: [
                        None,
                        "s2_pos.mzXML",
                        "some_unknown_sample.mzXML",
                    ],
                    MSRunsLoader.DataHeaders.ANNOTNAME: [
                        None,
                        None,
                        None,
                    ],
                    MSRunsLoader.DataHeaders.SEQNAME: [
                        None,
                        None,
                        None,
                    ],
                    MSRunsLoader.DataHeaders.SKIP: [
                        "skip",
                        "skip",
                        "skip",
                    ],
                }
            ),
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",  # Peak Annotation Dtls not used
            mzxml_files=["some_unknown_sample.mzXML"],
            debug=True,
        )
        msruns_loader_instance.check_mzxml_files()
        self.assertEqual(
            0, len(msruns_loader_instance.aggregated_errors_object.exceptions)
        )


class MSRunsLoaderArchiveTests(TracebaseArchiveTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    def test_load_data_no_infile(self):
        """This tests loading JUST the mzxml files with just the files themselves and defaults arguments (/command line
        options).  I.e. no dataframe (/infile)."""

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
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML"
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
                "Sequence": [f"Dick, polar-HILIC-25-min, {inst}, 1991-5-7"],
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
                "Sequence": [
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
                "Sequence": "Dick, polar-HILIC-25-min, QE, 1991-5-7",
                "Skip": False,
                "mzXML File Name": "s1_pos.mzXML",
            },
        }

        self.assertDictEqual(expected, msrsd)

    def test_clean_up_created_mzxmls_in_archive(self):
        msrl = MSRunsLoader()
        fl = "DataRepo/data/tests/same_name_mzxmls/mzxmls/BAT-xz971.mzXML"
        afr, created = ArchiveFile.objects.get_or_create(
            file_location=fl,
            data_type=DataType.objects.get(code="ms_data"),
            data_format=DataFormat.objects.get(code="mzxml"),
        )
        self.assertTrue(created)
        self.assertTrue(os.path.isfile(afr.file_location.path))
        afp = afr.file_location.path
        msrl.created_mzxml_archive_file_recs = [afr]

        delstatslists = msrl.clean_up_created_mzxmls_in_archive()
        self.assertDictEqual(
            {
                "deleted": [],
                "failures": [],
                "skipped": [],
            },
            delstatslists,
            msg="Deletions should fail if no fatal errors have been buffered.",
        )
        self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            msrl.aggregated_errors_object.exceptions[0], NotImplementedError
        )

        msrl.aggregated_errors_object.buffer_error(ValueError("Test"))
        delstatslists = msrl.clean_up_created_mzxmls_in_archive()
        self.assertDictEqual(
            {
                "deleted": [afp],
                "failures": [],
                "skipped": [],
            },
            delstatslists,
            msg="Deletions should succeed if fatal errors have been buffered.",
        )
        self.assertFalse(os.path.isfile(afr.file_location.path))
