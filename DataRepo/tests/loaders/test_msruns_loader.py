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
    ConditionallyRequiredArgs,
    InfileError,
    MissingSamples,
    MutuallyExclusiveArgs,
    MzxmlColocatedWithMultipleAnnot,
    MzxmlNotColocatedWithAnnot,
    MzxmlSampleHeaderMismatch,
    NoSamples,
    PossibleDuplicateSample,
    PossibleDuplicateSamples,
    RecordDoesNotExist,
    RequiredColumnValue,
    RequiredColumnValues,
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

        cls.bat_mzxml_file = (
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/"
            "BAT-xz971.mzXML"
        )
        with path.open(mode="rb") as f:
            cls.BAT_xz971_mz_af = ArchiveFile.objects.create(
                filename="BAT-xz971.mzXML",
                checksum=ArchiveFile.hash_file(Path(cls.bat_mzxml_file)),
                file_location=File(f, name=path.name),
                data_type=DataType.objects.get(code="ms_data"),
                data_format=DataFormat.objects.get(code="mzxml"),
            )
        cls.BAT_xz971_raw_af = ArchiveFile.objects.create(
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
                        "mzaf_record": cls.BAT_xz971_mz_af,
                        "rawaf_record": cls.BAT_xz971_raw_af,
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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)
        self.assertTrue(msrl.unpaired_mzxml_files_exist())

    def test_leftover_mzxml_files_exist_false(self):
        """Tests that leftover_mzxml_files_exist finds the existence of un-added mzXML files (i.e. those that were not
        described in the infile because they weren't used in the production of a peak annotation file).
        """
        msrl = MSRunsLoader()
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)
        msrl.mzxml_dict_by_header["BAT_xz971"][
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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)
        mzxml_metadata, mult_matches = msrl.get_matching_mzxml_metadata(
            "mysample1",  # Sample name - does not match
            "mysample1_neg_pos_scan2",  # Sample header - does not match (because of the "1")
            "mysample1_edited_filename.mzXML",  # file name without path will match multiple
        )
        expected = {
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
            "sample_name": None,
        }
        self.assertDictEqual(expected, mzxml_metadata)
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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)
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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)
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

    def test_get_msrun_sequence_no_default(self):
        """This test ensures that when there is no default, there is an error when sequence names are not provided.

        This serves to effectively test the check_seqname_column method.
        """
        # Create a Sample for the MSRunSample record to link to:
        Sample.objects.create(
            name="s1",
            tissue=self.tsu,
            animal=self.anml,
            researcher="John Clease",
            date=datetime.now(),
        )

        with self.assertRaises(AggregatedErrors) as ar:
            MSRunsLoader(
                # Supply 2 rows: one without skip, and one with - both missing a sequence name
                df=pd.DataFrame.from_dict(
                    {
                        MSRunsLoader.DataHeaders.SEQNAME: [None, None],
                        MSRunsLoader.DataHeaders.SAMPLENAME: ["s1", "s2"],
                        MSRunsLoader.DataHeaders.SAMPLEHEADER: ["s1_pos", "s2_pos"],
                        MSRunsLoader.DataHeaders.MZXMLNAME: [None, None],
                        MSRunsLoader.DataHeaders.ANNOTNAME: [None, None],
                        MSRunsLoader.DataHeaders.SKIP: [None, "skip"],
                    }
                ),
                mzxml_files=[
                    # Arbitrarily selected mzXML - whether it matches the sample doesn't matter for this test.
                    "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_lactate_mzxmls/BAT-xz971.mzXML"
                ],
            )

        agg_errs = ar.exception

        self.assertEqual(1, len(agg_errs.exceptions))
        self.assertIsInstance(agg_errs.exceptions[0], ConditionallyRequiredArgs)
        # NOTE: This essentially asserts that the skipped row is not among the reported missing sequence name rows (by
        # the fact it matches ['2'] and not ['2-3'])
        self.assertIn(
            f"1 rows ['2'] that do not have a value in the '{MSRunsLoader.DataHeaders.SEQNAME}' column",
            str(agg_errs.exceptions[0]),
        )

    def test_get_sample_by_name(self):
        msrl = MSRunsLoader()
        sample = msrl.get_sample_by_name("Sample Name")
        self.assertEqual(Sample.objects.get(name="Sample Name"), sample)
        # NOTE: See test_check_mzxml_files_buffers_exc_for_every_unmatched_file for testing the handling of unmatched
        # mzXML exceptions added by this method

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

        msrl.mzxml_dict_by_header = self.MOCK_MZXML_DICT

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

        msrl.mzxml_dict_by_header = self.MOCK_MZXML_DICT

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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)

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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)

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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)

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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)

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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)

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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)

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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)

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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)

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
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)

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

    def test_get_or_create_msrun_sample_from_row_header_to_sample_name_skips_none(
        self,
    ):
        """This test asserts that header_to_sample_name does not ever get a `None` key (which would result in
        PossibleDuplicateSample warnings when check_sample_headers inspects the header_to_sample_name dict).

        NOTE: This only applies to skipped rows, which are allowed to not have required column values (i.e. they're
        allowed to contain incomplete data).  It's also worth noting that the header_to_sample_name dict is otherwise
        intended to track skipped samples.
        """

        # Set up the loader object
        msruns_loader = MSRunsLoader()

        # The row data we will attempt to load
        row = pd.Series(
            {
                MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
                MSRunsLoader.DataHeaders.SAMPLENAME: "whatever",
                MSRunsLoader.DataHeaders.SAMPLEHEADER: None,
                MSRunsLoader.DataHeaders.MZXMLNAME: "2whatever.mzXML",
                MSRunsLoader.DataHeaders.ANNOTNAME: "accucor_file.xlsx",
                MSRunsLoader.DataHeaders.SKIP: "skip",
            }
        )

        # This is the method we're testing
        msruns_loader.get_or_create_msrun_sample_from_row(row)

        # Assert no errors have been buffered
        self.assertEqual(0, len(msruns_loader.aggregated_errors_object.exceptions))

        # Assert that the skipped row with no sample header did not result in an additional value added to
        # header_to_sample_name
        self.assertDictEqual({}, msruns_loader.header_to_sample_name)

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
        name1 = msrl1.make_sample_header_from_mzxml_name(
            "path/file-with-dashes_and_underscrore.mzXML"
        )
        self.assertEqual("file_with_dashes_and_underscrore", name1)
        name2 = msrl1.make_sample_header_from_mzxml_name(
            "file-with-dashes_and_underscrore.mzXML"
        )
        self.assertEqual("file_with_dashes_and_underscrore", name2)
        name3 = msrl1.make_sample_header_from_mzxml_name(
            "file-with-dashes_and_underscrore"
        )
        self.assertEqual("file_with_dashes_and_underscrore", name3)

        msrl2 = MSRunsLoader(exact_mode=True)
        name4 = msrl2.make_sample_header_from_mzxml_name(
            "path/file-with-dashes_and_underscrore.mzXML"
        )
        self.assertEqual("file-with-dashes_and_underscrore", name4)
        name5 = msrl2.make_sample_header_from_mzxml_name(
            "file-with-dashes_and_underscrore.mzXML"
        )
        self.assertEqual("file-with-dashes_and_underscrore", name5)
        name6 = msrl2.make_sample_header_from_mzxml_name(
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

    def test_skip_empty_rows_do_not_error(self):
        """Ensures that skipped rows never result in RequiredColumnValue errors.

        This also checks for the ConditionallyRequiredArgs error, because although the sequence name column does not
        require a value, that is because the sequence name has multiple means of supplying a default value.  The
        sequence name is used to set the MSRunSample.msrun_sequence foreign key, which is a required field.  Since the
        loading of mzXML files happens first and can take a long time, there is a quick check (whenever mzXML files are
        supplied) that looks for (unskipped) missing sequence name values when there are no defaults.  Note that that
        check happens whether there are mzXML files or not, but when there are mzXML files and the
        ConditionallyRequiredArgs exception occurs, it is immediately raised instead of buffered.

        This serves to effectively test the check_seqname_column method.
        """

        # Partially populated data with missing required values, except for completely empty rows, and no skips should
        # result in 4 RequiredColumnValue errors and a ConditionallyRequiredArgs error that does not include the empty
        # row.
        df_dict = {
            "Sample Name": ["s1", None, None, None, None],
            "Sample Data Header": [None, None, None, None, None],
            "mzXML File Name": [
                None,
                "s2_pos.mzXML",
                "BAT-xz971.mzXML",
                "BAT-xz971.mzXML",
                None,
            ],
            "Peak Annotation File Name": [
                "accucor.xlsx",
                "accucor2.xlsx",
                None,
                None,
                None,
            ],
            "Sequence": [
                "Dick, polar-HILIC-25-min, QE, 1991-5-7",
                "Dick, polar-HILIC-25-min, QE, 1991-5-7",
                None,
                None,
                None,
            ],
            "Skip": [None, None, None, None, None],
        }

        # First, test that if not skipped (and there are no mzXML files), we would get a RequiredColumnValue error for
        # each of the 4 rows with missing required data, and a ConditionallyRequiredArgs error.  (Without this
        # assertion, the check that they do not occur when skipped is nearly meaningless.)
        msr_loader = MSRunsLoader(
            df=pd.DataFrame.from_dict(df_dict),
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",
            skip_mzxmls=True,
        )
        msr_loader.check_dataframe_values()
        self.assertEqual(5, len(msr_loader.aggregated_errors_object.exceptions))
        self.assertEqual(
            set([RequiredColumnValue, ConditionallyRequiredArgs]),
            set(msr_loader.aggregated_errors_object.get_exception_types()),
        )
        # Assert that the empty row is not among those that need conditionally required args
        cra_err = msr_loader.aggregated_errors_object.get_exception_type(
            ConditionallyRequiredArgs
        )[0]
        self.assertIn(
            "there exists 2 rows ['4-5'] that do not have a value in the 'Sequence' column",
            str(cra_err),
        )

        # Now ensure that a ConditionallyRequiredArgs error does not occur in validate mode (because a user on the
        # validate page cannot supply command line arguments)
        msr_loader_validate = MSRunsLoader(
            df=pd.DataFrame.from_dict(df_dict),
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",
            skip_mzxmls=True,
            _validate=True,
        )
        msr_loader_validate.check_dataframe_values()
        # NOTE: In validate mode, the MSRunsLoader custom-handles the creation of the RequiredColumnValues summary
        # exception so that it can apply a case-specific suggestion.
        self.assertEqual(
            set([RequiredColumnValue, RequiredColumnValues]),
            set(msr_loader_validate.aggregated_errors_object.get_exception_types()),
        )
        self.assertEqual(
            5, len(msr_loader_validate.aggregated_errors_object.exceptions)
        )
        # The RequiredColumnValue errors are different from the ones associated with the Sequence column (which are
        # warnings).  We should assert that these are errors and do not contain the suggestion about the peak annotation
        # file name column
        rcv_err: RequiredColumnValue = (
            msr_loader_validate.aggregated_errors_object.get_exception_type(
                RequiredColumnValue
            )[0]
        )
        self.assertTrue(rcv_err.is_error)
        self.assertIsNone(rcv_err.suggestion)
        # Ensure that the suggestion of how to fix this was applied to the individual and summary exceptions
        rcvs_warn: RequiredColumnValues = (
            msr_loader_validate.aggregated_errors_object.get_exception_type(
                RequiredColumnValues
            )[0]
        )
        self.assertFalse(rcvs_warn.is_error)
        self.assertIn(
            "Filling in a valid 'Peak Annotation File Name' will associate a sample with the default sequence",
            str(rcvs_warn),
        )
        rcv_example_warn = rcvs_warn.exceptions[0]
        self.assertFalse(rcv_example_warn.is_error)
        self.assertIn(
            "Filling in a valid 'Peak Annotation File Name' will associate a sample with the default sequence",
            str(rcv_example_warn),
        )

        # Now check that we only get the ConditionallyRequiredArgs error when there are mzXML files and that it raises
        with self.assertRaises(AggregatedErrors) as ar:
            msr_loader = MSRunsLoader(
                df=pd.DataFrame.from_dict(df_dict),
                file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",
                mzxml_files=[
                    "DataRepo/data/tests/same_name_mzxmls/mzxmls/BAT-xz971.mzXML",
                    "DataRepo/data/tests/same_name_mzxmls/mzxmls/pos/BAT-xz971.mzXML",
                ],
            )
        aes = ar.exception
        # mrl.check_dataframe_values()
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(
            [ConditionallyRequiredArgs],
            aes.get_exception_types(),
        )

        # Now, if the rows (aside from the empty row) are skipped, there should be no exceptions at all
        df_dict["Skip"] = ["skip", "skip", "skip", "skip", None]
        msr_loader = MSRunsLoader(
            df=pd.DataFrame.from_dict(df_dict),
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",
            mzxml_files=[
                "DataRepo/data/tests/same_name_mzxmls/mzxmls/BAT-xz971.mzXML",
                "DataRepo/data/tests/same_name_mzxmls/mzxmls/pos/BAT-xz971.mzXML",
            ],
        )
        msr_loader.check_dataframe_values()
        self.assertEqual(0, len(msr_loader.aggregated_errors_object.exceptions))

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

    def test_check_mzxml_files_matches_relative_and_absolute_paths(self):
        """Test that skipped mzXML relative paths match absolute paths, meaning their check is skipped and that no
        errors result from the check.

        This sets the conditions for an error that we expect not to happen.  An error would happen for skipped rows
        while processing the actual files if the samples are different, the mzXML file matches the exact file referenced
        in the details sheet, but the 2 path being compared are not both relative or not both absolute.  The resulting
        error (if the relative/absolute paths could not be matched) would be a PossibleDuplicateSamples error.
        """
        msruns_loader = MSRunsLoader(
            df=pd.DataFrame.from_dict(
                {
                    MSRunsLoader.DataHeaders.SAMPLENAME: [
                        "BAT-xz971",
                        "BAT-xz972",
                    ],
                    MSRunsLoader.DataHeaders.SAMPLEHEADER: [
                        None,
                        None,
                    ],
                    MSRunsLoader.DataHeaders.MZXMLNAME: [
                        "DataRepo/data/tests/same_name_mzxmls/mzxmls/BAT-xz971.mzXML",
                        os.path.abspath(
                            "DataRepo/data/tests/same_name_mzxmls/mzxmls/pos/BAT-xz971.mzXML"
                        ),
                    ],
                    MSRunsLoader.DataHeaders.ANNOTNAME: [
                        None,
                        None,
                    ],
                    MSRunsLoader.DataHeaders.SEQNAME: [
                        None,
                        None,
                    ],
                    MSRunsLoader.DataHeaders.SKIP: [
                        "skip",
                        "skip",
                    ],
                }
            ),
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",  # Peak Annotation Dtls not used
            mzxml_files=[
                os.path.abspath(
                    "DataRepo/data/tests/same_name_mzxmls/mzxmls/BAT-xz971.mzXML"
                ),
                "DataRepo/data/tests/same_name_mzxmls/mzxmls/pos/BAT-xz971.mzXML",
            ],
            debug=True,
        )
        msruns_loader.check_mzxml_files()
        self.assertEqual(0, len(msruns_loader.aggregated_errors_object.exceptions))

    def test_get_msrun_sequence_from_dir_last_annot(self):
        """Tests that if there exists an unambiguous sequence assignment in the directory closest to an mzXML file (and
        an ambiguous sequence assignment in the directory above that), there is no error about a default sequence
        conflict that came from multiple peak annotation file sequence defaults (i.e. the unambiguous sequence assigned
        from the closest directory is used)."""
        # Create a sequence record
        defseq = MSRunSequence.objects.create(
            researcher="Rob",
            date=datetime.strptime("1972-11-24", "%Y-%m-%d"),
            instrument="QE",
            lc_method=LCMethod.objects.get(name__exact="polar-HILIC-25-min"),
        )

        msrunsloader = MSRunsLoader()
        # Create the metadata as if the 'Sequences' sheet had been loaded
        msrunsloader.annotdir_to_seq_dict = {
            "path/to": [
                "Rob, polar-HILIC-25-min, QE, 1972-11-24",
                "Zoe, polar-HILIC-25-min, QE, 1985-4-18",
            ],
            "path/to/scan2": [
                "Rob, polar-HILIC-25-min, QE, 1972-11-24",
            ],
        }
        # Test the target method where the requirements were implemented
        seq = msrunsloader.get_msrun_sequence_from_dir(
            "sample.mzXML",
            "path/to/scan2",
            None,
        )
        # Ensure there was no error
        self.assertEqual(0, len(msrunsloader.aggregated_errors_object.exceptions))
        # Ensure the correct record was obtained
        self.assertEqual(defseq, seq)

    def test_init_mzxml_files_default(self):
        """This test ensures that mzXML files are found and loaded by default"""
        msruns_loader = MSRunsLoader(
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx"
        )
        self.assertEqual(
            "DataRepo/data/tests/same_name_mzxmls",
            os.path.relpath(msruns_loader.mzxml_dir, os.getcwd()),
        )
        self.assertEqual(
            [
                "DataRepo/data/tests/same_name_mzxmls/mzxmls/BAT-xz971.mzXML",
                "DataRepo/data/tests/same_name_mzxmls/mzxmls/pos/BAT-xz971.mzXML",
            ],
            [os.path.relpath(p, os.getcwd()) for p in msruns_loader.mzxml_files],
        )

    def test_init_skip_mzxml_files(self):
        """This test ensures that mzXML file loads can be skipped"""
        msruns_loader = MSRunsLoader(
            file="DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx",
            skip_mzxmls=True,
        )
        self.assertIsNone(msruns_loader.mzxml_dir)
        self.assertEqual([], msruns_loader.mzxml_files)

    def test_load_data_ambiguous_match(self):
        """This tests loading 4 mzxml files that have the same name.  In the details sheet, 2 are assigned a peak
        annotation file, each mapping to different samples, but also there are 2 extra mzXML files with the same name in
        different directories that initially lead to a misleading `PossibleDuplicateSamples` warning.  Another warning
        about assigning a sequence suggests adding the sequences to the details sheet.  The expected outcome is that an
        AmbiguousMzxmlSampleMatch error is buffered and no PossibleDuplicateSamples is buffered.

        Normally, this would happen due to unanalyzed mzXML files that mapped to different samples in the details sheet
        via *different* mzXML files that loaded fine.  E.g. There are 2 biological samples, each with 2 mzXML files.
        One of each pair was used in a peak annotation file (e.g. the neg scan one), but the other was not.  However
        this scenario will be simulated by manually populating the data structures to produce the expected result.

        Test Design:
            Create an MSRunsLoader object that supplies:
                A df that has 2 samples, sample headers, the mzXML paths, and annot files (that have no need to exist)
                mzxml_files that includes the 2 in the df, plus two extras, all with the same name
                A default sequence
            Call load_data, expecting an AggregatedErrors exception to be raised
            Test that load_data buffers AmbiguousMzxmlSampleMatch
        """

        MSRunSequence.objects.create(
            researcher="L.C. McMethod",
            date=datetime.strptime("2024-05-06", "%Y-%m-%d"),
            instrument="QE2",
            lc_method=LCMethod.objects.get(name__exact="polar-HILIC-25-min"),
        )
        MSRunSequence.objects.create(
            researcher="L.C. McMethod",
            date=datetime.strptime("2024-05-08", "%Y-%m-%d"),
            instrument="QE2",
            lc_method=LCMethod.objects.get(name__exact="polar-HILIC-25-min"),
        )
        Sample.objects.create(
            name="BAT_xz971",
            tissue=self.tsu,
            animal=self.anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        Sample.objects.create(
            name="BAT_xz971_SCFA",
            tissue=self.tsu,
            animal=self.anml,
            researcher="John Doe",
            date=datetime.now(),
        )

        df = pd.DataFrame.from_dict(
            {
                # The _SCFA append is based on the real world data where this issue came up.  The assumption is that the
                # user was trying to differentiate 2 different biological samples.
                "Sample Name": ["BAT_xz971", "BAT_xz971_SCFA"],
                # One from a pos annot file and one from neg, but different biological samples with the same mzXML name
                # The user manually edited the header
                "Sample Data Header": ["BAT-xz971", "BAT-xz971_SCFA"],
                "mzXML File Name": [
                    (
                        "DataRepo/data/tests/small_obob_mzxmls_ambiguous/small_obob_maven_6eaas_inf_lactate_neg_mzxmls/"
                        "BAT-xz971.mzXML"
                    ),
                    (
                        "DataRepo/data/tests/small_obob_mzxmls_ambiguous/small_obob_maven_6eaas_inf_lactate_pos_mzxmls/"
                        "BAT-xz971.mzXML"
                    ),
                ],
                "Peak Annotation File Name": ["annot_neg.xlsx", "annot_pos.xlsx"],
                "Sequence": [
                    "L.C. McMethod, polar-HILIC-25-min, QE2, 2024-05-06",
                    "L.C. McMethod, polar-HILIC-25-min, QE2, 2024-05-08",
                ],
            },
        )

        # Set up the loader object
        msrl = MSRunsLoader(
            df=df,
            mzxml_dir="DataRepo/data/tests/small_obob_mzxmls_ambiguous/",
            mzxml_files=[
                # A sample in the sheet
                (
                    "DataRepo/data/tests/small_obob_mzxmls_ambiguous/small_obob_maven_6eaas_inf_lactate_neg_mzxmls/"
                    "BAT-xz971.mzXML"
                ),
                (
                    "DataRepo/data/tests/small_obob_mzxmls_ambiguous/small_obob_maven_6eaas_inf_lactate_pos_mzxmls/"
                    "BAT-xz971.mzXML"
                ),
                # 2 different files with the same name, mapped to different samples in mzxml_to_sample_name
                (
                    "DataRepo/data/tests/small_obob_mzxmls_ambiguous/small_obob_maven_6eaas_inf_glucose_mzxmls/"
                    "BAT-xz971.mzXML"
                ),
                (
                    "DataRepo/data/tests/small_obob_mzxmls_ambiguous/small_obob_maven_6eaas_inf_lactate_mzxmls/"
                    "BAT-xz971.mzXML"
                ),
            ],
            operator="L.C. McMethod",
            date="2024-05-06",
            instrument="QE2",
            lc_protocol_name="polar-HILIC-25-min",
        )

        with self.assertRaises(AggregatedErrors):
            # This is the method we're testing
            msrl.load_data()

        # This coincidentally exists as an exception/warning
        self.assertTrue(
            msrl.aggregated_errors_object.exception_type_exists(
                MzxmlSampleHeaderMismatch
            )
        )
        self.assertFalse(
            msrl.aggregated_errors_object.exception_type_exists(
                PossibleDuplicateSamples
            )
        )

        # BUG: There is a bug in PR #1714 that prevents this test from passing.  It will be fixed in the next PR.
        # BUG: See the BUG comment here for details: DataRepo.loaders.msruns_loader.MSRunsLoader
        # BUG: .get_matching_mzxml_metadata
        # BUG: Uncomment these when the bug is fixed in the next PR
        # self.assertEqual(2, len(msrl.aggregated_errors_object.exceptions))
        # self.assertIsInstance(msrl.aggregated_errors_object.exceptions[0], AmbiguousMzxmlSampleMatch)

    def test_set_mzxml_metadata(self):
        # Set up the loader object
        msrl = MSRunsLoader()

        msrl.set_mzxml_metadata(
            None,  # mzxml_metadata: Optional[dict],
            self.bat_mzxml_file,
            self.BAT_xz971_mz_af,
            self.BAT_xz971_raw_af,
        )

        expected = {
            "BAT_xz971": {
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls": [
                    {
                        "added": False,
                        "mzxml_dir": "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls",
                        "mzxml_filepath": self.bat_mzxml_file,
                        "polarity": None,
                        "mzxml_filename": "BAT-xz971.mzXML",
                        "raw_file_sha1": None,
                        "mz_min": None,
                        "mzaf_record": self.BAT_xz971_mz_af,
                        "mz_max": None,
                        "raw_file_name": None,
                        "rawaf_record": self.BAT_xz971_raw_af,
                        "sample_name": None,
                    },
                ],
            },
        }

        self.assertEquivalent(expected, msrl.mzxml_dict_by_header)

    # BUG: This is fixed in PR: #1718.  This change (to get_or_create_msrun_sample_from_row) was made in PR #1714.  Once
    # BUG: I get to #1718, uncomment and/or adjust this test.
    # def test_mzxml_file_does_not_exist_caught(self):
    #     """Assert that mzXML files provided in the details that don't actually exist, are gracefully handled using
    #     FileFromInputNotFound.
    #
    #     Test Design:
    #         Supply a row to get_or_create_msrun_sample_from_row that contains an mzXML with a path to a file that does
    #             not exist
    #         Assert that FileFromInputNotFound is buffered
    #     """
    #
    #     # Set up the loader object
    #     msrl = MSRunsLoader()
    #
    #     row = pd.Series(
    #         {
    #             MSRunsLoader.DataHeaders.SEQNAME: self.seqname,
    #             MSRunsLoader.DataHeaders.SAMPLENAME: self.sample_with_no_msr.name,
    #             MSRunsLoader.DataHeaders.SAMPLEHEADER: f"{self.sample_with_no_msr.name}_pos",
    #             MSRunsLoader.DataHeaders.MZXMLNAME: "does_not_exist/does_not_exist.mzXML",
    #             MSRunsLoader.DataHeaders.ANNOTNAME: "accucor_file.xlsx",
    #         }
    #     )
    #
    #     with self.assertRaises(AggregatedErrors):
    #         msrl.get_or_create_msrun_sample_from_row(row)
    #
    #     self.assertEqual(1, len(msrl.aggregated_errors_object.exceptions))
    #     self.assertIsInstance(msrl.aggregated_errors_object.exceptions[0], FileFromInputNotFound)

    def test_get_matching_mzxml_metadata_matches_with_sample_and_path(self):
        """Assert that get_matching_mzxml_metadata works when provided with just the sample name and mzXML path (no
        sample data header)

        Test Design:
            Set self.mzxml_dict_by_header to include an entry
            Call get_matching_mzxml_metadata and only supply the sample name and mzXML path
            Assert that the correct metadata dict is returned
        """
        msrl = MSRunsLoader()
        msrl.mzxml_dict_by_header = deepcopy(self.MOCK_MZXML_DICT)
        expected = self.MOCK_MZXML_DICT["Br_xz971"][
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls"
        ][0]
        mzxml_metadata, mult_matches = msrl.get_matching_mzxml_metadata(
            "Sample Name",  # Sample name - does match
            None,  # Sample header
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/Br-xz971.mzXML",
        )
        self.assertDictEqual(expected, mzxml_metadata)
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))
        self.assertFalse(mult_matches)

    def test_get_matching_mzxml_metadata_no_match_with_sample_and_path_returns_placeholder(
        self,
    ):
        """Assert that get_matching_mzxml_metadata works when provided with just the sample name and mzXML path (no
        sample data header)

        Test Design:
            Set self.mzxml_dict_by_header to an empty dict
            Call get_matching_mzxml_metadata and only supply the sample name and mzXML path
            Assert that (placeholder_mzxml_metadata, True) is returned
            Assert that a ValueError was buffered
        """
        msrl = MSRunsLoader()
        msrl.mzxml_dict_by_header = {}
        mzxml_metadata, mult_matches = msrl.get_matching_mzxml_metadata(
            "Sample Name",  # Sample name - does match
            None,  # Sample header
            "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/Br-xz971.mzXML",
        )

        expected_placeholder = {
            "added": False,
            "mz_max": None,
            "mz_min": None,
            "mzaf_record": None,
            "mzxml_dir": None,
            "mzxml_filename": None,
            "polarity": None,
            "raw_file_name": None,
            "raw_file_sha1": None,
            "rawaf_record": None,
            "sample_name": None,
        }

        self.assertDictEqual(expected_placeholder, mzxml_metadata)
        self.assertEqual(0, len(msrl.aggregated_errors_object.exceptions))
        self.assertFalse(mult_matches)


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
        msrl = MSRunsLoader(df=df, skip_mzxmls=True)
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
                "Sample Name": ["s1", "s2", "s3"],
                "Sample Data Header": ["s1_pos", "s2_pos", None],
                "mzXML File Name": ["s1_pos.mzXML", "s2_pos.mzXML", "s3_pos.mzXML"],
                "Peak Annotation File Name": [
                    "accucor.xlsx",
                    "accucor2.xlsx",
                    "accucor3.xlsx",
                ],
                "Sequence": [
                    f"Dick, polar-HILIC-25-min, {inst}, 1991-5-7",
                    f"Dick, polar-HILIC-25-min, {inst}, 1991-5-7",
                    f"Dick, polar-HILIC-25-min, {inst}, 1991-5-7",
                ],
                "Skip": [None, None, "skip"],
            },
        )

        msruns_loader = MSRunsLoader(df=df)
        msruns_dict = msruns_loader.get_loaded_msrun_sample_dict("accucor.xlsx")

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

        self.assertDictEqual(expected, msruns_dict)

        # Skipped rows without a sample data header are not included in the returned dict.
        msruns_dict2 = msruns_loader.get_loaded_msrun_sample_dict("accucor3.xlsx")
        self.assertDictEqual({}, msruns_dict2)

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
