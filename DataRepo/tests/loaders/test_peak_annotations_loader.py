from copy import deepcopy
from datetime import datetime, timedelta

import pandas as pd
from django.db import ProgrammingError

from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.loaders.peak_annotations_loader import (
    AccucorLoader,
    IsoautocorrLoader,
    IsocorrLoader,
    PeakAnnotationsLoader,
)
from DataRepo.loaders.study_loader import StudyV3Loader
from DataRepo.models import (
    ArchiveFile,
    Compound,
    CompoundSynonym,
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
from DataRepo.models.animal import Animal
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ComplexPeakGroupDuplicate,
    ConditionallyRequiredArgs,
    DuplicateCompoundIsotopes,
    DuplicatePeakGroupResolutions,
    DuplicateValues,
    MissingC12ParentPeak,
    MissingCompounds,
    MissingSamples,
    MultiplePeakGroupRepresentation,
    NoPeakAnnotationDetails,
    NoSamples,
    ProhibitedCompoundName,
    RecordDoesNotExist,
    ReplacingPeakGroupRepresentation,
    RollbackException,
    TechnicalPeakGroupDuplicate,
    UnexpectedSamples,
    UnskippedBlanks,
)
from DataRepo.utils.file_utils import read_from_file
from DataRepo.utils.infusate_name_parser import (
    ObservedIsotopeData,
    parse_infusate_name,
)

PeakGroupCompound = PeakGroup.compounds.through


class DerivedPeakAnnotationsLoaderTestCase(TracebaseTestCase):
    """This just contains data to be used in the AccucorLoaderTests, IsocorrLoaderTests, and IsoautocorrLoaderTests."""

    COMMON_DICT = {
        "medMz": [104.035217, 105.038544, 74.024673, 75.028],
        "medRt": [12.73, 12.722, 12.621, 12.614],
        "isotopeLabel": ["C12 PARENT", "C13-label-1", "C12 PARENT", "C13-label-1"],
        "compound": ["Serine", "Serine", "Glycine", "Glycine"],
        "formula": ["C3H7NO3", "C3H7NO3", "C2H5NO2", "C2H5NO2"],
    }

    RAW_SAMPLES = {
        "blank_1_404020": [1, 2, 3, 4],
        "072920_XXX1_1_TS1": [5, 6, 7, 8],
        "072920_XXX1_2_bra": [9, 10, 11, 12],
    }

    CORRECTED_SAMPLES = {
        "blank_1_404020": [966.2099201, 0, 1230.735038, 0],
        "072920_XXX1_1_TS1": [124298.5248, 393.3480206, 90053.99839, 0],
        "072920_XXX1_2_bra": [2106922.129, 0, 329490.6364, 4910.491834],
    }

    # For accucor, used in 2 derived classes below
    ORIG_DICT = {
        **COMMON_DICT,
        **RAW_SAMPLES,
        "compoundId": ["Serine", "Serine", "Glycine", "Glycine"],
    }
    CORR_DICT = {
        "Compound": ["Serine", "Serine", "Glycine", "Glycine"],
        "C_Label": [0, 1, 0, 1],
        **CORRECTED_SAMPLES,
    }
    ACCUCOR_DF_DICT = {
        "Original": pd.DataFrame.from_dict(ORIG_DICT),
        "Corrected": pd.DataFrame.from_dict(CORR_DICT),
    }

    def get_converted_with_raw_df(self):
        df = pd.DataFrame.from_dict(
            {
                "MedMz": [
                    104.035217,
                    105.038544,
                    74.024673,
                    75.028,
                    104.035217,
                    105.038544,
                    74.024673,
                    75.028,
                    104.035217,
                    105.038544,
                    74.024673,
                    75.028,
                ],
                "MedRt": [
                    12.73,
                    12.722,
                    12.621,
                    12.614,
                    12.73,
                    12.722,
                    12.621,
                    12.614,
                    12.73,
                    12.722,
                    12.621,
                    12.614,
                ],
                "IsotopeLabel": [
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                    "C12 PARENT",
                    "C13-label-1",
                ],
                "Formula": [
                    "C3H7NO3",
                    "C3H7NO3",
                    "C2H5NO2",
                    "C2H5NO2",
                    "C3H7NO3",
                    "C3H7NO3",
                    "C2H5NO2",
                    "C2H5NO2",
                    "C3H7NO3",
                    "C3H7NO3",
                    "C2H5NO2",
                    "C2H5NO2",
                ],
                "Compound": [
                    "Serine",
                    "Serine",
                    "Glycine",
                    "Glycine",
                    "Serine",
                    "Serine",
                    "Glycine",
                    "Glycine",
                    "Serine",
                    "Serine",
                    "Glycine",
                    "Glycine",
                ],
                "Sample Header": [
                    "blank_1_404020",
                    "blank_1_404020",
                    "blank_1_404020",
                    "blank_1_404020",
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_2_bra",
                    "072920_XXX1_2_bra",
                    "072920_XXX1_2_bra",
                    "072920_XXX1_2_bra",
                ],
                "Raw Abundance": [
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                ],
                "Corrected Abundance": [
                    966.2099201,
                    0,
                    1230.735038,
                    0,
                    124298.5248,
                    393.3480206,
                    90053.99839,
                    0,
                    2106922.129,
                    0,
                    329490.6364,
                    4910.491834,
                ],
            },
        )
        df.sort_values(by=["Sample Header", "Compound", "IsotopeLabel"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def get_converted_without_raw_df(self):
        return (
            self.get_converted_with_raw_df()
            .copy(deep=True)
            .drop(["Raw Abundance"], axis=1, errors="ignore")
        )


class PeakAnnotationsLoaderTests(DerivedPeakAnnotationsLoaderTestCase):
    """Interestingly, this class doesn't explicitly use the PeakAnnotationsLoader class directly.  It's not even
    imported.  It is an abstract base class, so this tests it using its derived classes.
    """

    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    INSTRUMENT = MSRunSequence.INSTRUMENT_CHOICES[0][0]

    @classmethod
    def setUpTestData(cls):
        cls.SERINE = Compound.objects.create(
            name="Serine", formula="C3H7NO3", hmdb_id="HMDB0000187"
        )
        Compound.objects.create(
            name="Glycine", formula="C2H5NO2", hmdb_id="HMDB0000123"
        )
        Compound.objects.create(
            name="Glucose", formula="C6H12O6", hmdb_id="HMDB0000122"
        )
        ido = parse_infusate_name("glucose-[13C6]", [200])
        inf, _ = Infusate.objects.get_or_create_infusate(ido)
        inf.save()
        cls.anml = Animal.objects.create(
            name="test_animal",
            age=timedelta(weeks=int(13)),
            sex="M",
            genotype="WT",
            body_weight=200,
            diet="normal",
            feeding_status="fed",
            infusate=inf,
        )
        cls.tsu = Tissue.objects.create(name="Brain")
        # Create a sequence for the load to retrieve
        cls.lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")
        cls.seq = MSRunSequence.objects.create(
            researcher="Dick",
            date=datetime.strptime("1991-5-7", "%Y-%m-%d"),
            instrument=cls.INSTRUMENT,
            lc_method=cls.lcm,
        )
        # Create sample for the load to retrieve
        Sample.objects.create(
            name="s1",
            tissue=cls.tsu,
            animal=cls.anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        ts1 = Sample.objects.create(
            name="072920_XXX1_1_TS1",
            tissue=cls.tsu,
            animal=cls.anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        bra = Sample.objects.create(
            name="072920_XXX1_2_bra",
            tissue=cls.tsu,
            animal=cls.anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        cls.msrs_ts1 = MSRunSample.objects.create(
            msrun_sequence=cls.seq,
            sample=ts1,
            polarity=None,  # Placeholder
            ms_raw_file=None,  # Placeholder
            ms_data_file=None,  # Placeholder
        )
        MSRunSample.objects.create(
            msrun_sequence=cls.seq,
            sample=bra,
            polarity=None,  # Placeholder
            ms_raw_file=None,  # Placeholder
            ms_data_file=None,  # Placeholder
        )
        # Create a peak annotation details dataframe to supply to the loader
        cls.peak_annotation_details_df = pd.DataFrame.from_dict(
            {
                "Sample Name": ["s1"],
                "Sample Data Header": ["s1_pos"],
                "mzXML File Name": [None],
                "Peak Annotation File Name": ["accucor1.xlsx"],
                "Sequence": [f"Dick, polar-HILIC-25-min, {cls.INSTRUMENT}, 1991-5-7"],
            },
        )
        super().setUpTestData()

    def test_initialize_msrun_data_success(self):
        al = AccucorLoader(
            peak_annotation_details_df=self.peak_annotation_details_df,
            file="accucor1.xlsx",
        )
        al.msrun_sample_dict = {}
        al.initialize_msrun_data()

        # Assert that no defaults were created
        self.assertIsNone(al.operator_default)
        self.assertIsNone(al.date_default)
        self.assertIsNone(al.lc_protocol_name_default)
        self.assertIsNone(al.instrument_default)

        # Assert that the msrun_sample_dict was correctly initialized
        expected_msrun_sample_dict = {
            "s1_pos": {
                "seen": False,
                "MSRunSample": None,
                al.msrunsloader.headers.SAMPLENAME: "s1",
                al.msrunsloader.headers.SAMPLEHEADER: "s1_pos",
                al.msrunsloader.headers.MZXMLNAME: None,
                al.msrunsloader.headers.SEQNAME: f"Dick, polar-HILIC-25-min, {self.INSTRUMENT}, 1991-5-7",
                al.msrunsloader.headers.ANNOTNAME: "accucor1.xlsx",
                al.msrunsloader.headers.SKIP: False,
            }
        }
        self.assertDictEqual(expected_msrun_sample_dict, al.msrun_sample_dict)

    def test_initialize_msrun_data_annot_file_absent(self):
        """This test asserts that NoPeakAnnotationDetails errors get buffered when the peak_annotation_details_df
        argument is set and the file being loaded is not in that sheet."""
        acc_ldr = AccucorLoader(
            peak_annotation_details_df=self.peak_annotation_details_df,
            file="annot_file_not_present_in_details.xlsx",
        )
        acc_ldr.msrun_sample_dict = {}
        acc_ldr.initialize_msrun_data()
        # There are 2 rows, so there are 2 buffered instances
        self.assertEqual(2, len(acc_ldr.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            acc_ldr.aggregated_errors_object.exceptions[0], NoPeakAnnotationDetails
        )
        self.assertIsInstance(
            acc_ldr.aggregated_errors_object.exceptions[1], NoPeakAnnotationDetails
        )

    def test_load_data(self):
        peak_annotation_details_df = pd.DataFrame.from_dict(
            {
                "Sample Name": [
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_2_bra",
                    "blank_1_404020",
                ],
                "Sample Data Header": [
                    "072920_XXX1_1_TS1",
                    "072920_XXX1_2_bra",
                    "blank_1_404020",
                ],
                "mzXML File Name": [None, None, None],
                "Peak Annotation File Name": [
                    "accucor1.xlsx",
                    "accucor1.xlsx",
                    "accucor1.xlsx",
                ],
                "Sequence": [
                    f"Dick, polar-HILIC-25-min, {self.INSTRUMENT}, 1991-5-7",
                    f"Dick, polar-HILIC-25-min, {self.INSTRUMENT}, 1991-5-7",
                    f"Dick, polar-HILIC-25-min, {self.INSTRUMENT}, 1991-5-7",
                ],
                "Skip": [None, None, "Skip"],
            },
        )
        al = AccucorLoader(
            df=self.ACCUCOR_DF_DICT,
            peak_annotation_details_df=peak_annotation_details_df,
            file="DataRepo/data/tests/data_submission/accucor1.xlsx",  # This is not what's in self.ACCUCOR_DF_DICT
        )
        al.load_data()
        self.assertEqual(1, ArchiveFile.objects.count())  # accucor1.xlsx
        # There are 4 PeakGroups because 2 (non-blank) samples and each has 2 compounds
        self.assertEqual(4, PeakGroup.objects.count())
        # We only created 1 label (carbon) per PeakGroup
        self.assertEqual(4, PeakGroupLabel.objects.count())
        # and 1 compound link per PeakGroup
        self.assertEqual(4, PeakGroupCompound.objects.count())
        # and 2 peakdata rows in each peakgroup
        self.assertEqual(8, PeakData.objects.count())
        # and 1 label in each peakdata row
        self.assertEqual(8, PeakDataLabel.objects.count())

    def test_get_or_create_annot_file(self):
        al = AccucorLoader(file="DataRepo/data/tests/data_submission/accucor1.xlsx")
        al.get_or_create_annot_file()
        ArchiveFile.objects.get(filename="accucor1.xlsx")
        # No exception = successful test

    def test_get_peak_group_name_and_compounds_delimited(self):
        """Tests that not only are multiple compounds parsed for the peak group name, but that the result is
        alphabetically ordered (for consistent search results)."""
        cit = Compound.objects.create(
            name="citrate", formula="C6H8O7", hmdb_id="HMDB0000094"
        )
        iso = Compound.objects.create(
            name="isocitrate", formula="C6H8O7", hmdb_id="HMDB0000193"
        )
        row = pd.Series({AccucorLoader.DataHeaders.COMPOUND: "isocitrate/citrate"})
        al = AccucorLoader()
        recs = al.get_peak_group_compounds_dict(row=row)
        self.assertDictEqual({"citrate": cit, "isocitrate": iso}, recs)

    def test_get_peak_group_name_and_compounds_synonym(self):
        """Tests that peak groups are always named using the provided compound synonym (because it could be a
        stereoisomer, which could be significant and we have no way of knowing if it's just a true synonym or a name
        that connotes an actual structural difference)."""
        CompoundSynonym.objects.create(name="ser", compound=self.SERINE)
        row = pd.Series({AccucorLoader.DataHeaders.COMPOUND: "ser"})
        al = AccucorLoader()
        recs = al.get_peak_group_compounds_dict(row=row)
        self.assertDictEqual({"ser": self.SERINE}, recs)

    def test_prohibited_delimiters(self):
        """Tests that prohibited delimiters are automatically replaced with dashes and a warning is buffered.

        This test assumes that the compound record was previously created with the same replacement and that when
        reading a peak annotation file with the unedited name, it makes the same replacement to find the edited compound
        record created using the Compounds sheet in the study doc.  NOTE: If the user manually edits that sheet and does
        not also edit the peak annotation file, the compound will not be found.  Thus, the warning is checked for that
        explanation.
        """
        CompoundSynonym.objects.create(name="ser:2", compound=self.SERINE)
        row = pd.Series({AccucorLoader.DataHeaders.COMPOUND: "ser;2"})
        al = AccucorLoader()
        recs = al.get_peak_group_compounds_dict(row=row)
        self.assertDictEqual({"ser:2": self.SERINE}, recs)
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(ProhibitedCompoundName)
        )
        self.assertIn(
            "You may choose to manually edit",
            str(al.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "automatically fixed compound name",
            str(al.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "in the peak annotation file",
            str(al.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "be sure to also fix any occurrences",
            str(al.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "in the 'Compound' and/or 'Synonyms' columns",
            str(al.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "of the 'Compounds' sheet as well",
            str(al.aggregated_errors_object.exceptions[0]),
        )

    def test_get_or_create_peak_group_rec(self):
        row = pd.Series(
            {
                AccucorLoader.DataHeaders.FORMULA: "C3H7NO3",
                AccucorLoader.DataHeaders.SAMPLEHEADER: "072920_XXX1_1_TS1",
            }
        )
        rec_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        paf, _ = ArchiveFile.objects.get_or_create(**rec_dict)
        al = AccucorLoader()
        _, created1 = al.get_or_create_peak_group(row, paf, {"serine": self.SERINE})
        self.assertTrue(created1)
        self.assertEqual(1, PeakGroup.objects.count())
        _, created2 = al.get_or_create_peak_group(row, paf, {"serine": self.SERINE})
        self.assertFalse(created2)

    def test_get_or_create_peak_group_technical_peak_group_duplicate(self):
        """This test asserts that get_or_create_peak_group catches a TechnicalPeakGroupDuplicate error when the
        peak annotation file has the same name, but its content differs (i.e. the checksum differs).
        """
        al = AccucorLoader()
        row = pd.Series(
            {
                AccucorLoader.DataHeaders.FORMULA: "C3H7NO3",
                AccucorLoader.DataHeaders.SAMPLEHEADER: "072920_XXX1_1_TS1",
            }
        )
        # Create an existing peak group
        rec_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        paf, _ = ArchiveFile.objects.get_or_create(**rec_dict)
        al.get_or_create_peak_group(row, paf, {"serine": self.SERINE})
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

        # Now create a version from an "edited" peak annotation file (i.e. different checksum)
        del rec_dict["file_location"]
        rec_dict["filename"] = "accucor1.xlsx"
        rec_dict["checksum"] = "01234567893"
        paf, _ = ArchiveFile.objects.get_or_create(**rec_dict)
        with self.assertRaises(RollbackException):
            al.get_or_create_peak_group(row, paf, {"serine": self.SERINE})
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            al.aggregated_errors_object.exceptions[0], TechnicalPeakGroupDuplicate
        )
        self.assertTrue(al.aggregated_errors_object.exceptions[0].is_error)

    def test_get_or_create_peak_group_complex_peak_group_duplicate(self):
        """This test asserts that get_or_create_peak_group catches a ComplexPeakGroupDuplicate error when the peak
        annotation file has the same name, but its content differs (i.e. the checksum differs) and the peak group has
        changed (e.g. the formula differs)."""
        row = pd.Series(
            {
                AccucorLoader.DataHeaders.FORMULA: "C3H7NO3",
                AccucorLoader.DataHeaders.SAMPLEHEADER: "072920_XXX1_1_TS1",
            }
        )
        # Create an existing peak group
        rec_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        paf, _ = ArchiveFile.objects.get_or_create(**rec_dict)
        al = AccucorLoader()
        al.get_or_create_peak_group(row, paf, {"serine": self.SERINE})

        # Now create a version from an "edited" peak annotation file (i.e. different checksum) where the formula has
        # changed
        row = pd.Series(
            {
                AccucorLoader.DataHeaders.FORMULA: "C3H6NO3",
                AccucorLoader.DataHeaders.SAMPLEHEADER: "072920_XXX1_1_TS1",
            }
        )
        del rec_dict["file_location"]
        rec_dict["filename"] = "accucor1.xlsx"
        rec_dict["checksum"] = "01234567893"
        paf, _ = ArchiveFile.objects.get_or_create(**rec_dict)
        with self.assertRaises(RollbackException):
            al.get_or_create_peak_group(row, paf, {"serine": self.SERINE})
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            al.aggregated_errors_object.exceptions[0], ComplexPeakGroupDuplicate
        )
        self.assertTrue(al.aggregated_errors_object.exceptions[0].is_error)

    def test_get_or_create_peak_group_multiple_representation(self):
        """This test asserts that get_or_create_peak_group catches a TechnicalPeakGroupDuplicate error when the
        peak annotation file has the same name, but its content differs (i.e. the checksum differs).
        """
        row = pd.Series(
            {
                AccucorLoader.DataHeaders.FORMULA: "C3H7NO3",
                AccucorLoader.DataHeaders.SAMPLEHEADER: "072920_XXX1_1_TS1",
            }
        )
        # Create an existing peak group
        rec_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        paf, _ = ArchiveFile.objects.get_or_create(**rec_dict)
        al = AccucorLoader()
        al.get_or_create_peak_group(row, paf, {"serine": self.SERINE})

        # Now create a version from a different peak annotation file
        row = pd.Series(
            {
                AccucorLoader.DataHeaders.FORMULA: "C3H6NO3",
                AccucorLoader.DataHeaders.SAMPLEHEADER: "072920_XXX1_1_TS1",
            }
        )
        del rec_dict["file_location"]
        rec_dict["filename"] = "accucor2.xlsx"
        rec_dict["checksum"] = "01234567893"
        paf, _ = ArchiveFile.objects.get_or_create(**rec_dict)
        with self.assertRaises(RollbackException):
            al.get_or_create_peak_group(row, paf, {"serine": self.SERINE})
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            al.aggregated_errors_object.exceptions[0], MultiplePeakGroupRepresentation
        )
        self.assertTrue(al.aggregated_errors_object.exceptions[0].is_error)

    def test_get_msrun_sample_no_annot_details_df(self):
        al = AccucorLoader()

        # Sample does not exist
        msrs1 = al.get_msrun_sample("does not exist")
        self.assertIsNone(msrs1)
        self.assertTrue(
            al.aggregated_errors_object.exception_exists(
                RecordDoesNotExist, "model", Sample
            )
        )

        # MSRunSample does not exist
        ts = Sample.objects.create(
            name="test_sample",
            tissue=self.tsu,
            animal=self.anml,
            researcher="John Doe",
            date=datetime.now(),
        )
        al.missing_headers_as_samples = []
        al.msrun_sample_dict = {}
        msrs2 = al.get_msrun_sample("test_sample")
        self.assertIsNone(msrs2)
        self.assertTrue(
            al.aggregated_errors_object.exception_exists(
                RecordDoesNotExist, "model", MSRunSample
            )
        )

        # Sample is enough
        al.aggregated_errors_object = AggregatedErrors()
        tmsrs = MSRunSample.objects.create(
            msrun_sequence=self.seq,
            sample=ts,
            polarity=None,  # Placeholder
            ms_raw_file=None,  # Placeholder
            ms_data_file=None,  # Placeholder
        )
        al.missing_headers_as_samples = []
        al.msrun_sample_dict = {}
        msrs3 = al.get_msrun_sample("test_sample")
        self.assertEqual(tmsrs, msrs3)
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

        # Sequence defaults are needed
        tseq = MSRunSequence.objects.create(
            researcher="Richard",
            date=datetime.strptime("1991-5-7", "%Y-%m-%d"),
            instrument=self.INSTRUMENT,
            lc_method=self.lcm,
        )
        tmsrs2 = MSRunSample.objects.create(
            msrun_sequence=tseq,
            sample=ts,
            polarity=None,  # Placeholder
            ms_raw_file=None,  # Placeholder
            ms_data_file=None,  # Placeholder
        )
        al.missing_headers_as_samples = []
        al.msrun_sample_dict = {}
        msrs4 = al.get_msrun_sample("test_sample")
        self.assertIsNone(msrs4)
        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(ConditionallyRequiredArgs)
        )

        # LC, instrument, and date not enough
        al.lc_protocol_name_default = self.lcm.name
        al.instrument_default = self.INSTRUMENT
        al.date_default = datetime.strptime("1991-5-7", "%Y-%m-%d")
        al.missing_headers_as_samples = []
        al.msrun_sample_dict = {}
        msrs5 = al.get_msrun_sample("test_sample")
        self.assertIsNone(msrs5)
        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(ConditionallyRequiredArgs)
        )

        # Operator is enough
        al.aggregated_errors_object = AggregatedErrors()
        al.lc_protocol_name_default = None
        al.instrument_default = None
        al.date_default = None
        al.operator_default = "Richard"
        al.missing_headers_as_samples = []
        al.msrun_sample_dict = {}
        msrs6 = al.get_msrun_sample("test_sample")
        self.assertEqual(tmsrs2, msrs6)
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

    def test_get_msrun_sample_with_annot_details_df(self):
        """This simulated that the peak_annotation_details_df was supplied by setting obj.msrun_sample_dict"""
        al = AccucorLoader()

        # Test missing
        al.missing_headers_as_samples = ["072920_XXX1_1_TS1"]
        msrs1 = al.get_msrun_sample("072920_XXX1_1_TS1")
        self.assertIsNone(msrs1)

        # Test seen (using a bogus "MSRunSample" - but it doesn't matter for the purposes of this test)
        al.msrun_sample_dict["072920_XXX1_2_bra"] = {
            "seen": False,
            MSRunSample.__name__: "not none",
        }
        msrs2 = al.get_msrun_sample("072920_XXX1_2_bra")
        self.assertEqual("not none", msrs2)
        self.assertTrue(al.msrun_sample_dict["072920_XXX1_2_bra"]["seen"])

        # Test skip
        al.msrun_sample_dict["blank_1_404020"] = {
            "seen": False,
            MSRunSample.__name__: None,
            "Skip": True,
        }
        msrs3 = al.get_msrun_sample("blank_1_404020")
        self.assertIsNone(msrs3)
        self.assertTrue(al.msrun_sample_dict["blank_1_404020"]["seen"])

    def create_peak_group(self):
        rec_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        paf, _ = ArchiveFile.objects.get_or_create(**rec_dict)
        return PeakGroup.objects.create(
            name="serine",
            formula="C3H7NO3",
            msrun_sample=self.msrs_ts1,
            peak_annotation_file=paf,
        )

    def test_get_or_create_peak_data_rec(self):
        al = AccucorLoader()
        row = pd.Series(
            {
                AccucorLoader.DataHeaders.MEDMZ: 5,
                AccucorLoader.DataHeaders.MEDRT: 3,
                AccucorLoader.DataHeaders.RAW: 9,
                AccucorLoader.DataHeaders.CORRECTED: 5,
            }
        )
        pgrec = self.create_peak_group()
        label_obs = [ObservedIsotopeData(element="C", mass_number=13, count=2)]
        rec, created = al.get_or_create_peak_data(row, pgrec, label_obs)
        self.assertTrue(created)
        self.assertEqual(1, PeakData.objects.count())
        self.assertEqual(rec.med_mz, 5)
        self.assertEqual(rec.med_rt, 3)
        self.assertEqual(rec.raw_abundance, 9)
        self.assertEqual(rec.corrected_abundance, 5)
        self.assertEqual(rec.peak_group, pgrec)

    def create_peak_data(self, pgrec):
        return PeakData.objects.create(
            peak_group=pgrec,
            raw_abundance=2,
            corrected_abundance=3,
            med_mz=4,
            med_rt=5,
        )

    def test_get_label_observations(self):
        al = AccucorLoader()
        row = pd.Series({AccucorLoader.DataHeaders.ISOTOPELABEL: "C13-label-1"})
        pgrec = self.create_peak_group()

        # This will work, but it will buffer an error because the peak group has no linked compounds
        label_obs = al.get_label_observations(row, pgrec)
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            al.aggregated_errors_object.exceptions[0], ProgrammingError
        )
        self.assertEqual(
            [{"count": 1, "element": "C", "mass_number": 13, "parent": False}],
            label_obs,
        )

        # Now create a compound link
        pgrec.get_or_create_compound_link(self.SERINE)
        al = AccucorLoader()
        label_obs = al.get_label_observations(row, pgrec)
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))
        self.assertEqual(
            [{"count": 1, "element": "C", "mass_number": 13, "parent": False}],
            label_obs,
        )

    def test_get_or_create_peak_group_label(self):
        pgrec = self.create_peak_group()
        al = AccucorLoader()
        rec, created = al.get_or_create_peak_group_label(pgrec, "C")
        self.assertTrue(created)
        self.assertEqual(rec.peak_group, pgrec)
        self.assertEqual(rec.element, "C")

    def test_get_or_create_peak_data_label(self):
        pgrec = self.create_peak_group()
        pdrec = self.create_peak_data(pgrec)
        al = AccucorLoader()
        rec, created = al.get_or_create_peak_data_label(pdrec, "C", 2, 13)
        self.assertTrue(created)
        self.assertEqual(rec.peak_data, pdrec)
        self.assertEqual(rec.element, "C")
        self.assertEqual(rec.count, 2)
        self.assertEqual(rec.mass_number, 13)

    def test_get_or_create_peak_group_compound_link(self):
        pgrec = self.create_peak_group()
        al = AccucorLoader()
        self.assertEqual(0, pgrec.compounds.count())
        rec, created = al.get_or_create_peak_group_compound_link(pgrec, self.SERINE)
        self.assertEqual(1, pgrec.compounds.count())
        self.assertTrue(created)
        self.assertEqual(rec.peakgroup, pgrec)
        self.assertEqual(rec.compound, self.SERINE)

    def test_handle_file_exceptions(self):
        al = AccucorLoader()
        # Buffer a duplicate values exception to assert it is summarized as a DuplicateCompoundIsotopes exception
        al.aggregated_errors_object.buffer_error(
            DuplicateValues(
                {
                    "s1, Lysine, C13-label-2": [13],
                },
                [
                    al.SAMPLEHEADER_KEY,
                    al.COMPOUND_KEY,
                    al.ISOTOPELABEL_KEY,
                ],
            )
        )
        # Buffer a RecordDoesNotExist exception for the Compound model to assert that it is summarized in a
        # MissingCompounds exception
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Compound,
                Compound.get_name_query_expression("Lysine"),
                rownum=3,
                column="compound",
                file="accucor.xlsx",
            )
        )
        # Set up a RecordDoesNotExist exception for the Sample model to assert it is summarized as a NoSamples exception
        al.msrun_sample_dict["s1"] = {}
        al.msrun_sample_dict["s1"]["seen"] = True
        al.msrun_sample_dict["s1"]["MSRunSample"] = None
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s1"},
                rownum=3,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )

        al.handle_file_exceptions()

        self.assertEqual(3, len(al.aggregated_errors_object.exceptions))
        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(DuplicateCompoundIsotopes)
        )
        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(MissingCompounds)
        )
        self.assertTrue(al.aggregated_errors_object.exception_type_exists(NoSamples))

    def test_report_discrepant_headers_some(self):
        al = AccucorLoader()
        # s1 = "loaded"
        # s2 = missing
        # s3 = unexpected
        # blank - blank
        for sample_header in ["s1", "s2", "s3", "blank"]:
            al.msrun_sample_dict[sample_header] = {}
            # This sample was encountered during processing
            al.msrun_sample_dict[sample_header]["seen"] = True
            # No MSRunSample record was created for it
            al.msrun_sample_dict[sample_header]["MSRunSample"] = None
            # No sample headers are skipped
            al.msrun_sample_dict[sample_header]["Skip"] = None
        # Change this sample to have not been encountered (i.e. it was in the peak annotation details, but not in the
        # accucor file)
        al.msrun_sample_dict["s3"]["seen"] = False
        # Change s1 to having has an MSRunSample record created for it
        al.msrun_sample_dict["s1"][
            "MSRunSample"
        ] = "not None"  # simulate "found"/"loaded"

        # Buffer errors aboud having search for, but not found Sample records
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s2"},
                rownum=3,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "blank"},
                rownum=4,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )

        al.report_discrepant_headers()

        self.assertEqual(3, len(al.aggregated_errors_object.exceptions))
        self.assertFalse(
            al.aggregated_errors_object.exception_type_exists(RecordDoesNotExist)
        )

        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(MissingSamples)
        )
        self.assertEqual(
            ["s2"],
            al.aggregated_errors_object.get_exception_type(MissingSamples)[
                0
            ].search_terms,
        )

        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(UnskippedBlanks)
        )
        self.assertEqual(
            ["blank"],
            al.aggregated_errors_object.get_exception_type(UnskippedBlanks)[
                0
            ].search_terms,
        )

        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(UnexpectedSamples)
        )
        self.assertEqual(
            ["s3"],
            al.aggregated_errors_object.get_exception_type(UnexpectedSamples)[
                0
            ].missing_samples,
        )

    def test_report_discrepant_headers_all(self):
        al = AccucorLoader()
        al.msrun_sample_dict["s1"] = {}
        al.msrun_sample_dict["s1"]["seen"] = True
        al.msrun_sample_dict["s1"]["MSRunSample"] = None

        # Buffer errors aboud having search for, but not found Sample records
        al.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "s1"},
                rownum=3,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )

        al.report_discrepant_headers()

        self.assertTrue(al.aggregated_errors_object.exception_type_exists(NoSamples))
        self.assertEqual(
            ["s1"],
            al.aggregated_errors_object.get_exception_type(NoSamples)[0].search_terms,
        )

    def test_report_discrepant_headers_blank_with_missing_annot(self):
        """This test asserts that the suggestion in the UnskippedBlanks exception is specific to the peak annotation
        file being absent from the details sheet"""
        acc_ldr = AccucorLoader()
        acc_ldr.msrun_sample_dict["s1"] = {}
        acc_ldr.msrun_sample_dict["s1"]["seen"] = True
        acc_ldr.msrun_sample_dict["s1"]["MSRunSample"] = None
        acc_ldr.friendly_file = "accucor.xlsx"
        acc_ldr.msrunsloader = MSRunsLoader()
        acc_ldr.msrunsloader.friendly_file = "study.xlsx"

        # Buffer errors aboud having search for, but not found Sample records
        acc_ldr.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "blank5"},
                rownum=4,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )

        # Test the case where the peak annot file is missing from the details sheet
        acc_ldr.missing_annot_file_details["accucor.xlsx"] = True
        acc_ldr.report_discrepant_headers()
        self.assertTrue(
            acc_ldr.aggregated_errors_object.exception_type_exists(UnskippedBlanks)
        )
        self.assertIn(
            "must be populated with the sample headers",
            str(acc_ldr.aggregated_errors_object.exceptions[0]),
        )

    def test_report_discrepant_headers_blank_with_present_annot(self):
        """This test asserts that the suggestion in the UnskippedBlanks exception is specific to the peak annotation
        file being present in the details sheet"""
        acc_ldr = AccucorLoader()
        acc_ldr.msrun_sample_dict["s1"] = {}
        acc_ldr.msrun_sample_dict["s1"]["seen"] = True
        acc_ldr.msrun_sample_dict["s1"]["MSRunSample"] = None
        acc_ldr.friendly_file = "accucor.xlsx"
        acc_ldr.msrunsloader = MSRunsLoader()
        acc_ldr.msrunsloader.friendly_file = "study.xlsx"

        # Buffer errors aboud having search for, but not found Sample records
        acc_ldr.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Sample,
                {"name": "blank5"},
                rownum=4,
                column="Sample Header",
                file="accucor.xlsx",
            )
        )

        # Test the case where the peak annot file is missing from the details sheet
        acc_ldr.missing_annot_file_details = {}
        acc_ldr.report_discrepant_headers()
        self.assertTrue(
            acc_ldr.aggregated_errors_object.exception_type_exists(UnskippedBlanks)
        )
        self.assertIn(
            "add the missing sample(s)",
            str(acc_ldr.aggregated_errors_object.exceptions[0]),
        )

    def test_PeakAnnotationsLoader_conflicting_peak_group_resolutions(self):
        """This tests a case where the user had to have manipulated the Peak Group Conflicts data (in
        conflicting_resolutions.tsv) where they duplicated a row and selected multiple conflicting peak annotation files
        (/'resolutions' for the conflict) for 1 compound/samples-combo combo.

        The assures that the conflict is detected and raised.
        """
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

        il = IsoautocorrLoader(
            df=read_from_file(
                "DataRepo/data/tests/multiple_representations/resolution_handling/negative_cor.xlsx",
                sheet=None,
            ),
            file="DataRepo/data/tests/multiple_representations/resolution_handling/negative_cor.xlsx",
            peak_group_conflicts_file=(
                "DataRepo/data/tests/multiple_representations/"
                "resolution_handling/conflicting_resolutions.tsv"
            ),
            peak_group_conflicts_sheet="Peak Group Conflicts",
            peak_group_conflicts_df=read_from_file(
                "DataRepo/data/tests/multiple_representations/resolution_handling/conflicting_resolutions.tsv",
            ),
            peak_annotation_details_file=(
                "DataRepo/data/tests/multiple_representations/"
                "resolution_handling/prereqs.xlsx"
            ),
            peak_annotation_details_sheet="Peak Annotation Details",
            peak_annotation_details_df=dfdict["Peak Annotation Details"],
        )
        with self.assertRaises(AggregatedErrors):
            il.load_data()
        self.assertEqual(
            (1, 0),
            (
                il.aggregated_errors_object.num_errors,
                il.aggregated_errors_object.num_warnings,
            ),
            msg=", ".join(
                e.__name__ for e in il.aggregated_errors_object.get_exception_types()
            ),
        )
        dpgrs = il.aggregated_errors_object.get_exception_type(
            DuplicatePeakGroupResolutions
        )
        self.assertEqual(1, len(dpgrs))
        dpgr = dpgrs[0]
        self.assertTrue(dpgr.conflicting)
        self.assertEqual("3-methylglutaconic acid", dpgr.pgname)
        self.assertEqual(["negative_cor.xlsx", "poshigh_cor.xlsx"], dpgr.selected_files)
        expected = {
            "ArchiveFile": {
                "created": 1,
                "deleted": 0,
                "errored": 0,
                "existed": 0,
                "skipped": 0,
                "updated": 0,
                "warned": 0,
            },
            "PeakData": {
                "created": 0,
                "deleted": 0,
                "errored": 0,
                "existed": 0,
                "skipped": 4,  # Load is attempted on each PeakData line (bu the peakgroup failed)
                "updated": 0,
                "warned": 0,
            },
            "PeakDataLabel": {
                "created": 0,
                "deleted": 0,
                "errored": 0,
                "existed": 0,
                "skipped": 3,  # Load is attempted on each PeakData line (that has labels)
                "updated": 0,
                "warned": 0,
            },
            "PeakGroup": {
                "created": 0,
                "deleted": 0,
                "errored": 4,  # Load is attempted on each PeakData line
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
                "skipped": 3,  # Load is attempted on each PeakData line (that has labels)
                "updated": 0,
                "warned": 0,
            },
            "PeakGroup_compounds": {
                "created": 0,
                "deleted": 0,
                "errored": 0,
                "existed": 0,
                "skipped": 4,  # Load is attempted on each PeakData line
                "updated": 0,
                "warned": 0,
            },
        }
        self.assertDictEqual(expected, il.get_load_stats())

    def test_PeakAnnotationsLoader_delete_existing_unselected_peak_group(self):
        """This tests that a previously loaded peak group is not the selected peakgroup on a subsequent load, thus it is
        deleted.  It simulates that the user was presented with the detected conflict and chose the prefered data for
        that compound in the peak group conflicts sheet."""
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

        # Simulate 2 submissions with the second submission resolving a conflict with the previous load by deleting the
        # old peak group and replacing it with the one the user selected.
        il1 = IsoautocorrLoader(
            df=read_from_file(
                "DataRepo/data/tests/multiple_representations/resolution_handling/negative_cor.xlsx",
                sheet=None,
            ),
            file="DataRepo/data/tests/multiple_representations/resolution_handling/negative_cor.xlsx",
            # No peak_group_conflicts, so that we load a peak group that a later load deletes, due to separate
            # submissions
            peak_annotation_details_file=(
                "DataRepo/data/tests/multiple_representations/"
                "resolution_handling/prereqs.xlsx"
            ),
            peak_annotation_details_sheet="Peak Annotation Details",
            peak_annotation_details_df=dfdict["Peak Annotation Details"],
        )
        il1.load_data()

        # Now perform the load with the selected/new peak group
        il2 = IsoautocorrLoader(
            df=read_from_file(
                "DataRepo/data/tests/multiple_representations/resolution_handling/poshigh_cor.xlsx",
                sheet=None,
            ),
            file="DataRepo/data/tests/multiple_representations/resolution_handling/poshigh_cor.xlsx",
            peak_group_conflicts_file=(
                "DataRepo/data/tests/multiple_representations/resolution_handling/poshigh_resolution.tsv"
            ),
            peak_group_conflicts_sheet="Peak Group Conflicts",
            peak_group_conflicts_df=read_from_file(
                "DataRepo/data/tests/multiple_representations/resolution_handling/poshigh_resolution.tsv"
            ),
            peak_annotation_details_file=(
                "DataRepo/data/tests/multiple_representations/resolution_handling/prereqs.xlsx"
            ),
            peak_annotation_details_sheet="Peak Annotation Details",
            peak_annotation_details_df=dfdict["Peak Annotation Details"],
        )
        il2.load_data()
        self.assertEqual(
            (0, 1),
            (
                il2.aggregated_errors_object.num_errors,
                il2.aggregated_errors_object.num_warnings,
            ),
            msg=", ".join(
                e.__name__ for e in il2.aggregated_errors_object.get_exception_types()
            ),
        )
        rpgrs = il2.aggregated_errors_object.get_exception_type(
            ReplacingPeakGroupRepresentation
        )
        self.assertEqual(1, len(rpgrs))
        rpgr = rpgrs[0]
        self.assertEqual("3-Methylglutaconic acid", rpgr.delete_rec.name)
        self.assertEqual("poshigh_cor.xlsx", rpgr.selected_file)
        expected = {
            "ArchiveFile": {
                "created": 1,
                "existed": 0,
                "deleted": 0,
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
            "PeakData": {
                "created": 2,
                "existed": 0,
                "deleted": 4,  # The previously loaded unselected PeakGroup with 4 PeakData rows
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
            "PeakDataLabel": {
                "created": 2,
                "existed": 0,
                "deleted": 4,  # The previously loaded unselected PeakGroup with 4 PeakData rows
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
            "PeakGroup": {
                "created": 1,
                "existed": 1,  # The PeakGroup isn't created when the second PeakData row is processed
                "deleted": 1,  # The previously loaded unselected PeakGroup
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
            "PeakGroupLabel": {
                "created": 1,
                "existed": 1,
                "deleted": 1,  # The previously loaded unselected PeakGroup's 1 label
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
            "PeakGroup_compounds": {
                "created": 1,
                "existed": 1,  # The PeakGroup's compound isn't linked when the second PeakData row is processed
                "deleted": 1,  # The previously loaded unselected PeakGroup's 1 compound link
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
            },
        }
        self.assertDictEqual(expected, il2.get_load_stats())

    def test_determine_matching_formats_accucor_xlsx(self):
        self.assertEqual(
            [AccucorLoader.format_code],
            PeakAnnotationsLoader.determine_matching_formats(
                read_from_file(
                    "DataRepo/data/tests/accucor_with_multiple_labels/accucor.xlsx",
                    sheet=None,
                )
            ),
        )

    def test_determine_matching_formats_isocorr_xlsx(self):
        self.assertEqual(
            [IsocorrLoader.format_code],
            PeakAnnotationsLoader.determine_matching_formats(
                read_from_file(
                    "DataRepo/data/tests/multiple_tracers/bcaafasted_cor.xlsx",
                    sheet=None,
                )
            ),
        )

    def test_determine_matching_formats_isocorr_csv(self):
        self.assertEqual(
            [IsocorrLoader.format_code, IsoautocorrLoader.format_code],
            PeakAnnotationsLoader.determine_matching_formats(
                read_from_file(
                    "DataRepo/data/tests/singly_labeled_isocorr/small_cor.csv",
                    sheet=None,
                )
            ),
        )

    def test_determine_matching_formats_invalid_xlsx(self):
        self.assertEqual(
            [],
            PeakAnnotationsLoader.determine_matching_formats(
                read_from_file(
                    "DataRepo/data/tests/submission_v3/study.xlsx", sheet=None
                )
            ),
        )

    def test_get_supported_formats(self):
        self.assertEqual(
            ["isocorr", "accucor", "isoautocorr", "unicorr"],
            PeakAnnotationsLoader.get_supported_formats(),
        )

    def test_get_compound(self):
        """Tests that the compound_lookup buffer is used to return non-results for compounds previously searcher for and
        not found"""
        al = AccucorLoader()
        expected = (
            None,
            RecordDoesNotExist(Compound, {"name": "L-Lysine"}),
            Compound.get_name_query_expression("L-Lysine"),
        )
        al.compound_lookup["L-Lysine"] = expected
        rec = al.get_compound("L-Lysine")
        self.assertIsNone(rec)
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            al.aggregated_errors_object.exceptions[0], RecordDoesNotExist
        )
        self.assertEqual(al.aggregated_errors_object.exceptions[0].model, Compound)

    def test_is_selected_peak_group_no_selection(self):
        """Test no source peak annotation file selection exists in the Peak Group Conflicts sheet.
        create:
          pgname = Serine
          msrun_sample = cls.msrs_ts1
          peak_annot_file = create ArchiveFile for "accucor1.xlsx"
          al: AccucorLoader with:
            al.peak_group_selections = {}
              assert al.is_selected_peak_group returns True
              assert no stat is incremented
              assert no buffered exceptions
        """
        al = AccucorLoader()
        al.peak_group_selections = {}
        expected_counts = deepcopy(al.record_counts)
        pgname = "Serine"
        msrun_sample = self.msrs_ts1
        paf_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        peak_annot_file, _ = ArchiveFile.objects.get_or_create(**paf_dict)
        ispg = al.is_selected_peak_group(pgname, peak_annot_file, msrun_sample)
        self.assertTrue(ispg)
        self.assertDictEqual(
            expected_counts,
            al.record_counts,
            msg="No counts should have been incremented",
        )
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

    def test_is_selected_peak_group_none_selected(self):
        """Test source peak annotation file selection exists in the Peak Group Conflicts sheet, but no selection made.
        create:
          pgname = Serine
          msrun_sample = cls.msrs_ts1
          peak_annot_file = create ArchiveFile for "accucor1.xlsx"
          al: AccucorLoader with:
            al.peak_group_selections[msrun_sample.sample.name][pgname.lower()]["filename"] = None
              assert al.is_selected_peak_group returns False
              assert errored stat is incremented
              assert only 1 buffered exception
              assert that ProgrammingError is buffered
              assert first buffered exception is equal to "Expected DuplicatePeakGroupResolutions exception missing."
        """
        pgname = "Serine"
        msrun_sample = self.msrs_ts1
        paf_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        peak_annot_file, _ = ArchiveFile.objects.get_or_create(**paf_dict)
        al = AccucorLoader()
        al.peak_group_selections[msrun_sample.sample.name][pgname.lower()][
            "filename"
        ] = None
        expected_counts = deepcopy(al.record_counts)
        expected_counts["PeakGroup"]["errored"] = 1
        ispg = al.is_selected_peak_group(pgname, peak_annot_file, msrun_sample)
        self.assertFalse(ispg)
        self.assertDictEqual(
            expected_counts,
            al.record_counts,
            msg="PeakGroup[errored] should have been incremented",
        )
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            al.aggregated_errors_object.exceptions[0],
            ProgrammingError,
        )
        self.assertEqual(
            "Expected DuplicatePeakGroupResolutions exception missing.",
            str(al.aggregated_errors_object.exceptions[0]),
        )

    def test_is_selected_peak_group_no_conflict_and_none_selected(self):
        """Test source peak annotation file selection exists, no selection made, and conflict exists.
        create:
          pgname = Serine
          msrun_sample = cls.msrs_ts1
          peak_annot_file = create ArchiveFile for "accucor1.xlsx"
          al: AccucorLoader with:
            al.peak_group_selections[msrun_sample.sample.name][pgname.lower()]["filename"] = None
              assert al.is_selected_peak_group returns False
              assert errored stat is incremented
              assert only 1 buffered exception
        """
        pgname = "Serine"
        msrun_sample = self.msrs_ts1
        paf_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        peak_annot_file, _ = ArchiveFile.objects.get_or_create(**paf_dict)
        al = AccucorLoader()
        al.peak_group_selections[msrun_sample.sample.name][pgname.lower()][
            "filename"
        ] = None
        expected_counts = deepcopy(al.record_counts)
        expected_counts["PeakGroup"]["errored"] = 1
        ispg = al.is_selected_peak_group(pgname, peak_annot_file, msrun_sample)
        self.assertFalse(ispg)
        self.assertDictEqual(
            expected_counts,
            al.record_counts,
            msg="PeakGroup[errored] should have been incremented",
        )
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            al.aggregated_errors_object.exceptions[0],
            ProgrammingError,
        )
        self.assertEqual(
            "Expected DuplicatePeakGroupResolutions exception missing.",
            str(al.aggregated_errors_object.exceptions[0]),
        )

    def test_is_selected_peak_group_other_selected(self):
        """Test source peak annotation file selection differs.
        create:
          pgname = Serine
          msrun_sample = cls.msrs_ts1
          peak_annot_file = create ArchiveFile for "accucor1.xlsx"
          peak_annot_file.filename = "accucor1.xlsx"
          al: AccucorLoader with:
            al.peak_group_selections[msrun_sample.sample.name][pgname.lower()]["filename"] = "does_not_match.xlsx"
            al.aggregated_errors.buffer_error(DuplicatePeakGroupResolutions())
              assert al.is_selected_peak_group returns False
              assert skipped stat is 1
              assert no buffered exceptions added
        """
        pgname = "Serine"
        msrun_sample = self.msrs_ts1
        paf_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        peak_annot_file, _ = ArchiveFile.objects.get_or_create(**paf_dict)
        al = AccucorLoader()
        al.peak_group_selections[msrun_sample.sample.name][pgname.lower()][
            "filename"
        ] = "does_not_match.xlsx"
        al.aggregated_errors_object.buffer_error(
            DuplicatePeakGroupResolutions("whatever", ["whatever"])
        )
        expected_counts = deepcopy(al.record_counts)
        expected_counts["PeakGroup"]["skipped"] = 1
        ispg = al.is_selected_peak_group(pgname, peak_annot_file, msrun_sample)
        self.assertFalse(ispg)
        self.assertDictEqual(
            expected_counts,
            al.record_counts,
            msg="PeakGroup[skipped] should have been incremented",
        )
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))

    def test_is_selected_peak_group_conflict_and_other_selected(self):
        """Test source peak annotation file selection differs, but a conflict exists.
        create:
          pgname = Serine
          msrun_sample = cls.msrs_ts1
          peak_annot_file = create ArchiveFile for "accucor1.xlsx"
          peak_annot_file.filename = "accucor1.xlsx"
          Create conflicting peakgroup: bpg = PeakGroup.objects.create(
              msrun_sample=msrun_sample,
              name=pgname,
              peak_annotation_file=peak_annot_file,
          )
          al: AccucorLoader with:
            al.peak_group_selections[msrun_sample.sample.name][pgname.lower()]["filename"] = "does_not_match.xlsx"
              assert al.is_selected_peak_group returns False
              assert deleted stat is incremented
              assert ReplacingPeakGroupRepresentation is buffered
              assert bpg is deleted
        """
        pgname = "Serine"
        msrun_sample = self.msrs_ts1
        paf_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        peak_annot_file, _ = ArchiveFile.objects.get_or_create(**paf_dict)
        PeakGroup.objects.create(
            name=pgname,
            formula="C3H7NO3",
            msrun_sample=self.msrs_ts1,
            peak_annotation_file=peak_annot_file,
        )
        al = AccucorLoader()
        al.peak_group_selections[msrun_sample.sample.name][pgname.lower()][
            "filename"
        ] = "does_not_match.xlsx"
        al.peak_group_selections[msrun_sample.sample.name][pgname.lower()]["rownum"] = 5
        expected_counts = deepcopy(al.record_counts)
        expected_counts["PeakGroup"]["deleted"] = 1
        ispg = al.is_selected_peak_group(pgname, peak_annot_file, msrun_sample)
        self.assertFalse(ispg)
        self.assertDictEqual(
            expected_counts,
            al.record_counts,
            msg="PeakGroup[deleted] should have been incremented",
        )
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            al.aggregated_errors_object.exceptions[0], ReplacingPeakGroupRepresentation
        )
        self.assertIn(
            "Replacing PeakGroup Serine",
            str(al.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "(previously loaded from file 'accucor1.xlsx') with the version from file 'does_not_match.xlsx'",
            str(al.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "Peak Group Conflict resolution selected on row [5]",
            str(al.aggregated_errors_object.exceptions[0]),
        )
        self.assertEqual(
            0,
            PeakGroup.objects.filter(
                msrun_sample__sample__name=msrun_sample.sample.name,
                name__iexact=pgname,
                peak_annotation_file=peak_annot_file,
            ).count(),
        )

    def test_is_selected_peak_group_selected(self):
        """Test source peak annotation file selection matches.
        create:
          pgname = Serine
          msrun_sample = cls.msrs_ts1
          peak_annot_file = create ArchiveFile for "accucor1.xlsx"
          peak_annot_file.filename = "accucor1.xlsx"
          Create exist peakgroup: bpg = PeakGroup.objects.create(
              msrun_sample=msrun_sample,
              name=pgname,
              peak_annotation_file=peak_annot_file,
          )
          al: AccucorLoader with:
            al.peak_group_selections[msrun_sample.sample.name][pgname.lower()]["filename"] = "accucor1.xlsx"
              assert al.is_selected_peak_group returns True
              assert nothing is incremented
              assert no exceptions are buffered
              assert bpg was not deleted
        """
        pgname = "Serine"
        msrun_sample = self.msrs_ts1
        paf_dict = {
            "file_location": "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "data_type": "ms_peak_annotation",
            "data_format": "accucor",
        }
        peak_annot_file, _ = ArchiveFile.objects.get_or_create(**paf_dict)
        PeakGroup.objects.create(
            name=pgname,
            formula="C3H7NO3",
            msrun_sample=self.msrs_ts1,
            peak_annotation_file=peak_annot_file,
        )
        al = AccucorLoader()
        al.peak_group_selections[msrun_sample.sample.name][pgname.lower()][
            "filename"
        ] = peak_annot_file.filename
        al.peak_group_selections[msrun_sample.sample.name][pgname.lower()]["rownum"] = 5
        expected_counts = deepcopy(al.record_counts)
        ispg = al.is_selected_peak_group(pgname, peak_annot_file, msrun_sample)
        self.assertTrue(ispg)
        self.assertDictEqual(
            expected_counts,
            al.record_counts,
            msg="PeakGroup[deleted] should have been incremented",
        )
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))
        self.assertEqual(
            1,
            PeakGroup.objects.filter(
                msrun_sample__sample__name=msrun_sample.sample.name,
                name__iexact=pgname,
                peak_annotation_file=peak_annot_file,
            ).count(),
        )

    def test_fix_elmaven_compound(self):
        self.assertIsNone(PeakAnnotationsLoader.fix_elmaven_compound(None))
        self.assertEqual(
            "mycompound", PeakAnnotationsLoader.fix_elmaven_compound("mycompound")
        )
        self.assertEqual(
            "mycompound", PeakAnnotationsLoader.fix_elmaven_compound("mycompound (1)")
        )
        self.assertEqual(
            "mycompound", PeakAnnotationsLoader.fix_elmaven_compound("mycompound (2)")
        )
        self.assertEqual(
            "citrate/isocitrate",
            PeakAnnotationsLoader.fix_elmaven_compound("citrate/isocitrate (1)"),
        )
        self.assertEqual(
            "mycompound",
            PeakAnnotationsLoader.fix_elmaven_compound(
                "mycompound (1) ", pattern=r" \(1\) $"
            ),
        )


class IsocorrLoaderTests(DerivedPeakAnnotationsLoaderTestCase):
    ABSO_DICT = {
        **DerivedPeakAnnotationsLoaderTestCase.COMMON_DICT,
        **DerivedPeakAnnotationsLoaderTestCase.CORRECTED_SAMPLES,
    }
    DF_DICT = {
        "absolte": pd.DataFrame.from_dict(ABSO_DICT),
    }

    def test_IsocorrLoader(self):
        il = IsocorrLoader(df=self.DF_DICT, file="isocorr.xlsx")
        pd.testing.assert_frame_equal(
            self.get_converted_without_raw_df(), il.df, check_like=True
        )

    def test_isocorr_elmaven_fix(self):
        il = IsocorrLoader()
        row1 = pd.Series({IsocorrLoader.DataHeaders.COMPOUND: "Serine (1)"})
        self.assertDictEqual(
            {"Serine": None}, il.get_peak_group_compounds_dict(row=row1)
        )
        self.assertDictEqual(
            {"Serine": None}, il.get_peak_group_compounds_dict(names_str="Serine (1)")
        )
        row2 = pd.Series({IsocorrLoader.DataHeaders.COMPOUND: "Serine"})
        self.assertDictEqual(
            {"Serine": None}, il.get_peak_group_compounds_dict(row=row2)
        )
        self.assertDictEqual(
            {"Serine": None}, il.get_peak_group_compounds_dict(names_str="Serine")
        )
        row3 = pd.Series({IsocorrLoader.DataHeaders.COMPOUND: "citrate/isocitrate"})
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            il.get_peak_group_compounds_dict(row=row3),
        )
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            il.get_peak_group_compounds_dict(names_str="citrate/isocitrate"),
        )
        row4 = pd.Series({IsocorrLoader.DataHeaders.COMPOUND: "citrate/isocitrate (2)"})
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            il.get_peak_group_compounds_dict(row=row4),
        )
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            il.get_peak_group_compounds_dict(names_str="citrate/isocitrate (2)"),
        )


class AccucorLoaderTests(DerivedPeakAnnotationsLoaderTestCase):
    def test_AccucorLoader(self):
        al = AccucorLoader(df=self.ACCUCOR_DF_DICT, file="accucor.xlsx")
        pd.testing.assert_frame_equal(
            self.get_converted_with_raw_df(),
            al.df,
            check_like=True,
        )

    def test_get_accucor_label_column_name(self):
        corrected_dict = deepcopy(self.CORR_DICT)
        corrected_dict["N_Label"] = corrected_dict.pop("C_Label")
        corrected_df = pd.DataFrame.from_dict(corrected_dict)
        self.assertEqual(
            "N_Label", AccucorLoader.get_accucor_label_column_name(corrected_df)
        )

    def test_get_accucor_isotope_string(self):
        corrected_dict = deepcopy(self.CORR_DICT)
        corrected_dict["D_Label"] = corrected_dict.pop("C_Label")
        corrected_df = pd.DataFrame.from_dict(corrected_dict)
        self.assertEqual("H2", AccucorLoader.get_accucor_isotope_string(corrected_df))
        corrected_dict["C_Label"] = corrected_dict.pop("D_Label")
        corrected_df = pd.DataFrame.from_dict(corrected_dict)
        self.assertEqual("C13", AccucorLoader.get_accucor_isotope_string(corrected_df))
        corrected_dict["N_Label"] = corrected_dict.pop("C_Label")
        corrected_df = pd.DataFrame.from_dict(corrected_dict)
        self.assertEqual("N15", AccucorLoader.get_accucor_isotope_string(corrected_df))
        corrected_dict["H_Label"] = corrected_dict.pop("N_Label")
        corrected_df = pd.DataFrame.from_dict(corrected_dict)
        self.assertEqual("H2", AccucorLoader.get_accucor_isotope_string(corrected_df))

    def test_check_c12_parents_called(self):
        """Checks that check_c12_parents (called from the constructor, buffers a MissingC12ParentPeak for each sample
        where the C12 parent row is missing.  Here is what happens...

        You start out with this:

               Compound IsotopeLabel  Formula      Sample Header
            0   Glycine   C12 PARENT  C2H5NO2  072920_XXX1_1_TS1  # <-- formula correct bec. of sorting and backfill
            1   Glycine  C13-label-1  C2H5NO2  072920_XXX1_1_TS1
            2    Serine   C12 PARENT  C3H7NO3  072920_XXX1_1_TS1
            3    Serine  C13-label-1  C3H7NO3  072920_XXX1_1_TS1
            4   Glycine   C12 PARENT  C3H7NO3  072920_XXX1_2_bra  # <-- formula WRONG bec. of fill and missing PARENT
            5   Glycine  C13-label-1  C2H5NO2  072920_XXX1_2_bra
            6    Serine   C12 PARENT  C3H7NO3  072920_XXX1_2_bra
            7    Serine  C13-label-1  C3H7NO3  072920_XXX1_2_bra
            8   Glycine   C12 PARENT  C3H7NO3     blank_1_404020  # <-- formula WRONG bec. of fill and missing PARENT
            9   Glycine  C13-label-1  C2H5NO2     blank_1_404020
            10   Serine   C12 PARENT  C3H7NO3     blank_1_404020
            11   Serine  C13-label-1  C3H7NO3     blank_1_404020

        check_c12_parents converts the above into:

              Compound  Formula                 INDEX  FIRSTINDEX
            0  Glycine  C2H5NO2          [0, 1, 5, 9]           0
            1  Glycine  C3H7NO3                [4, 8]           4  # <-- formula WRONG bec. of fill and missing PARENT
            2   Serine  C3H7NO3  [2, 3, 6, 7, 10, 11]           2

        It identifies the formula change in the same compound and assumes it's due to the fill-down.
        """
        orig_dict = {
            "medMz": [104.035217, 105.038544, 75.028],
            "medRt": [12.73, 12.722, 12.614],
            "isotopeLabel": ["C12 PARENT", "C13-label-1", "C13-label-1"],
            "compound": ["Serine", "Serine", "Glycine"],
            "compoundId": ["Serine", "Serine", "Glycine"],
            "formula": ["C3H7NO3", "C3H7NO3", "C2H5NO2"],
            "blank_1_404020": [1, 2, 4],
            "072920_XXX1_1_TS1": [5, 6, 8],
            "072920_XXX1_2_bra": [9, 10, 12],
        }
        corr_dict = {
            "Compound": ["Serine", "Serine", "Glycine", "Glycine"],
            "C_Label": [0, 1, 0, 1],
            "blank_1_404020": [966.2099201, 0, 0, 0],
            "072920_XXX1_1_TS1": [124298.5248, 393.3480206, 0, 0],
            "072920_XXX1_2_bra": [2106922.129, 0, 0, 4910.491834],
        }
        accucor_df_dict = {
            "Original": pd.DataFrame.from_dict(orig_dict),
            "Corrected": pd.DataFrame.from_dict(corr_dict),
        }
        al = AccucorLoader(df=accucor_df_dict, file="accucor.xlsx")

        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertTrue(
            al.aggregated_errors_object.exception_type_exists(MissingC12ParentPeak)
        )

    def test_check_c12_parents_works(self):
        al = AccucorLoader()
        df = {
            "Original": pd.DataFrame.from_dict(
                {
                    "isotopeLabel": [
                        "C12 PARENT",
                        "C13-label-1",
                        "C13-label-2",
                        "C13-label-3",  # PROBLEM: Compound changed & there was no C12 PARENT row before this
                    ],
                    "compoundId": [
                        "2-keto-isovalerate",
                        "2-keto-isovalerate",
                        "2-keto-isovalerate",
                        "3-hydroxyisobutyrate",  # NOTE: Compound changed
                    ],
                    "formula": [
                        "C5H8O3",
                        "C5H8O3",
                        "C5H8O3",
                        "C4H8O3",  # RESULT: The filldown will fill the generated C12 PARENT and labels with C5H8O3
                    ],
                },
            ),
        }
        al.check_c12_parents(df)
        self.assertEqual(1, len(al.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            al.aggregated_errors_object.exceptions[0], MissingC12ParentPeak
        )

    def test_accucor_elmaven_fix(self):
        al = AccucorLoader()
        row1 = pd.Series({AccucorLoader.DataHeaders.COMPOUND: "Serine (1)"})
        self.assertDictEqual(
            {"Serine": None}, al.get_peak_group_compounds_dict(row=row1)
        )
        self.assertDictEqual(
            {"Serine": None}, al.get_peak_group_compounds_dict(names_str="Serine (1)")
        )
        row2 = pd.Series({AccucorLoader.DataHeaders.COMPOUND: "Serine"})
        self.assertDictEqual(
            {"Serine": None}, al.get_peak_group_compounds_dict(row=row2)
        )
        self.assertDictEqual(
            {"Serine": None}, al.get_peak_group_compounds_dict(names_str="Serine")
        )
        row3 = pd.Series({AccucorLoader.DataHeaders.COMPOUND: "citrate/isocitrate"})
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            al.get_peak_group_compounds_dict(row=row3),
        )
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            al.get_peak_group_compounds_dict(names_str="citrate/isocitrate"),
        )
        row4 = pd.Series({AccucorLoader.DataHeaders.COMPOUND: "citrate/isocitrate (2)"})
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            al.get_peak_group_compounds_dict(row=row4),
        )
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            al.get_peak_group_compounds_dict(names_str="citrate/isocitrate (2)"),
        )


class IsoautocorrLoaderTests(DerivedPeakAnnotationsLoaderTestCase):
    ORIG_DICT = {
        **DerivedPeakAnnotationsLoaderTestCase.COMMON_DICT,
        **DerivedPeakAnnotationsLoaderTestCase.RAW_SAMPLES,
    }
    CORR_DICT = {
        **DerivedPeakAnnotationsLoaderTestCase.COMMON_DICT,
        **DerivedPeakAnnotationsLoaderTestCase.CORRECTED_SAMPLES,
    }
    DF_DICT = {
        "original": pd.DataFrame.from_dict(ORIG_DICT),
        "cor_abs": pd.DataFrame.from_dict(CORR_DICT),
    }

    def test_IsoautocorrLoader(self):
        il = IsoautocorrLoader(df=self.DF_DICT, file="isoautocorr.xlsx")
        pd.testing.assert_frame_equal(
            self.get_converted_with_raw_df(), il.df, check_like=True
        )

    def test_accucor_elmaven_fix(self):
        ial = IsoautocorrLoader()
        row1 = pd.Series({IsoautocorrLoader.DataHeaders.COMPOUND: "Serine (1)"})
        self.assertDictEqual(
            {"Serine": None}, ial.get_peak_group_compounds_dict(row=row1)
        )
        self.assertDictEqual(
            {"Serine": None}, ial.get_peak_group_compounds_dict(names_str="Serine (1)")
        )
        row2 = pd.Series({IsoautocorrLoader.DataHeaders.COMPOUND: "Serine"})
        self.assertDictEqual(
            {"Serine": None}, ial.get_peak_group_compounds_dict(row=row2)
        )
        self.assertDictEqual(
            {"Serine": None}, ial.get_peak_group_compounds_dict(names_str="Serine")
        )
        row3 = pd.Series({IsoautocorrLoader.DataHeaders.COMPOUND: "citrate/isocitrate"})
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            ial.get_peak_group_compounds_dict(row=row3),
        )
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            ial.get_peak_group_compounds_dict(names_str="citrate/isocitrate"),
        )
        row4 = pd.Series(
            {IsoautocorrLoader.DataHeaders.COMPOUND: "citrate/isocitrate (2)"}
        )
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            ial.get_peak_group_compounds_dict(row=row4),
        )
        self.assertDictEqual(
            {"citrate": None, "isocitrate": None},
            ial.get_peak_group_compounds_dict(names_str="citrate/isocitrate (2)"),
        )
