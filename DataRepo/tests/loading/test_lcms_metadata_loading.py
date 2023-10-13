import re
from datetime import datetime

import pandas as pd
from django.core.management import call_command

from DataRepo.models import LCMethod, MSRun, Protocol, Sample
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import (
    AccuCorDataLoader,
    AggregatedErrors,
    AggregatedErrorsSet,
    LCMethodFixturesMissing,
    LCMSDBSampleMissing,
    LCMSDefaultsRequired,
    MismatchedSampleHeaderMZXML,
    MissingLCMSSampleDataHeaders,
    MissingMZXMLFiles,
    NoMZXMLFiles,
    PeakAnnotFileMismatches,
    SampleTableLoader,
    UnexpectedLCMSSampleDataHeaders,
)
from DataRepo.utils.lcms_metadata_parser import (
    DuplicateSampleDataHeaders,
    InvalidLCMSHeaders,
    MissingPeakAnnotationFiles,
    MissingRequiredLCMSValues,
    check_peak_annotation_files,
    extract_dataframes_from_lcms_tsv,
    extract_dataframes_from_lcms_xlsx,
    get_lcms_metadata_dict_from_file,
    lcms_df_to_dict,
    lcms_headers_are_valid,
    lcms_metadata_to_samples,
)

# Tests related to issue 706
# https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/706


class LCMSMetadataAccucorMethodTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "lc_methods")

    def test_sample_header_to_default_mzxml(self):
        adl1 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date="",
            researcher="",
            ms_protocol_name="",
            lc_protocol_name="",
            instrument="",
            mzxml_files=["sample1.mzxml", "sample2.mzxml"],
        )
        mzxml1 = adl1.sample_header_to_default_mzxml("does-not-exist")
        self.assertEqual(
            "does-not-exist.mzxml",
            mzxml1,
            msg="The default mzxml file should be based on the sample_header supplied if mzxml_files are provided",
        )
        self.assertEqual(0, len(adl1.aggregated_errors_object.exceptions))

        mzxml2 = adl1.sample_header_to_default_mzxml("sample2")
        self.assertEqual(
            "sample2.mzxml",
            mzxml2,
            msg="The mzxml file is returned when its name (minus suffix) is supplied",
        )

        adl2 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date="",
            researcher="",
            ms_protocol_name="",
            lc_protocol_name="",
            instrument="",
            mzxml_files=None,
        )
        mzxml = adl2.sample_header_to_default_mzxml("sample")
        self.assertIsNone(mzxml, msg="If mzxml files array is None, None is returned")
        self.assertEqual(0, len(adl2.aggregated_errors_object.exceptions))

        adl3 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date="",
            researcher="",
            ms_protocol_name="",
            lc_protocol_name="",
            instrument="",
            mzxml_files=[],
        )
        mzxml = adl3.sample_header_to_default_mzxml("sample")
        self.assertIsNone(mzxml, msg="If mzxml files array is empty, None is returned")
        self.assertEqual(0, len(adl3.aggregated_errors_object.exceptions))

    def test_sample_header_to_default_mzxml_none(self):
        adl1 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date="",
            researcher="",
            ms_protocol_name="",
            lc_protocol_name="",
            instrument="",
        )
        mzxml = adl1.sample_header_to_default_mzxml("does-not-exist")
        self.assertIsNone(
            mzxml,
            msg="The default mzxml file should be None when no mzxml_files are provided",
        )
        self.assertEqual(0, len(adl1.aggregated_errors_object.exceptions))

    def test_check_mzxml(self):
        adl1 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date="1972-11-24",
            researcher="",
            ms_protocol_name="",
            lc_protocol_name="",
            instrument="",
            mzxml_files=["sample1.mzxml", "sample2.mzxml"],
        )
        adl1.check_mzxml("sample1", "sample1.mzxml")
        self.assertEqual([], adl1.missing_mzxmls)
        self.assertEqual([], adl1.mismatching_mzxmls)

        adl1.check_mzxml("sample2", "sample1.mzxml")
        pat = re.compile(r"^sample2\.").pattern
        self.assertEqual([], adl1.missing_mzxmls)
        self.assertEqual([["sample2", "sample1.mzxml", pat]], adl1.mismatching_mzxmls)

        adl1.check_mzxml("sample3", "sample3.mzxml")
        self.assertEqual(["sample3.mzxml"], adl1.missing_mzxmls)
        self.assertEqual([["sample2", "sample1.mzxml", pat]], adl1.mismatching_mzxmls)

    def test_validate_mzxmls(self):
        adl1 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="accucor.xlsx",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date="1972-11-24",
            researcher="Robert Leach",
            ms_protocol_name="Default",
            lc_protocol_name="polar-HILIC",
            instrument="default instrument",
            mzxml_files=["sample1.mzxml", "sample2.mzxml"],
            validate=False,
        )
        adl1.missing_mzxmls.append("sample3.mzxml")
        adl1.mismatching_mzxmls.append(
            ["sample", "sample2.mzxml", re.compile(r"^sample\.").pattern]
        )

        adl1.validate_mzxmls()

        self.assertEqual(
            2,
            len(adl1.aggregated_errors_object.exceptions),
            msg="There should be 2 exceptions",
        )

        self.assertEqual(
            MissingMZXMLFiles,
            type(adl1.aggregated_errors_object.exceptions[0]),
            msg="The first exception should be MissingMZXMLFiles.",
        )
        self.assertTrue(
            adl1.aggregated_errors_object.exceptions[0].is_error,
            msg="The MissingMZXMLFiles exception should be an error in loading mode when any mzxml files are provided.",
        )
        self.assertTrue(
            adl1.aggregated_errors_object.exceptions[0].is_fatal,
            msg="The MissingMZXMLFiles exception should be fatal in loading mode when any mzxml files are provided.",
        )
        self.assertEqual(
            list,
            type(adl1.missing_mzxmls),
            msg="The MissingMZXMLFiles exception should contain a list.",
        )
        self.assertEqual(
            adl1.missing_mzxmls,
            adl1.aggregated_errors_object.exceptions[0].mzxml_files,
            msg="The MissingMZXMLFiles exception's list should match the AccucorDataLoader object's missing list.",
        )

        self.assertEqual(
            MismatchedSampleHeaderMZXML,
            type(adl1.aggregated_errors_object.exceptions[1]),
            msg="The second exception should be MismatchedSampleHeaderMZXML.",
        )
        self.assertFalse(
            adl1.aggregated_errors_object.exceptions[1].is_error,
            msg="The second exception should be a warning in loading mode.",
        )
        self.assertFalse(
            adl1.aggregated_errors_object.exceptions[1].is_fatal,
            msg="The second exception should not be fatal in loading mode.",
        )
        self.assertEqual(
            list,
            type(adl1.mismatching_mzxmls),
            msg="The MismatchedSampleHeaderMZXML exception should contain a list.",
        )
        self.assertEqual(
            adl1.mismatching_mzxmls,
            adl1.aggregated_errors_object.exceptions[1].mismatching_mzxmls,
            msg="The MismatchedSampleHeaderMZXML exception's list should match the AccucorDataLoader object's "
            "mismatched list.",
        )

        adl2 = AccuCorDataLoader(
            None,
            None,
            date="1972-11-24",
            researcher="Robert Leach",
            ms_protocol_name="Default",
            lc_protocol_name="polar-HILIC",
            instrument="default instrument",
            peak_group_set_filename="accucor.xlsx",
            mzxml_files=["sample1.mzxml", "sample2.mzxml"],
            validate=True,
        )
        adl2.missing_mzxmls.append("sample3.mzxml")
        adl2.mismatching_mzxmls.append(
            ["sample", "sample2.mzxml", re.compile(r"^sample\.").pattern]
        )

        adl2.validate_mzxmls()

        self.assertEqual(2, len(adl2.aggregated_errors_object.exceptions))

        self.assertTrue(
            adl2.aggregated_errors_object.exceptions[0].is_error,
            msg="The first exception should be an error in validate made.",
        )
        self.assertTrue(
            adl2.aggregated_errors_object.exceptions[0].is_fatal,
            msg="The first exception should be fatal in validate mode.",
        )

        self.assertFalse(
            adl2.aggregated_errors_object.exceptions[1].is_error,
            msg="The second exception should be a warning in validate mode.",
        )
        self.assertTrue(
            adl2.aggregated_errors_object.exceptions[1].is_fatal,
            msg="The second exception should be fatal in validate mode.",
        )

    def test_validate_mzxmls_load_makes_mzxml_optl(self):
        adl = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="accucor.xlsx",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date="1972-11-24",
            researcher="Robert Leach",
            ms_protocol_name="Default",
            lc_protocol_name="polar-HILIC",
            instrument="default instrument",
            validate=False,
        )

        adl.validate_mzxmls()

        self.assertEqual(
            0,
            len(adl.aggregated_errors_object.exceptions),
            msg="mzxml files are optional in loading mode",
        )

    def test_get_missing_required_lcms_defaults(self):
        adl1 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="accucor.xlsx",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date="1972-11-24",
            researcher="Robert Leach",
            ms_protocol_name="Default",
            lc_protocol_name="polar-HILIC",
            instrument="default instrument",
            mzxml_files=[],
        )
        missing1 = adl1.get_missing_required_lcms_defaults()
        self.assertEqual(0, len(missing1), msg="No required defaults should be missing")
        self.assertEqual(0, len(adl1.aggregated_errors_object.exceptions))

        adl2 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename=None,
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date=None,
            researcher=None,
            ms_protocol_name=None,
            lc_protocol_name=None,
            instrument=None,
            mzxml_files=None,
        )
        missing2 = adl2.get_missing_required_lcms_defaults()
        expected_missing = [
            "lc_protocol_name",
            "ms_protocol_name",
            "date",
            "researcher",
            "instrument",
            "peak_annot_file",
        ]
        self.assertEqual(
            sorted(expected_missing),
            sorted(missing2),
            msg="All required defaults should be missing",
        )
        self.assertEqual(0, len(adl2.aggregated_errors_object.exceptions))

    def test_lcms_defaults_supplied(self):
        adl1 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="accucor.xlsx",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            date="1972-11-24",
            researcher="Robert Leach",
            ms_protocol_name="Default",
            lc_protocol_name="polar-HILIC",
            instrument="default instrument",
            mzxml_files=[],
        )
        self.assertTrue(
            adl1.lcms_defaults_supplied(),
            msg="LCMS defaults should show as having been supplied.",
        )
        self.assertEqual(0, len(adl1.aggregated_errors_object.exceptions))

        adl2 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename=None,
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
            ),
            mzxml_files=None,
            date=None,
            researcher=None,
            ms_protocol_name=None,
            lc_protocol_name=None,
            instrument=None,
        )
        self.assertFalse(
            adl2.lcms_defaults_supplied(),
            msg="LCMS defaults should show as not having been supplied.",
        )
        self.assertEqual(0, len(adl2.aggregated_errors_object.exceptions))

    def test_get_or_create_ms_protocol(self):
        """
        A test for this method (using the default --ms-protocol-name) already exists, so this method tests using the
        lcms_metadata_df argument.
        """
        adl1 = AccuCorDataLoader(
            None,
            pd.read_excel(
                "DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose_pos.xlsx",
                sheet_name=1,  # The second sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            peak_group_set_filename="small_obob_maven_6eaas_inf_glucose.xlsx",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos_no_extras.tsv"
            ),
            date="1972-11-24",
            researcher="Robert Leach",
            ms_protocol_name=None,  # Left none intentionally
            lc_protocol_name="polar-HILIC",
            instrument="default instrument",
            mzxml_files=[],
        )
        # Pre-processing the data will enable get_or_create_ms_protocol by creating the lcms_metadata dict
        adl1.preprocess_data()
        ptcl = adl1.get_or_create_ms_protocol("BAT-xz971_pos")
        self.assertEqual(Protocol, type(ptcl))
        self.assertEqual("Default", ptcl.name)
        self.assertEqual(0, len(adl1.aggregated_errors_object.exceptions))

        adl2 = AccuCorDataLoader(
            None,
            pd.read_excel(
                "DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                sheet_name=1,  # The second sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            peak_group_set_filename="small_obob_maven_6eaas_inf_glucose.xlsx",
            researcher="Robert Leach",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos_no_extras.tsv"
            ),
            date="1972-11-24",
            ms_protocol_name=None,  # Left none intentionally
            lc_protocol_name="polar-HILIC",
            instrument="default instrument",
            mzxml_files=[],
        )

        # Pre-processing the data will enable get_or_create_ms_protocol by creating the lcms_metadata dict
        adl2.preprocess_data()

        # Create a protocol for a sample whose header is not in the file, which should fall back to unknown
        ptcl = adl2.get_or_create_ms_protocol("BAT-xz971")
        self.assertIsNone(ptcl)
        self.assertEqual(2, len(adl2.aggregated_errors_object.exceptions))
        self.assertTrue(
            adl2.aggregated_errors_object.exception_type_exists(
                MissingLCMSSampleDataHeaders
            )
            and adl2.aggregated_errors_object.exception_type_exists(
                UnexpectedLCMSSampleDataHeaders
            )
        )

    def test_get_or_create_lc_protocol(self):
        adl1 = AccuCorDataLoader(
            None,
            pd.read_excel(
                "DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose_pos.xlsx",
                sheet_name=1,  # The second sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            peak_group_set_filename="small_obob_maven_6eaas_inf_glucose.xlsx",
            date="1972-11-24",
            instrument="default instrument",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos_no_extras.tsv"
            ),
            researcher="Robert Leach",
            ms_protocol_name="Default",
            lc_protocol_name=None,  # Left none intentionally
            mzxml_files=[],
        )

        # Pre-processing the data will enable get_or_create_ms_protocol by creating the lcms_metadata dict
        adl1.preprocess_data()

        ptcl1 = adl1.get_or_create_lc_protocol("BAT-xz971_pos")
        self.assertEqual(0, len(adl1.aggregated_errors_object.exceptions))
        self.assertEqual(LCMethod, type(ptcl1))
        self.assertEqual("polar-HILIC-25-min", ptcl1.name)

        newname = "new-lc-method-30-min"
        self.assertEqual(0, LCMethod.objects.filter(name=newname).count())
        ptcl2 = adl1.get_or_create_lc_protocol("Br-xz971_pos")
        self.assertEqual(LCMethod, type(ptcl2))
        self.assertEqual(newname, ptcl2.name)

        adl2 = AccuCorDataLoader(
            None,
            pd.read_excel(
                "DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                sheet_name=1,  # The second sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            peak_group_set_filename="small_obob_maven_6eaas_inf_glucose.xlsx",
            date="1972-11-24",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos_no_extras.tsv"
            ),
            researcher="Robert Leach",
            ms_protocol_name="Default",
            lc_protocol_name=None,  # Left none intentionally
            instrument="default instrument",
            mzxml_files=[],
        )

        # Pre-processing the data will enable get_or_create_ms_protocol by creating the lcms_metadata dict
        adl2.preprocess_data()

        # Create a protocol for a sample whose header is not in the file, which should fall back to unknown
        ptcl2 = adl2.get_or_create_lc_protocol("BAT-xz971")
        self.assertEqual("unknown", ptcl2.name)
        self.assertEqual(2, len(adl2.aggregated_errors_object.exceptions))
        self.assertTrue(
            adl2.aggregated_errors_object.exception_type_exists(
                MissingLCMSSampleDataHeaders
            )
            and adl2.aggregated_errors_object.exception_type_exists(
                UnexpectedLCMSSampleDataHeaders
            )
        )


class LCMSSampleTableLoaderMethodTests(TracebaseTestCase):
    def test_check_lcms_samples(self):
        stl = SampleTableLoader()
        stl.lcms_samples = [
            "BAT-xz971",
            "Br-xz971",
            "Dia-xz971",
            "gas-xz971",
            "gWAT-xz971",
            "H-xz971",
            "Kid-xz971",
            "Liv-xz971",
            "Lu-xz971",
            "Pc-xz971",
            "Q-xz971",
            "SI-xz971",
            "Sol-xz971",
            "Sp-xz971",
            "serum-xz971",
        ]
        stl.check_lcms_samples(stl.lcms_samples)
        self.assertEqual(
            0,
            len(stl.aggregated_errors_object.exceptions),
            msg="No buffered error when check_lcms_samples is tested on sample list where every sample from the LCMS "
            "Metadata is present.",
        )

        sample_table_samples_missing = [
            "BAT-xz971",  # The only one that's present
            "sample-not-present-in-lcms-metadata1",
            "sample-not-present-in-lcms-metadata2",
        ]
        stl.check_lcms_samples(sample_table_samples_missing)
        self.assertEqual(
            1,
            len(stl.aggregated_errors_object.exceptions),
            msg="check_lcms_samples buffers 1 error",
        )
        self.assertTrue(
            stl.aggregated_errors_object.exception_type_exists(LCMSDBSampleMissing),
            msg="check_lcms_samples buffers an LCMSSampleMismatch exception",
        )
        self.assertNotIn(
            "BAT-xz971", stl.aggregated_errors_object.exceptions[0].lcms_samples_missing
        )
        self.assertIn(
            "Br-xz971", stl.aggregated_errors_object.exceptions[0].lcms_samples_missing
        )
        self.assertEqual(
            14, len(stl.aggregated_errors_object.exceptions[0].lcms_samples_missing)
        )


class LCMSMetadataParserMethodTests(TracebaseTestCase):
    def test_lcms_df_to_dict(self):
        df = extract_dataframes_from_lcms_tsv(
            "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
        )
        dct = lcms_df_to_dict(df)
        self.assertTrue(isinstance(dct, dict))
        # Dict keyed on unique sample data header (15 rows)
        self.assertEqual(15, len(dct.keys()))

    def test_lcms_metadata_to_samples(self):
        df = extract_dataframes_from_lcms_tsv(
            "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
        )
        dct = lcms_df_to_dict(df)
        samples = lcms_metadata_to_samples(dct)
        self.assertEqual(15, len(samples))

    def test_extract_dataframes_from_lcms_xlsx(self):
        df = extract_dataframes_from_lcms_xlsx(
            "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.xlsx"
        )
        self.assertIsNotNone(df)
        self.assertEqual((15, 11), df.shape)

    def test_extract_dataframes_from_lcms_tsv(self):
        df = extract_dataframes_from_lcms_tsv(
            "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv"
        )
        self.assertIsNotNone(df)
        self.assertEqual((15, 11), df.shape)

    def test_lcms_headers_are_valid(self):
        case_unordered_lcms_headers = [
            "lc run length",  # Changed order
            "tracebase sample name",
            "sample data header",
            "mzxml filename",
            "peak annotation filename",
            "instrument",
            "operator",
            "DATE",  # Changed case
            "ms mode",
            "lc method",
            "lc description",
        ]
        self.assertTrue(lcms_headers_are_valid(case_unordered_lcms_headers))
        self.assertFalse(
            lcms_headers_are_valid(case_unordered_lcms_headers + ["extra"])
        )
        self.assertFalse(lcms_headers_are_valid(["bad", "headers"]))

    def test_get_lcms_metadata_dict_from_file(self):
        lcms_metadata1 = get_lcms_metadata_dict_from_file(
            "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.xlsx"
        )
        self.assertTrue(isinstance(lcms_metadata1, dict))
        self.assertEqual(15, len(lcms_metadata1.keys()))
        lcms_metadata2 = get_lcms_metadata_dict_from_file(
            "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
        )
        self.assertTrue(isinstance(lcms_metadata2, dict))
        self.assertEqual(15, len(lcms_metadata2.keys()))

    def test_check_peak_annotation_files(self):
        aes = AggregatedErrors()
        # No error.  All annot files supplied.
        check_peak_annotation_files(
            ["small_obob_maven_6eaas_inf_glucose.xlsx"],
            lcms_file="DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc_no_extras.tsv",
            aes=aes,
        )
        self.assertEqual(0, len(aes.exceptions))

        # No error.  All annot files supplied.
        check_peak_annotation_files(
            ["small_obob_maven_6eaas_inf_glucose.xlsx"],
            lcms_file="DataRepo/example_data/small_dataset/"
            "glucose_lcms_metadata_except_mzxml_and_lcdesc_unsupplied_annot.tsv",
            aes=aes,
        )
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(MissingPeakAnnotationFiles, type(aes.exceptions[0]))
        self.assertEqual(
            ["unsupplied_accucor_file.xlsx"],
            aes.exceptions[0].missing_peak_annot_files,
        )
        self.assertEqual(0, len(aes.exceptions[0].unmatching_peak_annot_files))
        self.assertEqual(
            "glucose_lcms_metadata_except_mzxml_and_lcdesc_unsupplied_annot.tsv",
            aes.exceptions[0].lcms_file,
        )


class LCMSMetadataRequirementsTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("loaddata", "lc_methods")
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )

    def load_samples(self):
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/small_dataset/small_obob_animal_and_sample_table.xlsx"
            ),
        )

    # Requirement 1.1 is tested by the method tests above

    def test_lcms_metadata_default_fallbacks_lcms_good_no_data(self):
        """
        Test item from issue #706:
        `1.2.` Test that values missing in the LCMS metadata fall back to the defaults from 1.1.
        This test case tests when the sample data headers in the LCMS metadata file do not match the accucor file and
        sample table file.
        """
        self.load_samples()
        self.assertEqual(0, MSRun.objects.count())
        call_command(
            "load_accucor_msruns",
            # We just need a different file name with the same data, so _2 is a copy of the original
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
            ms_protocol_name="Default",
            instrument="default instrument",
            lc_protocol_name="polar-HILIC-25-min",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            lcms_file="DataRepo/example_data/small_dataset/"
            "glucose_lcms_metadata_except_mzxml_and_lcdesc_unrelated_data_only.tsv",
        )
        self.assertEqual(2, MSRun.objects.count())
        msr1 = MSRun.objects.first()
        msr2 = MSRun.objects.last()
        mspr = Protocol.objects.get(name="Default", category="msrun_protocol")
        lcmr = LCMethod.objects.get(name="polar-HILIC-25-min")
        sample1 = Sample.objects.get(name="BAT-xz971")
        sample2 = Sample.objects.get(name="Br-xz971")
        researcher = "Michael Neinast"
        date = datetime.date(datetime.strptime("2021-04-29", "%Y-%m-%d"))
        # TODO: Test for Instrument (which is not yet saved)
        # TODO: Test for mzxml_file (which is not yet saved)

        self.assertEqual(researcher, msr1.researcher)
        self.assertEqual(researcher, msr2.researcher)
        self.assertEqual(date, msr1.date)
        self.assertEqual(date, msr2.date)
        self.assertEqual(lcmr, msr1.lc_method)
        self.assertEqual(lcmr, msr2.lc_method)
        self.assertEqual(mspr, msr1.protocol)
        self.assertEqual(mspr, msr2.protocol)
        self.assertEqual(sample1, msr1.sample)
        self.assertEqual(sample2, msr2.sample)

    def test_lcms_metadata_default_fallbacks_lcms_good_no_defaults(self):
        """
        Test item from issue #706:
        `1.2.` Test that values missing in the LCMS metadata fall back to the defaults from 1.1.
        This test case tests when the sample data headers in the LCMS metadata file match the accucor file and
        sample table file, but there are no values other than the sample name and header, and no defaults.  Assure only
        a single coherent error about supplying defaults.
        """
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                # We just need a different file name with the same data, so _2 is a copy of the original
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                new_researcher=True,
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_only_reqd_col_vals.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(LCMSDefaultsRequired, type(aes.exceptions[0]))

    def test_lcms_metadata_default_fallbacks_lcms_good(self):
        """
        Test item from issue #706:
        `1.2.` Test that values missing in the LCMS metadata fall back to the defaults from 1.1.
        This test case tests when the sample data headers in the LCMS metadata file match the accucor file and sample
        table file.
        """
        self.load_samples()
        self.assertEqual(0, MSRun.objects.count())
        call_command(
            "load_accucor_msruns",
            # We just need a different file name with the same data, so _2 is a copy of the original
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
            ms_protocol_name="Default",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            lcms_file="DataRepo/example_data/small_dataset/"
            "glucose_lcms_metadata_except_mzxml_and_lcdesc_only_reqd_col_vals.tsv",
        )
        self.assertEqual(2, MSRun.objects.count())
        msr1 = MSRun.objects.first()
        msr2 = MSRun.objects.last()
        lcmr = LCMethod.objects.get(name="polar-HILIC-25-min")
        mspr = Protocol.objects.get(name="Default", category="msrun_protocol")
        sample1 = Sample.objects.get(name="BAT-xz971")
        sample2 = Sample.objects.get(name="Br-xz971")
        date = datetime.date(datetime.strptime("2021-04-29", "%Y-%m-%d"))
        researcher = "Michael Neinast"
        # TODO: Test for Instrument (which is not yet saved)
        # TODO: Test for mzxml_file (which is not yet saved)

        self.assertEqual(researcher, msr2.researcher)
        self.assertEqual(researcher, msr1.researcher)
        self.assertEqual(date, msr1.date)
        self.assertEqual(date, msr2.date)
        self.assertEqual(lcmr, msr2.lc_method)
        self.assertEqual(lcmr, msr1.lc_method)
        self.assertEqual(mspr, msr1.protocol)
        self.assertEqual(mspr, msr2.protocol)
        self.assertEqual(sample1, msr1.sample)
        self.assertEqual(sample2, msr2.sample)

    def test_lcms_metadata_missing_header_error(self):
        """
        Test item from issue #706:
        - `1.3.1.` Tests
        1. Any missing sample header in the LCMS metadata file causes an error if not all required defaults are
        specified
        """
        self.load_samples()
        with self.assertRaises(InvalidLCMSHeaders) as ar:
            call_command(
                "load_accucor_msruns",
                # We just need a different file name with the same data, so _2 is a copy of the original
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                new_researcher=True,
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_missing_date_col.tsv",
            )
        ilh = ar.exception
        self.assertEqual(["date"], ilh.missing)
        self.assertEqual([], ilh.unexpected)

    def test_lcms_metadata_dupe_sample_header_error(self):
        """
        Test item from issue #706:
        - `1.3.1.` Tests
        3. Duplicate sample data headers (assumed to be to the same sample) cause an error
        """
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                # We just need a different file name with the same data, so _2 is a copy of the original
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                new_researcher=True,
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_dupe_sample_header.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(DuplicateSampleDataHeaders, type(aes.exceptions[0]))

    def test_lcms_metadata_unique_sample_good(self):
        """
        Test item from issue #706:
        - `1.3.2.` The LCMS sample column must correspond to a unique sample in the sample table loader
        """
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/small_dataset/small_obob_animal_and_sample_table.xlsx"
            ),
            lcms_file="DataRepo/example_data/small_dataset/"
            "glucose_lcms_metadata_except_mzxml_and_lcdesc_pos.tsv",
        )

    def test_lcms_metadata_unique_sample_missing(self):
        """
        Test item from issue #706:
        - `1.3.2.` The LCMS sample column must correspond to a unique sample in the sample table loader
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_animals_and_samples",
                animal_and_sample_table_filename=(
                    "DataRepo/example_data/small_dataset/small_obob_animal_and_sample_table.xlsx"
                ),
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_missing_db_sample.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertTrue(aes.exception_type_exists(LCMSDBSampleMissing))


class LCMSLoadingExceptionBehaviorTests(TracebaseTestCase):
    """
    Tests in this class are intended to trigger a single (new) exception that was added on branch
    peak_sample_data_header

    `6.` Tests
    2. Test that no exceptions are repeated
    3. Test that there are no exceptions aside from the expected ones

    Note, this test:
        1. Test that the accucor data loader processes every row despite exceptions
    was already implicitly tested in the tests above.
    """

    def load_prereqs(self):
        call_command("loaddata", "lc_methods")
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )

    def load_samples(self):
        call_command(
            "load_animals_and_samples",
            animal_and_sample_table_filename=(
                "DataRepo/example_data/small_dataset/small_obob_animal_and_sample_table.xlsx"
            ),
        )

    def load_peak_annotations(self, lcms_file):
        call_command(
            "load_accucor_msruns",
            # We just need a different file name with the same data, so _2 is a copy of the original
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
            ms_protocol_name="Default",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            lcms_file=lcms_file,
        )

    def test_UnexpectedLCMSSampleDataHeaders_no_annot_files(
        self,
    ):
        """
        Supply an LCMS metadata file with a sample data header not in the accucor file and no values in the peak
        annotation file column to ensure an UnexpectedLCMSSampleDataHeaders exception is raised.
        """
        self.load_prereqs()
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            self.load_peak_annotations(
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_extra_hdr_no_optl_data.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(UnexpectedLCMSSampleDataHeaders, type(aes.exceptions[0]))

    def test_UnexpectedLCMSSampleDataHeaders_with_annot_files(
        self,
    ):
        """
        Supply an LCMS metadata file with a sample data header not in the accucor file associated with the current peak
        annotation file to ensure an UnexpectedLCMSSampleDataHeaders exception is raised.
        """
        self.load_prereqs()
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            self.load_peak_annotations(
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_extra_hdr.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(UnexpectedLCMSSampleDataHeaders, type(aes.exceptions[0]))

    def test_PeakAnnotFileMismatches(self):
        """
        Supply an LCMS metadata file with a sample data header not in the accucor file associated with the current peak
        annotation file to ensure an UnexpectedLCMSSampleDataHeaders exception is raised.
        """
        self.load_prereqs()
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            self.load_peak_annotations(
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_bad_peak_annots.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(PeakAnnotFileMismatches, type(aes.exceptions[0]))

    def test_LCMethodFixturesMissing(self):
        # Load everything but the LCMethod fixtures
        call_command(
            "load_study",
            "DataRepo/example_data/small_dataset/small_obob_study_prerequisites.yaml",
        )
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            self.load_peak_annotations(
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_no_extras.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(LCMethodFixturesMissing, type(aes.exceptions[0]))

    def test_MissingRequiredLCMSValues(self):
        self.load_prereqs()
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            self.load_peak_annotations(
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_no_smpl_name_hdr.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(MissingRequiredLCMSValues, type(aes.exceptions[0]))
        expected_dict = {
            "sample data header": [4, 6],
            "tracebase sample name": [5, 6],
        }
        self.assertEqual(expected_dict, aes.exceptions[0].header_rownums_dict)

    def test_MissingPeakAnnotationFiles(self):
        call_command("loaddata", "lc_methods")
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        with self.assertRaises(AggregatedErrorsSet) as ar:
            call_command(
                "load_study",
                "DataRepo/example_data/small_dataset/small_obob_study_params_lcms_extra_accucor.yaml",
            )
        aess = ar.exception
        aes = aess.aggregated_errors_dict[
            "glucose_lcms_metadata_except_mzxml_and_lcdesc_unsupplied_annot.tsv"
        ]
        self.assertEqual(AggregatedErrors, type(aes))
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(MissingPeakAnnotationFiles, type(aes.exceptions[0]))
        self.assertEqual(
            ["unsupplied_accucor_file.xlsx"], aes.exceptions[0].missing_peak_annot_files
        )
        self.assertEqual([], aes.exceptions[0].unmatching_peak_annot_files)
        self.assertEqual(
            "glucose_lcms_metadata_except_mzxml_and_lcdesc_unsupplied_annot.tsv",
            aes.exceptions[0].lcms_file,
        )

    def test_MissingLCMSSampleDataHeaders(self):
        self.load_prereqs()
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                # We just need a different file name with the same data, so _2 is a copy of the original
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                new_researcher=True,
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_msng_hdr_row.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(MissingLCMSSampleDataHeaders, type(aes.exceptions[0]))

    def test_MissingMZXMLFiles(self):
        self.load_prereqs()
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                # We just need a different file name with the same data, so _2 is a copy of the original
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                ms_protocol_name="Default",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_no_extras.tsv",
                mzxml_files=["sample1.mzxml", "sample2.mzxml"],
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(MissingMZXMLFiles, type(aes.exceptions[0]))

    def test_NoMZXMLFiles(self):
        self.load_prereqs()
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                # We just need a different file name with the same data, so _2 is a copy of the original
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                ms_protocol_name="Default",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_no_extras.tsv",
                validate=True,
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(NoMZXMLFiles, type(aes.exceptions[0]))

    def test_MismatchedSampleHeaderMZXML(self):
        self.load_prereqs()
        self.load_samples()
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_accucor_msruns",
                # We just need a different file name with the same data, so _2 is a copy of the original
                accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                ms_protocol_name="Default",
                lc_protocol_name="polar-HILIC-25-min",
                instrument="default instrument",
                date="2021-04-29",
                researcher="Michael Neinast",
                new_researcher=True,
                lcms_file="DataRepo/example_data/small_dataset/"
                "glucose_lcms_metadata_except_mzxml_and_lcdesc_unmatching_mzxmls.tsv",
                mzxml_files=["BAT-xz971_pos.mzxml", "Br-xz971_pos.mzxml"],
                # This is a warning in any case, but in validate mode, the exception is raised
                validate=True,
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(MismatchedSampleHeaderMZXML, type(aes.exceptions[0]))

        # The following should succeed without error even though the sample headers do not match the mzxml files.  It
        # only prints a warning (not checked)
        call_command(
            "load_accucor_msruns",
            # We just need a different file name with the same data, so _2 is a copy of the original
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
            ms_protocol_name="Default",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="default instrument",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            lcms_file="DataRepo/example_data/small_dataset/"
            "glucose_lcms_metadata_except_mzxml_and_lcdesc_unmatching_mzxmls.tsv",
            mzxml_files=["BAT-xz971_pos.mzxml", "Br-xz971_pos.mzxml"],
        )
