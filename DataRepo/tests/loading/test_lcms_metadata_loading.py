import re

import pandas as pd
from django.core.management import call_command

from DataRepo.models import LCMethod, Protocol
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import (
    AccuCorDataLoader,
    LCMSSampleMismatch,
    MismatchedSampleHeaderMZXML,
    MissingLCMSSampleDataHeaders,
    MissingMZXMLFiles,
    SampleTableLoader,
)
from DataRepo.utils.lcms_metadata_parser import (
    extract_dataframes_from_lcms_tsv,
    extract_dataframes_from_lcms_xlsx,
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
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
            ),
            date="",
            researcher="",
            ms_protocol_name="",
            lc_protocol_name="",
            instrument="",
            mzxml_files=["sample1.mzxml", "sample2.mzxml"],
        )
        mzxml1 = adl1.sample_header_to_default_mzxml("does-not-exist")
        self.assertIsNone(
            mzxml1,
            msg="Failed lookups should result in None",
        )

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
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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

        adl3 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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

    def test_check_mzxml(self):
        adl1 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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
        self.assertFalse(
            adl1.aggregated_errors_object.exceptions[0].is_error,
            msg="The first exception should be a warning in loading mode.",
        )
        self.assertFalse(
            adl1.aggregated_errors_object.exceptions[0].is_fatal,
            msg="The first exception should not be fatal in loading mode.",
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

    def test_get_missing_required_lcms_defaults(self):
        adl1 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="accucor.xlsx",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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

        adl2 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename=None,
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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

    def test_lcms_defaults_supplied(self):
        adl1 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename="accucor.xlsx",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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

        adl2 = AccuCorDataLoader(
            None,
            None,
            peak_group_set_filename=None,
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
            ),
            date=None,
            researcher=None,
            ms_protocol_name=None,
            lc_protocol_name=None,
            instrument=None,
            mzxml_files=None,
        )
        self.assertFalse(
            adl2.lcms_defaults_supplied(),
            msg="LCMS defaults should show as not having been supplied.",
        )

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
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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

        adl2 = AccuCorDataLoader(
            None,
            pd.read_excel(
                "DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf_glucose.xlsx",
                sheet_name=1,  # The second sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            peak_group_set_filename="small_obob_maven_6eaas_inf_glucose.xlsx",
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
            ),
            date="1972-11-24",
            researcher="Robert Leach",
            ms_protocol_name=None,  # Left none intentionally
            lc_protocol_name="polar-HILIC",
            instrument="default instrument",
            mzxml_files=[],
        )
        # Pre-processing the data will enable get_or_create_ms_protocol by creating the lcms_metadata dict
        adl2.preprocess_data()
        ptcl = adl2.get_or_create_ms_protocol("BAT-xz971")
        self.assertIsNone(ptcl)
        self.assertEqual(1, len(adl2.aggregated_errors_object.exceptions))
        self.assertTrue(adl2.aggregated_errors_object.exception_type_exists(MissingLCMSSampleDataHeaders))

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
            lcms_metadata_df=extract_dataframes_from_lcms_tsv(
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
            ),
            researcher="Robert Leach",
            ms_protocol_name="Default",
            lc_protocol_name=None,  # Left none intentionally
            instrument="default instrument",
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
                "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
            ),
            researcher="Robert Leach",
            ms_protocol_name="Default",
            lc_protocol_name=None,  # Left none intentionally
            instrument="default instrument",
            mzxml_files=[],
        )

        # Pre-processing the data will enable get_or_create_ms_protocol by creating the lcms_metadata dict
        adl2.preprocess_data()

        ptcl2 = adl2.get_or_create_lc_protocol("BAT-xz971")
        self.assertEqual("unknown", ptcl2.name)
        self.assertEqual(1, len(adl2.aggregated_errors_object.exceptions))
        self.assertTrue(adl2.aggregated_errors_object.exception_type_exists(MissingLCMSSampleDataHeaders))


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
        self.assertFalse(
            stl.aggregated_errors_object.exception_type_exists(LCMSSampleMismatch),
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
            stl.aggregated_errors_object.exception_type_exists(LCMSSampleMismatch),
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
            "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
        )
        dct = lcms_df_to_dict(df)
        self.assertTrue(isinstance(dct, dict))
        # Dict keyed on unique sample data header (15 rows)
        self.assertEqual(15, len(dct.keys()))

    def test_lcms_metadata_to_samples(self):
        df = extract_dataframes_from_lcms_tsv(
            "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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
            "DataRepo/example_data/small_dataset/glucose_lcms_metadata_except_mzxml_and_lcdesc.tsv"
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


class LCMSMetadataRequirementsTests(TracebaseTestCase):
    def test_lcms_default_options(self):
        """
        - `1.1.` Test that these options/arguments exist for the accucor load:
            - LCMethod name
            - Peak Annotation Filename
            - Researcher
            - Date
            - Instrument
            - MS Mode
            - `1.1.` Tests
        """
        pass

    def test_lcms_metadata_xlsx(self):
        """
        2. LCMS metadata file option accepts XLSX
        """
        pass

    def test_lcms_metadata_tsv(self):
        """
        2. LCMS metadata file option accepts TSV

        """
        pass

    def test_lcms_metadata_columns(self):
        """
        3. The columns in the LCMS metadata file include
            - LCMethod Type
            - LCMethod Run Length
            - LCMethod Run Description
            - Peak Annotation Filename
            - Researcher
            - Date
            - MS Mode (e.g. positive ion mode)
            - Sample Name
            - Sample Data Header
            - mzXML Filename
            - Instrument
        """
        pass

    def test_lcms_metadata_default_fallbacks(self):
        """
        `1.2.` Test that values missing in the LCMS metadata fall back to the defaults from 1.1.
        """
        pass

    def test_lcms_metadata_missing_header_error(self):
        """
        - `1.3.1.` Tests
        1. Any missing sample header in the LCMS metadata file causes an error if not all required defaults are
        specified
        """
        pass

    def test_lcms_metadata_missing_value_error(self):
        """
        - `1.3.1.` Tests
        2. Any missing column value LCMS metadata file causes an error about either needing a value or supply a default
        """
        pass

    def test_lcms_metadata_dupe_sample_header_error(self):
        """
        - `1.3.1.` Tests
        3. Duplicate sample data headers (assumed to be to the same sample) cause an error
        """
        pass

    def test_lcms_metadata_unique_sample_data_headers(self):
        """
        - `1.3.2.` The LCMS sample column must correspond to a unique sample in the sample table loader
        """
        pass

    def test_lcms_metadata_mzxml_option(self):
        """
        - `2.` Test that an option/arg exists for multiple mzXML files
        """
        pass

    def test_lcms_metadata_lcmethod_creation(self):
        """
        - `3.` Test that LCMethod records are created
        """
        pass

    def test_msrun_links_to_lcmethod(self):
        """
        - `4.` Test that MSRun records link to LCMethod records
        """
        pass

    def test_msrun_links_to_ms_protocol(self):
        """
        - `5.` Test `msrun_protocol` `Protocol` records are created
        """
        pass

    def test_lcms_buffers_exceptions(self):
        """
        - `6.` Tests
        1. Test that the accucor data loader processes every row despite exceptions
        """
        pass

    def test_no_repeated_exceptions(self):
        """
        - `6.` Tests
        2. Test that no exceptions are repeated
        """
        pass

    def test_no_unexpected_exceptions(self):
        """
        - `6.` Tests
        3. Test that there are no exceptions aside from the expected ones
        """
        pass
