from DataRepo.tests.tracebase_test_case import TracebaseTestCase
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
    def test_create_name(self):
        pass

    def test_get_name(self):
        pass

    def test_sample_header_to_default_mzxml(self):
        pass

    def test_check_mzxml(self):
        pass

    def test_validate_mzxmls(self):
        pass

    def test_get_missing_required_lcms_defaults(self):
        pass

    def test_lcms_defaults_supplied(self):
        pass

    def test_get_or_create_ms_protocol(self):
        pass

    def test_get_or_create_lc_protocol(self):
        pass

    def test_exception_type_exists(self):
        pass

    def test_lcms_df_to_dict(self):
        pass

    def test_lcms_metadata_to_samples(self):
        pass

    def test_extract_dataframes_from_lcms_xlsx(self):
        pass

    def test_extract_dataframes_from_lcms_csv(self):
        pass

    def test_lcms_headers_are_valid(self):
        pass

    def test_check_lcms_samples(self):
        pass


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

    def test_extract_dataframes_from_lcms_csv(self):
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
