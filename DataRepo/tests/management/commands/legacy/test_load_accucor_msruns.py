import pandas as pd
from django.core.management import call_command

from DataRepo.loaders.legacy.accucor_data_loader import AccuCorDataLoader
from DataRepo.models import DataFormat, Infusate, MaintainedModel
from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import read_from_file
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs


class MSRunSampleSequenceTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        call_command(
            "legacy_load_study",
            "DataRepo/data/tests/small_obob/small_obob_study_prerequisites.yaml",
        )

        Study.objects.create(name="Small OBOB")
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )

        call_command(
            "load_animals",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )
        call_command(
            "load_samples",
            infile=(
                "DataRepo/data/tests/small_obob/"
                "small_obob_animal_and_sample_table.xlsx"
            ),
        )

        call_command(
            "legacy_load_accucor_msruns",
            accucor_file="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_glucose.xlsx",
            skip_samples=("blank"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-04-29",
            researcher="Michael Neinast",
            new_researcher=True,
            mzxml_files=[
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/BAT-xz971.mzXML",
                "DataRepo/data/tests/small_obob_mzxmls/small_obob_maven_6eaas_inf_glucose_mzxmls/Br-xz971.mzXML",
            ],
        )

        cls.MSRUNSAMPLE_COUNT = 2
        cls.MSRUNSEQUENCE_COUNT = 1

        super().setUpTestData()

    def create_populated_AccuCorDataLoader_object(self, lcms_file):
        xlsx = "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_lactate.xlsx"
        adl = AccuCorDataLoader(
            # Original dataframe
            pd.read_excel(
                xlsx,
                sheet_name=0,  # The first sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            # Corrected dataframe
            pd.read_excel(
                xlsx,
                sheet_name=1,  # The second sheet
                engine="openpyxl",
            ).dropna(axis=0, how="all"),
            # Peak annot file name
            xlsx,
            data_format=DataFormat.objects.get(code="accucor"),
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="1972-11-24",
            researcher="Michael Neinast",
            lcms_metadata_df=read_from_file(lcms_file),
        )
        adl.prepare_metadata()
        return adl

    @MaintainedModel.no_autoupdates()
    def test_get_sample_header_by_mzxml_basename_one_match(self):
        """
        get_sample_header_by_mzxml_basename returns the sample header from the line of the lcms metadata file that has
        the matching mzxml file basename on it
        """
        adl = self.create_populated_AccuCorDataLoader_object(
            "DataRepo/data/tests/small_obob_lcms_metadata/lactate_neg.tsv",
        )
        hdr = adl.get_sample_header_by_mzxml_basename("BAT-xz971_neg.mzXML")
        self.assertEqual("BAT-xz971", hdr)
        self.assertEqual(0, adl.aggregated_errors_object.num_errors)

    @MaintainedModel.no_autoupdates()
    def test_get_sample_header_by_mzxml_basename_no_match(self):
        """
        get_sample_header_by_mzxml_basename returns None if the mzxml file basename isn't in the lcms metadata file
        (because mzxml files are not required)
        """
        adl = self.create_populated_AccuCorDataLoader_object(
            "DataRepo/data/tests/small_obob_lcms_metadata/lactate_neg.tsv",
        )
        hdr = adl.get_sample_header_by_mzxml_basename("BAT-xz971.mzXML")
        self.assertIsNone(hdr)
        self.assertEqual(0, adl.aggregated_errors_object.num_errors)

    @MaintainedModel.no_autoupdates()
    def test_get_sample_header_by_mzxml_basename_multiple_matches(self):
        """
        Exception if the same mzxml file basename occurs in the LCMS metadata file multiple times
        """
        adl = self.create_populated_AccuCorDataLoader_object(
            "DataRepo/data/tests/small_obob_lcms_metadata/lactate_neg_multiple.tsv",
        )
        hdr = adl.get_sample_header_by_mzxml_basename("BAT-xz971_neg.mzXML")
        self.assertIsNone(hdr)
        self.assertEqual(1, len(adl.aggregated_errors_object.exceptions))
        self.assertEqual(ValueError, type(adl.aggregated_errors_object.exceptions[0]))
        self.assertEqual(
            "2 instances of mzxml file [BAT-xz971_neg.mzXML] in the LCMS metadata file.",
            str(adl.aggregated_errors_object.exceptions[0]),
        )
