from django.core.management import call_command

from DataRepo.compositeviews import BaseAdvancedSearchView
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class CompositeViewTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table_2ndstudy.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
        )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_serum.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=False,
        )

    def getQueryObject(self):
        return {
            "selectedtemplate": "pgtemplate",
            "searches": {
                "pgtemplate": {
                    "tree": {
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "static": False,
                                "ncmp": "icontains",
                                "fld": "msrun__sample__animal__studies__name",
                                "val": "obob_fasted",
                            }
                        ],
                    },
                    "name": "PeakGroups",
                },
                "pdtemplate": {"name": "PeakData", "tree": {}},
                "fctemplate": {"name": "Fcirc", "tree": {}},
            },
        }

    def getPdtemplateChoicesTuple(self):
        return (
            ("peak_group__msrun__sample__animal__name", "Animal"),
            ("peak_group__msrun__sample__animal__body_weight", "Body Weight (g)"),
            ("corrected_abundance", "Corrected Abundance"),
            ("peak_group__msrun__sample__animal__diet", "Diet"),
            ("peak_group__msrun__sample__animal__feeding_status", "Feeding Status"),
            ("peak_group__formula", "Formula"),
            ("peak_group__msrun__sample__animal__genotype", "Genotype"),
            ("labeled_count", "Labeled Count"),
            ("labeled_element", "Labeled Element"),
            (
                "peak_group__compounds__synonyms__name",
                "Measured Compound (Any Synonym)",
            ),
            ("peak_group__compounds__name", "Measured Compound (Primary Synonym)"),
            ("med_mz", "Median M/Z"),
            ("med_rt", "Median RT"),
            ("peak_group__name", "Peak Group"),
            ("peak_group__peak_group_set__filename", "Peak Group Set Filename"),
            ("raw_abundance", "Raw Abundance"),
            ("peak_group__msrun__sample__name", "Sample"),
            ("peak_group__msrun__sample__animal__sex", "Sex"),
            ("peak_group__msrun__sample__animal__studies__name", "Study"),
            ("peak_group__msrun__sample__tissue__name", "Tissue"),
            (
                "peak_group__msrun__sample__animal__tracer_compound__name",
                "Tracer Compound (Primary Synonym)",
            ),
            (
                "peak_group__msrun__sample__animal__tracer_infusion_concentration",
                "Tracer Infusion Concentration (mM)",
            ),
            (
                "peak_group__msrun__sample__animal__tracer_infusion_rate",
                "Tracer Infusion Rate (ul/min/g)",
            ),
            ("peak_group__msrun__sample__animal__treatment__name", "Treatment"),
        )

    def getPgtemplateChoicesTuple(self):
        return (
            ("msrun__sample__animal__name", "Animal"),
            ("msrun__sample__animal__body_weight", "Body Weight (g)"),
            ("msrun__sample__animal__diet", "Diet"),
            ("msrun__sample__animal__feeding_status", "Feeding Status"),
            ("formula", "Formula"),
            ("msrun__sample__animal__genotype", "Genotype"),
            ("compounds__synonyms__name", "Measured Compound (Any Synonym)"),
            ("compounds__name", "Measured Compound (Primary Synonym)"),
            ("name", "Peak Group"),
            ("peak_group_set__filename", "Peak Group Set Filename"),
            ("msrun__sample__name", "Sample"),
            ("msrun__sample__animal__sex", "Sex"),
            ("msrun__sample__animal__studies__name", "Study"),
            ("msrun__sample__tissue__name", "Tissue"),
            (
                "msrun__sample__animal__tracer_compound__name",
                "Tracer Compound (Primary Synonym)",
            ),
            (
                "msrun__sample__animal__tracer_infusion_concentration",
                "Tracer Infusion Concentration (mM)",
            ),
            (
                "msrun__sample__animal__tracer_infusion_rate",
                "Tracer Infusion Rate (ul/min/g)",
            ),
            ("msrun__sample__animal__tracer_labeled_atom", "Tracer Labeled Element"),
            ("msrun__sample__animal__treatment__name", "Treatment"),
        )

    def test_getSearchFieldChoicesDict(self):
        basv = BaseAdvancedSearchView()
        sfcd = basv.getSearchFieldChoicesDict()
        sfcd_expected = {
            "fctemplate": (
                ("msrun__sample__animal__name", "Animal"),
                ("msrun__sample__animal__body_weight", "Body Weight (g)"),
                ("msrun__sample__animal__diet", "Diet"),
                ("msrun__sample__animal__feeding_status", "Feeding Status"),
                ("msrun__sample__animal__genotype", "Genotype"),
                ("msrun__sample__animal__sex", "Sex"),
                ("msrun__sample__animal__studies__name", "Study"),
                (
                    "msrun__sample__time_collected",
                    "Time Collected (hh:mm:ss since infusion)",
                ),
                ("msrun__sample__animal__tracer_compound__name", "Tracer Compound"),
                (
                    "msrun__sample__animal__tracer_infusion_concentration",
                    "Tracer Infusion Concentration (mM)",
                ),
                (
                    "msrun__sample__animal__tracer_infusion_rate",
                    "Tracer Infusion Rate (ul/min/g)",
                ),
                (
                    "msrun__sample__animal__tracer_labeled_atom",
                    "Tracer Labeled Element",
                ),
                ("msrun__sample__animal__treatment__name", "Treatment"),
            ),
            "pdtemplate": self.getPdtemplateChoicesTuple(),
            "pgtemplate": self.getPgtemplateChoicesTuple(),
        }
        self.assertDictEqual(sfcd, sfcd_expected)

    def test_getAllSearchFieldChoices(self):
        basv = BaseAdvancedSearchView()
        sfct = basv.getAllSearchFieldChoices()
        sfct_expected = self.getPgtemplateChoicesTuple()
        sfct_expected += self.getPdtemplateChoicesTuple()
        sfct_expected += (
            (
                "msrun__sample__time_collected",
                "Time Collected (hh:mm:ss since infusion)",
            ),
        )
        self.assertTupleEqual(sfct, sfct_expected)
