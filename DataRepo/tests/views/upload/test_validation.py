import re

from DataRepo.loaders import ProtocolsLoader, SampleTableLoader, TissuesLoader
from DataRepo.models import Protocol, Tissue
from DataRepo.tests.tracebase_test_case import TracebaseTransactionTestCase
from DataRepo.utils.exceptions import (
    AllMissingSamplesError,
    AllMissingTissues,
    AllMissingTreatments,
    MissingTissue,
    MissingTreatment,
    MultiLoadStatus,
    NonUniqueSampleDataHeader,
)
from DataRepo.views.upload.validation import DataValidationView


class DataValidationViewTests(TracebaseTransactionTestCase):
    LCMS_DICT = {
        "a": {
            "sort level": 0,
            "tracebase sample name": "a",
            "sample data header": "a",
            "peak annotation filename": "accucor.xlsx",
        },
        "b": {
            "sort level": 0,
            "tracebase sample name": "b",
            "sample data header": "b",
            "peak annotation filename": "accucor.xlsx",
        },
        "d_pos": {
            "sort level": 0,
            "tracebase sample name": "d",
            "sample data header": "d_pos",
            "peak annotation filename": "accucor.xlsx",
        },
        "c": {
            "sort level": 1,
            "error": NonUniqueSampleDataHeader("c", {"accucor.xlsx": 2}),
            "tracebase sample name": "c",
            "sample data header": "c",
            "peak annotation filename": "accucor.xlsx",
        },
    }

    def test_build_lcms_dict(self):
        dvv = DataValidationView()
        lcms_dict = dvv.build_lcms_dict(
            ["a", "b", "c", "c", "d_pos"],  # Sample headers
            "accucor.xlsx",  # Peak annot file
        )
        # assertDictEqual does not work with the exception object, so asserting each individually & comparing exception
        # strings
        self.assertEqual(
            len(self.LCMS_DICT.keys()),
            len(lcms_dict.keys()),
        )
        self.assertDictEqual(
            self.LCMS_DICT["a"],
            lcms_dict["a"],
        )
        self.assertDictEqual(
            self.LCMS_DICT["b"],
            lcms_dict["b"],
        )
        self.assertEqual(
            str(self.LCMS_DICT["c"]["error"]),
            str(lcms_dict["c"]["error"]),
        )
        self.assertEqual(
            self.LCMS_DICT["c"]["peak annotation filename"],
            lcms_dict["c"]["peak annotation filename"],
        )
        self.assertEqual(
            self.LCMS_DICT["c"]["sample data header"],
            lcms_dict["c"]["sample data header"],
        )
        self.assertEqual(
            self.LCMS_DICT["c"]["sort level"],
            lcms_dict["c"]["sort level"],
        )
        self.assertEqual(
            self.LCMS_DICT["c"]["tracebase sample name"],
            lcms_dict["c"]["tracebase sample name"],
        )
        self.assertDictEqual(
            self.LCMS_DICT["d_pos"],
            lcms_dict["d_pos"],
        )
        self.assertEqual(
            str(self.LCMS_DICT["c"]["error"]), str(dvv.lcms_build_errors.nusdh_list[0])
        )

    def test_get_approx_sample_header_replacement_regex_default(self):
        pattern = DataValidationView.get_approx_sample_header_replacement_regex()
        samplename = re.sub(pattern, "", "mysample_neg_pos_scan2")
        self.assertEqual("mysample", samplename)

    def test_get_approx_sample_header_replacement_regex_add_custom(self):
        pattern = DataValidationView.get_approx_sample_header_replacement_regex(
            [r"_blah"]
        )
        samplename = re.sub(pattern, "", "mysample_pos_blah_scan1")
        self.assertEqual("mysample", samplename)

    def test_get_approx_sample_header_replacement_regex_just_custom(self):
        pattern = DataValidationView.get_approx_sample_header_replacement_regex(
            [r"_blah"], add=False
        )
        samplename = re.sub(pattern, "", "mysample_pos_blah")
        self.assertEqual("mysample_pos", samplename)

    def test_lcms_dict_to_tsv_string(self):
        lcms_data = DataValidationView.lcms_dict_to_tsv_string(self.LCMS_DICT)
        self.assertEqual(
            (
                "tracebase sample name\tsample data header\tpeak annotation filename\n"
                "a\ta\taccucor.xlsx\n"
                "b\tb\taccucor.xlsx\n"
                "d\td_pos\taccucor.xlsx\n"
                "c\tc\taccucor.xlsx\n"
            ),
            lcms_data,
        )

    def test_get_or_create_study_dataframes_create(self):
        """
        This tests that a new dataframe dict is created and that existing tissues and treatments are pre-populated.

        It also tests that the filter criteria for the protocols (category=animal_treatment) works (at least that
        there's not error and results are returned, since there are no other categories, currently).

        This also indirectly tests create_study_dfs_dict, animals_dict, and samples_dict.
        """
        Tissue.objects.create(name="test1", description="test description 1")
        Tissue.objects.create(name="test2", description="test description 2")
        Protocol.objects.create(
            name="test", category="animal_treatment", description="test description"
        )

        dvv = DataValidationView()
        dvv.animal_sample_file = None

        dfs_dict = dvv.get_or_create_study_dataframes()
        expected = {
            "Animals": {
                "Age": {},
                "Animal Body Weight": {},
                "Animal Genotype": {},
                "Animal ID": {},
                "Animal Treatment": {},
                "Diet": {},
                "Feeding Status": {},
                "Infusate": {},
                "Infusion Rate": {},
                "Sex": {},
                "Study Description": {},
                "Study Name": {},
                "Tracer Concentrations": {},
            },
            "Samples": {
                "Animal ID": {},
                "Collection Time": {},
                "Date Collected": {},
                "Researcher Name": {},
                "Sample Name": {},
                "Tissue": {},
            },
            "Treatments": {
                "Treatment Description": {0: "test description"},
                "Animal Treatment": {0: "test"},
            },
            "Tissues": {
                "Description": {0: "test description 1", 1: "test description 2"},
                "Tissue": {0: "test1", 1: "test2"},
            },
        }

        self.assert_dfs_dicts(expected, dfs_dict)

    def assert_dfs_dicts(self, expected, dfs_dict):
        self.assertEqual(len(expected.keys()), len(dfs_dict.keys()))
        self.assertDictEqual(expected["Animals"], dfs_dict["Animals"])
        self.assertDictEqual(expected["Samples"], dfs_dict["Samples"])
        self.assertDictEqual(expected["Treatments"], dfs_dict["Treatments"])
        self.assertDictEqual(expected["Tissues"], dfs_dict["Tissues"])

    def test_get_or_create_study_dataframes_get(self):
        """
        This tests that an existing dataframe dict is returned and that missing columns are added and filled to the
        number of rows of other columns with None values.

        This also indirectly tests get_study_dfs_dict and fill_in_missing_columns.
        """
        dvv = DataValidationView()
        dvv.animal_sample_file = (
            "DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table.xlsx"
        )

        dfs_dict = dvv.get_or_create_study_dataframes()
        expected = {
            "Animals": {
                "Age": {0: None},
                "Animal Body Weight": {0: 26.3},
                "Animal Genotype": {0: "WT"},
                "Animal ID": {0: "971"},
                "Animal Treatment": {0: "obob_fasted"},
                "Diet": {0: None},
                "Feeding Status": {0: "Fasted"},
                "Infusate": {0: "lysine-[13C6]"},
                "Infusion Rate": {0: 0.11},
                "Sex": {0: None},
                "Study Description": {
                    0: (
                        "Infusion was actually 6 labeled tracers:  Histidine, Lysine, Methionine, Phenylalanine, "
                        "Threonine, Tryptophan"
                    ),
                },
                "Study Name": {0: "Small OBOB"},
                "Tracer Concentrations": {0: 23.2},
            },
            "Samples": {
                # These are repeating values, so I create them on the fly...
                "Animal ID": dict((i, "971") for i in range(16)),
                "Collection Time": dict((i, 150) for i in range(16)),
                "Date Collected": dict(
                    (i, "11/19/20") if i == 14 else (i, "2020-11-19") for i in range(16)
                ),
                "Researcher Name": dict((i, "Xianfeng Zeng") for i in range(16)),
                "Sample Name": {
                    0: "BAT-xz971",
                    1: "Br-xz971",
                    2: "Dia-xz971",
                    3: "gas-xz971",
                    4: "gWAT-xz971",
                    5: "H-xz971",
                    6: "Kid-xz971",
                    7: "Liv-xz971",
                    8: "Lu-xz971",
                    9: "Pc-xz971",
                    10: "Q-xz971",
                    11: "SI-xz971",
                    12: "Sol-xz971",
                    13: "Sp-xz971",
                    14: "serum-xz971",
                    15: "PREFIX_newsample",
                },
                "Tissue": {
                    13: "spleen",
                    14: "serum_plasma_unspecified_location",
                    15: "brown_adipose_tissue",
                },
            },
            "Treatments": {
                "Treatment Description": {
                    0: "No manipulation besides what is already described in other fields.",
                    1: "ob/ob homozygouse mice were fasted",
                },
                "Animal Treatment": {0: "no treatment", 1: "obob_fasted"},
            },
            "Tissues": {
                "Description": {
                    0: "brown adipose tissue",
                    1: "whole brain",
                    2: "diaphragm muscle",
                    3: "gastrocnemius muscle",
                    4: "gonadal white adipose tissue",
                    5: "heart muscle (ventricle)",
                    6: "kidney",
                    7: "liver",
                    8: "lung",
                    9: "pancreas",
                    10: "quadricep muscle",
                    11: "small intestine",
                    12: "soleus",
                    13: "unspecified skeletal muscle - only use this if source muscle is unknown",
                    14: "colon",
                    15: "inguinal white adipose tissue",
                    16: "unspecified white adipose tissue - only use this if source fat depot is unknown",
                    17: "spleen",
                    18: "serum or plasma collected from tail snip",
                    19: "serum or plasma collected from any artery",
                    20: "serum or plasma collected from portal vein",
                    21: "serum or plasma - only use when the source is unknown",
                    22: "thymus",
                    23: "outer ear, usually collected as a representative skin sample",
                    24: "unspecified source of skin - only use this if source of skin is unknown",
                    25: "stomach",
                    26: "cecum",
                    27: "cecum contents",
                    28: "tibialus anterior muscle",
                    29: "eyeball",
                    30: "testical",
                    31: "ovary",
                    32: "uterus",
                    33: "nonspecified tumor - only use this if other information is unknown",
                    34: "xenograft tumor of HCT116 cells",
                },
                "Tissue": {
                    # See below
                    13: "skeletal_muscle_unspecified_location",
                    14: "colon",
                    15: "white_adipose_tissue_inguinal",
                    16: "white_adipose_tissue_unspecified_location",
                    17: "spleen",
                    18: "serum_plasma_tail",
                    19: "serum_plasma_artery",
                    20: "serum_plasma_portal",
                    21: "serum_plasma_unspecified_location",
                    22: "thymus",
                    23: "ear",
                    24: "skin",
                    25: "stomach",
                    26: "cecum",
                    27: "cecum_contents",
                    28: "tibialus_anterior",
                    29: "eye",
                    30: "testicle",
                    31: "ovary",
                    32: "uterus",
                    33: "tumor_nonspecific",
                    34: "tumor_hct116",
                },
            },
            "Infusions": None,  # Ignoring this one
        }

        # The following is to avoid a JSCPD error.  Silly hoop jumping...
        tissue_name_segment = {
            0: "brown_adipose_tissue",
            1: "brain",
            2: "diaphragm",
            3: "gastrocnemius",
            4: "white_adipose_tissue_gonadal",
            5: "heart",
            6: "kidney",
            7: "liver",
            8: "lung",
            9: "pancreas",
            10: "quadricep",
            11: "small_intestine",
            12: "soleus",
        }
        expected["Tissues"]["Tissue"].update(tissue_name_segment)
        expected["Samples"]["Tissue"].update(tissue_name_segment)

        self.assert_dfs_dicts(expected, dfs_dict)

    def test_get_study_dtypes_dict(self):
        dvv = DataValidationView()
        # TODO: Eliminate the need for a dummy file (with an xls extension).  The protocol headers change for the
        # treatments sheet if it's an excel file.  The data is not needed - just the headers.
        dvv.animal_sample_file = "dummy.xlsx"
        expected = {
            "Animals": {
                "Animal ID": str,
                "Animal Treatment": str,
            },
            "Samples": {"Animal ID": str},
            "Treatments": {
                "Treatment Description": str,
                "Category": str,
                "Animal Treatment": str,
            },
            "Tissues": {
                "Description": str,
                "Tissue": str,
            },
        }
        self.assertDictEqual(expected, dvv.get_study_dtypes_dict())

    def get_data_validation_object_with_errors(self):
        vo = DataValidationView()
        vo.load_status_data = MultiLoadStatus(
            load_keys=[
                "All Samples present",
                "All Tissues present",
                "All Treatments present",
            ]
        )
        missing_sample_dict = {
            "files_missing_all": {},
            "files_missing_some": {"s1": ["accucor1.xlsx"]},
            "all_missing_samples": {"s1": ["accucor1.xlsx"]},
        }
        amse_err = AllMissingSamplesError(missing_sample_dict)
        amse_err.is_error = True
        amse_warn = AllMissingSamplesError(missing_sample_dict)
        amse_warn.is_error = False
        amse_warn2 = AllMissingSamplesError(missing_sample_dict)
        amse_warn2.is_error = False

        amti_err = AllMissingTissues([MissingTissue("elbow pit")])
        amti_err.is_error = True
        amti_warn = AllMissingTissues([MissingTissue("elbow pit")])
        amti_warn.is_error = False
        amti_warn2 = AllMissingTissues([MissingTissue("elbow pit")])
        amti_warn2.is_error = False

        amtr_err = AllMissingTreatments([MissingTreatment("wined-and-dined")])
        amtr_err.is_error = True
        amtr_warn = AllMissingTreatments([MissingTreatment("wined-and-dined")])
        amtr_warn.is_error = False
        amtr_warn2 = AllMissingTreatments([MissingTreatment("wined-and-dined")])
        amtr_warn2.is_error = False

        vo.load_status_data.set_load_exception(amse_err, "All Samples present")
        vo.load_status_data.set_load_exception(amse_warn, "file1.xlsx")
        vo.load_status_data.set_load_exception(amse_warn2, "file2.xlsx")
        vo.load_status_data.set_load_exception(amti_err, "All Tissues present")
        vo.load_status_data.set_load_exception(amti_warn, "file1.xlsx")
        vo.load_status_data.set_load_exception(amti_warn2, "file2.xlsx")
        vo.load_status_data.set_load_exception(amtr_err, "All Treatments present")
        vo.load_status_data.set_load_exception(amtr_warn, "file1.xlsx")
        vo.load_status_data.set_load_exception(amtr_warn2, "file2.xlsx")

        return vo

    def test_extract_autofill_exceptions(self):
        vo = self.get_data_validation_object_with_errors()

        vo.extract_autofill_exceptions()

        self.assertEqual(
            1, len(vo.extracted_exceptions[AllMissingSamplesError.__name__]["errors"])
        )
        self.assertEqual(
            2, len(vo.extracted_exceptions[AllMissingSamplesError.__name__]["warnings"])
        )
        self.assertEqual(
            1, len(vo.extracted_exceptions[AllMissingTissues.__name__]["errors"])
        )
        self.assertEqual(
            2, len(vo.extracted_exceptions[AllMissingTissues.__name__]["warnings"])
        )
        self.assertEqual(
            1, len(vo.extracted_exceptions[AllMissingTreatments.__name__]["errors"])
        )
        self.assertEqual(
            2, len(vo.extracted_exceptions[AllMissingTreatments.__name__]["warnings"])
        )
        self.assertDictEqual(
            {
                "Samples": {"s1": {"Sample Name": "s1"}},
                "Tissues": {"elbow pit": {"Tissue": "elbow pit"}},
                "Treatments": {
                    "wined-and-dined": {"Animal Treatment": "wined-and-dined"}
                },
            },
            vo.autofill_dict,
        )
        self.assertIn("Autofill Note", vo.load_status_data.statuses.keys())
        self.assertIn("All Samples present", vo.load_status_data.statuses.keys())
        self.assertIn("All Tissues present", vo.load_status_data.statuses.keys())
        self.assertIn("All Treatments present", vo.load_status_data.statuses.keys())
        self.assertEqual(
            1,
            len(
                vo.load_status_data.statuses["Autofill Note"][
                    "aggregated_errors"
                ].exceptions
            ),
        )
        self.assertEqual(0, vo.load_status_data.statuses["Autofill Note"]["num_errors"])
        self.assertEqual(
            1, vo.load_status_data.statuses["Autofill Note"]["num_warnings"]
        )
        self.assertEqual(
            "WARNING", vo.load_status_data.statuses["Autofill Note"]["state"]
        )
        self.assertEqual(
            "PASSED", vo.load_status_data.statuses["All Samples present"]["state"]
        )
        self.assertEqual(
            "PASSED", vo.load_status_data.statuses["All Tissues present"]["state"]
        )
        self.assertEqual(
            "PASSED", vo.load_status_data.statuses["All Treatments present"]["state"]
        )
        self.assertEqual("PASSED", vo.load_status_data.statuses["file1.xlsx"]["state"])
        self.assertEqual("PASSED", vo.load_status_data.statuses["file2.xlsx"]["state"])

    def test_extract_all_missing_samples(self):
        vo = DataValidationView()
        vo.extract_all_missing_samples(
            AllMissingSamplesError(
                {
                    "files_missing_all": {"accucor1.xlsx": ["s1", "s2"]},
                    "files_missing_some": {
                        "s3": ["accucor2.xlsx", "accucor3.xlsx"],
                        "s1": ["accucor4.xlsx"],
                    },
                    "all_missing_samples": {
                        "s1": ["accucor1.xlsx", "accucor4.xlsx"],
                        "s2": ["accucor1.xlsx"],
                        "s3": ["accucor2.xlsx", "accucor3.xlsx"],
                    },
                }
            )
        )
        expected = {
            vo.SAMPLES_SHEET: {
                "s1": {SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_NAME: "s1"},
                "s2": {SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_NAME: "s2"},
                "s3": {SampleTableLoader.DefaultSampleTableHeaders.SAMPLE_NAME: "s3"},
            },
            ProtocolsLoader.DataSheetName: {},
            TissuesLoader.DataSheetName: {},
        }
        self.assertDictEqual(expected, vo.autofill_dict)

    def test_extract_all_missing_tissues(self):
        vo = DataValidationView()
        vo.extract_all_missing_tissues(
            AllMissingTissues(
                [
                    MissingTissue("elbow pit"),
                    MissingTissue("earlobe"),
                ]
            )
        )
        expected = {
            vo.SAMPLES_SHEET: {},
            ProtocolsLoader.DataSheetName: {},
            TissuesLoader.DataSheetName: {
                "elbow pit": {TissuesLoader.DataHeaders.NAME: "elbow pit"},
                "earlobe": {TissuesLoader.DataHeaders.NAME: "earlobe"},
            },
        }
        self.assertDictEqual(expected, vo.autofill_dict)

    def test_extract_all_missing_treatments(self):
        vo = DataValidationView()
        vo.extract_all_missing_treatments(
            AllMissingTreatments(
                [
                    MissingTreatment("berated"),
                    MissingTreatment("wined-and-dined"),
                ]
            )
        )
        expected = {
            vo.SAMPLES_SHEET: {},
            ProtocolsLoader.DataSheetName: {
                "berated": {ProtocolsLoader.DataHeadersExcel.NAME: "berated"},
                "wined-and-dined": {
                    ProtocolsLoader.DataHeadersExcel.NAME: "wined-and-dined"
                },
            },
            TissuesLoader.DataSheetName: {},
        }
        self.assertDictEqual(expected, vo.autofill_dict)

    def test_add_extracted_autofill_data(self):
        """Asserts that extracted data is added to the dfs_dict.  This indirectly also tests add_autofill_data."""
        # Obtain a DataValidationView object containing errors
        vo = self.get_data_validation_object_with_errors()
        # Create the dfs_dict (to which data will be added)
        vo.dfs_dict = vo.create_study_dfs_dict()
        # Extract the errors into the autofill_dict (in the object)
        vo.extract_autofill_exceptions()
        # Add the extracted data to the dfs_dict
        vo.add_extracted_autofill_data()
        self.assertDictEqual(
            {
                "Animals": {
                    "Study Name": {},
                    "Study Description": {},
                    "Animal ID": {},
                    "Animal Body Weight": {},
                    "Animal Genotype": {},
                    "Animal Treatment": {},
                    "Age": {},
                    "Sex": {},
                    "Diet": {},
                    "Feeding Status": {},
                    "Infusate": {},
                    "Infusion Rate": {},
                    "Tracer Concentrations": {},
                },
                "Samples": {
                    "Animal ID": {0: None},
                    "Collection Time": {0: None},
                    "Date Collected": {0: None},
                    "Researcher Name": {0: None},
                    "Sample Name": {0: "s1"},  # ADDED
                    "Tissue": {0: None},
                },
                "Tissues": {
                    "Description": {0: None},
                    "Tissue": {0: "elbow pit"},  # ADDED
                },
                "Treatments": {
                    "Animal Treatment": {0: "wined-and-dined"},  # ADDED
                    "Treatment Description": {0: None},
                },
            },
            vo.dfs_dict,
        )

    def test_get_output_study_file(self):
        # TODO: Implement test once method fleshed out in step 11. of issue #829 in comment:
        # https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/829#issuecomment-2015852430
        pass
