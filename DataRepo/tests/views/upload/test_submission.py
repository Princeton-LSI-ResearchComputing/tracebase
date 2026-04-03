import base64
import os
from copy import deepcopy
from io import BytesIO

from django.core.exceptions import PermissionDenied
from django.core.management import call_command
from django.test import override_settings
from django.urls import reverse

from DataRepo.loaders import ProtocolsLoader, TissuesLoader
from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.compounds_loader import CompoundsLoader
from DataRepo.loaders.infusates_loader import InfusatesLoader
from DataRepo.loaders.lcprotocols_loader import LCProtocolsLoader
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.loaders.peak_annotation_files_loader import (
    PeakAnnotationFilesLoader,
)
from DataRepo.loaders.peak_group_conflicts import PeakGroupConflicts
from DataRepo.loaders.samples_loader import SamplesLoader
from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.loaders.studies_loader import StudiesLoader
from DataRepo.loaders.tracers_loader import TracersLoader
from DataRepo.models import Protocol, Tissue
from DataRepo.models.infusate import Infusate
from DataRepo.models.maintained_model import (
    MaintainedModel,
    UncleanBufferError,
)
from DataRepo.models.sample import Sample
from DataRepo.models.utilities import get_all_models
from DataRepo.tests.tracebase_test_case import TracebaseTransactionTestCase
from DataRepo.utils.exceptions import (
    AllMissingSamples,
    AllMissingTissues,
    AllMissingTreatments,
    MultiLoadStatus,
    PeakAnnotationFileConflict,
    RecordDoesNotExist,
)
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs
from DataRepo.views.upload.submission import BuildSubmissionView


class BuildSubmissionViewTests1(TracebaseTransactionTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

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

        dvv = BuildSubmissionView()
        dvv.study_file = None

        dfs_dict = dvv.get_or_create_dfs_dict()
        expected = {
            "Animals": {
                "Age": {},
                "Weight": {},
                "Genotype": {},
                "Animal Name": {},
                "Treatment": {},
                "Diet": {},
                "Feeding Status": {},
                "Infusate": {},
                "Infusion Rate": {},
                "Sex": {},
                "Study": {},
            },
            "Samples": {
                "Animal": {},
                "Collection Time": {},
                "Date Collected": {},
                "Researcher Name": {},
                "Sample": {},
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
            "Compounds": {
                "Compound": {},
                "Formula": {},
                "HMDB ID": {},
                "Synonyms": {},
            },
            "Study": {
                "Description": {},
                "Name": {},
            },
            "Tracers": {
                "Compound": {},
                "Element": {},
                "Label Count": {},
                "Label Positions": {},
                "Mass Number": {},
                "Tracer Name": {},
                "Tracer Row Group": {},
            },
            "Infusates": {
                "Infusate Name": {},
                "Infusate Row Group": {},
                "Tracer Concentration": {},
                "Tracer Group Name": {},
                "Tracer": {},
            },
            "LC Protocols": {
                "Description": str,
                "LC Protocol": str,
                "Name": str,
                "Run Length": int,
            },
            "Sequences": {
                "Date": str,
                "Instrument": str,
                "LC Protocol Name": str,
                "Notes": str,
                "Operator": str,
                "Sequence Name": str,
            },
            "Peak Annotation Details": {
                "Peak Annotation File Name": str,
                "Sample Data Header": str,
                "Sample Name": str,
                "Sequence": str,
                "Skip": bool,
                "mzXML File Name": str,
            },
            "Peak Annotation Files": {
                "Default Sequence": str,
                "File Format": str,
                "Peak Annotation File": str,
            },
            "Peak Group Conflicts": {
                "Peak Group Conflict": str,
                "Selected Peak Annotation File": str,
                "Common Sample Count": int,
                "Example Samples": str,
                "Common Samples": str,
            },
        }

        self.assert_dfs_dicts(expected, dfs_dict)

    def assert_dfs_dicts(self, expected, dfs_dict):
        self.assertEqual(
            len(expected.keys()),
            len(dfs_dict.keys()),
            msg=f"dfs_dict keys: {expected.keys()} =? {dfs_dict.keys()}",
        )
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
        dvv = BuildSubmissionView()
        dvv.study_file = (
            "DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table.xlsx"
        )
        dvv.all_infile_names = ["small_obob_animal_and_sample_table.xlsx"]

        dfs_dict = dvv.get_or_create_dfs_dict()
        expected = {
            "Animals": {
                "Age": {0: None},
                "Weight": {0: 26.3},
                "Genotype": {0: "WT"},
                "Animal Name": {0: "971"},
                "Treatment": {0: "obob_fasted"},
                "Diet": {0: None},
                "Feeding Status": {0: "Fasted"},
                "Infusate": {0: "lysine-[13C6][23.2]"},
                "Infusion Rate": {0: 0.11},
                "Sex": {0: None},
                "Study": {0: "Small OBOB"},
            },
            "Samples": {
                # These are repeating values, so I create them on the fly...
                "Animal": dict((i, "971") for i in range(16)),
                "Collection Time": dict((i, 150) for i in range(16)),
                "Date Collected": dict(
                    (i, "11/19/20") if i == 14 else (i, "2020-11-19") for i in range(16)
                ),
                "Researcher Name": dict((i, "Xianfeng Zeng") for i in range(16)),
                "Sample": {
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
                    1: "ob/ob homozygous mice were fasted",
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
                    30: "testicle",
                    31: "ovary",
                    32: "uterus",
                    33: "unspecified tumor - only use this if other information is unknown",
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
            "Compounds": {
                "Compound": {},
                "Formula": {},
                "HMDB ID": {},
                "Synonyms": {},
            },
            "Study": {
                "Description": {},
                "Name": {},
            },
            "Tracers": {
                "Compound": {},
                "Element": {},
                "Label Count": {},
                "Label Positions": {},
                "Mass Number": {},
                "Tracer Name": {},
                "Tracer Row Group": {},
            },
            "Infusates": {
                "Infusate Name": {},
                "Infusate Row Group": {},
                "Tracer Concentration": {},
                "Tracer Group Name": {},
                "Tracer": {},
            },
            "LC Protocols": {
                "Description": {},
                "LC Protocol": {},
                "Name": {},
                "Run Length": {},
            },
            "Sequences": {
                "Date": {},
                "Instrument": {},
                "LC Protocol Name": {},
                "Notes": {},
                "Operator": {},
                "Sequence Name": {},
            },
            "Peak Annotation Details": {
                "Peak Annotation File Name": {},
                "Sample Data Header": {},
                "Sample Name": {},
                "Sequence": {},
                "Skip": {},
                "mzXML File Name": {},
            },
            "Peak Annotation Files": {
                "Default Sequence": {},
                "File Format": {},
                "Peak Annotation File": {},
            },
            "Peak Group Conflicts": {
                "Peak Group Conflict": {},
                "Selected Peak Annotation File": {},
                "Common Sample Count": {},
                "Example Samples": {},
                "Common Samples": {},
            },
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
        dvv = BuildSubmissionView()
        # TODO: Eliminate the need for a dummy file (with an xls extension).  The protocol headers change for the
        # treatments sheet if it's an excel file.  The data is not needed - just the headers.
        dvv.study_file = "dummy.xlsx"
        expected = {
            "Animals": {
                "Animal Name": str,
                # "Age": int,
                "Diet": str,
                "Feeding Status": str,
                "Genotype": str,
                "Infusate": str,
                # "Infusion Rate": float,
                "Sex": str,
                "Study": str,
                "Treatment": str,
                # "Weight": float,
            },
            "Samples": {
                "Animal": str,
                # "Collection Time": float,
                "Researcher Name": str,
                "Sample": str,
                "Tissue": str,
            },
            "Treatments": {
                "Treatment Description": str,
                "Category": str,
                "Animal Treatment": str,
            },
            "Tissues": {
                "Description": str,
                "Tissue": str,
            },
            "Study": {
                "Description": str,
                "Name": str,
            },
            "Compounds": {
                "Compound": str,
                "HMDB ID": str,
                "Formula": str,
                "Synonyms": str,
            },
            "Tracers": {
                "Compound": str,
                "Element": str,
                # "Label Count": int,
                "Label Positions": str,
                # "Mass Number": int,
                "Tracer Name": str,
                # "Tracer Row Group": int,
            },
            "Infusates": {
                "Infusate Name": str,
                # "Infusate Row Group": int,
                # "Tracer Concentration": float,
                "Tracer Group Name": str,
                "Tracer": str,
            },
            "LC Protocols": {
                "Description": str,
                "LC Protocol": str,
                "Name": str,
                # "Run Length": int,
            },
            "Sequences": {
                "Date": str,
                "Instrument": str,
                "LC Protocol Name": str,
                "Notes": str,
                "Operator": str,
                "Sequence Name": str,
            },
            "Peak Annotation Details": {
                "Peak Annotation File Name": str,
                "Sample Data Header": str,
                "Sample Name": str,
                "Sequence": str,
                "Skip": str,
                "mzXML File Name": str,
            },
            "Peak Annotation Files": {
                "Default Sequence": str,
                "File Format": str,
                "Peak Annotation File": str,
            },
            "Peak Group Conflicts": {
                "Common Samples": str,
                "Example Samples": str,
                "Peak Group Conflict": str,
                "Selected Peak Annotation File": str,
            },
        }
        self.assertDictEqual(expected, dvv.get_study_dtypes_dict())

    def get_data_validation_object_with_errors(self):
        vo = BuildSubmissionView()
        vo.load_status_data = MultiLoadStatus(
            load_keys=[
                "All Samples present",
                "All Tissues present",
                "All Treatments present",
            ]
        )
        amse_err = AllMissingSamples([RecordDoesNotExist(Sample, {"name": "s1"})])
        amse_err.is_error = True
        amse_warn = AllMissingSamples([RecordDoesNotExist(Sample, {"name": "s1"})])
        amse_warn.is_error = False
        amse_warn2 = AllMissingSamples([RecordDoesNotExist(Sample, {"name": "s1"})])
        amse_warn2.is_error = False

        amti_err = AllMissingTissues(
            [RecordDoesNotExist(Tissue, {"name": "elbow pit"})]
        )
        amti_err.is_error = True
        amti_warn = AllMissingTissues(
            [RecordDoesNotExist(Tissue, {"name": "elbow pit"})]
        )
        amti_warn.is_error = False
        amti_warn2 = AllMissingTissues(
            [RecordDoesNotExist(Tissue, {"name": "elbow pit"})]
        )
        amti_warn2.is_error = False

        amtr_err = AllMissingTreatments(
            [RecordDoesNotExist(Protocol, {"name": "wined-and-dined"})]
        )
        amtr_err.is_error = True
        amtr_warn = AllMissingTreatments(
            [RecordDoesNotExist(Protocol, {"name": "wined-and-dined"})]
        )
        amtr_warn.is_error = False
        amtr_warn2 = AllMissingTreatments(
            [RecordDoesNotExist(Protocol, {"name": "wined-and-dined"})]
        )
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

        vo.extract_autofill_from_exceptions()

        self.assertEqual(
            # The 1 error is modified to a warning because it gets fixed/autofilled
            0,
            len(vo.extracted_exceptions[AllMissingSamples.__name__]["errors"]),
        )
        self.assertEqual(
            # The 1 error is modified to a warning because it gets fixed/autofilled
            3,
            len(vo.extracted_exceptions[AllMissingSamples.__name__]["warnings"]),
        )
        self.assertEqual(
            # The 1 error is modified to a warning because it gets fixed/autofilled
            0,
            len(vo.extracted_exceptions[AllMissingTissues.__name__]["errors"]),
        )
        self.assertEqual(
            # The 1 error is modified to a warning because it gets fixed/autofilled
            3,
            len(vo.extracted_exceptions[AllMissingTissues.__name__]["warnings"]),
        )
        self.assertEqual(
            # The 1 error is modified to a warning because it gets fixed/autofilled
            0,
            len(vo.extracted_exceptions[AllMissingTreatments.__name__]["errors"]),
        )
        self.assertEqual(
            # The 1 error is modified to a warning because it gets fixed/autofilled
            3,
            len(vo.extracted_exceptions[AllMissingTreatments.__name__]["warnings"]),
        )
        self.assertDictEqual(
            {
                "Samples": {"s1": {"Sample": "s1"}},
                "Tissues": {"elbow pit": {"Tissue": "elbow pit"}},
                "Treatments": {
                    "wined-and-dined": {"Animal Treatment": "wined-and-dined"}
                },
                "Compounds": {},
                "Study": {},
                "Animals": {},
                "Tracers": {},
                "Infusates": {},
                "LC Protocols": {},
                "Sequences": {},
                "Peak Annotation Details": {},
                "Peak Annotation Files": {},
                "Peak Group Conflicts": {},
            },
            vo.autofill_dict,
        )
        self.assertIn("All Samples present", vo.load_status_data.statuses.keys())
        self.assertIn("All Tissues present", vo.load_status_data.statuses.keys())
        self.assertIn("All Treatments present", vo.load_status_data.statuses.keys())
        self.assertEqual(
            "WARNING", vo.load_status_data.statuses["All Samples present"]["state"]
        )
        self.assertEqual(
            "WARNING", vo.load_status_data.statuses["All Tissues present"]["state"]
        )
        self.assertEqual(
            "WARNING", vo.load_status_data.statuses["All Treatments present"]["state"]
        )
        self.assertEqual("WARNING", vo.load_status_data.statuses["file1.xlsx"]["state"])
        self.assertEqual("WARNING", vo.load_status_data.statuses["file2.xlsx"]["state"])

    def test_extract_all_missing_samples(self):
        vo = BuildSubmissionView()
        vo.extract_all_missing_values(
            AllMissingSamples(
                [
                    RecordDoesNotExist(Sample, {"name": "s1"}, file="accucor1.xlsx"),
                    RecordDoesNotExist(Sample, {"name": "s2"}, file="accucor1.xlsx"),
                    RecordDoesNotExist(Sample, {"name": "s3"}, file="accucor2.xlsx"),
                    RecordDoesNotExist(Sample, {"name": "s3"}, file="accucor3.xlsx"),
                    RecordDoesNotExist(Sample, {"name": "s1"}, file="accucor4.xlsx"),
                ]
            ),
            "Samples",
            "Sample",
        )
        expected = {
            SamplesLoader.DataSheetName: {
                "s1": {SamplesLoader.DataHeaders.SAMPLE: "s1"},
                "s2": {SamplesLoader.DataHeaders.SAMPLE: "s2"},
                "s3": {SamplesLoader.DataHeaders.SAMPLE: "s3"},
            },
            ProtocolsLoader.DataSheetName: {},
            TissuesLoader.DataSheetName: {},
            CompoundsLoader.DataSheetName: {},
            StudiesLoader.DataSheetName: {},
            AnimalsLoader.DataSheetName: {},
            TracersLoader.DataSheetName: {},
            InfusatesLoader.DataSheetName: {},
            LCProtocolsLoader.DataSheetName: {},
            SequencesLoader.DataSheetName: {},
            MSRunsLoader.DataSheetName: {},
            PeakAnnotationFilesLoader.DataSheetName: {},
            PeakGroupConflicts.DataSheetName: {},
        }
        self.assertDictEqual(expected, vo.autofill_dict)

    def test_extract_all_missing_tissues(self):
        vo = BuildSubmissionView()
        vo.extract_all_missing_values(
            AllMissingTissues(
                [
                    RecordDoesNotExist(Tissue, {"name": "elbow pit"}),
                    RecordDoesNotExist(Tissue, {"name": "earlobe"}),
                ]
            ),
            "Tissues",
            "Tissue",
        )
        expected = {
            CompoundsLoader.DataSheetName: {},
            SamplesLoader.DataSheetName: {},
            ProtocolsLoader.DataSheetName: {},
            TissuesLoader.DataSheetName: {
                "elbow pit": {TissuesLoader.DataHeaders.NAME: "elbow pit"},
                "earlobe": {TissuesLoader.DataHeaders.NAME: "earlobe"},
            },
            StudiesLoader.DataSheetName: {},
            AnimalsLoader.DataSheetName: {},
            TracersLoader.DataSheetName: {},
            InfusatesLoader.DataSheetName: {},
            LCProtocolsLoader.DataSheetName: {},
            SequencesLoader.DataSheetName: {},
            MSRunsLoader.DataSheetName: {},
            PeakAnnotationFilesLoader.DataSheetName: {},
            PeakGroupConflicts.DataSheetName: {},
        }
        self.assertDictEqual(expected, vo.autofill_dict)

    def test_extract_all_missing_treatments(self):
        vo = BuildSubmissionView()
        vo.extract_all_missing_values(
            AllMissingTreatments(
                [
                    RecordDoesNotExist(Protocol, {"name": "berated"}),
                    RecordDoesNotExist(Protocol, {"name": "wined-and-dined"}),
                ]
            ),
            "Treatments",
            "Animal Treatment",
        )
        expected = {
            CompoundsLoader.DataSheetName: {},
            SamplesLoader.DataSheetName: {},
            ProtocolsLoader.DataSheetName: {
                "berated": {ProtocolsLoader.DataHeadersExcel.NAME: "berated"},
                "wined-and-dined": {
                    ProtocolsLoader.DataHeadersExcel.NAME: "wined-and-dined"
                },
            },
            TissuesLoader.DataSheetName: {},
            StudiesLoader.DataSheetName: {},
            AnimalsLoader.DataSheetName: {},
            TracersLoader.DataSheetName: {},
            InfusatesLoader.DataSheetName: {},
            LCProtocolsLoader.DataSheetName: {},
            SequencesLoader.DataSheetName: {},
            MSRunsLoader.DataSheetName: {},
            PeakAnnotationFilesLoader.DataSheetName: {},
            PeakGroupConflicts.DataSheetName: {},
        }
        self.assertDictEqual(expected, vo.autofill_dict)

    def test_extract_all_missing_values_does_not_overwrite(self):
        """This tests that the extract_all_missing_values method does not add autofill data from an exception if a row
        for that sample already exists - and that the following call to add_extracted_autofill_data does not wipe out
        the data already in the dfs_dict."""
        vo = BuildSubmissionView()
        vo.autofill_only_mode = False
        # Set the data for the samples sheet as if it had been read from a submitted sheet
        vo.dfs_dict["Samples"] = {
            "Sample": {0: "s1"},
            "Date Collected": {0: "2025-01-04"},
            "Researcher Name": {0: "Howard the Duck"},
            "Tissue": {0: "bill"},
            "Collection Time": {0: 0},
            "Animal": {0: "duck1"},
        }
        expected_dfs_dict_samples = deepcopy(vo.dfs_dict["Samples"])
        vo.extract_all_missing_values(
            AllMissingSamples(
                [RecordDoesNotExist(Sample, {"name": "s1"}, file="accucor1.xlsx")]
            ),
            "Samples",
            "Sample",
        )
        self.assertEqual(
            0, len([d for d in vo.autofill_dict.values() if len(d.keys()) > 0])
        )
        vo.add_extracted_autofill_data()
        self.assertDictEqual(expected_dfs_dict_samples, vo.dfs_dict["Samples"])

    def get_autofilled_study_dfs_dict(self):
        return {
            "Animals": {
                "Study": {},
                "Animal Name": {},
                "Weight": {},
                "Genotype": {},
                "Treatment": {},
                "Age": {},
                "Sex": {},
                "Diet": {},
                "Feeding Status": {},
                "Infusate": {},
                "Infusion Rate": {},
            },
            "Samples": {
                "Animal": {0: None},
                "Collection Time": {0: None},
                "Date Collected": {0: None},
                "Researcher Name": {0: None},
                "Sample": {0: "s1"},  # ADDED
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
            "Compounds": {
                "Compound": {},
                "Formula": {},
                "HMDB ID": {},
                "Synonyms": {},
            },
            "Study": {
                "Description": {},
                "Name": {},
            },
            "Tracers": {
                "Compound": {},
                "Element": {},
                "Label Count": {},
                "Label Positions": {},
                "Mass Number": {},
                "Tracer Name": {},
                "Tracer Row Group": {},
            },
            "Infusates": {
                "Infusate Name": {},
                "Infusate Row Group": {},
                "Tracer Concentration": {},
                "Tracer Group Name": {},
                "Tracer": {},
            },
            "LC Protocols": {
                "Description": {},
                "LC Protocol": {},
                "Name": {},
                "Run Length": {},
            },
            "Sequences": {
                "Date": {},
                "Instrument": {},
                "LC Protocol Name": {},
                "Notes": {},
                "Operator": {},
                "Sequence Name": {},
            },
            "Peak Annotation Details": {
                "Peak Annotation File Name": {},
                "Sample Data Header": {},
                "Sample Name": {},
                "Sequence": {},
                "Skip": {},
                "mzXML File Name": {},
            },
            "Peak Annotation Files": {
                "Default Sequence": {},
                "File Format": {},
                "Peak Annotation File": {},
            },
            "Peak Group Conflicts": {
                "Peak Group Conflict": {},
                "Selected Peak Annotation File": {},
                "Common Sample Count": {},
                "Example Samples": {},
                "Common Samples": {},
            },
        }

    def test_add_extracted_autofill_data(self):
        """Asserts that extracted data is added to the dfs_dict.  This indirectly also tests add_autofill_data."""
        # Obtain a DataValidationView object containing errors
        vo = self.get_data_validation_object_with_errors()
        # Create the dfs_dict (to which data will be added)
        vo.dfs_dict = vo.create_or_repair_study_dfs_dict()
        # Extract the errors into the autofill_dict (in the object)
        vo.extract_autofill_from_exceptions()

        # Artificially turn off autofill-only mode so we can test the "Autofill Note"
        vo.autofill_only_mode = False

        # Add the extracted data to the dfs_dict
        vo.add_extracted_autofill_data()
        self.assertDictEqual(
            self.get_autofilled_study_dfs_dict(),
            vo.dfs_dict,
        )

        # Check that add_autofill_data added the autofill note
        self.assertIn("Autofill Note", vo.load_status_data.statuses.keys())
        self.assertEqual(
            1,
            len(
                vo.load_status_data.statuses["Autofill Note"][
                    "aggregated_errors"
                ].exceptions
            ),
        )
        self.assertEqual(
            0,
            vo.load_status_data.statuses["Autofill Note"][
                "aggregated_errors"
            ].num_errors,
        )
        self.assertEqual(
            1,
            vo.load_status_data.statuses["Autofill Note"][
                "aggregated_errors"
            ].num_warnings,
        )
        self.assertEqual(
            "WARNING", vo.load_status_data.statuses["Autofill Note"]["state"]
        )

    def get_expected_autofill_dict(self, annotf_name: str):
        return {
            "Compounds": {
                "Glycine": {
                    "Compound": "Glycine",
                    "Formula": "C2H5NO2",
                },
                "Serine": {
                    "Compound": "Serine",
                    "Formula": "C3H7NO3",
                },
            },
            "Animals": {},
            "Samples": {
                "072920_XXX1_1_TS1": {"Sample": "072920_XXX1_1_TS1"},
                "072920_XXX1_2_bra": {"Sample": "072920_XXX1_2_bra"},
            },
            "Tissues": {},
            "Treatments": {},
            "Study": {},
            "Tracers": {},
            "Infusates": {},
            "LC Protocols": {},
            "Sequences": {},
            "Peak Annotation Details": {
                f"072920_XXX1_1_TS1__DELIM__{annotf_name}": {
                    "Peak Annotation File Name": annotf_name,
                    "Sample Data Header": "072920_XXX1_1_TS1",
                    "Sample Name": "072920_XXX1_1_TS1",
                    "Sequence": None,
                    "Skip": None,
                },
                f"072920_XXX1_2_bra__DELIM__{annotf_name}": {
                    "Peak Annotation File Name": annotf_name,
                    "Sample Data Header": "072920_XXX1_2_bra",
                    "Sample Name": "072920_XXX1_2_bra",
                    "Sequence": None,
                    "Skip": None,
                },
                f"blank_1_404020__DELIM__{annotf_name}": {
                    "Peak Annotation File Name": annotf_name,
                    "Sample Data Header": "blank_1_404020",
                    "Sample Name": "blank_1_404020",
                    "Sequence": None,
                    "Skip": "skip",
                },
            },
            "Peak Annotation Files": {
                annotf_name: {
                    "File Format": "accucor",
                    "Peak Annotation File": annotf_name,
                    "Default Sequence": None,
                },
            },
            "Peak Group Conflicts": {},
        }

    def test_extract_autofill_from_peak_annotation_files(self):
        dvv = BuildSubmissionView()
        dvv.set_files(
            peak_annot_files=["DataRepo/data/tests/data_submission/accucor1.xlsx"]
        )
        dvv.extract_autofill_from_peak_annotation_files_forms()
        self.assertDictEqual(
            self.get_expected_autofill_dict("accucor1.xlsx"), dvv.autofill_dict
        )

    def test_catch_peak_annotation_file_edits(self):
        """This test asserts that previously loaded peak annotation files that have been edited (and resubmitted in a
        supplemental load) are reported as PeakAnnotationFileConflict exceptions.
        """
        from DataRepo.models import ArchiveFile, DataFormat, DataType

        # Prepare to create an "edited" file (loaded and being loaded)
        ms_peak_annotation = DataType.objects.get(code="ms_peak_annotation")
        accucor_format = DataFormat.objects.get(code="accucor")

        # The annot file saved in this ArchiveFile record should not match the same named file submitted below
        ArchiveFile.objects.create(
            filename="accucor1.xlsx",
            file_location=None,
            checksum="558ea654d7f2914ca4527580edf4fac11bd151d0",
            data_type=ms_peak_annotation,
            data_format=accucor_format,
        )

        dvv = BuildSubmissionView()
        dvv.set_files(
            peak_annot_files=["DataRepo/data/tests/data_submission/accucor1.xlsx"]
        )
        self.assertEqual(
            1,
            len(
                dvv.load_status_data.statuses["accucor1.xlsx"][
                    "aggregated_errors"
                ].exceptions
            ),
        )
        self.assertIsInstance(
            dvv.load_status_data.statuses["accucor1.xlsx"][
                "aggregated_errors"
            ].exceptions[0],
            PeakAnnotationFileConflict,
        )

    def test_extract_autofill_from_peak_annotation_file_with_elmaven_modified_compounds(
        self,
    ):
        """Tests that the autofill dict has the " (1)" strings removed"""
        dvv = BuildSubmissionView()
        dvv.set_files(
            peak_annot_files=[
                "DataRepo/data/tests/data_submission/accucor3_with_elmaven_compound_numbers.xlsx"
            ]
        )
        dvv.extract_autofill_from_peak_annotation_files_forms()
        self.assertDictEqual(
            self.get_expected_autofill_dict(
                "accucor3_with_elmaven_compound_numbers.xlsx"
            ),
            dvv.autofill_dict,
        )

    def test_determine_study_file_readiness_no_peak_files(self):
        dvv = BuildSubmissionView()
        dvv.set_files(
            study_file="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table.xlsx",
            study_filename=None,
            peak_annot_files=None,
            peak_annot_filenames=None,
        )
        # Should be true because no peak annotation files, so all there is to do is validation of the sample study doc
        self.assertTrue(dvv.determine_study_file_validation_readiness())

    def test_determine_study_file_readiness_study_file_with_sample_names_only(self):
        dvv = BuildSubmissionView()
        dvv.study_file = "study.xlsx"  # Invalid, but does not matter
        dvv.peak_annot_files = ["accucor.xlsx"]  # Invalid, but does not matter
        dvv.dfs_dict = self.get_autofilled_study_dfs_dict()
        # Should be false because no manually fleshed data (nothing to validate)
        self.assertFalse(dvv.determine_study_file_validation_readiness())

    def test_determine_study_file_readiness_fleshed_study_file(self):
        dvv = BuildSubmissionView()
        dvv.study_file = "study.xlsx"  # Invalid, but does not matter
        dvv.peak_annot_files = ["accucor.xlsx"]  # Invalid, but does not matter
        dvv.dfs_dict = self.get_autofilled_study_dfs_dict()
        # Flesh it (with a single manually entered value into an optional column)
        dvv.dfs_dict["Samples"]["Collection Time"] = {0: 90}
        # Should be true because manually added data (Collection Time) in addition to the sample names
        self.assertTrue(dvv.determine_study_file_validation_readiness())

    def test_create_study_file_writer(self):
        # This also tests annotate_study_excel and dfs_dict_is_valid (indirectly)
        dvv = BuildSubmissionView()
        study_stream = BytesIO()
        xlsxwriter = dvv.create_study_file_writer(study_stream)
        dvv.annotate_study_excel(xlsxwriter)
        xlsxwriter.close()
        self.assertEqual(0, len(base64.b64encode(study_stream.read()).decode("utf-8")))

    def test_header_to_cell(self):
        dvv = BuildSubmissionView()

        # Get cell location success
        result = dvv.header_to_cell("Animals", "Age")
        self.assertEqual("B1", result)

        # Get column letter success
        result2 = dvv.header_to_cell("Animals", "Age", letter_only=True)
        self.assertEqual("B", result2)

        # Invalid column name
        with self.assertRaises(ValueError):
            result2 = dvv.header_to_cell("Animals", "Invalid")

        # Invalid sheet name
        with self.assertRaises(ValueError):
            result2 = dvv.header_to_cell("Invalid", "Age")

    @override_settings(READONLY=True)
    def test_readonly_exception(self):
        with self.assertRaises(PermissionDenied):
            BuildSubmissionView()

    def test_get_existing_dfs_index(self):
        # TODO: Implement test
        pass


def assert_coordinator_state_is_initialized():
    # Obtain all coordinators that exist
    all_coordinators = [MaintainedModel._get_default_coordinator()]
    all_coordinators.extend(MaintainedModel._get_coordinator_stack())
    if 1 != len(all_coordinators):
        raise ValueError(
            f"Before setting up test data, there are {len(all_coordinators)} (not 1) MaintainedModelCoordinators."
        )
    if all_coordinators[0].auto_update_mode != "always":
        raise ValueError(
            "Before setting up test data, the default coordinator is not in always autoupdate mode."
        )
    if 0 != all_coordinators[0].buffer_size():
        raise UncleanBufferError()


class BuildSubmissionViewTests2(TracebaseTransactionTestCase):
    """
    Note, without the TransactionTestCase (derived) class (and the with transaction.atomic block below), the infusate-
    related model managers produce the following error:
        django.db.transaction.TransactionManagementError: An error occurred in the current transaction. You can't
        execute queries until the end of the 'atomic' block.
    ...associated with the outer atomic transaction of any normal test case.  See:
    https://stackoverflow.com/questions/21458387/transactionmanagementerror-you-cant-execute-queries-until-the-end-of-the-atom
    """

    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    def assert_coordinator_state_is_initialized(
        self, msg="MaintainedModelCoordinators are in the default state."
    ):
        # Obtain all coordinators that exist
        all_coordinators = [MaintainedModel._get_default_coordinator()]
        all_coordinators.extend(MaintainedModel._get_coordinator_stack())
        # Make sure there is only the default coordinator
        self.assertEqual(
            1, len(all_coordinators), msg=msg + "  The coordinator_stack is empty."
        )
        # Make sure that its mode is "always"
        self.assertEqual(
            "always",
            all_coordinators[0].auto_update_mode,
            msg=msg + "  Mode should be 'always'.",
        )
        # Make sure that the buffer is empty to start
        for coordinator in all_coordinators:
            self.assertEqual(
                0, coordinator.buffer_size(), msg=msg + "  The buffer is empty."
            )

    @classmethod
    def initialize_databases(cls):
        # Ensure the auto-update buffer is empty.  If it's not, then a previously run test didn't clean up after itself
        assert_coordinator_state_is_initialized()

        call_command("loaddata", "lc_methods", "data_types", "data_formats")
        call_command(
            "load_tissues",
            infile="DataRepo/data/tests/tissues/tissues.tsv",
        )
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/consolidated_tracebase_compound_list.tsv",
        )

    @classmethod
    def clear_database(cls):
        """
        Clears out the contents of the supplied database and confirms it's empty.
        """
        # Note that get_all_models is implemented to return the models in an order that facilitates this deletion
        for mdl in get_all_models():
            mdl.objects.all().delete()
        # Make sure the database is actually empty so that the tests are meaningful
        sum = cls.sum_record_counts()
        assert sum == 0

    @classmethod
    def sum_record_counts(cls):
        record_counts = cls.get_record_counts()
        sum = 0
        for cnt in record_counts:
            sum += cnt
        return sum

    @classmethod
    def get_record_counts(cls):
        record_counts = []
        for mdl in get_all_models():
            record_counts.append(mdl.objects.all().count())
        return record_counts

    def test_validate_view(self):
        """
        Do a simple validation view test
        """
        response = self.client.get(reverse("submission"))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "submission/submission.html")

    def test_validate_files_good(self):
        """
        Do a file validation test
        """
        # Files/inputs we will test
        sf = "DataRepo/data/tests/data_submission/animal_sample_good_v3.xlsx"
        afs = [
            "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "DataRepo/data/tests/data_submission/accucor2.xlsx",
        ]

        sfkey = "animal_sample_good_v3.xlsx"
        af1key = "accucor1.xlsx"
        af2key = "accucor2.xlsx"

        # Test the get_validation_results function
        # This call indirectly tests that ValidationView.validate_stody returns a MultiLoadStatus object on success
        # It also indirectly ensures that create_yaml(dir) puts a loading.yaml file in the dir
        [results, valid, exceptions, _, _] = self.validate_some_files(sf, afs)

        # There is a researcher named "anonymous", but that name is ignored
        self.assertTrue(
            valid, msg=f"There should be no errors in any file: {exceptions}"
        )

        # The sample file's researcher is "Anonymous" and it's not in the database, but the researcher check ignores
        # researchers named "anonymous" (case-insensitive)
        self.assertEqual("PASSED", results[sfkey])
        self.assertEqual(0, len(exceptions[sfkey]))

        # Check the accucor file details
        self.assert_accucor_files_pass([af1key, af2key], results, exceptions)

    # TODO: Uncomment when the peak annotation details is included in validation (cannot have a successful load without
    # TODO: MSRunSample records
    @override_settings(DEBUG=True)
    def test_validate_files_with_sample_warning(self):
        """
        Do a file validation test
        """
        call_command("loaddata", "data_types", "data_formats")
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_no_newsample.xlsx",
            exclude_sheets=["Peak Annotation Files"],
        )
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx",
        )

        # Files/inputs we will test
        sf = "DataRepo/data/tests/data_submission/animal_sample_unknown_researcher_v3.xlsx"
        afs = [
            "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "DataRepo/data/tests/data_submission/accucor2.xlsx",
        ]

        sfkey = "animal_sample_unknown_researcher_v3.xlsx"
        af1key = "accucor1.xlsx"
        af2key = "accucor2.xlsx"

        # Test the get_validation_results function
        [
            results,
            valid,
            exceptions,
            num_errors,
            num_warnings,
        ] = self.validate_some_files(sf, afs)

        # NOTE: When the unknown researcher error is raised, the sample table load would normally be rolled back.  The
        # subsequent accucor load would then fail (to find any more errors), because it can't find the same names in
        # the database.  Sample table loader needs to raise the exception to communicate the issues to the validate
        # interface, so in validation mode, it raises the exception outside of the atomic transaction block, which
        # won't rollback the erroneous load, so the validation code wraps everything in an outer atomic transaction
        # and rolls back everything at the end.

        # There is a researcher named "George Costanza" that should be unknown, making the overall status false.  Any
        # error or warning will cause is_valid to be false
        self.assertFalse(
            valid,
            msg=(
                "Should be valid. The 'George Costanza' researcher should cause a warning, so there should be 1 "
                f"warning: [{exceptions}] for the sample file."
            ),
        )

        # The sample file's researcher is "Anonymous" and it's not in the database, but the researcher check ignores
        # researchers named "anonymous" (case-insensitive)
        self.assertEqual(
            "WARNING",
            results[sfkey],
            msg=f"There should only be 1 warning for file {sfkey}: {exceptions[sfkey]}",
        )
        self.assertEqual(0, num_errors[sfkey])
        self.assertEqual(1, num_warnings[sfkey])
        self.assertEqual("NewResearchers", exceptions[sfkey][0]["type"])

        # Check the accucor file details
        self.assert_accucor_files_pass([af1key, af2key], results, exceptions)

    def test_databases_unchanged(self):
        """
        Test to ensure that validating user submitted data does not change the database
        """
        # Get initial record counts for all models
        tb_init_counts = self.get_record_counts()
        pre_load_maintained_values = MaintainedModel.get_all_maintained_field_values(
            "DataRepo.models"
        )

        sample_file = "DataRepo/data/tests/data_submission/animal_sample_good_v3.xlsx"
        accucor_files = [
            "DataRepo/data/tests/data_submission/accucor1.xlsx",
            "DataRepo/data/tests/data_submission/accucor2.xlsx",
        ]

        [
            _,
            valid,
            _,
            _,
            _,
        ] = self.validate_some_files(sample_file, accucor_files)

        # Test case is for passing data, so it only works if it passes
        self.assertTrue(valid)

        # Get record counts for all models
        tb_post_counts = self.get_record_counts()
        post_load_maintained_values = MaintainedModel.get_all_maintained_field_values(
            "DataRepo.models"
        )

        self.assertListEqual(tb_init_counts, tb_post_counts)
        self.assertEqual(pre_load_maintained_values, post_load_maintained_values)

    def test_accucor_validation_error(self):
        self.clear_database()
        self.initialize_databases()

        # No infusates sheet included, so creating the infusate
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name_with_concs("lysine-[13C6][23.2]")
        )

        # The study doc has 12 samples
        sample_file = (
            "DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table.xlsx"
        )
        # The accucor file intentionally has 1 sample that's not in the study doc
        accucor_files = [
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf_req_prefix.xlsx",
        ]
        sfkey = "small_obob_animal_and_sample_table.xlsx"
        afkey = "small_obob_maven_6eaas_inf_req_prefix.xlsx"
        [
            results,
            valid,
            exceptions,
            num_errors,
            num_warnings,
        ] = self.validate_some_files(sample_file, accucor_files)

        # Sample file should be OK.  It's loaded first.
        self.assertTrue(sfkey in results)
        self.assertEqual(
            "PASSED",
            results[sfkey],
            msg=f"There should be no exceptions for file {sfkey}: {exceptions[sfkey]}",
        )

        self.assertTrue(sfkey in exceptions)
        self.assertEqual(0, num_errors[sfkey])
        self.assertEqual(0, num_warnings[sfkey])

        # Accucor file will have an error about a missing sample that will have been converted into a warning (because
        # it would have been converted to an AllMissingSamples error in an exception group), but it will also have an
        # error about headers that the Peak AnnotationDetails sheet says are in this file, but they were not found.
        # That will remain an error.
        self.assertTrue(afkey in results)
        self.assertEqual("FAILED", results[afkey])

        self.assertTrue(
            afkey in exceptions,
            msg=f"{afkey} should be a key in the exceptions dict.  Its keys are: {exceptions.keys()}",
        )
        self.assertEqual(1, num_errors[afkey])
        self.assertEqual(
            1, num_warnings[afkey], msg=f"All exceptions: {exceptions[afkey]}"
        )
        self.assertEqual("NoSamples", exceptions[afkey][0]["type"])
        self.assertFalse(exceptions[afkey][0]["is_error"])
        self.assertEqual("UnexpectedSamples", exceptions[afkey][1]["type"])
        self.assertTrue(exceptions[afkey][1]["is_error"])

        # All samples in sample table combined error
        groupkey = "Peak Annotation Samples Check"
        self.assertTrue(groupkey in results)
        self.assertEqual("FAILED", results[groupkey])

        self.assertTrue(
            groupkey in exceptions,
            msg=f"{groupkey} should be a key in the exceptions dict.  Its keys are: {exceptions.keys()}",
        )
        self.assertEqual(1, num_errors[groupkey])
        self.assertEqual("AllMissingSamples", exceptions[groupkey][0]["type"])
        self.assertEqual(0, num_warnings[groupkey])

        expected_status_keys = set(
            [
                "small_obob_animal_and_sample_table.xlsx",
                "Studies Check",
                "Samples Check",
                "Peak Annotation Samples Check",
                "Peak Annotation Blanks Check",
                "Tissues Check",
                "Treatments Check",
                "Compounds Check",
                "Peak Groups Check",
                "Contamination Check",
                "small_obob_maven_6eaas_inf_req_prefix.xlsx",
            ]
        )

        self.assertEqual(expected_status_keys, set(results.keys()))
        self.assertEqual(expected_status_keys, set(exceptions.keys()))
        self.assertEqual(expected_status_keys, set(num_errors.keys()))
        self.assertEqual(expected_status_keys, set(num_warnings.keys()))

        self.assertFalse(valid)

    def validate_some_files(self, sample_file, accucor_files):
        # Test the get_validation_results function
        vo = BuildSubmissionView()
        vo.set_files(study_file=sample_file, peak_annot_files=accucor_files)
        # Now try validating the load files
        vo.validate_study()
        vo.format_results_for_template()
        valid = vo.valid
        results = vo.results
        exceptions = vo.exceptions

        file_keys = []
        file_keys.append(os.path.basename(sample_file))
        for afile in accucor_files:
            file_keys.append(os.path.basename(afile))

        for file_key in [os.path.basename(f) for f in [sample_file, *accucor_files]]:
            self.assertIn(file_key, results)
            self.assertIn(file_key, exceptions)

        num_errors = {}
        num_warnings = {}
        for file in exceptions.keys():
            num_errors[file] = 0
            num_warnings[file] = 0
            for exc in exceptions[file]:
                if exc["is_error"]:
                    num_errors[file] += 1
                else:
                    num_warnings[file] += 1

        return results, valid, exceptions, num_errors, num_warnings

    def assert_accucor_files_pass(self, accucor_file_keys, results, exceptions):
        for afkey in accucor_file_keys:
            # There may be missing samples, but they should be ignored if they contain the substring "blank".  (The
            # user should not be bothered with a warning they cannot do anything about.)  We are checking in validate
            # mode, but if we weren't, an exception would have been raised.
            self.assertTrue(afkey in results)
            self.assertEqual("PASSED", results[afkey])

            self.assertTrue(afkey in exceptions)
            self.assertEqual(0, len(exceptions[afkey]))

    def test_fill_missing_from_db(self):
        # TODO: Implement test
        pass

    def test_fill_missing_compound_data_from_db(self):
        # TODO: Implement test
        pass

    def test_add_dynamic_dropdown_data(self):
        # TODO: Implement test
        pass

    def test_add_dynamic_dropdown_tracer_data(self):
        # TODO: Implement test
        pass

    def test_add_dynamic_dropdown_infusate_data(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_files(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_study_doc(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_animals_sheet(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_samples_sheet(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_peak_annot_files_sheet(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_peak_annot_details_sheet(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_infusates_sheet(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_tracers_sheet(self):
        # TODO: Implement test
        pass

    def test_init_row_group_nums(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_infusate_data(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_tracer_data(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_compound_name(self):
        # TODO: Implement test
        pass

    def test_get_existing_compound_primary_name(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_sequence_name(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_lcprotocol_name(self):
        # TODO: Implement test
        pass

    def test_extract_autofill_from_sequences_sheet(self):
        # TODO: Implement test
        pass

    def test_add_dropdowns(self):
        # TODO: Implement test
        pass

    def test_add_static_dropdowns(self):
        # TODO: Implement test
        pass

    def test_add_formulas(self):
        # TODO: Implement test
        pass

    def test_create_format_objects(self):
        # TODO: Implement test
        pass
