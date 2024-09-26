from copy import deepcopy
from datetime import timedelta

import pandas as pd

from DataRepo.loaders.samples_loader import SamplesLoader
from DataRepo.models import Animal, Compound, Infusate
from DataRepo.models.fcirc import FCirc
from DataRepo.models.sample import Sample
from DataRepo.models.tissue import Tissue
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    ConflictingValueError,
    InfileError,
    MissingTissues,
    NewResearcher,
    RecordDoesNotExist,
    RollbackException,
)
from DataRepo.utils.file_utils import string_to_date
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs


class SamplesLoaderTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        Compound.objects.create(
            name="Leucine", formula="C6H13NO2", hmdb_id="HMDB0000687"
        )
        infusatedata = parse_infusate_name_with_concs("Leucine-[1,2-13C2][1.0]")
        cls.infusate, _ = Infusate.objects.get_or_create_infusate(infusatedata)

        cls.anml1nm = "test animal 1"
        cls.anml1 = Animal.objects.create(
            name=cls.anml1nm, genotype="WT", infusate=cls.infusate
        )

        cls.tiss1nm = "serum"
        cls.tiss1 = Tissue.objects.create(name=cls.tiss1nm)

        cls.row = pd.Series(
            {
                SamplesLoader.DataHeaders.SAMPLE: "s1",
                SamplesLoader.DataHeaders.HANDLER: "Ralph",
                SamplesLoader.DataHeaders.DATE: "2024-6-15",
                SamplesLoader.DataHeaders.DAYS_INFUSED: 90,
            }
        )

        cls.rec_counts = {
            Sample.__name__: {
                "created": 0,
                "existed": 0,
                "deleted": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
                "updated": 0,
            },
            FCirc.__name__: {
                "created": 0,
                "existed": 0,
                "deleted": 0,
                "skipped": 0,
                "errored": 0,
                "warned": 0,
                "updated": 0,
            },
        }

        super().setUpTestData()

    def test_samples_loader_load_data(self):
        df = pd.DataFrame.from_dict(
            {
                SamplesLoader.DataHeaders.SAMPLE: ["s1"],
                SamplesLoader.DataHeaders.HANDLER: ["Ralph"],
                SamplesLoader.DataHeaders.DATE: ["2024-6-15"],
                SamplesLoader.DataHeaders.DAYS_INFUSED: [90],
                SamplesLoader.DataHeaders.ANIMAL: self.anml1nm,
                SamplesLoader.DataHeaders.TISSUE: self.tiss1nm,
            }
        )
        sl = SamplesLoader(df=df)
        self.assertEqual(0, Sample.objects.count())
        sl.load_data()
        Sample.objects.get(
            name="s1",
            researcher="Ralph",
            date=string_to_date("2024-6-15"),
            time_collected=timedelta(minutes=90),
            animal=self.anml1,
            tissue=self.tiss1,
        )  # No exception = successful test
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))
        counts = deepcopy(self.rec_counts)
        counts[Sample.__name__]["created"] = 1
        counts[FCirc.__name__]["created"] = 1
        self.assertDictEqual(counts, sl.record_counts)

    def test_get_or_create_sample_success(self):
        sl = SamplesLoader()
        rec, cre = sl.get_or_create_sample(self.row, self.anml1, self.tiss1)
        self.assertTrue(cre)
        self.assertIsInstance(rec, Sample)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))
        counts = deepcopy(self.rec_counts)
        counts[Sample.__name__]["created"] = 1
        self.assertDictEqual(counts, sl.record_counts)

        rec2, cre2 = sl.get_or_create_sample(self.row, self.anml1, self.tiss1)
        self.assertFalse(cre2)
        self.assertIsInstance(rec2, Sample)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))
        counts = deepcopy(self.rec_counts)
        counts[Sample.__name__]["created"] = 1  # From previous test
        counts[Sample.__name__]["existed"] = 1
        self.assertDictEqual(counts, sl.record_counts)

    def create_test_sample(self):
        Sample.objects.create(
            name="s1",
            researcher="Ralph",
            date=string_to_date("2024-6-15"),
            time_collected=timedelta(days=90),
            animal=self.anml1,
            tissue=self.tiss1,
        )

    def test_get_or_create_sample_researcher_warning(self):
        # Need an existing researcher to make a name variant warning possible
        self.create_test_sample()
        sl = SamplesLoader(_validate=True)
        row = pd.Series(
            {
                SamplesLoader.DataHeaders.SAMPLE: "s2",
                SamplesLoader.DataHeaders.HANDLER: "Ralpholemule",
                SamplesLoader.DataHeaders.DATE: "2024-6-15",
                SamplesLoader.DataHeaders.DAYS_INFUSED: 80,
                SamplesLoader.DataHeaders.ANIMAL: self.anml1nm,
                SamplesLoader.DataHeaders.TISSUE: self.tiss1nm,
            }
        )
        rec, cre = sl.get_or_create_sample(row, self.anml1, self.tiss1)
        self.assertTrue(cre)
        self.assertIsInstance(rec, Sample)
        self.assertEqual(1, len(sl.aggregated_errors_object.exceptions))
        self.assertEqual(1, sl.aggregated_errors_object.num_warnings)
        self.assertIsInstance(sl.aggregated_errors_object.exceptions[0], NewResearcher)
        counts = deepcopy(self.rec_counts)
        counts[Sample.__name__]["created"] = 1
        counts[Sample.__name__]["warned"] = 1
        self.assertDictEqual(counts, sl.record_counts)

    def test_get_or_create_sample_date_time_and_unique_error(self):
        # Need an existing researcher to make a name variant warning possible
        self.create_test_sample()
        sl = SamplesLoader()
        row = pd.Series(
            {
                SamplesLoader.DataHeaders.SAMPLE: "s1",  # ConflictingValueError (due to researcher or fallbacks)
                SamplesLoader.DataHeaders.HANDLER: "Jim",
                SamplesLoader.DataHeaders.DATE: "invalid",  # ValueError
                SamplesLoader.DataHeaders.DAYS_INFUSED: "ninety",  # InfileError
            }
        )

        with self.assertRaises(RollbackException):
            sl.get_or_create_sample(row, self.anml1, self.tiss1)

        self.assertEqual(4, len(sl.aggregated_errors_object.exceptions))

        self.assertIsInstance(sl.aggregated_errors_object.exceptions[0], NewResearcher)
        self.assertIn(
            "A new researcher [Jim] is being added",
            str(sl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "Ralph",
            str(sl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIsInstance(sl.aggregated_errors_object.exceptions[1], InfileError)
        self.assertIn(
            "Unknown string format: invalid  Location: column [Date Collected]",
            str(sl.aggregated_errors_object.exceptions[1]),
        )
        self.assertIsInstance(sl.aggregated_errors_object.exceptions[2], InfileError)
        self.assertIn(
            "Must be numeric.",
            str(sl.aggregated_errors_object.exceptions[2]),
        )
        self.assertIsInstance(
            sl.aggregated_errors_object.exceptions[3], ConflictingValueError
        )
        self.assertIn("researcher", str(sl.aggregated_errors_object.exceptions[3]))

        self.assertEqual(1, sl.aggregated_errors_object.num_warnings)

        counts = deepcopy(self.rec_counts)
        counts[Sample.__name__]["errored"] = 1  # One record (3 errors)
        counts[Sample.__name__]["warned"] = 1
        self.assertDictEqual(counts, sl.record_counts)

    def assert_skipped(self, sl, rec, cre):
        self.assertFalse(cre)
        self.assertIsNone(rec)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

        counts = deepcopy(self.rec_counts)
        counts[Sample.__name__]["skipped"] = 1
        self.assertDictEqual(counts, sl.record_counts)

    def test_get_or_create_sample_skip_null_animal(self):
        sl = SamplesLoader()
        rec, cre = sl.get_or_create_sample(self.row, None, self.tiss1)
        self.assert_skipped(sl, rec, cre)

    def test_get_or_create_sample_skip_null_tissue(self):
        sl = SamplesLoader()
        rec, cre = sl.get_or_create_sample(self.row, self.anml1, None)
        self.assert_skipped(sl, rec, cre)

    def assert_dne(self, sl):
        self.assertEqual(1, len(sl.aggregated_errors_object.exceptions))
        self.assertIsInstance(
            sl.aggregated_errors_object.exceptions[0], RecordDoesNotExist
        )

    def test_samples_loader_get_animal(self):
        sl = SamplesLoader()

        # Test expected behavior when none
        row1 = pd.Series({SamplesLoader.DataHeaders.ANIMAL: None})
        rec1 = sl.get_animal(row1)
        self.assertIsNone(rec1)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

        # Test when record exists
        row2 = pd.Series({SamplesLoader.DataHeaders.ANIMAL: self.anml1nm})
        rec2 = sl.get_animal(row2)
        self.assertEqual(self.anml1, rec2)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

        # Test when record does not exist
        nen = "nonexistentname"
        row3 = pd.Series({SamplesLoader.DataHeaders.ANIMAL: nen})
        rec3 = sl.get_animal(row3)
        self.assertIsNone(rec3)
        self.assert_dne(sl)
        self.assertEqual(sl.aggregated_errors_object.exceptions[0].model, Animal)

    def test_samples_loader_get_tissue(self):
        sl = SamplesLoader()

        # Test expected behavior when none
        row1 = pd.Series({SamplesLoader.DataHeaders.TISSUE: None})
        rec1 = sl.get_tissue(row1)
        self.assertIsNone(rec1)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

        # Test when record exists
        row2 = pd.Series({SamplesLoader.DataHeaders.TISSUE: self.tiss1nm})
        rec2 = sl.get_tissue(row2)
        self.assertEqual(self.tiss1, rec2)
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

        # Test when record does not exist
        nen = "nonexistentname"
        row3 = pd.Series({SamplesLoader.DataHeaders.TISSUE: nen})
        rec3 = sl.get_tissue(row3)
        self.assertIsNone(rec3)
        self.assert_dne(sl)
        self.assertEqual(sl.aggregated_errors_object.exceptions[0].model, Tissue)

    def test_samples_loader_repackage_exceptions(self):
        sl = SamplesLoader()
        sl.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Tissue,
                {"name": "coccyx"},
                column="Tissue",
                file="study.xlsx",
                sheet="Samples",
                rownum=3,
            )
        )
        sl.aggregated_errors_object.buffer_error(
            RecordDoesNotExist(
                Tissue,
                {"name": "elbow pit"},
                column="Tissue",
                file="study.xlsx",
                sheet="Samples",
                rownum=9,
            )
        )
        sl.repackage_exceptions()
        self.assertEqual(1, len(sl.aggregated_errors_object.exceptions))
        self.assertIsInstance(sl.aggregated_errors_object.exceptions[0], MissingTissues)

    def test_get_or_create_fcirc(self):
        sample = Sample.objects.create(
            name="s1",
            researcher="Ralph",
            date=string_to_date("2024-6-15"),
            time_collected=timedelta(minutes=90),
            animal=self.anml1,
            tissue=self.tiss1,
        )  # No exception = successful test
        tracer = self.infusate.tracers.first()
        sl = SamplesLoader()
        sl.get_or_create_fcirc(sample, tracer, "C")
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))
        counts = deepcopy(self.rec_counts)
        counts[FCirc.__name__]["created"] = 1
        self.assertDictEqual(counts, sl.record_counts)
