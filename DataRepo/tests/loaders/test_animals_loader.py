from datetime import timedelta

import pandas as pd

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.models.animal import Animal
from DataRepo.models.compound import Compound
from DataRepo.models.infusate import Infusate
from DataRepo.models.protocol import Protocol
from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    InfileDatabaseError,
    InfileError,
    RecordDoesNotExist,
    RequiredHeadersError,
)
from DataRepo.utils.infusate_name_parser import parse_infusate_name


class AnimalsLoaderTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        Compound.objects.create(
            name="Leucine", formula="C6H13NO2", hmdb_id="HMDB0000687"
        )
        infusatedata = parse_infusate_name("Leucine-[1,2-13C2]", [1.0])
        cls.infusate, _ = Infusate.objects.get_or_create_infusate(infusatedata)
        super().setUpTestData()

    def test_animals_loader_get_or_create_animal_reqd_only_nones_ok(self):
        """Successful minimum animal create/get"""
        al = AnimalsLoader()
        row = pd.Series(
            {
                AnimalsLoader.DataHeaders.NAME: "anml1",
                AnimalsLoader.DataHeaders.GENOTYPE: "WT",
            }
        )

        # Test create
        rec, cre = al.get_or_create_animal(row, self.infusate)
        self.assertTrue(cre)
        self.assertEqual("anml1", rec.name)

        # Test get
        rec2, cre2 = al.get_or_create_animal(row, self.infusate)
        self.assertFalse(cre2)
        self.assertEqual("anml1", rec2.name)

    def test_animals_loader_get_or_create_animal_full(self):
        """Successful full animal create/get"""
        al = AnimalsLoader()
        row = pd.Series(
            {
                AnimalsLoader.DataHeaders.NAME: "anml1",
                AnimalsLoader.DataHeaders.GENOTYPE: "WT",
                AnimalsLoader.DataHeaders.WEIGHT: 5.0,
                AnimalsLoader.DataHeaders.AGE: 2.0,
                AnimalsLoader.DataHeaders.SEX: "M",
                AnimalsLoader.DataHeaders.DIET: "n/a",
                AnimalsLoader.DataHeaders.FEEDINGSTATUS: "fasted",
            }
        )
        treatment = Protocol.objects.create(
            name="test", category=Protocol.ANIMAL_TREATMENT
        )
        rec, cre = al.get_or_create_animal(row, self.infusate, treatment)
        self.assertTrue(cre)
        self.assertEqual("anml1", rec.name)
        self.assertEqual("WT", rec.genotype)
        self.assertEqual(5.0, rec.body_weight)
        self.assertEqual(timedelta(weeks=2.0), rec.age)
        self.assertEqual("M", rec.sex)
        self.assertEqual("n/a", rec.diet)
        self.assertEqual("fasted", rec.feeding_status)
        self.assertEqual("test", rec.treatment.name)

        # Test get
        rec2, cre2 = al.get_or_create_animal(row, self.infusate, treatment)
        self.assertFalse(cre2)
        self.assertEqual("anml1", rec2.name)

    def test_animals_loader_load_data_success(self):
        """Successful full load"""
        Protocol.objects.create(name="trt", category=Protocol.ANIMAL_TREATMENT)
        Study.objects.create(name="stud1")
        Study.objects.create(name="stud2")
        df = pd.DataFrame.from_dict(
            {
                AnimalsLoader.DataHeaders.NAME: ["anml1"],
                AnimalsLoader.DataHeaders.GENOTYPE: ["WT"],
                AnimalsLoader.DataHeaders.WEIGHT: [5.0],
                AnimalsLoader.DataHeaders.AGE: [2.0],
                AnimalsLoader.DataHeaders.SEX: ["M"],
                AnimalsLoader.DataHeaders.DIET: ["n/a"],
                AnimalsLoader.DataHeaders.FEEDINGSTATUS: ["fasted"],
                AnimalsLoader.DataHeaders.TREATMENT: ["trt"],
                AnimalsLoader.DataHeaders.STUDIES: ["stud1; stud2"],
                AnimalsLoader.DataHeaders.INFUSATE: ["Leucine-[1,2-13C2][1]"],
                AnimalsLoader.DataHeaders.INFUSIONRATE: [6.0],
            }
        )
        al = AnimalsLoader(df=df)
        al.load_data()
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))
        self.assertEqual(1, Animal.objects.count())

    def test_animals_loader_load_data_missing_required(self):
        """Test that missing required columns are raised right away."""
        df = pd.DataFrame.from_dict(
            {
                # AnimalsLoader.DataHeaders.NAME: ["anml1"],  # expect required error
                # AnimalsLoader.DataHeaders.GENOTYPE: ["WT"],  # expect required error
                AnimalsLoader.DataHeaders.WEIGHT: [5.0],  # type error
                # AnimalsLoader.DataHeaders.INFUSATE: ["Leucine-[1,2-13C2][1]"],  # expect required error
                # AnimalsLoader.DataHeaders.STUDIES: ["stud1"],  # expect required error
            }
        )
        al = AnimalsLoader(df=df)
        with self.assertRaises(AggregatedErrors) as ar:
            al.load_data()
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(RequiredHeadersError, type(aes.exceptions[0]))
        self.assertIn(
            "missing: [Animal Name, Genotype, Infusate, Study]",
            str(aes.exceptions[0]),
        )
        self.assertEqual(0, Animal.objects.count())

    def test_animals_loader_load_data_invalid(self):
        """Test that invalid data raises as many errors in 1 go as possible."""
        df = pd.DataFrame.from_dict(
            {
                # Required
                AnimalsLoader.DataHeaders.GENOTYPE: ["WT"],  # no error
                AnimalsLoader.DataHeaders.INFUSATE: [
                    "Leucine-[1,2-13C2][1]"
                ],  # no error (needed for animal errors)
                AnimalsLoader.DataHeaders.STUDIES: [
                    "stud1"
                ],  # DoesNotExist, and AnimalStudy skipped due to no animal
                # Invalid values
                AnimalsLoader.DataHeaders.NAME: ["anml1"],  # no error
                AnimalsLoader.DataHeaders.WEIGHT: ["5g"],  # type error
                AnimalsLoader.DataHeaders.AGE: ["2w"],  # type error
                # Types are not yet checked in TableLoader, so the remaining 3 errors are occluded by the DB error for
                # body_weight
                # TODO: Add type and enum checks in check_dataframe_values.
                AnimalsLoader.DataHeaders.SEX: ["XY"],  # no error (yet)
                AnimalsLoader.DataHeaders.INFUSIONRATE: ["6.0mM"],  # no error (yet)
            }
        )
        al = AnimalsLoader(df=df)
        with self.assertRaises(AggregatedErrors) as ar:
            al.load_data()
        aes = ar.exception
        self.assertEqual(3, len(aes.exceptions))
        self.assertIsInstance(aes.exceptions[0], InfileError)
        self.assertIn("timedelta", str(aes.exceptions[0]))
        self.assertIsInstance(aes.exceptions[1], InfileDatabaseError)
        self.assertIn("Field 'body_weight' expected a number", str(aes.exceptions[1]))
        self.assertIsInstance(aes.exceptions[2], RecordDoesNotExist)
        self.assertIn("Study", str(aes.exceptions[2]))
        self.assertEqual(0, Animal.objects.count())
        self.assertDictEqual(
            {
                "Animal": {
                    "created": 0,
                    "existed": 0,
                    "skipped": 0,
                    "errored": 1,
                    "updated": 0,
                },
                "Animal_studies": {
                    "created": 0,
                    "existed": 0,
                    "skipped": 1,
                    "errored": 0,
                    "updated": 0,
                },
            },
            al.record_counts,
        )

    def test_animals_loader_get_infusate(self):
        al = AnimalsLoader()
        row = pd.Series({AnimalsLoader.DataHeaders.INFUSATE: "Leucine-[1,2-13C2][1]"})
        rec = al.get_infusate(row)
        self.assertIsNotNone(rec)
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

    def test_animals_loader_get_treatment(self):
        al = AnimalsLoader()
        row = pd.Series({AnimalsLoader.DataHeaders.TREATMENT: "trt"})
        Protocol.objects.create(name="trt", category=Protocol.ANIMAL_TREATMENT)
        rec = al.get_treatment(row)
        self.assertIsNotNone(rec)
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

    def test_animals_loader_get_studies(self):
        s1 = Study.objects.create(name="stud1")
        s2 = Study.objects.create(name="stud2")
        row = pd.Series({AnimalsLoader.DataHeaders.STUDIES: "stud1; stud2"})
        al = AnimalsLoader()
        recs = al.get_studies(row)
        self.assertEqual([s1, s2], recs)
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

    def test_animals_loader_get_or_create_animal_study_link(self):
        stdy = Study.objects.create(name="stud1")
        anml = Animal.objects.create(name="a1", genotype="WT", infusate=self.infusate)
        al = AnimalsLoader()
        rec, cre = al.get_or_create_animal_study_link(anml, stdy)
        self.assertTrue(cre)
        self.assertEqual("Animal_studies", type(rec).__name__)

        rec2, cre2 = al.get_or_create_animal_study_link(anml, stdy)
        self.assertFalse(cre2)
        self.assertEqual(rec, rec2)
