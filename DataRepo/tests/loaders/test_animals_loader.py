from datetime import timedelta

import pandas as pd

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.models import Animal, Compound, Infusate, Protocol, Study
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
                AnimalsLoader.DataHeaders.STUDY: ["stud1; stud2"],
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
                # AnimalsLoader.DataHeaders.STUDY: ["stud1"],  # expect required error
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
                AnimalsLoader.DataHeaders.STUDY: [
                    "stud1"
                ],  # DoesNotExist, and AnimalStudy skipped due to no animal
                # Invalid values
                AnimalsLoader.DataHeaders.NAME: ["anml1"],  # no error
                AnimalsLoader.DataHeaders.WEIGHT: ["5g"],  # type error (from db)
                AnimalsLoader.DataHeaders.AGE: ["2w"],  # type error (from code)
                # Types are not yet checked in TableLoader, so some of these errors (the ones coming from the database)
                # are occluded by the DB error for body_weight
                # TODO: Add type and enum checks in check_dataframe_values.
                AnimalsLoader.DataHeaders.SEX: ["XY"],  # type error (from code)
                AnimalsLoader.DataHeaders.INFUSIONRATE: ["6.0mM"],  # no error (yet)
            }
        )
        al = AnimalsLoader(df=df)
        with self.assertRaises(AggregatedErrors) as ar:
            al.load_data()
        aes = ar.exception
        self.assertEqual(4, len(aes.exceptions))
        self.assertIsInstance(aes.exceptions[0], InfileError)
        self.assertIn("timedelta", str(aes.exceptions[0]))
        self.assertIsInstance(aes.exceptions[1], InfileError)
        self.assertIn(
            "must be one of [('F', 'female'), ('M', 'male')]", str(aes.exceptions[1])
        )
        self.assertIsInstance(aes.exceptions[2], InfileDatabaseError)
        self.assertIn("Field 'body_weight' expected a number", str(aes.exceptions[2]))
        self.assertIsInstance(aes.exceptions[3], RecordDoesNotExist)
        self.assertIn("Study", str(aes.exceptions[3]))
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
                "AnimalLabel": {
                    "created": 0,
                    "existed": 0,
                    "skipped": 1,
                    "errored": 0,
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

    def test_animals_loader_get_infusate_int(self):
        al = AnimalsLoader()
        row = pd.Series({AnimalsLoader.DataHeaders.INFUSATE: "Leucine-[1,2-13C2][1]"})
        rec = al.get_infusate(row)
        self.assertIsNotNone(rec)
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

    def test_animals_loader_get_infusate_sig_dig_float(self):
        """Infusate names in the input file contain concentrations (which is new), and the names constructed by the
        database use significant digits (eee Infusate.CONCENTRATION_SIGNIFICANT_FIGURES (3)).  The actual data does not.
        We should be able to retrieve the infusate when the name either has significant digits, or the user entered the
        actual concentration value."""
        # Create the study
        Study.objects.create(name="stud1")
        # Create the infusate
        infusatedata = parse_infusate_name("Leucine-[13C6]", [148.88])
        inf, _ = Infusate.objects.get_or_create_infusate(infusatedata)
        df = pd.DataFrame.from_dict(
            {
                AnimalsLoader.DataHeaders.NAME: ["anml2"],
                AnimalsLoader.DataHeaders.GENOTYPE: ["WT"],
                AnimalsLoader.DataHeaders.STUDY: ["stud1"],
                AnimalsLoader.DataHeaders.INFUSATE: ["Leucine-[13C6][148.88]"],
            }
        )
        al = AnimalsLoader(df=df)
        al.load_data()
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

        # Now get the infusate using the Infusate.CONCENTRATION_SIGNIFICANT_FIGURES (3), i.e. 149
        row = pd.Series({AnimalsLoader.DataHeaders.INFUSATE: "Leucine-[13C6][149]"})
        rec = al.get_infusate(row)
        self.assertEqual(inf, rec)

        # Now get the infusate using the number supplied by the user, i.e. 148.88
        row = pd.Series({AnimalsLoader.DataHeaders.INFUSATE: "Leucine-[13C6][148.88]"})
        rec2 = al.get_infusate(row)
        self.assertEqual(inf, rec2)

    def test_animals_loader_get_treatment(self):
        al = AnimalsLoader()
        row = pd.Series({AnimalsLoader.DataHeaders.TREATMENT: "trt"})
        Protocol.objects.create(name="trt", category=Protocol.ANIMAL_TREATMENT)
        rec = al.get_treatment(row)
        self.assertIsNotNone(rec)
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

    def test_animals_loader_get_studies(self):
        al = AnimalsLoader()

        row = pd.Series({AnimalsLoader.DataHeaders.STUDY: None})
        recs = al.get_studies(row)
        # None is OK - Results in an included None, just so the skipped count gets incremented once.
        self.assertEqual([None], recs)
        self.assertEqual(0, len(al.aggregated_errors_object.exceptions))

        row = pd.Series({AnimalsLoader.DataHeaders.STUDY: "stud1; stud2"})
        recs = al.get_studies(row)
        # Results include None for each not-found record, just so the skipped count gets incremented.
        self.assertEqual([None, None], recs)
        self.assertEqual(2, len(al.aggregated_errors_object.exceptions))
        self.assertEqual(
            2, len(al.aggregated_errors_object.get_exception_type(RecordDoesNotExist))
        )
        # Reset the exceptions
        al.aggregated_errors_object = AggregatedErrors()

        s1 = Study.objects.create(name="stud1")
        s2 = Study.objects.create(name="stud2")
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

    def test_get_labeled_elements(self):
        al = AnimalsLoader()

        # None is OK - Results in an included None, just so the skipped count gets incremented.
        elems = al.get_labeled_elements(None)
        self.assertEqual([None], elems)

        # 1 label
        elems = al.get_labeled_elements(self.infusate)
        self.assertEqual(["C"], elems)

        # Multiple labels from different tracers
        Compound.objects.create(
            name="Isoleucine", formula="C6H13NO2", hmdb_id="HMDB0000172"
        )
        infusate_string = "test {isoleucine-[13C6,15N1];leucine-[13C6,15N1]}"
        infdata = parse_infusate_name(infusate_string, [1, 2])
        inf, _ = Infusate.objects.get_or_create_infusate(infdata)
        elems = al.get_labeled_elements(inf)
        self.assertEqual(set(["C", "N"]), set(elems))

    def test_get_or_create_animal_label(self):
        al = AnimalsLoader()
        anml = Animal.objects.create(name="a1", genotype="WT", infusate=self.infusate)
        rec, cre = al.get_or_create_animal_label(anml, "C")
        self.assertTrue(cre)
        self.assertIsNotNone(rec)

        rec2, cre2 = al.get_or_create_animal_label(anml, "C")
        self.assertFalse(cre2)
        self.assertIsNotNone(rec2)
