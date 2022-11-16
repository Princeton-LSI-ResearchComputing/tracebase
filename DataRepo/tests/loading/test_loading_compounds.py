import pandas as pd
from django.conf import settings
from django.core.management import CommandError, call_command
from django.test import override_settings, tag

from DataRepo.models import Compound, CompoundSynonym
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import AmbiguousCompoundDefinitionError, CompoundsLoader
from DataRepo.utils.compounds_loader import CompoundExists, CompoundNotFound


@tag("compounds")
@tag("loading")
class LoadCompoundsTests(TracebaseTestCase):
    """Tests Loading of Compounds"""

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        super().setUpTestData()

    def test_compound_loading(self):
        """Test the compounds and synonyms are loaded"""

    def test_compound_loading_failure(self):
        """Test that an error during compound loading doesn't load any compounds"""

        with self.assertRaisesRegex(
            CommandError,
            "Validation errors when loading compounds, no compounds were loaded",
        ):
            call_command(
                "load_compounds",
                compounds="DataRepo/example_data/testing_data/test_study_1/test_study_1_compounds_dupes.tsv",
            )
        self.assertEqual(Compound.objects.count(), 0)


@override_settings(CACHES=settings.TEST_CACHES)
@tag("compound_loading")
class CompoundLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):

        primary_compound_file = (
            "DataRepo/example_data/consolidated_tracebase_compound_list.tsv"
        )

        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds=primary_compound_file,
            verbosity=0,
        )
        cls.ALL_COMPOUNDS_COUNT = 51

        cls.COMPOUND_WITH_MANY_NAMES = Compound.objects.get(name="a-ketoglutarate")

        # and do it again, to be able to test the class
        compounds_df = pd.read_csv(
            primary_compound_file, sep="\t", keep_default_na=False
        )
        cls.LOADER_INSTANCE = CompoundsLoader(compounds_df=compounds_df)

        super().setUpTestData()

    def test_compounds_loaded(self):
        self.assertEqual(Compound.objects.all().count(), self.ALL_COMPOUNDS_COUNT)

    def test_compound_loaded(self):
        self.assertEqual(
            "HMDB0000208",
            self.COMPOUND_WITH_MANY_NAMES.hmdb_id,
        )

    def test_synonym_loaded(self):
        cs = CompoundSynonym.objects.get(name="oxoglutarate")
        self.assertEqual(
            cs.compound,
            self.COMPOUND_WITH_MANY_NAMES,
        )

    def test_synonymous_compound_retrieval(self):
        synonymous_compound = Compound.compound_matching_name_or_synonym(
            "alpha-ketoglutaric acid"
        )
        self.assertEqual(
            synonymous_compound,
            self.COMPOUND_WITH_MANY_NAMES,
        )

    def test_nonsense_synonym_retrieval(self):
        synonymous_compound = Compound.compound_matching_name_or_synonym("nonsense")
        self.assertIsNone(synonymous_compound)

    @tag("compound_for_row")
    def test_missing_compounds_keys_in_find_compound_for_row(self):
        # this test used the SetUp-inserted data to retrieve spoofed data with
        # only synonyms
        dict = {
            CompoundsLoader.KEY_COMPOUND_NAME: "nonsense",
            CompoundsLoader.KEY_FORMULA: "nonsense",
            CompoundsLoader.KEY_HMDB: "nonsense",
            CompoundsLoader.KEY_SYNONYMS: "Fructose 1,6-bisphosphate;Fructose-1,6-diphosphate;"
            "Fructose 1,6-diphosphate;Diphosphofructose",
        }
        # create series from dictionary
        ser = pd.Series(dict)
        compound = self.LOADER_INSTANCE.find_compound_for_row(ser)
        self.assertEqual(compound.name, "fructose-1-6-bisphosphate")

    @tag("compound_for_row")
    def test_ambiguous_synonym_in_find_compound_for_row(self):
        """
        Test that an exception is raised when synonyms on one row refer to two
        existing compound records in the database
        """
        dict = {
            CompoundsLoader.KEY_COMPOUND_NAME: "nonsense",
            CompoundsLoader.KEY_FORMULA: "nonsense",
            CompoundsLoader.KEY_HMDB: "nonsense",
            CompoundsLoader.KEY_SYNONYMS: "Fructose 1,6-bisphosphate;glucose",
        }
        # create series from dictionary
        ser = pd.Series(dict)
        with self.assertRaises(AmbiguousCompoundDefinitionError):
            self.LOADER_INSTANCE.find_compound_for_row(ser)


@override_settings(CACHES=settings.TEST_CACHES)
@tag("compound_loading")
class CompoundLoadingTestErrors(TracebaseTestCase):
    """Tests loading of Compounds with errors"""

    def test_compound_loading_failure(self):
        """Test that an error during compound loading doesn't load any compounds"""

        with self.assertRaisesRegex(
            CommandError, "Validation errors when loading compounds"
        ):
            call_command(
                "load_compounds",
                compounds="DataRepo/example_data/testing_data/test_study_1/test_study_1_compounds_dupes.tsv",
            )
        self.assertEqual(Compound.objects.count(), 0)


@override_settings(CACHES=settings.TEST_CACHES)
class CompoundsLoaderTests(TracebaseTestCase):
    def get_dataframe(self):
        return pd.read_csv(
            "DataRepo/example_data/testing_data/short_compound_list.tsv",
            sep="\t",
            keep_default_na=False,
        )

    def test_compound_exists_error(self):
        df = self.get_dataframe()
        cl = CompoundsLoader(df)
        new_compound = Compound(
            name="test name",
            formula="C5",
            hmdb_id="1",
        )
        new_compound2 = Compound(
            name="test name",
            formula="C5",
            hmdb_id="1",
        )
        cl.validated_new_compounds_for_insertion = [new_compound, new_compound2]
        with self.assertRaises(CompoundExists):
            cl.load_validated_compounds()

    def test_compound_not_found_error(self):
        df = self.get_dataframe()
        cl = CompoundsLoader(df)
        with self.assertRaises(CompoundNotFound):
            cl.load_synonyms_per_db()


@override_settings(CACHES=settings.TEST_CACHES)
@tag("compound_loading")
class CompoundValidationLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
            validate_only=True,
            verbosity=0,
        )
        # validate only; nothing gets loaded
        cls.ALL_COMPOUNDS_COUNT = 0

        super().setUpTestData()

    def test_compounds_loaded(self):
        self.assertEqual(Compound.objects.all().count(), self.ALL_COMPOUNDS_COUNT)
