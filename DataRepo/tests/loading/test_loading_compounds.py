import pandas as pd
from django.conf import settings
from django.core.management import call_command
from django.test import override_settings, tag

from DataRepo.models import Compound, CompoundSynonym
from DataRepo.models.compound import SynonymExistsAsMismatchedCompound
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import (
    AggregatedErrors,
    # AmbiguousCompoundDefinitionError,
    CompoundsLoader,
    ConflictingValueError,
    DuplicateValues,
)
from DataRepo.utils.compounds_loader import CompoundNotFound


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

        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_compounds",
                compounds="DataRepo/example_data/testing_data/test_study_1/test_study_1_compounds_dupes.tsv",
            )
        aes = ar.exception
        self.assertEqual(4, len(aes.exceptions))
        self.assertEqual(
            4,
            len([exc for exc in aes.exceptions if type(exc) == DuplicateValues]),
            msg="All 4 exceptions are about duplicate values",
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
        print(f"PREVIOUS CONTENTS OF DEFAULT DB SYNONYMS ({CompoundSynonym.objects.using('default').count()}):")
        for r in CompoundSynonym.objects.using('default').all():
            print(f"{r}")
        print(f"PREVIOUS CONTENTS OF VALIDATION DB SYNONYMS ({CompoundSynonym.objects.using('validation').count()}):")
        for r in CompoundSynonym.objects.using('validation').all():
            print(f"{r}")
        try:
            call_command(
                "load_compounds",
                compounds=primary_compound_file,
                verbosity=0,
            )
        except AggregatedErrors as aes:
            print("setUpTestData ERRORS:")
            aes.print_summary()
            raise aes
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
    def test_new_synonyms_added_to_existing_compound(self):
        """
        This (renamed) test used to assure that the synonym is associated with a previously loaded compound when the
        data for the compound in the load was nonsense.  However, this is no longer supported.  The data in the load
        must be consistent and the load script should not associate a synonym with what is defined as a different
        compound in the load file.  So instead, this test will assure that new synonyms added to a correctly defined
        compound is associated with a previously loaded compound, to add NEW valid synonyms.
        """
        # create dataframe from dictionary
        cl = CompoundsLoader(compounds_df=pd.DataFrame.from_dict({
            CompoundsLoader.NAME_HEADER: ["fructose-1-6-bisphosphate"],
            CompoundsLoader.FORMULA_HEADER: ["C6H14O12P2"],
            CompoundsLoader.HMDB_ID_HEADER: ["HMDB0001058"],
            CompoundsLoader.SYNONYMS_HEADER: [(
                "Fructose 1,6-bisphosphate;Fructose-1,6-diphosphate;"
                "Fructose 1,6-diphosphate;Diphosphofructose;new valid synonym"
            )],
        }))
        cl.load_compounds()
        self.assertEqual(
            1,
            cl.num_existing_compounds[settings.TRACEBASE_DB],
            msg="Compound default insert should be skipped",
        )
        self.assertEqual(
            1,
            cl.num_existing_compounds[settings.VALIDATION_DB],
            msg="Compound validation insert should be skipped",
        )
        self.assertEqual(
            0,
            cl.num_inserted_compounds[settings.TRACEBASE_DB],
            msg="No compounds default should be inserted",
        )
        self.assertEqual(
            0,
            cl.num_inserted_compounds[settings.VALIDATION_DB],
            msg="No compounds validation should be inserted",
        )
        self.assertEqual(
            0,
            cl.num_erroneous_compounds[settings.TRACEBASE_DB],
            msg="No compounds default should be in error",
        )
        self.assertEqual(
            0,
            cl.num_erroneous_compounds[settings.VALIDATION_DB],
            msg="No compounds validation should be in error",
        )


        self.assertEqual(
            4,
            cl.num_existing_synonyms[settings.TRACEBASE_DB],
            msg="4 synonym default inserts should be skipped",
        )
        self.assertEqual(
            4,
            cl.num_existing_synonyms[settings.VALIDATION_DB],
            msg="4 synonym validation inserts should be skipped",
        )
        self.assertEqual(
            1,
            cl.num_inserted_synonyms[settings.TRACEBASE_DB],
            msg="1 synonym default should be inserted",
        )
        self.assertEqual(
            1,
            cl.num_inserted_synonyms[settings.VALIDATION_DB],
            msg="1 synonym validation should be inserted",
        )
        self.assertEqual(
            0,
            cl.num_erroneous_synonyms[settings.TRACEBASE_DB],
            msg="No synonyms default should be in error",
        )
        self.assertEqual(
            0,
            cl.num_erroneous_synonyms[settings.VALIDATION_DB],
            msg="No synonyms validation should be in error",
        )
        self.assertEqual(
            "fructose-1-6-bisphosphate",
            CompoundSynonym.objects.get(name__exact="new valid synonym").compound.name,
            msg="Assure new synonym is associated with the correct existing compound record.",
        )

    @tag("compound_for_row")
    def test_ambiguous_synonym_in_find_compound_for_row(self):
        """
        Test that an exception is raised when synonyms on one row refer to two
        existing compound records in the database
        """
        # create dataframe from dictionary
        cl = CompoundsLoader(compounds_df=pd.DataFrame.from_dict({
            CompoundsLoader.NAME_HEADER: ["nonsense"],
            CompoundsLoader.FORMULA_HEADER: ["nonsense"],
            CompoundsLoader.HMDB_ID_HEADER: ["nonsense"],
            CompoundsLoader.SYNONYMS_HEADER: ["Fructose 1,6-bisphosphate;glucose"],
        }))
        with self.assertRaises(AggregatedErrors) as ar:
            cl.load_compounds()
        aes = ar.exception
        self.assertEqual(4, aes.num_errors)
        self.assertEqual(
            4,
            len([
                exc for exc in aes.exceptions
                if type(exc) == ConflictingValueError and exc.consistent_field == "compound"
            ]),
            msg="All 8 exceptions are conflicting value errors about the compound field",
        )


@override_settings(CACHES=settings.TEST_CACHES)
class CompoundsLoaderTests(TracebaseTestCase):
    def get_dataframe(self):
        return pd.read_csv(
            "DataRepo/example_data/testing_data/short_compound_list.tsv",
            sep="\t",
            keep_default_na=False,
        )

    def test_compound_exists_skipped(self):
        df = self.get_dataframe()
        cl = CompoundsLoader(df)
        cl.load_compounds()
        cl2 = CompoundsLoader(df)
        cl2.load_compounds()
        self.assertEqual(
            {settings.TRACEBASE_DB: 0, settings.VALIDATION_DB: 0}, cl2.num_inserted_compounds
        )
        self.assertEqual(
            {settings.TRACEBASE_DB: 0, settings.VALIDATION_DB: 0}, cl2.num_erroneous_compounds
        )
        self.assertEqual(
            {settings.TRACEBASE_DB: 1, settings.VALIDATION_DB: 1}, cl2.num_existing_compounds
        )
        self.assertEqual(
            {settings.TRACEBASE_DB: 0, settings.VALIDATION_DB: 0}, cl2.num_inserted_synonyms
        )
        self.assertEqual(
            {settings.TRACEBASE_DB: 0, settings.VALIDATION_DB: 0}, cl2.num_erroneous_synonyms
        )
        self.assertEqual(
            {settings.TRACEBASE_DB: 0, settings.VALIDATION_DB: 0}, cl2.num_existing_synonyms
        )

    def test_compound_not_found_error(self):
        df = self.get_dataframe()
        cl = CompoundsLoader(df)
        cl.load_synonyms_per_db()
        aes = cl.aggregated_errors_obj
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(type(aes.exceptions[0]), CompoundNotFound)


@override_settings(CACHES=settings.TEST_CACHES)
@tag("compound_loading")
class CompoundValidationLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
            dry_run=True,
            verbosity=0,
        )
        # validate only; nothing gets loaded
        cls.ALL_COMPOUNDS_COUNT = 0

        super().setUpTestData()

    def test_compounds_loaded(self):
        self.assertEqual(self.ALL_COMPOUNDS_COUNT, Compound.objects.all().count())
