import pandas as pd
from django.conf import settings
from django.core.management import call_command
from django.test import override_settings, tag

from DataRepo.models import Compound, CompoundSynonym
from DataRepo.models.compound import (
    CompoundExistsAsMismatchedSynonym,
    SynonymExistsAsMismatchedCompound,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import (  # AmbiguousCompoundDefinitionError,
    AggregatedErrors,
    CompoundsLoader,
    ConflictingValueError,
    DuplicateValues,
    UnknownHeadersError,
)


@tag("compounds")
@tag("loading")
class LoadCompoundsTests(TracebaseTestCase):
    """Tests Loading of Compounds"""

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/data/examples/tissues/loading.yaml")
        super().setUpTestData()

    def test_compound_loading(self):
        """Test the compounds and synonyms are loaded"""

    def test_compound_loading_failure(self):
        """Test that an error during compound loading doesn't load any compounds"""

        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_compounds",
                compounds="DataRepo/data/examples/testing_data/test_study_1/test_study_1_compounds_dupes.tsv",
            )
        aes = ar.exception
        self.assertEqual(2, len(aes.exceptions))
        self.assertEqual(UnknownHeadersError, type(aes.exceptions[0]))
        self.assertEqual(["m/z", "RT"], aes.exceptions[0].unknowns)
        self.assertEqual(DuplicateValues, type(aes.exceptions[1]))
        self.assertIn("lactate", aes.exceptions[1].dupe_dict.keys())
        self.assertIn("L-Lactic acid", aes.exceptions[1].dupe_dict.keys())
        self.assertEqual(Compound.objects.count(), 0)


@override_settings(CACHES=settings.TEST_CACHES)
@tag("compound_loading")
class CompoundLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        primary_compound_file = (
            "DataRepo/data/examples/consolidated_tracebase_compound_list.tsv"
        )

        call_command("load_study", "DataRepo/data/examples/tissues/loading.yaml")
        try:
            call_command(
                "load_compounds",
                compounds=primary_compound_file,
                verbosity=0,
            )
        except AggregatedErrors as aes:
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
        cl = CompoundsLoader(
            compounds_df=pd.DataFrame.from_dict(
                {
                    CompoundsLoader.NAME_HEADER: ["fructose-1-6-bisphosphate"],
                    CompoundsLoader.FORMULA_HEADER: ["C6H14O12P2"],
                    CompoundsLoader.HMDB_ID_HEADER: ["HMDB0001058"],
                    CompoundsLoader.SYNONYMS_HEADER: [
                        (
                            "Fructose 1,6-bisphosphate;Fructose-1,6-diphosphate;"
                            "Fructose 1,6-diphosphate;Diphosphofructose;new valid synonym"
                        )
                    ],
                }
            )
        )
        cl.load_compound_data()
        self.assertEqual(
            1,
            cl.num_existing_compounds,
            msg="Compound default insert should be skipped",
        )
        self.assertEqual(
            1,
            cl.num_existing_compounds,
            msg="Compound validation insert should be skipped",
        )
        self.assertEqual(
            0,
            cl.num_inserted_compounds,
            msg="No compounds default should be inserted",
        )
        self.assertEqual(
            0,
            cl.num_inserted_compounds,
            msg="No compounds validation should be inserted",
        )
        self.assertEqual(
            0,
            cl.num_erroneous_compounds,
            msg="No compounds default should be in error",
        )
        self.assertEqual(
            0,
            cl.num_erroneous_compounds,
            msg="No compounds validation should be in error",
        )

        self.assertEqual(
            4,
            cl.num_existing_synonyms,
            msg="4 synonym default inserts should be skipped",
        )
        self.assertEqual(
            4,
            cl.num_existing_synonyms,
            msg="4 synonym validation inserts should be skipped",
        )
        self.assertEqual(
            1,
            cl.num_inserted_synonyms,
            msg="1 synonym default should be inserted",
        )
        self.assertEqual(
            1,
            cl.num_inserted_synonyms,
            msg="1 synonym validation should be inserted",
        )
        self.assertEqual(
            0,
            cl.num_erroneous_synonyms,
            msg="No synonyms default should be in error",
        )
        self.assertEqual(
            0,
            cl.num_erroneous_synonyms,
            msg="No synonyms validation should be in error",
        )
        self.assertEqual(
            "fructose-1-6-bisphosphate",
            CompoundSynonym.objects.get(name__exact="new valid synonym").compound.name,
            msg="Assure new synonym is associated with the correct existing compound record.",
        )

    @tag("compound_for_row")
    def test_synonym_matches_existing_compound_inconsistent_with_load_data(self):
        """
        Test that an exception is raised when synonyms on one row refer to two
        existing compound records in the database
        """
        # create dataframe from dictionary
        cl = CompoundsLoader(
            compounds_df=pd.DataFrame.from_dict(
                {
                    CompoundsLoader.NAME_HEADER: ["nonsense"],
                    CompoundsLoader.FORMULA_HEADER: ["nonsense"],
                    CompoundsLoader.HMDB_ID_HEADER: ["nonsense"],
                    CompoundsLoader.SYNONYMS_HEADER: [
                        "Fructose 1,6-bisphosphate;glucose"
                    ],
                }
            )
        )
        with self.assertRaises(AggregatedErrors) as ar:
            cl.load_compound_data()
        aes = ar.exception
        self.assertEqual(2, aes.num_errors)
        self.assertEqual(
            2,
            len(
                [
                    exc
                    for exc in aes.exceptions
                    if type(exc) == ConflictingValueError
                    and exc.consistent_field == "compound"
                ]
            ),
            msg="Both exceptions are conflicting value errors about the compound field",
        )

    @tag("compound_for_row")
    def test_synonym_compound_mismatches(self):
        """
        Test that an exception is raised when synonyms on one row refer to two
        existing compound records in the database
        """
        # Somewhat of a useless test, because the save override in Compound always saves the compound name as a
        # synonym, so there will always be a ConflictingValueError.  And if the inconsistency is inside the file only,
        # it will be reported as a DuplicateValuesError.  However, if anything ever changes in that regard, or a bug
        # prevents name/synonym symmetry, these will be raised.
        # We get around this for this test by manually creating the records without name/synonym symmetry.
        existing_compound_1 = Compound(
            name="existing compound name",
            formula="C1",
            hmdb_id="HMDB1111111",
        )
        existing_compound_1.save()
        # Delete the synonyms created by the save override in Compound
        existing_compound_1.synonyms.all().delete()
        existing_compound_2 = Compound(
            name="Does not matter",
            formula="C2",
            hmdb_id="HMDB2222222",
        )
        existing_compound_2.save()
        existing_compound_2.synonyms.all().delete()
        existing_synonym = CompoundSynonym(
            name="existing synonym",
            compound=existing_compound_2,
        )
        existing_synonym.save()
        # create dataframe from dictionary
        cl = CompoundsLoader(
            compounds_df=pd.DataFrame.from_dict(
                {
                    CompoundsLoader.NAME_HEADER: [
                        "new compound 1",
                        "existing synonym",  # New compound name that already exists as a synonym of a different cmpd
                    ],
                    CompoundsLoader.FORMULA_HEADER: [
                        "C3",
                        "C4",
                    ],
                    CompoundsLoader.HMDB_ID_HEADER: [
                        "HMDB3333333",
                        "HMDB4444444",
                    ],
                    CompoundsLoader.SYNONYMS_HEADER: [
                        "existing compound name",  # New synonym that already exists as a name of a different compound
                        "",
                    ],
                }
            )
        )
        with self.assertRaises(AggregatedErrors) as ar:
            cl.load_compound_data()
        aes = ar.exception
        self.assertEqual(2, aes.num_errors)
        self.assertEqual(SynonymExistsAsMismatchedCompound, type(aes.exceptions[0]))
        self.assertEqual(CompoundExistsAsMismatchedSynonym, type(aes.exceptions[1]))


@override_settings(CACHES=settings.TEST_CACHES)
class CompoundsLoaderTests(TracebaseTestCase):
    def get_dataframe(self):
        return pd.read_csv(
            "DataRepo/data/examples/testing_data/short_compound_list.tsv",
            sep="\t",
            keep_default_na=False,
        )

    def test_compound_exists_skipped(self):
        df = self.get_dataframe()
        cl = CompoundsLoader(df)
        cl.load_compound_data()
        cl2 = CompoundsLoader(df)
        cl2.load_compound_data()
        self.assertEqual(0, cl2.num_inserted_compounds)
        self.assertEqual(0, cl2.num_erroneous_compounds)
        self.assertEqual(1, cl2.num_existing_compounds)
        self.assertEqual(0, cl2.num_inserted_synonyms)
        self.assertEqual(0, cl2.num_erroneous_synonyms)
        self.assertEqual(0, cl2.num_existing_synonyms)

    def test_synonym_created_from_compound_name(self):
        # Make sure the compound/synonym do not exist before the test
        self.assertEqual(
            0, Compound.objects.filter(name__exact="my new compound").count()
        )
        self.assertEqual(
            0, CompoundSynonym.objects.filter(name__exact="my new compound").count()
        )

        cl = CompoundsLoader(
            compounds_df=pd.DataFrame.from_dict(
                {
                    CompoundsLoader.NAME_HEADER: ["my new compound"],
                    CompoundsLoader.FORMULA_HEADER: ["C11H24N72"],
                    CompoundsLoader.HMDB_ID_HEADER: ["HMDB1111111"],
                    CompoundsLoader.SYNONYMS_HEADER: ["placeholder synonym"],
                }
            )
        )
        cl.load_compound_data()

        # The fact these 2 gets don't raise an exception is a test that the load worked
        ncpd = Compound.objects.get(name__exact="my new compound")
        ns = CompoundSynonym.objects.get(name__exact="my new compound")
        self.assertEqual(ncpd, ns.compound, msg="Compound name is saved as a synonym")


@override_settings(CACHES=settings.TEST_CACHES)
@tag("compound_loading")
class CompoundValidationLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/data/examples/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/data/examples/consolidated_tracebase_compound_list.tsv",
            dry_run=True,
            verbosity=0,
        )
        # validate only; nothing gets loaded
        cls.ALL_COMPOUNDS_COUNT = 0

        super().setUpTestData()

    def test_compounds_loaded(self):
        self.assertEqual(self.ALL_COMPOUNDS_COUNT, Compound.objects.all().count())
