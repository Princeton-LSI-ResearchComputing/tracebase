import pandas as pd
from django.conf import settings
from django.core.management import call_command
from django.test import override_settings, tag

from DataRepo.loaders import CompoundsLoader
from DataRepo.models import Compound, CompoundSynonym
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils import (
    AggregatedErrors,
    CompoundExistsAsMismatchedSynonym,
    ConflictingValueErrors,
    DuplicateValueErrors,
    DuplicateValues,
    SynonymExistsAsMismatchedCompound,
    UnknownHeaders,
)


@tag("compounds")
@tag("loading")
class LoadCompoundsTests(TracebaseTestCase):
    """Tests Loading of Compounds"""

    def test_compound_loading_failure(self):
        """Test that an error during compound loading doesn't load any compounds"""

        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_compounds",
                infile="DataRepo/data/tests/compounds/test_study_1_compounds_dupes.tsv",
            )
        aes = ar.exception
        self.assertEqual(
            2,
            len(aes.exceptions),
            msg=f"Should be 2 exceptions, but got: {', '.join([type(exc).__name__ for exc in aes.exceptions])}",
        )
        self.assertEqual(UnknownHeaders, type(aes.exceptions[0]))
        self.assertEqual(["m/z", "RT"], aes.exceptions[0].unknowns)
        self.assertEqual(DuplicateValueErrors, type(aes.exceptions[1]))
        self.assertIn("lactate", str(aes.exceptions[1]))
        self.assertIn("L-Lactic acid", str(aes.exceptions[1]))
        self.assertEqual(Compound.objects.count(), 0)

    def test_excel_with_compounds_sheet(self):
        self.assertEqual(0, Compound.objects.filter(name__exact="C18:2").count())
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/c182_compounds.xlsx",
        )
        Compound.objects.get(name__exact="C18:2")

    def test_excel_with_generic_first_sheet_name(self):
        """
        If a sheet with the default "Compounds" sheet name doesn't exist, the loader should fall back to the first sheet
        """
        self.assertEqual(0, Compound.objects.filter(name__exact="C18:2").count())
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/c182_sheet1.xlsx",
        )
        Compound.objects.get(name__exact="C18:2")

    def test_excel_with_alternate_sheet_name(self):
        self.assertEqual(0, Compound.objects.filter(name__exact="C18:2").count())
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/c182_things.xlsx",
            data_sheet="Things",
        )
        Compound.objects.get(name__exact="C18:2")


@override_settings(CACHES=settings.TEST_CACHES)
@tag("compound_loading")
class CompoundLoadingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        primary_compound_file = (
            "DataRepo/data/tests/compounds/consolidated_tracebase_compound_list.tsv"
        )

        try:
            call_command(
                "load_compounds",
                infile=primary_compound_file,
                verbosity=0,
            )
        except AggregatedErrors as aes:
            raise aes
        cls.ALL_COMPOUNDS_COUNT = 51

        cls.COMPOUND_WITH_MANY_NAMES = Compound.objects.get(name="a-ketoglutarate")

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
        with self.assertRaises(Compound.DoesNotExist) as ar:
            Compound.compound_matching_name_or_synonym("nonsense")
        self.assertIn("nonsense", str(ar.exception))

    @tag("compound_for_row")
    def test_new_synonyms_added_to_existing_compound(self):
        """
        This (renamed) test used to assure that the synonym is associated with a previously loaded compound when the
        data for the compound in the load was nonsense.  However, this is no longer supported.  The data in the load
        must be consistent and the load script should not associate a synonym with what is defined as a different
        compound in the load file.  So instead, this test will assure that new synonyms added to a correctly defined
        compound is associated with a previously loaded compound, to add NEW valid synonyms.
        """
        datadict = {
            "Compound": ["fructose-1-6-bisphosphate"],
            "Formula": ["C6H14O12P2"],
            "HMDB ID": ["HMDB0001058"],
            "Synonyms": [
                (
                    "Fructose 1,6-bisphosphate;Fructose-1,6-diphosphate;"
                    "Fructose 1,6-diphosphate;Diphosphofructose;new valid synonym"
                )
            ],
        }
        # create dataframe from dictionary
        cl = CompoundsLoader(df=pd.DataFrame.from_dict(datadict))
        cl.load_data()
        self.assertEqual(
            1,
            cl.record_counts["Compound"]["existed"],
            msg="Compound default insert should be skipped",
        )
        self.assertEqual(
            0,
            cl.record_counts["Compound"]["created"],
            msg="No compounds default should be inserted",
        )
        self.assertEqual(
            0,
            cl.record_counts["Compound"]["errored"],
            msg="No compounds default should be in error",
        )

        self.assertEqual(
            4,
            cl.record_counts["CompoundSynonym"]["existed"],
            msg="4 synonym default inserts should be skipped",
        )
        self.assertEqual(
            1,
            cl.record_counts["CompoundSynonym"]["created"],
            msg="1 synonym default should be inserted",
        )
        self.assertEqual(
            0,
            cl.record_counts["CompoundSynonym"]["errored"],
            msg="No synonyms default should be in error",
        )
        self.assertEqual(
            "fructose-1-6-bisphosphate",
            CompoundSynonym.objects.get(name__exact="new valid synonym").compound.name,
            msg="Assure new synonym is associated with the correct existing compound record.",
        )

    @tag("compound_for_row")
    def test_synonym_matches_existing_compound_inconsistent_with_load_data(self):
        """
        Test that an exception is raised when synonyms on one row refer to two existing compound records in the database
        Synonym "Fructose 1,6-bisphosphate" refers to compound "fructose-1-6-bisphosphate" and synonym "glucose" refers
        to compound "glucose".
        """
        datadict = CompoundsLoader.header_key_to_name(
            {
                CompoundsLoader.NAME_KEY: ["nonsense"],
                CompoundsLoader.HMDBID_KEY: ["nonsense"],
                CompoundsLoader.FORMULA_KEY: ["nonsense"],
                CompoundsLoader.SYNONYMS_KEY: ["Fructose 1,6-bisphosphate;glucose"],
            }
        )
        # create dataframe from dictionary
        cl = CompoundsLoader(df=pd.DataFrame.from_dict(datadict))
        with self.assertRaises(AggregatedErrors) as ar:
            cl.load_data()
        aes = ar.exception
        self.assertEqual(1, aes.num_errors)
        self.assertEqual(ConflictingValueErrors, type(aes.exceptions[0]))
        self.assertEqual(
            2,
            len(aes.exceptions[0].exceptions),
            msg="There are 2 conflicts",
        )
        self.assertEqual(
            2,
            len(
                [
                    exc
                    for exc in aes.exceptions[0].exceptions
                    if "compound" in exc.differences.keys()
                ]
            ),
            msg="Both exceptions are conflicting value errors about the compound field",
        )

    @tag("compound_for_row")
    def test_synonym_compound_mismatches(self):
        """
        Test that an exception is raised when synonyms on one row refer to two existing compound records in the database
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
        datadict = CompoundsLoader.header_key_to_name(
            {
                CompoundsLoader.NAME_KEY: [
                    "new compound 1",
                    "existing synonym",  # New compound name that already exists as a synonym of a different cmpd
                ],
                CompoundsLoader.FORMULA_KEY: [
                    "C3",
                    "C4",
                ],
                CompoundsLoader.HMDBID_KEY: [
                    "HMDB3333333",
                    "HMDB4444444",
                ],
                CompoundsLoader.SYNONYMS_KEY: [
                    "existing compound name",  # New synonym that already exists as a name of a different compound
                    "",
                ],
            }
        )
        # create dataframe from dictionary
        cl = CompoundsLoader(df=pd.DataFrame.from_dict(datadict))
        with self.assertRaises(AggregatedErrors) as ar:
            cl.load_data()
        aes = ar.exception
        self.assertEqual(2, aes.num_errors)
        self.assertEqual(SynonymExistsAsMismatchedCompound, type(aes.exceptions[0]))
        self.assertEqual(CompoundExistsAsMismatchedSynonym, type(aes.exceptions[1]))


@override_settings(CACHES=settings.TEST_CACHES)
class CompoundsLoaderTests(TracebaseTestCase):
    def get_dataframe(self):
        return pd.read_csv(
            "DataRepo/data/tests/compounds/short_compound_list.tsv",
            sep="\t",
            keep_default_na=False,
        )

    def test_compound_exists_skipped(self):
        df = self.get_dataframe()
        cl = CompoundsLoader(df=df)
        cl.load_data()
        cl2 = CompoundsLoader(df=df)
        cl2.load_data()
        self.assertEqual(0, cl2.record_counts["Compound"]["created"])
        self.assertEqual(0, cl2.record_counts["Compound"]["errored"])
        self.assertEqual(1, cl2.record_counts["Compound"]["existed"])
        self.assertEqual(0, cl2.record_counts["CompoundSynonym"]["created"])
        self.assertEqual(0, cl2.record_counts["CompoundSynonym"]["errored"])
        self.assertEqual(0, cl2.record_counts["CompoundSynonym"]["existed"])

    def test_synonym_created_from_compound_name(self):
        # Make sure the compound/synonym do not exist before the test
        self.assertEqual(
            0, Compound.objects.filter(name__exact="my new compound").count()
        )
        self.assertEqual(
            0, CompoundSynonym.objects.filter(name__exact="my new compound").count()
        )

        datadict = CompoundsLoader.header_key_to_name(
            {
                CompoundsLoader.NAME_KEY: ["my new compound"],
                CompoundsLoader.FORMULA_KEY: ["C11H24N72"],
                CompoundsLoader.HMDBID_KEY: ["HMDB1111111"],
                CompoundsLoader.SYNONYMS_KEY: ["placeholder synonym"],
            }
        )
        cl = CompoundsLoader(df=pd.DataFrame.from_dict(datadict))
        cl.load_data()

        # The fact these 2 gets don't raise an exception is a test that the load worked
        ncpd = Compound.objects.get(name__exact="my new compound")
        ns = CompoundSynonym.objects.get(name__exact="my new compound")
        self.assertEqual(ncpd, ns.compound, msg="Compound name is saved as a synonym")

    def test_check_for_cross_column_name_duplicates(self):
        """
        Make sure that check_for_cross_column_name_duplicates buffers exceptions for compounds whose name matches a
        synonym of another compound, and buffers exceptions for synonyms common between compound rows.
        """
        datadict = CompoundsLoader.header_key_to_name(
            {
                CompoundsLoader.NAME_KEY: ["C1", "B"],
                CompoundsLoader.FORMULA_KEY: ["C1", "C2"],
                CompoundsLoader.HMDBID_KEY: ["HMDB1", "HMDB2"],
                CompoundsLoader.SYNONYMS_KEY: ["A;B", "A"],
            }
        )
        cl = CompoundsLoader(df=pd.DataFrame.from_dict(datadict))
        cl.check_for_cross_column_name_duplicates()
        self.assertEqual(
            (2, 0),
            (
                cl.aggregated_errors_object.num_errors,
                cl.aggregated_errors_object.num_warnings,
            ),
        )
        self.assertEqual(
            [DuplicateValues, DuplicateValues],
            [type(e) for e in cl.aggregated_errors_object.exceptions],
        )
        print(str(cl.aggregated_errors_object.exceptions[0]))
        self.assertIn(
            (
                "The following unique column (or column combination) ['Synonyms'] was found to have duplicate "
                "occurrences in the load file data on the indicated rows:\n\tA (rows*: 2-3)"
            ),
            str(cl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            (
                "The following unique column (or column combination) ['Compound and Synonyms'] was found to have "
                "duplicate occurrences in the load file data on the indicated rows:\n\tB (rows*: 2-3)"
            ),
            str(cl.aggregated_errors_object.exceptions[1]),
        )


@override_settings(CACHES=settings.TEST_CACHES)
@tag("compound_loading")
class CompoundValidationLoadingTests(TracebaseTestCase):
    def test_compounds_not_loaded_in_dry_run(self):
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/consolidated_tracebase_compound_list.tsv",
            dry_run=True,
            verbosity=0,
        )
        self.assertEqual(0, Compound.objects.all().count())
