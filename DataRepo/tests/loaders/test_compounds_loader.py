from DataRepo.loaders.compounds_loader import CompoundsLoader
from DataRepo.models.compound import Compound, CompoundSynonym
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import ProhibitedCompoundNames
from DataRepo.utils.file_utils import read_from_file


class CompoundsLoaderTests(TracebaseTestCase):
    def test_prohibited_delimiters(self):
        cl = CompoundsLoader(
            file="DataRepo/data/tests/compounds/compounds_with_prohibited_delimiters.tsv",
            df=read_from_file(
                "DataRepo/data/tests/compounds/compounds_with_prohibited_delimiters.tsv"
            ),
        )
        cl.load_data()
        # Test fails if either of these raises
        Compound.objects.get(name="2:hydroxyglutarate")
        CompoundSynonym.objects.get(name="L-2\\hydroxyglutarate")
        self.assertEqual(1, len(cl.aggregated_errors_object.exceptions))
        self.assertTrue(
            cl.aggregated_errors_object.exception_type_exists(ProhibitedCompoundNames)
        )
        self.assertFalse(cl.aggregated_errors_object.exceptions[0].is_error)
        self.assertFalse(cl.aggregated_errors_object.exceptions[0].is_fatal)
        # Check the suggestion added by the loader
        self.assertIn(
            "You may manually edit the compound names",
            str(cl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "with whatever replacement characters you wish",
            str(cl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "do so in both the study doc's Compounds sheet",
            str(cl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "AND in all peak annotation files.",
            str(cl.aggregated_errors_object.exceptions[0]),
        )
