from django.core.management import CommandError, call_command
from django.test import TestCase, tag

from DataRepo.models import Compound


@tag("compounds")
@tag("loading")
class CompoundLoadingTests(TestCase):
    """Tests Loading of Compounds"""

    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")

    def testCompoundLoading(self):
        """Test the compounds and synonyms are loaded"""

    def testCompoundLoadingFailure(self):
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
