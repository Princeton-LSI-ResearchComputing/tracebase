from django.core.management import CommandError, call_command
from django.test import TestCase, tag

from DataRepo.models import Tissue


@tag("tissues")
class TissueLoadingTests(TestCase):
    """Test Tissue Loader"""

    def test_load_tissue_command(self):
        """Test the load_tissue management command"""
        call_command(
            "load_tissues",
            tissues="DataRepo/example_data/tissues/tissues.tsv",
        )
        self.assertEqual(Tissue.objects.count(), 36)

    def test_load_tissue_command_dry_run(self):
        """Test dry run of the load_tissue management command"""
        call_command(
            "load_tissues",
            tissues="DataRepo/example_data/tissues/tissues.tsv",
            dry_run=True,
        )
        # Dry run should not load any records
        self.assertEqual(Tissue.objects.count(), 0)

    def test_load_tissue_command_with_errors(self):
        """Test the load_tissue management command with file containing errors"""
        with self.assertRaisesRegex(
            CommandError,
            r"2 errors loading tissue records from .*tissues_with_errors\.tsv - NO RECORDS SAVED",
        ):
            call_command(
                "load_tissues",
                tissues="DataRepo/example_data/testing_data/tissues/tissues_with_errors.tsv",
            )
        # If errors are found, no records shold be loaded
        self.assertEqual(Tissue.objects.count(), 0)
