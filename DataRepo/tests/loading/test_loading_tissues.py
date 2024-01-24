from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.db import IntegrityError
from django.test import tag

from DataRepo.models import Tissue
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueErrors,
    DuplicateValues,
    RequiredValueErrors,
)


@tag("tissues")
class TissueLoadingTests(TracebaseTestCase):
    """Test Tissue Loader"""

    def test_load_tissue_command(self):
        """Test the load_tissue management command"""
        call_command(
            "load_tissues",
            tissues="DataRepo/data/tests/tissues/tissues.tsv",
        )
        self.assertEqual(Tissue.objects.count(), 37)

    def test_load_tissue_command_dry_run(self):
        """Test dry run of the load_tissue management command"""
        call_command(
            "load_tissues",
            tissues="DataRepo/data/tests/tissues/tissues.tsv",
            dry_run=True,
        )
        # Dry run should not load any records
        self.assertEqual(Tissue.objects.count(), 0)

    def test_load_tissue_command_with_errors(self):
        """Test the load_tissue management command with file containing errors"""
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_tissues",
                tissues="DataRepo/data/tests/tissues/tissues_with_errors.tsv",
                verbosity=2,
            )
        aes = ar.exception
        self.assertEqual(2, aes.num_errors)  # ConflictingValueErrors
        self.assertEqual(1, aes.num_warnings)  # DuplicateValues

        # First exception
        self.assertEqual(DuplicateValues, type(aes.exceptions[0]))
        self.assertIn("2 values in unique column(s) ['Tissue']", str(aes.exceptions[0]))
        self.assertFalse(aes.exceptions[0].is_error)

        # Second error
        self.assertEqual(ConflictingValueErrors, type(aes.exceptions[1]))
        self.assertIn("description", aes.exceptions[1].conflicting_value_errors[0].differences.keys())
        self.assertEqual(
            "a different description should cause an error",
            aes.exceptions[1].conflicting_value_errors[0].differences["description"]["new"],
        )
        self.assertEqual(
            "a description", aes.exceptions[1].conflicting_value_errors[0].differences["description"]["orig"]
        )
        self.assertEqual(3, aes.exceptions[1].conflicting_value_errors[0].rownum)

        # Third error
        self.assertEqual(RequiredValueErrors, type(aes.exceptions[2]))
        self.assertIn(
            "Column: [name] on row(s): 6",
            str(aes.exceptions[2]),
        )
        self.assertIn(
            "Column: [description] on row(s): 7",
            str(aes.exceptions[2]),
        )

        # If errors are found, no records should be loaded
        self.assertEqual(Tissue.objects.count(), 0)
