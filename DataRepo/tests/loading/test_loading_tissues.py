from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.db import IntegrityError
from django.test import tag

from DataRepo.models import Tissue
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    LoadFileError,
)


@tag("tissues")
class TissueLoadingTests(TracebaseTestCase):
    """Test Tissue Loader"""

    def test_load_tissue_command(self):
        """Test the load_tissue management command"""
        call_command(
            "load_tissues",
            tissues="DataRepo/data/examples/tissues/tissues.tsv",
        )
        self.assertEqual(Tissue.objects.count(), 37)

    def test_load_tissue_command_dry_run(self):
        """Test dry run of the load_tissue management command"""
        call_command(
            "load_tissues",
            tissues="DataRepo/data/examples/tissues/tissues.tsv",
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
        self.assertEqual(3, aes.num_errors)
        self.assertEqual(0, aes.num_warnings)

        # First error
        self.assertEqual(ConflictingValueError, type(aes.exceptions[0]))
        self.assertEqual("description", aes.exceptions[0].consistent_field)
        self.assertEqual(
            "a different description should cause an error",
            aes.exceptions[0].differing_value,
        )
        self.assertEqual("a description", aes.exceptions[0].existing_value)
        self.assertEqual(3, aes.exceptions[0].rownum)

        # Second error
        self.assertEqual(LoadFileError, type(aes.exceptions[1]))
        self.assertEqual(6, aes.exceptions[1].line_num)
        self.assertEqual(IntegrityError, type(aes.exceptions[1].exception))
        self.assertIn(
            'null value in column "name"',
            str(aes.exceptions[1].exception),
        )
        self.assertIn(
            "violates not-null constraint",
            str(aes.exceptions[1].exception),
        )

        # Third error
        self.assertEqual(LoadFileError, type(aes.exceptions[2]))
        self.assertEqual(8, aes.exceptions[2].line_num)
        self.assertEqual(ValidationError, type(aes.exceptions[2].exception))
        self.assertIn(
            (
                "Tissue with name 'space but no description is a problem' cannot contain a space unless a description "
                "is provided.  Either the space(s) must be changed to a tab character or a description must be "
                "provided."
            ),
            str(aes.exceptions[2].exception),
        )

        # If errors are found, no records should be loaded
        self.assertEqual(Tissue.objects.count(), 0)
