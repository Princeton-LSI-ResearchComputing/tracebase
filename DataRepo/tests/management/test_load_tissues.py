from django.core.management import call_command
from django.test import tag

from DataRepo.models import Tissue
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    DryRun,
    DuplicateValueErrors,
    RequiredColumnValues,
)


@tag("tissues")
class TissueLoadingTests(TracebaseTestCase):
    """Test Tissue Loader"""

    def test_load_tissue_command(self):
        """Test the load_tissue management command"""
        call_command(
            "load_tissues",
            infile="DataRepo/data/tests/tissues/tissues.tsv",
        )
        self.assertEqual(Tissue.objects.count(), 37)

    def test_load_tissue_command_dry_run(self):
        """Test dry run of the load_tissue management command"""
        with self.assertRaises(DryRun):
            call_command(
                "load_tissues",
                infile="DataRepo/data/tests/tissues/tissues.tsv",
                dry_run=True,
            )
        # Dry run should not load any records
        self.assertEqual(0, Tissue.objects.count())

    def test_load_tissue_command_with_errors(self):
        """Test the load_tissue management command with file containing errors"""
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_tissues",
                infile="DataRepo/data/tests/tissues/tissues_with_errors.tsv",
                verbosity=2,
            )
        aes = ar.exception
        self.assertEqual(2, aes.num_errors)
        # Used to get a ConflictingValueErrors exception, but adding the unique constraint check made it redundant
        self.assertEqual(
            0,
            aes.num_warnings,
            msg=(
                "There should be 0 warnings (2 exceptions total). Exceptions: "
                f"{', '.join([type(e).__name__ for e in aes.exceptions])}"
            ),
        )

        dves = aes.get_exception_type(DuplicateValueErrors)
        self.assertEqual(1, len(dves))
        self.assertIn(
            "Column(s) ['Tissue']",
            str(dves[0]),
            msg=f"Expected [Column(s) ['Tissue']] in exception, but it is: [{dves[0]}]",
        )
        self.assertIn(
            "brown_adipose_tissue (rows*: 2-3)",
            str(dves[0]),
            msg=f"Expected [brown_adipose_tissue (rows*: 2-3)] in exception, but it is: [{dves[0]}]",
        )
        self.assertIn(
            "brain (rows*: 4-5)",
            str(dves[0]),
            msg=f"Expected [brain (rows*: 4-5)] in exception, but it is: [{dves[0]}]",
        )
        self.assertTrue(aes.exceptions[1].is_error)

        rcvs = aes.get_exception_type(RequiredColumnValues)
        self.assertEqual(1, len(rcvs))
        self.assertIn(
            (
                "Required column values missing on the indicated rows:\n"
                "\tDataRepo/data/tests/tissues/tissues_with_errors.tsv\n"
                "\t\tColumn: [Tissue] on rows: ['6']\n"
                "\t\tColumn: [Description] on rows: ['7']\n"
            ),
            str(rcvs[0]),
        )
        self.assertIn(
            (
                "Required column values missing on the indicated rows:\n"
                "\tDataRepo/data/tests/tissues/tissues_with_errors.tsv\n"
                "\t\tColumn: [Tissue] on rows: ['6']\n"
                "\t\tColumn: [Description] on rows: ['7']\n"
            ),
            str(rcvs[0]),
        )

        # If errors are found, no records should be loaded
        self.assertEqual(Tissue.objects.count(), 0)
