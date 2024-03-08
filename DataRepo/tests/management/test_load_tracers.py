from django.core.management import call_command

from DataRepo.models import Compound, Tracer
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import AggregatedErrors, InfileError
from DataRepo.utils.infusate_name_parser import IsotopeData, TracerData


class LoadTracersCommandTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    LYSINE_TRACER_DATA = TracerData(
        unparsed_string="lysine-[13C6]",
        compound_name="lysine",
        isotopes=[
            IsotopeData(
                element="C",
                mass_number=13,
                count=6,
                positions=None,
            )
        ],
    )

    PARTIAL_LYSINE_TRACER_DATA = TracerData(
        unparsed_string="lysine-[1,2,3,4,5-13C5]",
        compound_name="lysine",
        isotopes=[
            IsotopeData(
                element="C",
                mass_number=13,
                count=5,
                positions=[1, 2, 3, 4, 5],
            )
        ],
    )

    @classmethod
    def setUpTestData(cls):
        Compound.objects.create(
            name="lysine", formula="C6H14N2O2", hmdb_id="HMDB0000182"
        )
        Compound.objects.create(
            name="isoleucine", formula="C6H13NO2", hmdb_id="HMDB0000172"
        )
        Compound.objects.create(
            name="glutamine", formula="C5H10N2O3", hmdb_id="HMDB0000641"
        )
        Compound.objects.create(
            name="alanine", formula="C3H7NO2", hmdb_id="HMDB0000161"
        )
        Compound.objects.create(
            name="valine", formula="C5H11NO2", hmdb_id="HMDB0000883"
        )
        Compound.objects.create(
            name="leucine", formula="C6H13NO2", hmdb_id="HMDB0000687"
        )
        Compound.objects.create(
            name="threonine", formula="C4H9NO3", hmdb_id="HMDB0000167"
        )
        super().setUpTestData()

    def test_names_only_ok(self):
        call_command(
            "load_tracers",
            infile="DataRepo/data/tests/tracers/lysine_name_only.tsv",
        )
        self.assertEqual(1, Tracer.objects.count())
        self.assertIsNotNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

    def test_names_only_excel_ok(self):
        call_command(
            "load_tracers",
            infile="DataRepo/data/tests/tracers/lysine_name_only.xlsx",
        )
        self.assertEqual(1, Tracer.objects.count())
        self.assertIsNotNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

    def test_column_data_only_ok(self):
        call_command(
            "load_tracers",
            infile="DataRepo/data/tests/tracers/lysine_data_only.tsv",
        )
        self.assertIsNotNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

    def test_name_and_column_mix_ok(self):
        call_command(
            "load_tracers",
            infile="DataRepo/data/tests/tracers/lysine_full.tsv",
        )
        self.assertIsNotNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

    def test_partially_labeled(self):
        call_command(
            "load_tracers",
            infile="DataRepo/data/tests/tracers/lysine_not_fully_labeled.tsv",
        )
        self.assertIsNotNone(Tracer.objects.get_tracer(self.PARTIAL_LYSINE_TRACER_DATA))

    def test_name_with_multiple_numbers_error(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_tracers",
                infile="DataRepo/data/tests/tracers/tracers_with_errors.tsv",
            )
        aes = ar.exception
        expected_num_exceptions = 7
        self.assertEqual(expected_num_exceptions, len(aes.exceptions))

        self.assertEqual(
            expected_num_exceptions, len(aes.get_exception_type(InfileError))
        )

        # EXCEPTION1(WARNING): InfileError: There are [2] rows [2, 3] of data defining isotopes for Tracer Name
        # [lysine-[13C6]] in file [DataRepo/data/tests/tracers/tracers_with_errors.tsv], but the number of labels parsed
        # from the Tracer Name [1] does not match the number of rows for Tracer Number 1.  Perhaps Tracer Number 1 is on
        # the wrong number of rows?
        self.assertIn("[2] rows [2, 3]", str(aes.exceptions[0]))
        self.assertIn("[lysine-[13C6]]", str(aes.exceptions[0]))
        self.assertIn("parsed from the Tracer Name [1]", str(aes.exceptions[0]))
        self.assertIn("Tracer Number 1.", str(aes.exceptions[0]))

        # EXCEPTION2(ERROR): InfileError: Isotope data from columns [Element: C, Mass Number: 14, Label Count: 4, Label
        # Positions: None] on row(s) [4, 5] does not match any of the isotopes parsed from the Tracer Name
        # [threonine-[13C4]] on row [4] in file [DataRepo/data/tests/tracers/tracers_with_errors.tsv].
        self.assertIn(
            "[Element: C, Mass Number: 14, Label Count: 4, Label Positions: None]",
            str(aes.exceptions[1]),
        )
        self.assertIn("row(s) [4, 5]", str(aes.exceptions[1]))
        self.assertIn(
            "Tracer Name [threonine-[13C4]] on row [4]", str(aes.exceptions[1])
        )
        self.assertIn("does not match any of the isotopes", str(aes.exceptions[1]))

        # EXCEPTION3(WARNING): InfileError: There are [2] rows [4, 5] of data defining isotopes for Tracer Name
        # [threonine-[13C4]] in file [DataRepo/data/tests/tracers/tracers_with_errors.tsv], but the number of labels
        # parsed from the Tracer Name [1] does not match the number of rows for Tracer Number 2.  Perhaps Tracer Number
        # 2 is on the wrong number of rows?
        self.assertIn("[2] rows [4, 5]", str(aes.exceptions[2]))
        self.assertIn("Tracer Name [threonine-[13C4]]", str(aes.exceptions[2]))
        self.assertIn("parsed from the Tracer Name [1]", str(aes.exceptions[2]))
        self.assertIn("Tracer Number 2.", str(aes.exceptions[2]))

        # EXCEPTION4(ERROR): InfileError: Compound name from column [aspartate] does not match the name parsed from the
        # tracer name (aspartame-[13C4]): [aspartame]: column [Compound Name] on row [7] in file
        # [DataRepo/data/tests/tracers/tracers_with_errors.tsv]
        self.assertIn("[aspartate]", str(aes.exceptions[3]))
        self.assertIn("Tracer Name (aspartame-[13C4])", str(aes.exceptions[3]))
        self.assertIn("row [7]", str(aes.exceptions[3]))

        # EXCEPTION5(ERROR): InfileError: column [Tracer Number and Compound Name] in file
        # [DataRepo/data/tests/tracers/tracers_with_errors.tsv]:
        # Tracer Number 1 is associated with multiple Compound Names on the indicated rows.  Only one Compound Name is
        # allowed per Tracer Number.
        #     lysine (on rows: [2])
        #     asparagine (on rows: [3])
        self.assertIn("[Tracer Number and Compound Name]", str(aes.exceptions[4]))
        self.assertIn("lysine (on rows: [2])", str(aes.exceptions[4]))
        self.assertIn("asparagine (on rows: [3])", str(aes.exceptions[4]))

        # EXCEPTION6(ERROR): InfileError: column [Tracer Name and Tracer Number] in file
        # [DataRepo/data/tests/tracers/tracers_with_errors.tsv]:
        # Tracer Number 2 is associated with multiple Tracer Names on the indicated rows.  Only one Tracer Name is
        # allowed per Tracer Number.
        #     threonine-[13C4] (on rows: [4])
        #     threonine-[14C4] (on rows: [5])
        self.assertIn("[Tracer Name and Tracer Number]", str(aes.exceptions[5]))
        self.assertIn("threonine-[13C4] (on rows: [4])", str(aes.exceptions[5]))
        self.assertIn("threonine-[14C4] (on rows: [5])", str(aes.exceptions[5]))

        # EXCEPTION7(ERROR): InfileError: column [Tracer Number and Tracer Name] in file
        # [DataRepo/data/tests/tracers/tracers_with_errors.tsv]:
        # Tracer Name aspartame-[13C4] is associated with multiple Tracer Numbers on the indicated rows.  Only one
        # Tracer Number is allowed per Tracer Name.
        #     3 (on rows: [6])
        #     4 (on rows: [7])
        self.assertIn("[Tracer Number and Tracer Name]", str(aes.exceptions[6]))
        self.assertIn("3 (on rows: [6])", str(aes.exceptions[6]))
        self.assertIn("4 (on rows: [7])", str(aes.exceptions[6]))

    def test_dupe_isotopes_ok_when_second_isotopes_differ(self):
        call_command(
            "load_tracers",
            infile="DataRepo/data/tests/tracers/two_dual_labeled_isoptopes.tsv",
        )
        self.assertEqual(2, Tracer.objects.count())
        Tracer.objects.get(name="lysine-[13C6,15N2]")
        Tracer.objects.get(name="isoleucine-[13C6,15N1]")
