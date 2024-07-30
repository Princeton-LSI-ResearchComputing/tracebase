import pandas as pd
from django.core.management import call_command

from DataRepo.models import Compound, Infusate, Tracer
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    InfileDatabaseError,
    InfileError,
)
from DataRepo.utils.infusate_name_parser import (
    InfusateData,
    InfusateTracerData,
    IsotopeData,
    TracerData,
)


class LoadInfusatesCommandTests(TracebaseTestCase):
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

    ISOLEUCINE_TRACER_DATA = TracerData(
        unparsed_string="isoleucine-[13C6]",
        compound_name="isoleucine",
        isotopes=[
            IsotopeData(
                element="C",
                mass_number=13,
                count=6,
                positions=None,
            )
        ],
    )

    GLUTAMINE_TRACER_DATA = TracerData(
        unparsed_string="glutamine-[13C5]",
        compound_name="glutamine",
        isotopes=[
            IsotopeData(
                element="C",
                mass_number=13,
                count=5,
                positions=None,
            )
        ],
    )

    DUDERINO_INFUSATE_DATA = InfusateData(
        unparsed_string="duderino{lysine-[13C6]}",
        infusate_name="duderino",
        tracers=[
            InfusateTracerData(
                tracer=LYSINE_TRACER_DATA,
                concentration=20,
            ),
        ],
    )

    DUDERINO_INFUSATE_DATAFRAME = pd.DataFrame.from_dict(
        {
            "Infusate Row Group": [1],
            "Infusate Name": ["duderino{lysine-[13C6]}"],
            "Tracer Group Name": ["duderino"],
            "Tracer Name": ["lysine-[13C6]"],
            "Tracer Concentration": [20],
        },
    )

    INFUSATES_DICT = {
        1: {
            "infusate_name": "duderino{lysine-[13C6]}",
            "tracer_group_name": "duderino",
            "tracers": [
                {
                    "tracer_name": "lysine-[13C6]",
                    "tracer_concentration": 20,
                    "row_index": 0,
                    "rownum": 2,
                },
            ],
            "row_index": 0,
            "rownum": 2,
        },
    }

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
        Tracer.objects.get_or_create_tracer(cls.LYSINE_TRACER_DATA)
        Tracer.objects.get_or_create_tracer(cls.ISOLEUCINE_TRACER_DATA)
        Tracer.objects.get_or_create_tracer(cls.GLUTAMINE_TRACER_DATA)
        super().setUpTestData()

    def test_names_concs_numbers_only_ok(self):
        call_command(
            "load_infusates",
            infile="DataRepo/data/tests/infusates/lysine_num_name_conc_only.tsv",
        )
        self.assertEqual(1, Infusate.objects.count())
        self.assertIsNotNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

    def test_names_concs_numbers_only_excel_ok(self):
        call_command(
            "load_infusates",
            infile="DataRepo/data/tests/infusates/lysine_num_name_conc_only.xlsx",
        )
        self.assertEqual(1, Infusate.objects.count())
        self.assertIsNotNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

    def test_column_data_only_ok(self):
        call_command(
            "load_infusates",
            infile="DataRepo/data/tests/infusates/lysine_data_only.tsv",
        )
        self.assertIsNotNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

    def test_name_and_column_mix_ok(self):
        call_command(
            "load_infusates",
            infile="DataRepo/data/tests/infusates/lysine_full.tsv",
        )
        self.assertIsNotNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

    def test_name_with_multiple_numbers_error(self):
        """Since Infusate name, tracer name, group name, and concentration must be unique, 1 infusate name with multiple
        infusate numbers comes out as a DuplicateValueError.  Note, row 3 (with a different conc is fine.
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_infusates",
                infile="DataRepo/data/tests/infusates/name_with_mult_nums.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(1, len(aes.get_exception_type(InfileError)))
        self.assertIn(
            "duplicate infusates",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "lysine-[13C6][20.0] (on rows with 'Infusate Row Group's: [1, 3])",
            str(aes.exceptions[0]),
        )

    def test_number_with_multiple_names_error(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_infusates",
                infile="DataRepo/data/tests/infusates/num_with_mult_names.tsv",
            )
        aes = ar.exception
        self.assertEqual(2, len(aes.exceptions))
        self.assertEqual(2, len(aes.get_exception_type(InfileError)))
        self.assertIn(
            "InfusateParsingError",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "Concentration(s): [20.0, 20.0] on row(s) [2, 3]",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "'Infusate Row Group' 1 is associated with multiple 'Infusate Name's",
            str(aes.exceptions[1]),
        )
        self.assertIn(
            "duderino{lysine-[13C6]} (on rows: [2])",
            str(aes.exceptions[1]),
        )
        self.assertIn(
            "duderino{aspartame-[13C6]} (on rows: [3])",
            str(aes.exceptions[1]),
        )

    def test_one_tracer_with_multiple_concs_error(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_infusates",
                infile="DataRepo/data/tests/infusates/one_tracer_with_mult_concs.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(1, len(aes.get_exception_type(InfileError)))
        self.assertIn(
            "InfusateParsingError",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "Concentration(s): [30.0, 40.0] on row(s) [2, 3]",
            str(aes.exceptions[0]),
        )

    def test_number_with_multiple_tracer_group_names_error(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_infusates",
                infile="DataRepo/data/tests/infusates/num_with_mult_group_names.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(1, len(aes.get_exception_type(InfileError)))
        self.assertIn(
            "Only one 'Tracer Group Name' is allowed per 'Infusate Row Group'",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "duderino (on rows: [2])",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "rinodude (on rows: [3])",
            str(aes.exceptions[0]),
        )

    def test_group_name_with_different_tracers_error(self):
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_infusates",
                infile="DataRepo/data/tests/infusates/group_name_with_diff_tracers.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(1, len(aes.get_exception_type(InfileError)))
        self.assertIn(
            "Tracer Group Name: 'duderino'",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "lysine-[13C6] (on rows with 'Infusate Row Group's: [1])",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "isoleucine-[13C6] (on rows with 'Infusate Row Group's: [2])",
            str(aes.exceptions[0]),
        )

    def test_same_tracers_different_group_names_error(self):
        """If an infusate exists with the same tracers in the same load file (with different concentrations) it is
        added."""
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_infusates",
                infile="DataRepo/data/tests/infusates/same_tracers_differing_group_names.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(1, len(aes.get_exception_type(InfileError)))
        self.assertIn(
            "differing 'Tracer Group Name's",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "same assortment of tracers [lysine-[13C6]]",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "duderino (on rows with 'Infusate Row Group's: [1])",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "rinodude (on rows with 'Infusate Row Group's: [2])",
            str(aes.exceptions[0]),
        )

    def test_infusate_exists_in_db_with_no_name_error(self):
        """If an infusate without a group name exists in the db with the same tracers, an error is raised
        because existing infusates should be consistently applied group names.  We should not have to modify existing
        records.  If a new name is desired to be applied, the existing record would have to be edited.
        """
        GLUTAMINE_TRACER_DATA = TracerData(
            unparsed_string="glutamine-[13C5]",
            compound_name="glutamine",
            isotopes=[
                IsotopeData(
                    element="C",
                    mass_number=13,
                    count=5,
                    positions=None,
                )
            ],
        )
        NOGROUPNAME_INFUSATE_DATA = InfusateData(
            unparsed_string="glutamine-[13C5]",
            infusate_name=None,
            tracers=[
                InfusateTracerData(
                    tracer=GLUTAMINE_TRACER_DATA,
                    concentration=20,
                ),
            ],
        )
        self.assertEqual(0, Infusate.objects.count())
        Infusate.objects.get_or_create_infusate(NOGROUPNAME_INFUSATE_DATA)
        self.assertEqual(1, Infusate.objects.count())
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_infusates",
                infile="DataRepo/data/tests/infusates/glutamine_num_name_conc_only.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(1, len(aes.get_exception_type(InfileDatabaseError)))
        self.assertIn(
            "new infusate: [glutarino {glutamine-[13C5][20]}] with group name: [glutarino]",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "like [glutamine-[13C5][20]]",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "different tracer group name: [None]",
            str(aes.exceptions[0]),
        )

    def test_infusate_exists_in_db_with_name_error(self):
        """If an infusate with a group name exists in the db with the same tracers, an error is raised because existing
        infusates should be consistently applied group names.  We should not have to modify existing records.  If an
        infusate with the same tracers is desired to be applied without a group name, the existing record would have to
        be edited."""
        self.assertEqual(0, Infusate.objects.count())
        Infusate.objects.get_or_create_infusate(self.DUDERINO_INFUSATE_DATA)
        self.assertEqual(1, Infusate.objects.count())
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_infusates",
                infile="DataRepo/data/tests/infusates/lysine_no_group_name.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(1, len(aes.get_exception_type(InfileDatabaseError)))
        self.assertIn(
            "new infusate: [lysine-[13C6][20]] with group name: [None]",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "like [duderino {lysine-[13C6][20]}]",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "different tracer group name: [duderino]",
            str(aes.exceptions[0]),
        )

    def test_infusate_exists_in_file_with_diff_name_error(self):
        """If 2 or more infusates with the same set of tracers exist in the file with different tracer group names, an
        error is raised because the same tracer group must consistently be given the same group name.
        """
        with self.assertRaises(AggregatedErrors) as ar:
            call_command(
                "load_infusates",
                infile="DataRepo/data/tests/infusates/lysine_with_and_without_group_name.tsv",
            )
        aes = ar.exception
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(1, len(aes.get_exception_type(InfileError)))
        self.assertIn(
            "None (on rows with 'Infusate Row Group's: [1])",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "duderino (on rows with 'Infusate Row Group's: [2])",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "same assortment of tracers [lysine-[13C6]]",
            str(aes.exceptions[0]),
        )
        self.assertIn(
            "differing 'Tracer Group Name's",
            str(aes.exceptions[0]),
        )

    def test_dupe_tracer_and_conc_ok_when_second_infusates_differ(self):
        """Assert that 2 infusates can contain the same tracer at the same concentration without error as long as the
        second tracer differs."""
        self.assertEqual(0, Infusate.objects.count())
        call_command(
            "load_infusates",
            infile="DataRepo/data/tests/infusates/two_dual_tracer_infusates.tsv",
        )
        self.assertEqual(3, Infusate.objects.count())
