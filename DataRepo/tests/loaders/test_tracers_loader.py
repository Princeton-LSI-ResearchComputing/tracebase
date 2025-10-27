import pandas as pd

from DataRepo.loaders.tracers_loader import TracersLoader
from DataRepo.models import Compound, Tracer, TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import InfileError
from DataRepo.utils.infusate_name_parser import IsotopeData, TracerData


class TracersLoaderTests(TracebaseTestCase):
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

    LYSINE_TRACER_DATAFRAME = pd.DataFrame.from_dict(
        {
            "Tracer Row Group": [1],
            "Compound": ["lysine"],
            "Element": ["C"],
            "Mass Number": [13],
            "Label Count": [6],
            "Label Positions": [None],
            "Tracer Name": ["lysine-[13C6]"],
        },
    )

    TRACER_DICT = {
        1: {
            "compound_name": "lysine",
            "isotopes": [
                {
                    "count": 6,
                    "element": "C",
                    "mass_number": 13,
                    "positions": None,
                    "row_index": 0,
                    "rownum": 2,
                },
            ],
            "row_index": 0,
            "rownum": 2,
            "tracer_name": "lysine-[13C6]",
        },
    }

    @classmethod
    def setUpTestData(cls):
        cls.LYSINE = Compound.objects.create(
            name="lysine", formula="C6H14N2O2", hmdb_id="HMDB0000182"
        )
        super().setUpTestData()

    def test_init_load(self):
        tl = TracersLoader()

        tl.init_load()

        self.assertEqual(0, len(tl.tracers_dict.keys()))
        self.assertEqual(0, len(tl.tracer_name_to_number.keys()))
        self.assertEqual(0, len(tl.valid_tracers.keys()))
        self.assertEqual(0, len(tl.inconsistent_compounds.keys()))
        self.assertEqual(0, len(tl.inconsistent_names.keys()))
        self.assertEqual(0, len(tl.inconsistent_numbers.keys()))

    def test_tracer_loader_load_data(self):
        # Establish that the tracer does not exist at first
        self.assertIsNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        tl.load_data()

        self.assertEqual(1, Tracer.objects.count())
        self.assertEqual(1, TracerLabel.objects.count())
        self.assertIsNotNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

    def test_build_tracers_dict(self):
        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        tl.build_tracers_dict()
        self.assertDictEqual(
            self.TRACER_DICT,
            tl.tracers_dict,
        )

    def test_load_tracers_dict(self):
        # Establish that the tracer does not exist at first
        self.assertIsNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        tl.build_tracers_dict()
        tl.load_tracers_dict()

        self.assertIsNotNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

    def test_get_row_data(self):
        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        tl.init_load()
        for _, row in tl.iterate_table_rows():
            break

        (
            tracer_number,
            compound_name,
            tracer_name,
            element,
            mass_number,
            count,
            positions,
        ) = tl.get_row_data(row)

        self.assertEqual(1, tracer_number)
        self.assertEqual("lysine", compound_name)
        self.assertEqual("lysine-[13C6]", tracer_name)
        self.assertEqual("C", element)
        self.assertEqual(13, mass_number)
        self.assertEqual(6, count)
        self.assertIsNone(positions)

    def test_check_extract_name_data(self):
        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        tl.init_load()
        tl.tracers_dict = {
            1: {
                "compound_name": None,
                "isotopes": [],
                "row_index": 0,
                "rownum": 2,
                "tracer_name": "lysine-[13C6]",
            },
        }
        tl.check_extract_name_data()
        self.assertDictEqual(self.TRACER_DICT, tl.tracers_dict)

    def test_tracer_loader_get_or_create_tracer(self):
        # Establish that the tracer does not exist at first
        self.assertIsNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        rec, created = tl.get_or_create_tracer(self.TRACER_DICT[1])

        self.assertIsNotNone(rec)
        self.assertTrue(created)
        self.assertIsNotNone(Tracer.objects.get(compound=self.LYSINE))

    def test_tracer_loader_get_tracer(self):
        Tracer.objects.get_or_create_tracer(self.LYSINE_TRACER_DATA)
        # Establish that the tracer does not exist at first
        self.assertIsNotNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        rec = tl.get_tracer(self.TRACER_DICT[1])

        self.assertIsNotNone(rec)

    def test_tracer_loader_get_compound(self):
        tl = TracersLoader()
        rec = tl.get_compound("lysine")
        self.assertEqual(Compound, type(rec))
        self.assertIsNotNone(rec)
        self.assertEqual("lysine", rec.name)

    def test_tracer_loader_create_tracer(self):
        tl = TracersLoader()
        rec = tl.create_tracer(self.LYSINE)
        self.assertEqual(Tracer, type(rec))
        self.assertIsNotNone(rec)

    def test_tracer_loader_get_or_create_tracer_label(self):
        self.assertEqual(0, TracerLabel.objects.count())
        tl = TracersLoader()
        tracer_rec = tl.create_tracer(self.LYSINE)
        irec, created = tl.get_or_create_tracer_label(
            self.TRACER_DICT[1]["isotopes"][0], tracer_rec
        )
        self.assertIsNotNone(irec)
        self.assertEqual(TracerLabel, type(irec))
        self.assertTrue(created)
        self.assertEqual(1, TracerLabel.objects.count())

    def test_parse_label_positions(self):
        tl = TracersLoader()
        positions = tl.parse_label_positions("1,2,3,4,5")
        self.assertEqual([1, 2, 3, 4, 5], positions)

    def test_tracer_loader_check_data_is_consistent(self):
        """Assert that tracer name, number, or compound inconsistencies are logged in their respective lists."""

        tl = TracersLoader()
        tl.init_load()
        tl.tracers_dict = {
            1: {
                "compound_name": "lysine",
                "isotopes": [
                    {
                        "count": None,
                        "element": None,
                        "mass_number": None,
                        "positions": None,
                        "row_index": 0,
                        "rownum": 2,
                    },
                ],
                "row_index": 0,
                "rownum": 2,
                "tracer_name": "lysine-[13C6]",
            },
            2: {
                "compound_name": "threonine",
                "isotopes": [
                    {
                        "count": None,
                        "element": None,
                        "mass_number": None,
                        "positions": None,
                        "row_index": 0,
                        "rownum": 3,
                    },
                ],
                "row_index": 0,
                "rownum": 3,
                "tracer_name": "threonine-[13C6]",
            },
            3: {
                "compound_name": "aspartame",
                "isotopes": [
                    {
                        "count": None,
                        "element": None,
                        "mass_number": None,
                        "positions": None,
                        "row_index": 0,
                        "rownum": 4,
                    },
                ],
                "row_index": 0,
                "rownum": 4,
                "tracer_name": "aspartame-[13C6]",
            },
            4: {
                "compound_name": "aspartame",
                "isotopes": [
                    {
                        "count": None,
                        "element": None,
                        "mass_number": None,
                        "positions": None,
                        "row_index": 0,
                        "rownum": 5,
                    },
                ],
                "row_index": 0,
                "rownum": 5,
                "tracer_name": "aspartame-[13C6]",
            },
        }

        tl.tracer_name_to_number = {
            "lysine-[13C6]": {1: 2},
            "threonine-[13C6]": {2: 3},
            "aspartame-[13C6]": {3: 4},  # Note: see comment below
        }

        self.assertEqual(0, len(tl.inconsistent_compounds))
        self.assertEqual(0, len(tl.inconsistent_names))
        self.assertEqual(0, len(tl.inconsistent_numbers))

        # Check multiple compounds per number
        tl.rownum = 2  # check_data_is_consistent uses self.rownum
        tl.check_data_is_consistent(1, "lysine", "lysine-[13C6]")
        tl.rownum += 1
        tl.check_data_is_consistent(1, "asparagine", "lysine-[13C6]")
        self.assertEqual(1, len(tl.inconsistent_compounds))

        # Check multiple tracer names per number
        tl.rownum += 1
        tl.check_data_is_consistent(2, "threonine", "threonine-[13C6]")
        tl.rownum += 1
        tl.check_data_is_consistent(2, "threonine", "threonine-[14C6]")
        self.assertEqual(1, len(tl.inconsistent_names))

        # Check multiple tracer numbers per name
        # Note: check_data_is_consistent is not the only method that updates inconsistent_numbers, so tracer number 4
        # was intentionally left out of the tracer_name_to_number dict above
        tl.rownum += 1
        tl.check_data_is_consistent(3, "aspartame", "aspartame-[13C6]")
        tl.rownum += 1
        tl.check_data_is_consistent(4, "aspartame", "aspartame-[13C6]")
        self.assertEqual(1, len(tl.inconsistent_numbers))

    def test_tracer_loader_buffer_consistency_issues(self):
        """Assert that an exception is buffered when the tracer name, number, or compound are inconsistent."""

        tl = TracersLoader()
        tl.init_load()

        # 2 different compounds associated with 1 tracer number
        tl.inconsistent_compounds[1]["lysine"] = [2]
        tl.inconsistent_compounds[1]["asparagine"] = [3]

        # 2 different tracer names associated with 1 tracer number
        tl.inconsistent_names[2]["threonine-[13C6]"] = [4]
        tl.inconsistent_names[2]["threonine-[14C6]"] = [5]

        # 2 different tracer numbers associated with 1 tracer name
        tl.inconsistent_numbers["aspartame-[13C6]"][3] = [6]
        tl.inconsistent_numbers["aspartame-[13C6]"][4] = [7]

        tl.buffer_consistency_issues()

        self.assertEqual(3, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(InfileError, type(tl.aggregated_errors_object.exceptions[0]))
        self.assertEqual(InfileError, type(tl.aggregated_errors_object.exceptions[1]))
        self.assertEqual(InfileError, type(tl.aggregated_errors_object.exceptions[2]))

        self.assertIn(
            "Tracer Row Group and Compound",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "Tracer Row Group 1 ", str(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertIn(
            "one Compound is allowed per Tracer Row Group",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "lysine (on rows: [2])", str(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertIn(
            "asparagine (on rows: [3])", str(tl.aggregated_errors_object.exceptions[0])
        )

        self.assertIn(
            "Tracer Name and Tracer Row Group",
            str(tl.aggregated_errors_object.exceptions[1]),
        )
        self.assertIn(
            "Tracer Row Group 2 ", str(tl.aggregated_errors_object.exceptions[1])
        )
        self.assertIn(
            "one Tracer Name is allowed per Tracer Row Group",
            str(tl.aggregated_errors_object.exceptions[1]),
        )
        self.assertIn(
            "threonine-[13C6] (on rows: [4])",
            str(tl.aggregated_errors_object.exceptions[1]),
        )
        self.assertIn(
            "threonine-[14C6] (on rows: [5])",
            str(tl.aggregated_errors_object.exceptions[1]),
        )

        self.assertIn(
            "Tracer Row Group and Tracer Name",
            str(tl.aggregated_errors_object.exceptions[2]),
        )
        self.assertIn(
            "Tracer Name aspartame-[13C6]",
            str(tl.aggregated_errors_object.exceptions[2]),
        )
        self.assertIn(
            "one Tracer Row Group is allowed per Tracer Name",
            str(tl.aggregated_errors_object.exceptions[2]),
        )
        self.assertIn(
            "3 (on rows: [6])", str(tl.aggregated_errors_object.exceptions[2])
        )
        self.assertIn(
            "4 (on rows: [7])", str(tl.aggregated_errors_object.exceptions[2])
        )

    def test_check_tracer_name_consistent(self):
        """Assert that an exception is buffered when the supplied tracer name doesn't match the DB generated one."""

        rec, _ = Tracer.objects.get_or_create_tracer(self.LYSINE_TRACER_DATA)
        tl = TracersLoader()

        self.assertEqual(0, len(tl.aggregated_errors_object.exceptions))

        with self.assertRaises(InfileError):
            tl.check_tracer_name_consistent(
                rec,
                {
                    "tracer_name": "lysine-[14C6]",
                    "compound_name": "lysine",
                    "rownum": 2,
                    "isotopes": [{"rownum": 2}],
                },
            )

        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(InfileError, type(tl.aggregated_errors_object.exceptions[0]))
        self.assertIn(
            "supplied tracer name 'lysine-[14C6]'",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "generated name 'lysine-[13C6]'",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn("on row(s) ['2']", str(tl.aggregated_errors_object.exceptions[0]))
