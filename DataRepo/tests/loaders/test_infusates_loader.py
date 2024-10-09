import pandas as pd

from DataRepo.loaders.infusates_loader import InfusatesLoader
from DataRepo.models import Compound, Infusate, InfusateTracer, Tracer
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import InfileError
from DataRepo.utils.infusate_name_parser import (
    InfusateData,
    InfusateTracerData,
    IsotopeData,
    TracerData,
)


class InfusatesLoaderTests(TracebaseTestCase):
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
            "Tracer": ["lysine-[13C6]"],
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
        cls.LYSINE = Compound.objects.create(
            name="lysine", formula="C6H14N2O2", hmdb_id="HMDB0000182"
        )
        cls.LYSINE_TRACER = Tracer.objects.get_or_create_tracer(cls.LYSINE_TRACER_DATA)
        super().setUpTestData()

    def test_init_load(self):
        tl = InfusatesLoader()

        tl.init_load()

        self.assertEqual(0, len(tl.infusates_dict.keys()))
        self.assertEqual(0, len(tl.infusate_name_to_number.keys()))
        self.assertEqual(0, len(tl.valid_infusates.keys()))
        self.assertEqual(0, len(tl.inconsistent_group_names["mult_names"].keys()))
        # This (mult_nums) is effectively tested in test_load_infusates
        self.assertEqual(0, len(tl.inconsistent_group_names["mult_nums"].keys()))
        self.assertEqual(0, len(tl.inconsistent_names.keys()))
        self.assertEqual(0, len(tl.inconsistent_numbers.keys()))
        # These are effectively tested in test_load_infusates
        self.assertEqual(0, len(tl.inconsistent_tracer_groups["mult_names"]))
        self.assertEqual(0, len(tl.inconsistent_tracer_groups["dupes"]))

    def test_infusate_loader_load_data(self):
        # Establish that the infusate does not exist at first
        self.assertIsNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

        tl = InfusatesLoader(df=self.DUDERINO_INFUSATE_DATAFRAME)
        tl.load_data()

        self.assertEqual(1, Infusate.objects.count())
        self.assertEqual(1, InfusateTracer.objects.count())
        self.assertIsNotNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

    def test_build_infusates_dict(self):
        tl = InfusatesLoader(df=self.DUDERINO_INFUSATE_DATAFRAME)
        tl.build_infusates_dict()
        self.assertDictEqual(
            self.INFUSATES_DICT,
            tl.infusates_dict,
        )

    def test_load_infusates_dict(self):
        # Establish that the tracer does not exist at first
        self.assertIsNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

        tl = InfusatesLoader(df=self.DUDERINO_INFUSATE_DATAFRAME)
        tl.build_infusates_dict()
        tl.load_infusates_dict()

        self.assertIsNotNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

    def test_get_row_data(self):
        tl = InfusatesLoader(df=self.DUDERINO_INFUSATE_DATAFRAME)
        tl.init_load()
        for _, row in self.DUDERINO_INFUSATE_DATAFRAME.iterrows():
            break

        (
            infusate_number,
            tracer_group_name,
            infusate_name,
            tracer_name,
            tracer_concentration,
        ) = tl.get_row_data(row)

        self.assertEqual(1, infusate_number)
        self.assertEqual("duderino", tracer_group_name)
        self.assertEqual("duderino{lysine-[13C6]}", infusate_name)
        self.assertEqual("lysine-[13C6]", tracer_name)
        self.assertEqual(20, tracer_concentration)

    def test_check_extract_name_data(self):
        tl = InfusatesLoader(df=self.DUDERINO_INFUSATE_DATAFRAME)
        tl.init_load()
        tl.infusates_dict = {
            1: {
                "infusate_name": "duderino{lysine-[13C6]}",
                "tracer_group_name": None,
                "tracers": [
                    {
                        "tracer_name": None,
                        "tracer_concentration": 20,
                        "row_index": 0,
                        "rownum": 2,
                    },
                ],
                "row_index": 0,
                "rownum": 2,
            },
        }
        tl.check_extract_name_data()
        self.assertDictEqual(self.INFUSATES_DICT, tl.infusates_dict)

    def test_infusate_loader_get_or_create_infusate(self):
        # Establish that the infusate does not exist at first
        self.assertIsNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

        tl = InfusatesLoader(df=self.DUDERINO_INFUSATE_DATAFRAME)
        rec, created = tl.get_or_create_just_infusate(self.INFUSATES_DICT[1])

        self.assertIsNotNone(rec)
        self.assertTrue(created)
        self.assertIsNotNone(
            Infusate.objects.get(
                tracer_group_name=self.DUDERINO_INFUSATE_DATA["infusate_name"]
            )
        )

    def test_infusate_loader_get_infusate(self):
        Infusate.objects.get_or_create_infusate(self.DUDERINO_INFUSATE_DATA)
        # Establish that the infusate does not exist at first
        self.assertIsNotNone(Infusate.objects.get_infusate(self.DUDERINO_INFUSATE_DATA))

        tl = InfusatesLoader(df=self.DUDERINO_INFUSATE_DATAFRAME)
        rec = tl.get_infusate(self.INFUSATES_DICT[1])

        self.assertIsNotNone(rec)

    def test_infusate_loader_create_infusate(self):
        tl = InfusatesLoader()
        rec = tl.create_infusate(self.INFUSATES_DICT[1])
        self.assertEqual(Infusate, type(rec))
        self.assertIsNotNone(rec)

    def test_infusate_loader_get_or_create_infusate_tracer(self):
        self.assertEqual(0, InfusateTracer.objects.count())
        tl = InfusatesLoader()
        infusate_rec = tl.create_infusate(self.INFUSATES_DICT[1])
        irec, created = tl.get_or_create_infusate_tracer(
            self.INFUSATES_DICT[1]["tracers"][0], infusate_rec
        )
        self.assertIsNotNone(irec)
        self.assertEqual(InfusateTracer, type(irec))
        self.assertTrue(created)
        self.assertEqual(1, InfusateTracer.objects.count())

    def test_infusate_loader_check_data_is_consistent(self):
        """Assert that infusate name, number, or tracer group name inconsistencies are logged in their respective
        lists."""

        tl = InfusatesLoader()
        tl.init_load()
        tl.infusates_dict = {
            1: {
                "tracer_group_name": "duderino",
                "tracers": [
                    {
                        "tracer_name": None,
                        "concentration": 20,
                        "row_index": 0,
                        "rownum": 2,
                    },
                ],
                "row_index": 0,
                "rownum": 2,
                "infusate_name": "duderino{lysine-[13C6]}",
            },
            2: {
                "tracer_group_name": "myinfusate",
                "tracers": [
                    {
                        "tracer_name": None,
                        "concentration": 30,
                        "row_index": 0,
                        "rownum": 3,
                    },
                ],
                "row_index": 0,
                "rownum": 3,
                "infusate_name": "myinfusate{lysine-[13C6]}",
            },
            3: {
                "tracer_group_name": "hectorPinfusate",
                "tracers": [
                    {
                        "tracer_name": None,
                        "concentration": 40,
                        "row_index": 0,
                        "rownum": 4,
                    },
                ],
                "row_index": 0,
                "rownum": 4,
                "infusate_name": "hectorPinfusate{lysine-[13C6]}",
            },
            4: {
                "tracer_group_name": "hectorPinfusate",
                "tracers": [
                    {
                        "tracer_name": None,
                        "concentration": 40,
                        "row_index": 0,
                        "rownum": 5,
                    },
                ],
                "row_index": 0,
                "rownum": 5,
                "infusate_name": "hectorPinfusate{lysine-[13C6]}",
            },
        }

        tl.infusate_name_to_number = {
            "duderino{lysine-[13C6]}": {1: 2},
            "myinfusate{lysine-[13C6]}": {2: 3},
            "hectorPinfusate{lysine-[13C6]}": {3: 4},  # Note: see comment below
        }

        self.assertEqual(0, len(tl.inconsistent_group_names["mult_names"]))
        self.assertEqual(0, len(tl.inconsistent_names))
        self.assertEqual(0, len(tl.inconsistent_numbers))

        # Check multiple group names per number
        tl.rownum = 2  # check_data_is_consistent uses self.rownum
        tl.check_data_is_consistent(1, "duderino", "duderino{lysine-[13C6]}")
        tl.rownum += 1
        tl.check_data_is_consistent(1, "rinodude", "duderino{lysine-[13C6]}")
        self.assertEqual(1, len(tl.inconsistent_group_names["mult_names"]))

        # Check multiple infusate names per number
        tl.rownum += 1
        tl.check_data_is_consistent(2, "myinfusate", "myinfusate{lysine-[13C6]}")
        tl.rownum += 1
        tl.check_data_is_consistent(2, "myinfusate", "myinfusate{lysine-[14C6]}")
        self.assertEqual(1, len(tl.inconsistent_names))

        # Check multiple infusate numbers per name
        # Note: check_data_is_consistent is not the only method that updates inconsistent_numbers, so infusate number 4
        # was intentionally left out of the infusate_name_to_number dict above
        tl.rownum += 1
        tl.check_data_is_consistent(
            3, "hectorPinfusate", "hectorPinfusate{lysine-[13C6]}"
        )
        tl.rownum += 1
        tl.check_data_is_consistent(
            4, "hectorPinfusate", "hectorPinfusate{lysine-[13C6]}"
        )
        self.assertEqual(1, len(tl.inconsistent_numbers))

    def test_infusate_loader_buffer_consistency_issues(self):
        """Assert that an exception is buffered when the infusate name, number, or group are inconsistent."""

        tl = InfusatesLoader()
        tl.init_load()

        # 2 different compound names associated with 1 infusate number
        tl.inconsistent_group_names["mult_names"][1]["duderino"] = [2]
        tl.inconsistent_group_names["mult_names"][1]["rinodude"] = [3]

        # 2 different infusate names associated with 1 infusate number
        tl.inconsistent_names[2]["myinfusate{lysine-[13C6]}"] = [4]
        tl.inconsistent_names[2]["myinfusate{lysine-[14C6]}"] = [5]

        # 2 different infusate numbers associated with 1 infusate name
        tl.inconsistent_numbers["hectorPinfusate{lysine-[13C6]}"][3] = [6]
        tl.inconsistent_numbers["hectorPinfusate{lysine-[13C6]}"][4] = [7]

        tl.buffer_consistency_issues()

        self.assertEqual(3, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(InfileError, type(tl.aggregated_errors_object.exceptions[0]))
        self.assertEqual(InfileError, type(tl.aggregated_errors_object.exceptions[1]))
        self.assertEqual(InfileError, type(tl.aggregated_errors_object.exceptions[2]))

        self.assertIn(
            "Infusate Row Group and Tracer Group Name",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "'Infusate Row Group' 1 ", str(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertIn(
            "one 'Tracer Group Name' is allowed per 'Infusate Row Group'",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "duderino (on rows: [2])", str(tl.aggregated_errors_object.exceptions[0])
        )
        self.assertIn(
            "rinodude (on rows: [3])", str(tl.aggregated_errors_object.exceptions[0])
        )

        self.assertIn(
            "Infusate Name and Infusate Row Group",
            str(tl.aggregated_errors_object.exceptions[1]),
        )
        self.assertIn(
            "'Infusate Row Group' 2 ", str(tl.aggregated_errors_object.exceptions[1])
        )
        self.assertIn(
            "one 'Infusate Name' is allowed per 'Infusate Row Group'",
            str(tl.aggregated_errors_object.exceptions[1]),
        )
        self.assertIn(
            "myinfusate{lysine-[13C6]} (on rows: [4])",
            str(tl.aggregated_errors_object.exceptions[1]),
        )
        self.assertIn(
            "myinfusate{lysine-[14C6]} (on rows: [5])",
            str(tl.aggregated_errors_object.exceptions[1]),
        )

        self.assertIn(
            "Infusate Row Group and Infusate Name",
            str(tl.aggregated_errors_object.exceptions[2]),
        )
        self.assertIn(
            "'Infusate Name' hectorPinfusate{lysine-[13C6]}",
            str(tl.aggregated_errors_object.exceptions[2]),
        )
        self.assertIn(
            "one 'Infusate Row Group' is allowed per 'Infusate Name'",
            str(tl.aggregated_errors_object.exceptions[2]),
        )
        self.assertIn(
            "3 (on rows: [6])", str(tl.aggregated_errors_object.exceptions[2])
        )
        self.assertIn(
            "4 (on rows: [7])", str(tl.aggregated_errors_object.exceptions[2])
        )

    def test_check_infusate_name_consistent(self):
        """Assert that an exception is buffered when the supplied infusate name doesn't match the DB generated one."""

        rec, _ = Infusate.objects.get_or_create_infusate(self.DUDERINO_INFUSATE_DATA)
        tl = InfusatesLoader()

        self.assertEqual(0, len(tl.aggregated_errors_object.exceptions))

        with self.assertRaises(InfileError):
            tl.check_infusate_name_consistent(
                rec,
                {
                    "infusate_name": "duderino{lysine-[14C6]}",
                    "rownum": 2,
                    "row_index": 0,
                    "tracers": [
                        {
                            "tracer_name": "lysine-[14C6]",
                            "tracer_concentration": 20.0,
                            "rownum": 2,
                            "row_index": 0,
                        },
                    ],
                },
            )

        self.assertEqual(1, len(tl.aggregated_errors_object.exceptions))
        self.assertEqual(InfileError, type(tl.aggregated_errors_object.exceptions[0]))
        self.assertIn(
            "supplied Infusate Name [duderino{lysine-[14C6]}] and tracer concentrations [20.0] from row [2]",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn(
            "generated name (shown with concentrations) [duderino {lysine-[13C6][20]}]",
            str(tl.aggregated_errors_object.exceptions[0]),
        )
        self.assertIn("on rows ['2']", str(tl.aggregated_errors_object.exceptions[0]))
