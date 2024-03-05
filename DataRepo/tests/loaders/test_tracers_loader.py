from collections import defaultdict

import pandas as pd

from DataRepo.loaders.tracers_loader import TracersLoader
from DataRepo.models import Compound, Tracer, TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.infusate_name_parser import IsotopeData, TracerData


class TracersLoaderTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    maxDiff = None

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
            "Tracer Number": [1],
            "Compound Name": ["lysine"],
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

        self.assertTrue(hasattr(tl, "tracer_dict"))
        self.assertTrue(hasattr(tl, "tracer_name_to_number"))
        self.assertTrue(hasattr(tl, "valid_tracers"))
        self.assertTrue(hasattr(tl, "inconsistent_compounds"))
        self.assertTrue(hasattr(tl, "inconsistent_names"))
        self.assertTrue(hasattr(tl, "inconsistent_numbers"))

        self.assertEqual(defaultdict, type(tl.tracer_dict))
        self.assertEqual(defaultdict, type(tl.tracer_name_to_number))
        self.assertEqual(dict, type(tl.valid_tracers))
        self.assertEqual(defaultdict, type(tl.inconsistent_compounds))
        self.assertEqual(defaultdict, type(tl.inconsistent_names))
        self.assertEqual(defaultdict, type(tl.inconsistent_numbers))

        self.assertEqual(0, len(tl.tracer_dict.keys()))
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

    def test_build_tracer_dict(self):
        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        tl.build_tracer_dict()
        self.assertDictEqual(
            self.TRACER_DICT,
            tl.tracer_dict,
        )

    def test_load_tracer_dict(self):
        # Establish that the tracer does not exist at first
        self.assertIsNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        tl.build_tracer_dict()
        tl.load_tracer_dict()

        self.assertIsNotNone(Tracer.objects.get_tracer(self.LYSINE_TRACER_DATA))

    def test_get_row_data(self):
        tl = TracersLoader(df=self.LYSINE_TRACER_DATAFRAME)
        tl.init_load()
        for _, row in self.LYSINE_TRACER_DATAFRAME.iterrows():
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
        tl.tracer_dict = {
            1: {
                "compound_name": None,
                "isotopes": [],
                "row_index": 0,
                "rownum": 2,
                "tracer_name": "lysine-[13C6]",
            },
        }
        tl.check_extract_name_data()
        self.assertDictEqual(self.TRACER_DICT, tl.tracer_dict)

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

    def test_check_data_is_consistent(self):
        # TODO: Implement test
        pass

    def test_buffer_consistency_issues(self):
        # TODO: Implement test
        pass

    def test_check_tracer_name_consistent(self):
        # TODO: Implement test
        pass
