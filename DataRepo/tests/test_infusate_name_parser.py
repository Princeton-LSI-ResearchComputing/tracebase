from django.test import tag

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.infusate_name_parser import (
    InfusateData,
    IsotopeData,
    IsotopeParsingError,
    TracerData,
    TracerParsingError,
    parse_infusate_name,
    parse_isotope_string,
    parse_tracer_string,
)


@tag("parsing")
class InfusateParsingTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        cls.isotope_13c6 = IsotopeData(
            labeled_element="13C",
            labeled_count=6,
            labeled_positions=None,
        )
        cls.isotope_13c5 = IsotopeData(
            labeled_element="13C",
            labeled_count=5,
            labeled_positions=None,
        )
        cls.isotope_15n1 = IsotopeData(
            labeled_element="15N",
            labeled_count=1,
            labeled_positions=None,
        )
        cls.isotope_13c2 = IsotopeData(
            labeled_element="13C",
            labeled_count=2,
            labeled_positions=[1, 2],
        )
        cls.tracer_l_leucine = TracerData(
            original_tracer="L-Leucine-[1,2-13C2]",
            compound_name="L-Leucine",
            isotopes=[cls.isotope_13c2],
        )
        cls.infusate_bcaas = InfusateData(
            original_infusate="BCAAs {isoleucine-[13C6,15N1];leucine-[13C6,15N1];valine-[13C5,15N1]}",
            infusate_name="BCAAs",
            tracers=[
                TracerData(
                    original_tracer="isoleucine-[13C6,15N1]",
                    compound_name="isoleucine",
                    isotopes=[cls.isotope_13c6, cls.isotope_15n1],
                ),
                TracerData(
                    original_tracer="leucine-[13C6,15N1]",
                    compound_name="leucine",
                    isotopes=[cls.isotope_13c6, cls.isotope_15n1],
                ),
                TracerData(
                    original_tracer="valine-[13C5,15N1]",
                    compound_name="valine",
                    isotopes=[cls.isotope_13c5, cls.isotope_15n1],
                ),
            ],
        )
        cls.infusate_l_leucine = InfusateData(
            original_infusate="L-Leucine-[1,2-13C2]",
            infusate_name=None,
            tracers=[cls.tracer_l_leucine],
        )

        pass

    def test_isotope_parsing_single(self):
        isotope_string = "13C6"
        self.assertEqual(parse_isotope_string(isotope_string), [self.isotope_13c6])

    def test_isotope_parsing_double(self):
        isotope_string = "13C6,15N1"
        self.assertEqual(
            parse_isotope_string(isotope_string), [self.isotope_13c6, self.isotope_15n1]
        )

    def test_isotope_parsing_positions(self):
        isotope_string = "1,2-13C2"
        self.assertEqual(parse_isotope_string(isotope_string), [self.isotope_13c2])

    def test_tracer_parsing(self):
        tracer_string = "L-Leucine-[1,2-13C2]"
        self.assertEqual(parse_tracer_string(tracer_string), self.tracer_l_leucine)

    def test_infusate_parsing_with_name_1(self):
        infusate_string = (
            "BCAAs {isoleucine-[13C6,15N1];leucine-[13C6,15N1];valine-[13C5,15N1]}"
        )
        self.assertEqual(parse_infusate_name(infusate_string), self.infusate_bcaas)

    def test_infusate_parsing_without_name_1(self):
        infusate_string = "L-Leucine-[1,2-13C2]"
        self.assertEqual(parse_infusate_name(infusate_string), self.infusate_l_leucine)

    def test_malformed_infusate_parsing_1(self):
        name = "not a {properly encoded tracer-[NAME1]}"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_2(self):
        name = "not a properly encoded tracer name"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_3(self):
        # Test back-to-back occurrences of curlies expressions
        name = "myshortname{lysine-[13C5]}{glucose-[13C4]}"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_4(self):
        # Test multiple names delimited by hard return
        name = "myshortname1{lysine-[13C5]}\nmyshortname2{glucose-[13C4]}"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_5(self):
        # Test leading & trailing whitespace
        name = "  myshortname{lysine-[13C5]}  "
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_6(self):
        # Test trailing whitespace in short_name
        name = "short_name1 {lysine-[13C5]}"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_tracer_parsing_1(self):
        # Test back-to-back occurrences of square bracket expressions
        name = "lysine-[13C5]-[19O2]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_tracer_string(name)

    def test_malformed_tracer_parsing_2(self):
        # Test multiple labeled compounds delimited by hard return
        name = "lysine-[13C5]\nlysine-[19O2]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_tracer_string(name)

    def test_malformed_tracer_parsing_3(self):
        # Test bad isotope pattern not silently skipped
        name = "1,2,3-13C3,badlabel,19O2"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_tracer_parsing_4(self):
        # Test bad tracer delimiter (',' instead of ';')
        name = "lysine-[13C5],glucose-[19O2]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_tracer_string(name)

    def test_malformed_isotope_parsing_1(self):
        # Test empty labels list
        name = "lysine-[]"
        with self.assertRaisesRegex(IsotopeParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)
