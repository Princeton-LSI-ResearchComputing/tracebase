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
            element="C",
            mass_number=13,
            labeled_count=6,
            labeled_positions=None,
        )
        cls.isotope_13c5 = IsotopeData(
            labeled_element="13C",
            element="C",
            mass_number=13,
            labeled_count=5,
            labeled_positions=None,
        )
        cls.isotope_15n1 = IsotopeData(
            labeled_element="15N",
            element="N",
            mass_number=15,
            labeled_count=1,
            labeled_positions=None,
        )
        cls.isotope_13c2 = IsotopeData(
            labeled_element="13C",
            element="C",
            mass_number=13,
            labeled_count=2,
            labeled_positions=[1, 2],
        )
        cls.tracer_l_leucine = TracerData(
            unparsed_string="L-Leucine-[1,2-13C2]",
            compound_name="L-Leucine",
            isotopes=[cls.isotope_13c2],
        )
        cls.infusate_bcaas = InfusateData(
            unparsed_string="BCAAs {isoleucine-[13C6,15N1];leucine-[13C6,15N1];valine-[13C5,15N1]}",
            infusate_name="BCAAs",
            tracers=[
                TracerData(
                    unparsed_string="isoleucine-[13C6,15N1]",
                    compound_name="isoleucine",
                    isotopes=[cls.isotope_13c6, cls.isotope_15n1],
                ),
                TracerData(
                    unparsed_string="leucine-[13C6,15N1]",
                    compound_name="leucine",
                    isotopes=[cls.isotope_13c6, cls.isotope_15n1],
                ),
                TracerData(
                    unparsed_string="valine-[13C5,15N1]",
                    compound_name="valine",
                    isotopes=[cls.isotope_13c5, cls.isotope_15n1],
                ),
            ],
        )
        cls.infusate_l_leucine = InfusateData(
            unparsed_string="L-Leucine-[1,2-13C2]",
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

    def test_infusate_parsing_with_named_complex(self):
        infusate_string = (
            "BCAAs {isoleucine-[13C6,15N1];leucine-[13C6,15N1];valine-[13C5,15N1]}"
        )
        self.assertEqual(parse_infusate_name(infusate_string), self.infusate_bcaas)

    def test_infusate_parsing_without_optional_name(self):
        infusate_string = "L-Leucine-[1,2-13C2]"
        self.assertEqual(parse_infusate_name(infusate_string), self.infusate_l_leucine)

    def test_infusate_parsing_with_intervening_whitespace(self):
        # Test trailing whitespace after short_name
        name = "short_name1 {lysine-[13C5]}"
        data = parse_infusate_name(name)
        self.assertEqual(data["infusate_name"], "short_name1")

    def test_infusate_parsing_with_whitespace(self):
        # Test leading & trailing whitespace
        name = "  myshortname{lysine-[13C5]}  "
        data = parse_infusate_name(name)
        self.assertEqual(data["infusate_name"], "myshortname")

    def test_malformed_infusate_parsing(self):
        name = "not a {properly encoded tracer-[NAME1]}"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_no_isotope_encoding(self):
        name = "not a properly encoded tracer name"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_multiple_brace_groups(self):
        # Test back-to-back occurrences of curlies expressions
        name = "myshortname{lysine-[13C5]}{glucose-[13C4]}"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_with_new_line(self):
        # Test multiple names delimited by hard return
        name = "myshortname1{lysine-[13C5]}\nmyshortname2{glucose-[13C4]}"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_tracer_parsing_multiple_isotopic_definitions(self):
        # Test back-to-back occurrences of square bracket expressions
        name = "lysine-[13C5]-[19O2]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_tracer_string(name)

    def test_malformed_tracer_parsing_with_new_line(self):
        # Test multiple labeled compounds delimited by hard return
        name = "lysine-[13C5]\nlysine-[19O2]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_tracer_string(name)

    def test_malformed_tracer_parsing_with_improper_delimiter(self):
        # Test bad tracer delimiter (',' instead of ';')
        name = "lysine-[13C5],glucose-[19O2]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_tracer_string(name)

    def test_malformed_tracer_parsing_with_null_isotopic_specification(self):
        # Test empty labels list
        name = "lysine-[]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_tracer_parsing_with_bad_isotopic_specification(self):
        # Test bad isotope pattern not silently skipped
        name = "1,2,3-13C3,badlabel,19O2"
        with self.assertRaisesRegex(IsotopeParsingError, "disallowed characters"):
            _ = parse_isotope_string(name)

    def test_malformed_isotope_parsing_with_incomplete_parsing(self):
        # Test bad isotope pattern not silently skipped
        name = "1,2,3-13C3,S5,19O2"
        with self.assertRaisesRegex(
            IsotopeParsingError, "cannot be completely interpreted"
        ):
            _ = parse_isotope_string(name)

    def test_malformed_isotope_parsing_with_bad_isotopic_specification(self):
        # Test bad isotope pattern not silently skipped
        name = "13F"
        with self.assertRaisesRegex(IsotopeParsingError, "disallowed characters"):
            _ = parse_isotope_string(name)

    def test_malformed_isotope_parsing_with_null_isotopic_specification(self):
        # Test empty labels list
        name = ""
        with self.assertRaisesRegex(IsotopeParsingError, "requires a defined string"):
            _ = parse_isotope_string(name)
