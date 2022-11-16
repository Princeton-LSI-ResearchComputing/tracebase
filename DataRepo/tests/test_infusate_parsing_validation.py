from django.core.exceptions import ValidationError
from django.test import tag

from DataRepo.models import Compound, CompoundSynonym, Infusate, Tracer
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.infusate_name_parser import (
    InfusateData,
    InfusateParsingError,
    InfusateTracer,
    IsotopeData,
    IsotopeParsingError,
    TracerData,
    TracerParsingError,
    parse_infusate_name,
    parse_isotope_string,
    parse_tracer_concentrations,
    parse_tracer_string,
)


class InfusateTest(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        cls.isotope_data_13c6 = IsotopeData(
            element="C",
            mass_number=13,
            count=6,
            positions=None,
        )
        cls.isotope_data_13c5 = IsotopeData(
            element="C",
            mass_number=13,
            count=5,
            positions=None,
        )
        cls.isotope_data_15n1 = IsotopeData(
            element="N",
            mass_number=15,
            count=1,
            positions=None,
        )
        cls.isotope_data_13c2 = IsotopeData(
            element="C",
            mass_number=13,
            count=2,
            positions=[1, 2],
        )

        # L-Leucine
        cls.tracer_data_l_leucine = TracerData(
            unparsed_string="L-Leucine-[1,2-13C2]",
            compound_name="L-Leucine",
            isotopes=[cls.isotope_data_13c2],
        )
        cls.leucine = Compound.objects.create(
            name="leucine", formula="C6H13NO2", hmdb_id="HMDB0000687"
        )
        CompoundSynonym.objects.create(name="L-Leucine", compound=cls.leucine)
        cls.infusate_concentrations_leucine = [1.5, 2.5]
        cls.infusate_data_l_leucine = InfusateData(
            unparsed_string="L-Leucine-[1,2-13C2]",
            infusate_name=None,
            tracers=[
                InfusateTracer(
                    tracer=cls.tracer_data_l_leucine,
                    concentration=cls.infusate_concentrations_leucine[0],
                )
            ],
        )

        cls.infusate_data_leucine_named_1 = InfusateData(
            unparsed_string="leucine {L-Leucine-[1,2-13C2]}",
            infusate_name="leucine",
            tracers=[
                InfusateTracer(
                    tracer=cls.tracer_data_l_leucine,
                    concentration=cls.infusate_concentrations_leucine[0],
                )
            ],
        )

        cls.infusate_data_leucine_named_2 = InfusateData(
            unparsed_string="leucine {L-Leucine-[1,2-13C2]}",
            infusate_name="leucine",
            tracers=[
                InfusateTracer(
                    tracer=cls.tracer_data_l_leucine,
                    concentration=cls.infusate_concentrations_leucine[1],
                )
            ],
        )

        # Isoleucine
        cls.isoleucine = Compound.objects.create(
            name="isoleucine", formula="C6H13NO2", hmdb_id="HMDB0000172"
        )

        # Valine
        cls.valine = Compound.objects.create(
            name="valine", formula="C5H11NO2", hmdb_id="HMDB0000883"
        )

        # BCAAs
        cls.infusate_concentrations_bcaas = [1.0, 2.0, 3.0]
        cls.infusate_data_bcaas = InfusateData(
            unparsed_string="BCAAs {isoleucine-[13C6,15N1];leucine-[13C6,15N1];valine-[13C5,15N1]}",
            infusate_name="BCAAs",
            tracers=[
                InfusateTracer(
                    tracer=TracerData(
                        unparsed_string="isoleucine-[13C6,15N1]",
                        compound_name="isoleucine",
                        isotopes=[cls.isotope_data_13c6, cls.isotope_data_15n1],
                    ),
                    concentration=cls.infusate_concentrations_bcaas[0],
                ),
                InfusateTracer(
                    tracer=TracerData(
                        unparsed_string="leucine-[13C6,15N1]",
                        compound_name="leucine",
                        isotopes=[cls.isotope_data_13c6, cls.isotope_data_15n1],
                    ),
                    concentration=cls.infusate_concentrations_bcaas[1],
                ),
                InfusateTracer(
                    tracer=TracerData(
                        unparsed_string="valine-[13C5,15N1]",
                        compound_name="valine",
                        isotopes=[cls.isotope_data_13c5, cls.isotope_data_15n1],
                    ),
                    concentration=cls.infusate_concentrations_bcaas[2],
                ),
            ],
        )

        super().setUpTestData()


@tag("parsing")
class InfusateParsingTests(InfusateTest):
    def test_isotope_parsing_single(self):
        """Test parsing a single isotope string"""
        isotope_string = "13C6"
        self.assertEqual(parse_isotope_string(isotope_string), [self.isotope_data_13c6])

    def test_isotope_parsing_double(self):
        """Test parsing a double isotope string"""
        isotope_string = "13C6,15N1"
        self.assertEqual(
            parse_isotope_string(isotope_string),
            [self.isotope_data_13c6, self.isotope_data_15n1],
        )

    def test_isotope_parsing_positions(self):
        """Test parsing an isotope string with positions"""
        isotope_string = "1,2-13C2"
        self.assertEqual(parse_isotope_string(isotope_string), [self.isotope_data_13c2])

    def test_tracer_parsing(self):
        """Test parsing a valid tracer string with positions"""
        tracer_string = "L-Leucine-[1,2-13C2]"
        self.assertEqual(parse_tracer_string(tracer_string), self.tracer_data_l_leucine)

    def test_infusate_parsing_with_named_complex(self):
        """Test parsing an infusate string with a tracer group name"""
        infusate_string = (
            "BCAAs {isoleucine-[13C6,15N1];leucine-[13C6,15N1];valine-[13C5,15N1]}"
        )
        self.assertEqual(
            parse_infusate_name(infusate_string, self.infusate_concentrations_bcaas),
            self.infusate_data_bcaas,
        )

    def test_infusate_parsing_without_optional_name(self):
        """Test parsing an infusate string without a tracer group name"""
        infusate_string = "L-Leucine-[1,2-13C2]"
        self.assertEqual(
            parse_infusate_name(
                infusate_string, [self.infusate_concentrations_leucine[0]]
            ),
            self.infusate_data_l_leucine,
        )

    def test_infusate_parsing_with_intervening_whitespace(self):
        """Test infusate parsing with trailing whitespace after short_name"""
        name = "short_name1 {lysine-[13C5]}"
        data = parse_infusate_name(name)
        self.assertEqual(data["infusate_name"], "short_name1")

    def test_infusate_parsing_with_whitespace(self):
        """Test infusate parsing with leading & trailing whitespace"""
        name = "  myshortname{lysine-[13C5]}  "
        data = parse_infusate_name(name)
        self.assertEqual(data["infusate_name"], "myshortname")

    def test_malformed_infusate_parsing(self):
        """Test parsing an invalid infusate string (extra curly brace '{')"""
        name = "not a {{properly encoded tracer-[13C1]}"
        with self.assertRaisesRegex(
            InfusateParsingError, "Unable to parse infusate string"
        ):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_no_isotope_encoding(self):
        """Test parsing an invalid infusate record (no isotope encoding)"""
        name = "not a properly encoded tracer name"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_multiple_brace_groups(self):
        """Test back-to-back occurrences of curlies expressions"""
        name = "myshortname{lysine-[13C5]}{glucose-[13C4]}"
        with self.assertRaisesRegex(
            InfusateParsingError, "Unable to parse infusate string"
        ):
            _ = parse_infusate_name(name)

    def test_malformed_infusate_parsing_with_new_line(self):
        """Test multiple names delimited by hard return"""
        name = "myshortname1{lysine-[13C5]}\nmyshortname2{glucose-[13C4]}"
        with self.assertRaisesRegex(
            InfusateParsingError, "Unable to parse infusate string"
        ):
            _ = parse_infusate_name(name)

    def test_malformed_tracer_parsing_multiple_isotopic_definitions(self):
        """Test back-to-back occurrences of square bracket expressions"""
        name = "lysine-[13C5]-[19O2]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_tracer_string(name)

    def test_malformed_tracer_parsing_with_new_line(self):
        """Test multiple labeled compounds delimited by hard return"""
        name = "lysine-[13C5]\nlysine-[19O2]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_tracer_string(name)

    def test_malformed_tracer_parsing_with_improper_delimiter(self):
        """Test bad tracer delimiter (',' instead of ';')"""
        name = "lysine-[13C5],glucose-[19O2]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_tracer_string(name)

    def test_malformed_tracer_parsing_with_null_isotopic_specification(self):
        """Test empty labels list"""
        name = "lysine-[]"
        with self.assertRaisesRegex(TracerParsingError, "cannot be parsed"):
            _ = parse_infusate_name(name)

    def test_malformed_tracer_parsing_with_bad_isotopic_specification(self):
        """Test bad isotope pattern not silently skipped"""
        name = "1,2,3-13C3,badlabel,19O2"
        with self.assertRaisesRegex(
            IsotopeParsingError, "Only the following were parsed"
        ):
            _ = parse_isotope_string(name)

    def test_malformed_isotope_parsing_with_incomplete_parsing(self):
        """Test bad isotope pattern not silently skipped"""
        name = "1,2,3-13C3,S5,19O2"
        with self.assertRaisesRegex(
            IsotopeParsingError, "Only the following were parsed"
        ):
            _ = parse_isotope_string(name)

    def test_malformed_isotope_parsing_with_bad_isotopic_specification(self):
        """Test bad isotope pattern not silently skipped"""
        name = "13F"
        with self.assertRaisesRegex(IsotopeParsingError, "cannot be parsed"):
            _ = parse_isotope_string(name)

    def test_malformed_isotope_parsing_with_null_isotopic_specification(self):
        """Test isotope parsing empty labels list"""
        name = ""
        with self.assertRaisesRegex(IsotopeParsingError, "requires a defined string"):
            _ = parse_isotope_string(name)

    def test_parse_tracer_concentrations(self):
        self.assertAlmostEqual(
            [10.0, 20.0, 30.0], parse_tracer_concentrations("10; 20;30")
        )


class InfusateValidationTests(InfusateTest):
    def test_tracer_creation(self):
        """Test creation of a valid tracer object from TracerData"""
        (tracer, created) = Tracer.objects.get_or_create_tracer(
            self.tracer_data_l_leucine
        )
        self.assertTrue(created)
        self.assertEqual(tracer.name, "leucine-[1,2-13C2]")
        self.assertEqual(tracer.compound.name, "leucine")
        self.assertEqual(len(tracer.labels.all()), 1)

    def test_tracer_validation_positions_notequal_count(self):
        """Test tracer validation when number of positions does not equal element count"""
        tracer_data_leucine_mismatched_positions = parse_tracer_string(
            "Leucine-[1-13C2]"
        )
        with self.assertRaises(ValidationError):
            _ = Tracer.objects.get_or_create_tracer(
                tracer_data_leucine_mismatched_positions
            )

    def test_tracer_validation_element_doesnot_exist(self):
        """Test tracer validation when labeled element does not exist in compound formula"""
        tracer_data_leucine_compound_doesnot_exist = parse_tracer_string(
            "Leucine-[1,2-33S2]"
        )
        with self.assertRaisesRegex(
            ValidationError, "Labeled element S does not exist"
        ):
            _ = Tracer.objects.get_or_create_tracer(
                tracer_data_leucine_compound_doesnot_exist
            )

    def test_tracer_validation_count_too_large(self):
        """Test tracer validation when labeled element count is greater than element count in compound formula"""
        tracer_data_leucine_count_too_large = parse_tracer_string("Leucine-[13C7]")
        with self.assertRaisesRegex(
            ValidationError, "Count of labeled element C exceeds"
        ):
            _ = Tracer.objects.get_or_create_tracer(tracer_data_leucine_count_too_large)

    def test_tracer_validation_fully_labeled_no_positions(self):
        """Test tracer validation allows no postions on fully labeled compound"""
        tracer_data_leucine_fully_labeled = parse_tracer_string("Leucine-[13C6]")
        (tracer, _) = Tracer.objects.get_or_create_tracer(
            tracer_data_leucine_fully_labeled
        )
        self.assertEqual(tracer.name, "leucine-[13C6]")

    def test_tracer_validation_missing_positions(self):
        """Test tracer validation requires positions when partially labeled"""
        tracer_data_leucine_missing_postions = parse_tracer_string("Leucine-[13C5]")
        with self.assertRaisesRegex(
            ValidationError, "Positions required for partially labeled tracer compound"
        ):
            _ = Tracer.objects.get_or_create_tracer(
                tracer_data_leucine_missing_postions
            )

    def test_infusion_creation(self):
        """Test the creation of a valid infusate from InfusateData"""
        (infusate, created) = Infusate.objects.get_or_create_infusate(
            self.infusate_data_bcaas
        )
        self.assertTrue(created)
        self.assertEqual(
            infusate.name,
            "BCAAs {isoleucine-[13C6,15N1][1];leucine-[13C6,15N1][2];valine-[13C5,15N1][3]}",
        )
        self.assertEqual(infusate.tracer_group_name, "BCAAs")
        self.assertEqual(len(infusate.tracers.all()), 3)

    def test_infusion_creation_without_optional_name(self):
        """Test the creation of a valid infusate without the optional name"""
        (infusate, created) = Infusate.objects.get_or_create_infusate(
            self.infusate_data_l_leucine
        )
        self.assertTrue(created)
        self.assertEqual(
            infusate.name,
            "leucine-[1,2-13C2][1.5]",
        )
        self.assertEqual(infusate.tracer_group_name, None)
        self.assertEqual(len(infusate.tracers.all()), 1)

    def test_infusion_validation_name_conflict(self):
        """Test infusate validation when two groups of tracers share the same infusate name"""
        (infusate, _) = Infusate.objects.get_or_create_infusate(
            self.infusate_data_bcaas
        )
        self.assertEqual(infusate.tracer_group_name, "BCAAs")
        with self.assertRaisesRegex(
            ValidationError, "Tracer group name BCAAs is inconsistent."
        ):
            Infusate.objects.get_or_create_infusate(
                parse_infusate_name(
                    "BCAAs {L-Leucine-[1,2-13C2]}",
                    [self.infusate_concentrations_leucine[0]],
                )
            )

    def test_infusion_validation_same_name(self):
        """Test invusate validation when two infusates have the same group of tracers and the same name"""
        (infusate_conc1, created1) = Infusate.objects.get_or_create_infusate(
            self.infusate_data_leucine_named_1
        )
        self.assertTrue(created1)
        self.assertEqual(infusate_conc1.tracer_group_name, "leucine")

        (infusate_conc2, created2) = Infusate.objects.get_or_create_infusate(
            self.infusate_data_leucine_named_2
        )
        self.assertTrue(created2)
        self.assertEqual(infusate_conc2.tracer_group_name, "leucine")

    def test_infusion_get_by_data(self):
        """Test getting infusion record using InfusateData"""
        (infusate, created) = Infusate.objects.get_or_create_infusate(
            self.infusate_data_bcaas
        )
        self.assertTrue(created)
        infusate_found = Infusate.objects.get_infusate(self.infusate_data_bcaas)
        self.assertEqual(infusate, infusate_found)

    def test_infusion_get_by_data_diffconc(self):
        """Test that concentration must match to get infusion record using InfusateData"""
        (_, created) = Infusate.objects.get_or_create_infusate(
            self.infusate_data_leucine_named_1
        )
        self.assertTrue(created)
        infusate_found = Infusate.objects.get_infusate(
            self.infusate_data_leucine_named_2
        )
        self.assertIsNone(infusate_found)
