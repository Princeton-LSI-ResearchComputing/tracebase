from django.core.management import call_command
from django.utils import dateparse

from DataRepo.models import CompoundSynonym, MaintainedModel, PeakGroup, Study
from DataRepo.templatetags.customtags import (
    append,
    append_unique,
    compile_stats,
    display_filter,
    format_date,
    get_case_insensitive_synonyms,
    get_many_related_rec,
    intmultiply,
    lte,
    multiply,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class CustomTagsTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    @MaintainedModel.no_autoupdates()
    def setUpTestData(cls):
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_no_newsample.xlsx",
            exclude_sheets=["Peak Annotation Files"],
        )
        call_command(
            "load_study",
            infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_no_newsample_2ndstudy.xlsx",
            exclude_sheets=["Peak Annotation Files"],
        )
        call_command(
            "load_peak_annotations",
            infile="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx",
            lc_protocol_name="polar-HILIC-25-min",
            instrument="unknown",
            date="2021-06-03",
            operator="Michael Neinast",
        )
        super().setUpTestData()

    def test_get_case_insensitive_synonyms(self):
        csqs = CompoundSynonym.objects.filter(name__icontains="glucose")
        qnames = list(csqs.values_list("name", flat=True))
        # Make sure there are expected case variants
        self.assertIn("glucose", qnames)
        self.assertIn("Glucose", qnames)
        # Expected case insensitive list:  ['D-Glucose', 'glucose', 'glucose-6-phosphate']
        # Input case variant list:  ['D-Glucose', 'Glucose', 'glucose', 'Glucose-6-phosphate', 'glucose-6-phosphate']
        csls = get_case_insensitive_synonyms(csqs)
        self.assertListEqual(csls, ["D-Glucose", "glucose", "glucose-6-phosphate"])

    def test_get_many_related_rec_value(self):
        """Ensure the one record matching the pk is returned in a 1-member list."""
        pgs = PeakGroup.objects.filter(
            msrun_sample__sample__animal__studies__name__iexact="Small OBOB"
        )[0:1]
        of = Study.objects.get(name__iexact="obob_fasted")
        recs = get_many_related_rec(pgs[0].msrun_sample.sample.animal.studies, of.pk)
        self.assertEqual(recs, [of])

    def test_get_many_related_rec_novalue(self):
        """Ensure the supplied records are returned if pk is empty."""
        pgs = PeakGroup.objects.filter(
            msrun_sample__sample__animal__studies__name__iexact="Small OBOB"
        )[0:1]
        of = Study.objects.filter(name__icontains="obob")
        recs = get_many_related_rec(pgs[0].msrun_sample.sample.animal.studies, "")
        self.assertEqual(recs.count(), of.count())
        self.assertEqual(recs.count(), 2)
        self.assertEqual(
            set([recs[0].name, recs[1].name]), set(["obob_fasted", "Small OBOB"])
        )

    def test_display_filter(self):
        filter = {
            "type": "group",
            "val": "all",
            "static": False,
            "queryGroup": [
                {
                    "type": "query",
                    "pos": "",
                    "ncmp": "istartswith",
                    "fld": "msrun_sample__sample__tissue__name",
                    "val": "brai",
                    "static": False,
                },
            ],
        }
        expected = "istartswith brai"
        got = display_filter(filter)
        self.assertEqual(got, expected)

    def test_compile_stats(self):
        dcts = [
            {
                "val": "testing",
                "cnt": 9,
            },
            {
                "val": "trying",
                "cnt": 100,
            },
        ]
        got = compile_stats(dcts, num_chars=19)
        expected = {
            "full": "testing (9), trying (100)",
            "short": "testing (9), try...",
        }
        self.assertEqual(got, expected)

    def test_multiply(self):
        self.assertEqual(6.2, multiply(2.0, 3.1))

    def test_intmultiply(self):
        self.assertEqual(6, intmultiply(2.0, 3.1))

    def test_format_date(self):
        self.assertEqual("1977.02.11", format_date("1977-02-11", "%Y.%m.%d"))
        self.assertEqual(
            "1977.02.11",
            format_date(dateparse.parse_datetime("1977-02-11"), "%Y.%m.%d"),
        )
        # dateparse.parse_datetime cannot parse "Feb 11, 1977".  Fallback is the input date
        self.assertEqual("Feb 11, 1977", format_date("Feb 11, 1977", "%Y-$m-%d"))

    def test_append_unique(self):
        lst = [1, 2, 3]
        self.assertEqual("", append_unique(lst, 4))
        self.assertEqual([1, 2, 3, 4], lst)
        append_unique(lst, 3)
        self.assertEqual([1, 2, 3, 4], lst)

    def test_append(self):
        lst = [1, 2, 3]
        self.assertEqual("", append(lst, 4))
        self.assertEqual([1, 2, 3, 4], lst)
        append(lst, 4)
        self.assertEqual([1, 2, 3, 4, 4], lst)

    def test_lte(self):
        self.assertTrue(lte(3, 4))
        self.assertTrue(lte(4, 4))
        self.assertFalse(lte(5, 4))
        self.assertTrue(lte("a", "b"))
        self.assertTrue(lte("b", "b"))
        self.assertFalse(lte("c", "b"))
