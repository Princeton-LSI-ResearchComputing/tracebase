from django.core.management import call_command

from DataRepo.models import CompoundSynonym, PeakGroup, Study
from DataRepo.templatetags.customtags import (
    compile_stats,
    display_filter,
    get_case_insensitive_synonyms,
    get_many_related_rec,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class CustomTagsTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
        call_command(
            "load_compounds",
            compounds="DataRepo/example_data/consolidated_tracebase_compound_list.tsv",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        call_command(
            "load_samples",
            "DataRepo/example_data/small_dataset/small_obob_sample_table_2ndstudy.tsv",
            sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
        )
        call_command(
            "load_accucor_msruns",
            protocol="Default",
            accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
            date="2021-06-03",
            researcher="Michael Neinast",
            new_researcher=True,
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
            msrun__sample__animal__studies__name__iexact="small_obob"
        )[0:1]
        of = Study.objects.get(name__iexact="obob_fasted")
        recs = get_many_related_rec(pgs[0].msrun.sample.animal.studies, of.pk)
        self.assertEqual(recs, [of])

    def test_get_many_related_rec_novalue(self):
        """Ensure the supplied records are returned if pk is empty."""
        pgs = PeakGroup.objects.filter(
            msrun__sample__animal__studies__name__iexact="small_obob"
        )[0:1]
        of = Study.objects.filter(name__icontains="obob")
        recs = get_many_related_rec(pgs[0].msrun.sample.animal.studies, "")
        self.assertEqual(recs.count(), of.count())
        self.assertEqual(recs.count(), 2)
        self.assertEqual([recs[0].name, recs[1].name], ["obob_fasted", "small_obob"])

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
                    "fld": "msrun__sample__tissue__name",
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
