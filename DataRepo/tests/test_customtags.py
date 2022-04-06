from django.core.management import call_command

from DataRepo.models.compound import CompoundSynonym
from DataRepo.models.peak_group import PeakGroup
from DataRepo.models.study import Study
from DataRepo.templatetags.customtags import (
    get_case_insensitive_synonyms,
    get_manytomany_rec,
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

    def test_get_manytomany_rec_value(self):
        """Ensure the one record matching the pk is returned in a 1-member list."""
        pgs = PeakGroup.objects.filter(
            msrun__sample__animal__studies__name__iexact="small_obob"
        )[0:1]
        of = Study.objects.get(name__iexact="obob_fasted")
        recs = get_manytomany_rec(pgs[0].msrun.sample.animal.studies, of.pk)
        self.assertEqual(recs, [of])

    def test_get_manytomany_rec_novalue(self):
        """Ensure the supplied records are returned if pk is empty."""
        pgs = PeakGroup.objects.filter(
            msrun__sample__animal__studies__name__iexact="small_obob"
        )[0:1]
        of = Study.objects.filter(name__icontains="obob")
        recs = get_manytomany_rec(pgs[0].msrun.sample.animal.studies, "")
        self.assertEqual(recs.count(), of.count())
        self.assertEqual(recs.count(), 2)
        self.assertEqual([recs[0].name, recs[1].name], ["obob_fasted", "small_obob"])
