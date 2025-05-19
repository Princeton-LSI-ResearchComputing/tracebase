from django.conf import settings
from django.db import IntegrityError
from django.test import override_settings

from DataRepo.models import Compound, CompoundSynonym
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import ProhibitedStringValue


@override_settings(CACHES=settings.TEST_CACHES)
class CompoundTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()
        Compound.objects.create(
            name="alanine", formula="C3H7NO2", hmdb_id="HMDB0000161"
        )

    def test_name(self):
        """Compound lookup by name"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.name, "alanine")

    def test_hmdb_url(self):
        """Compound hmdb url"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.hmdb_url, f"{Compound.HMDB_CPD_URL}/{alanine.hmdb_id}")

    def test_atom_count_3(self):
        """Compound atom_count"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.atom_count("C"), 3)

    def test_atom_count_zero(self):
        """Compound atom_count"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.atom_count("F"), 0)

    def test_atom_count_invalid(self):
        """Compound atom_count"""
        alanine = Compound.objects.get(name="alanine")
        with self.assertWarns(UserWarning):
            self.assertEqual(alanine.atom_count("Abc"), None)

    def test_get_name_query_expression(self):
        q_exp = Compound.get_name_query_expression("alanine")
        self.assertEqual(
            "(OR: ('name__iexact', 'alanine'), ('synonyms__name__iexact', 'alanine'))",
            str(q_exp),
        )

    def test_validate_compound_name(self):
        with self.assertRaises(ProhibitedStringValue):
            Compound.validate_compound_name("bad;name")
        self.assertEqual(
            "bad-name", Compound.validate_compound_name("bad;name", fix=True)
        )

    def test_prohibited_delimiters(self):
        with self.assertRaises(ProhibitedStringValue) as ar1:
            c = Compound.objects.create(
                name="hexadecanoic/acid", formula="C16H32O2", hmdb_id="HMDB0000220"
            )
            c.full_clean()
        self.assertIn("Prohibited character(s) ['/'] encountered", str(ar1.exception))
        self.assertIn("(in 'hexadecanoic/acid')", str(ar1.exception))
        self.assertIn(
            "None of the following reserved substrings are allowed: [';', '/'].",
            str(ar1.exception),
        )
        with self.assertRaises(ProhibitedStringValue) as ar2:
            c = Compound.objects.create(
                name="hexadecanoic;acid", formula="C16H32O2", hmdb_id="HMDB0000221"
            )
            c.full_clean()
        self.assertIn("Prohibited character(s) [';'] encountered", str(ar2.exception))
        self.assertIn("(in 'hexadecanoic;acid')", str(ar2.exception))
        self.assertIn(
            "None of the following reserved substrings are allowed: [';', '/'].",
            str(ar2.exception),
        )


@override_settings(CACHES=settings.TEST_CACHES)
class CompoundSynonymTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()
        self.PRIMARY_COMPOUND = Compound.objects.create(
            name="hexadecanoic acid", formula="C16H32O2", hmdb_id="HMDB0000220"
        )
        # just the act of creating a compound (above) creates two synonyms for
        # it, in this case
        self.ALIASES_SETUP_COUNT = 2
        aliases = ["palmitic acid", "C16:0"]
        self.ALIASES_SETUP_COUNT += len(aliases)
        self.PRIMARY_ALIASES = aliases
        # make synonyms
        for alias in aliases:
            CompoundSynonym.objects.create(name=alias, compound=self.PRIMARY_COMPOUND)

        self.SECONDARY_COMPOUND = Compound.objects.create(
            name="alanine", formula="C3H7NO2", hmdb_id="HMDB0000161"
        )

    def test_compound_synonym_insertion1(self):
        #  validates all the aliases created during setUp
        self.assertTrue(
            self.PRIMARY_COMPOUND.synonyms.filter(name="hexadecanoic acid").exists()
        )
        self.assertTrue(
            self.PRIMARY_COMPOUND.synonyms.filter(name="Hexadecanoic acid").exists()
        )
        for alias in self.PRIMARY_ALIASES:
            self.assertTrue(self.PRIMARY_COMPOUND.synonyms.filter(name=alias).exists())
        # setup insertions count
        self.assertEqual(
            len(self.PRIMARY_COMPOUND.synonyms.all()), self.ALIASES_SETUP_COUNT
        )

    def test_compound_synonym_insertion2(self):
        # test CompoundSynonym's intrinsic class creation method
        alt_name = "Palmitate"
        CompoundSynonym.objects.create(name=alt_name, compound=self.PRIMARY_COMPOUND)
        self.assertTrue(self.PRIMARY_COMPOUND.synonyms.filter(name=alt_name).exists())

    def test_compound_synonym_insertion3(self):
        # test Compound's utility instance creation method
        alt_name = "Hexadecanoate"
        self.PRIMARY_COMPOUND.get_or_create_synonym(alt_name)
        self.assertTrue(self.PRIMARY_COMPOUND.synonyms.filter(name=alt_name).exists())

    def test_compound_synonym_duplication1(self):
        # test that duplicate insertion fails
        with self.assertRaises(IntegrityError):
            CompoundSynonym.objects.create(
                name=self.PRIMARY_ALIASES[0], compound=self.PRIMARY_COMPOUND
            )

    def test_compound_synonym_duplication2(self):
        # test that attempting to use the same synonym for multiple compounds fails
        with self.assertRaises(IntegrityError):
            CompoundSynonym.objects.create(
                name=self.PRIMARY_ALIASES[0], compound=self.SECONDARY_COMPOUND
            )

    def test_compound_deletion(self):
        # compound deletion should remove all synonyms
        c = Compound.objects.create(
            name="1-Methylhistidine", formula="C7H11N3O2", hmdb_id="HMDB0000001"
        )
        alias = "1 methylhistidine"
        CompoundSynonym.objects.create(name=alias, compound=c)
        self.assertTrue(CompoundSynonym.objects.filter(name=alias).exists())
        c.delete()
        self.assertFalse(CompoundSynonym.objects.filter(name=alias).exists())

    def test_compound_synonym_deletion(self):
        # synonym deletion does not alter the compound record
        c = Compound.objects.create(
            name="1-Methylhistidine", formula="C7H11N3O2", hmdb_id="HMDB0000001"
        )
        alias = "1 methylhistidine"
        cs = CompoundSynonym.objects.create(name=alias, compound=c)
        cs.delete()
        self.assertTrue(Compound.objects.filter(name="1-Methylhistidine").exists())

    def test_prohibited_delimiters(self):
        with self.assertRaises(ProhibitedStringValue) as ar1:
            c = CompoundSynonym.objects.create(
                name="hexadecanoic/acid", compound=self.PRIMARY_COMPOUND
            )
            c.full_clean()
        self.assertIn("(in 'hexadecanoic/acid')", str(ar1.exception))
        self.assertIn("Prohibited character(s) ['/'] encountered", str(ar1.exception))
        self.assertIn(
            "None of the following reserved substrings are allowed: [';', '/'].",
            str(ar1.exception),
        )
        with self.assertRaises(ProhibitedStringValue) as ar2:
            c = CompoundSynonym.objects.create(
                name="hexadecanoic;acid", compound=self.PRIMARY_COMPOUND
            )
            c.full_clean()
        self.assertIn("Prohibited character(s) [';'] encountered", str(ar2.exception))
        self.assertIn("(in 'hexadecanoic;acid')", str(ar2.exception))
        self.assertIn(
            "None of the following reserved substrings are allowed: [';', '/'].",
            str(ar2.exception),
        )
