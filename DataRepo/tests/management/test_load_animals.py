from django.conf import settings
from django.core.management import call_command
from django.db.models.deletion import RestrictedError
from django.test import override_settings

from DataRepo.models.animal import Animal
from DataRepo.models.compound import Compound
from DataRepo.models.element_label import ElementLabel
from DataRepo.models.infusate import Infusate
from DataRepo.models.study import Study
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.infusate_name_parser import parse_infusate_name


@override_settings(CACHES=settings.TEST_CACHES)
class LoadAnimalsSmallObob2Tests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        Study.objects.create(name="obob_fasted")
        Study.objects.create(name="exp024_michael lactate timecourse")
        call_command(
            "load_protocols",
            infile="DataRepo/data/tests/small_obob2/protocols.tsv",
        )
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/small_obob2/compounds_for_animals.tsv",
        )
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name("C16:0-[13C16]", [1])
        )
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name("lysine-[13C6]", [2])
        )
        Infusate.objects.get_or_create_infusate(
            parse_infusate_name("lactate-[13C3]", [148.88])
        )
        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob2/animals_table.tsv",
            headers="DataRepo/data/tests/small_obob2/animal_headers.yaml",
        )
        call_command(
            "load_animals",
            infile="DataRepo/data/tests/small_obob2/serum_lactate_animal_table.tsv",
            headers="DataRepo/data/tests/small_obob2/animal_headers.yaml",
        )

    def test_animals_loaded(self):
        self.assertEqual(8, Animal.objects.count())
        Study.objects.get(name="obob_fasted")
        self.assertEqual(7, Animal.objects.filter(studies__name="obob_fasted").count())
        self.assertEqual(8, Animal.objects.filter(labels__element="C").count())
        self.assertEqual(
            ["C"], list(Animal.objects.first().labels.values_list("element", flat=True))
        )

    def test_animal_tracers(self):
        a = Animal.objects.get(name="969")
        c = Compound.objects.get(name="C16:0")
        self.assertEqual(a.infusate.tracers.first().compound, c)
        self.assertEqual(
            a.infusate.tracers.first().labels.first().element, ElementLabel.CARBON
        )
        self.assertEqual(a.infusate.tracers.count(), 1)
        self.assertEqual(a.infusate.tracers.first().labels.count(), 1)
        self.assertEqual(a.sex, None)

    def test_animal_treatments_loaded(self):
        a = Animal.objects.get(name="969")
        self.assertEqual(a.treatment, None)
        a = Animal.objects.get(name="exp024f_M2")
        self.assertEqual(a.treatment.name, "T3")
        self.assertEqual(
            a.treatment.description,
            "For protocol's full text, please consult Michael Neinast.",
        )

    def test_restricted_animal_treatment_deletion(self):
        treatment = Animal.objects.get(name="exp024f_M2").treatment
        with self.assertRaises(RestrictedError):
            # test a restricted deletion
            treatment.delete()
