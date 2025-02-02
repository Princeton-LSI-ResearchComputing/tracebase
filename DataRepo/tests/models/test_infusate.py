from django.core.management import call_command
from django.test import tag

from DataRepo.models.compound import Compound
from DataRepo.models.infusate import Infusate
from DataRepo.models.infusate_tracer import InfusateTracer
from DataRepo.models.maintained_model import (
    MaintainedFieldNotSettable,
    MaintainedModel,
)
from DataRepo.models.tracer import Tracer
from DataRepo.models.tracer_label import TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


def create_infusate_records():
    (glu, _) = Compound.objects.get_or_create(
        name="glucose", formula="C6H12O6", hmdb_id="HMDB0000122"
    )
    (c16, _) = Compound.objects.get_or_create(
        name="C16:0", formula="C16H32O2", hmdb_id="HMDB0000220"
    )

    glu_t = Tracer.objects.create(compound=glu)
    TracerLabel.objects.create(
        tracer=glu_t, count=2, element="C", positions=[2, 3], mass_number=13
    )
    TracerLabel.objects.create(
        tracer=glu_t, count=1, element="O", positions=[4], mass_number=17
    )

    c16_t = Tracer.objects.create(compound=c16)
    TracerLabel.objects.create(
        tracer=c16_t, count=2, element="C", positions=[5, 6], mass_number=13
    )
    TracerLabel.objects.create(tracer=c16_t, count=2, element="O", mass_number=17)

    # NOTE: Cannot create the Tracer records first and then all the linked TracerLabel records due to the unique
    # DataRepo_tracer_name_key constraint.  The name field is automatically updated, so an alternative to changing the
    # record creation order is to apply a defer_autoupdates decorator to this method...
    c16_t2 = Tracer.objects.create(compound=c16)
    TracerLabel.objects.create(
        tracer=c16_t2, count=2, element="C", positions=[4, 5, 6], mass_number=13
    )
    TracerLabel.objects.create(tracer=c16_t2, count=2, element="O", mass_number=17)
    io = Infusate.objects.create(tracer_group_name="ti")
    InfusateTracer.objects.create(infusate=io, tracer=glu_t, concentration=1.0)
    InfusateTracer.objects.create(infusate=io, tracer=c16_t, concentration=2.0)
    io2 = Infusate.objects.create()
    InfusateTracer.objects.create(infusate=io2, tracer=glu_t, concentration=3.0)
    InfusateTracer.objects.create(infusate=io2, tracer=c16_t2, concentration=4.0)

    # ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}
    # C16:0-[5,6-13C2,17O2][4];glucose-[2,3-13C2,4-17O1][3]
    return io, io2


@tag("load_study")
class InfusateTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()
        MaintainedModel._reset_coordinators()
        # INFUSATE1: ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}
        # INFUSATE2: C16:0-[5,6-13C2,17O2][4];glucose-[2,3-13C2,4-17O1][3]
        self.INFUSATE1, self.INFUSATE2 = create_infusate_records()

    @classmethod
    def setUpTestData(cls):
        call_command(
            "load_tissues",
            infile="DataRepo/data/tests/tissues/tissues.tsv",
        )
        call_command(
            "load_compounds",
            infile="DataRepo/data/tests/compounds/consolidated_tracebase_compound_list.tsv",
            verbosity=2,
        )
        super().setUpTestData()

    def test_infusate_record(self):
        infusate = Infusate.objects.first()
        infusate.full_clean()

    def test_infusate_name_method(self):
        self.assertEqual(
            "ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}",
            self.INFUSATE1._name(),
        )
        self.assertEqual(
            "C16:0-[4,5,6-13C2,17O2][4];glucose-[2,3-13C2,4-17O1][3]",
            self.INFUSATE2._name(),
        )

    def test_name_not_settable(self):
        with self.assertRaises(MaintainedFieldNotSettable):
            Infusate.objects.create(
                name="test infusate",
                tracer_group_name="ti2",
            )

    def test_name_autoupdated(self):
        """
        Make sure that the name field was set automatically - triggered by the InfusateTracer record creation.
        """
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(
            name="ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}"
        )

    def test_name_none(self):
        """
        Make sure that the name field was set automatically - triggered by the InfusateTracer record creation.
        """
        self.assertEqual(
            "C16:0-[4,5,6-13C2,17O2][4];glucose-[2,3-13C2,4-17O1][3]",
            self.INFUSATE2.name,
        )
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(
            name__exact="C16:0-[4,5,6-13C2,17O2][4];glucose-[2,3-13C2,4-17O1][3]"
        )

    def test_name_self_autoupdated(self):
        """
        Make sure that the name field was set automatically - triggered by the Infusate record creation.
        """
        ti3 = Infusate.objects.create(tracer_group_name="ti3")
        self.assertEqual("ti3", ti3.name)
        # Throws DoesNotExist exception if not found
        Infusate.objects.get(name="ti3")

    def test_delete_autoupdate(self):
        """
        Make sure parent records are updated when a child record is deleted
        """
        tl = TracerLabel.objects.get(name="2,3-13C2")
        tl.delete()
        # get fresh objects
        i1 = Infusate.objects.get(id__exact=self.INFUSATE1.id)
        i2 = Infusate.objects.get(id__exact=self.INFUSATE2.id)
        # The deletion affects the tracer name (which should have been autoupdated)
        self.assertEqual("glucose-[4-17O1]", tl.tracer.name)
        # The deletion also affects the names of both infusates that had that tracer
        self.assertEqual("ti {C16:0-[5,6-13C2,17O2][2];glucose-[4-17O1][1]}", i1.name)
        self.assertEqual("C16:0-[4,5,6-13C2,17O2][4];glucose-[4-17O1][3]", i2.name)

    def test_name_and_concentrations(self):
        # self.INFUSATE1.name: ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}
        # name_and_concentrations returns a name without the concentrations, and a list of same-ordered concentrations
        name, concs = self.INFUSATE1.name_and_concentrations()
        self.assertEqual("ti {C16:0-[5,6-13C2,17O2];glucose-[2,3-13C2,4-17O1]}", name)
        self.assertAlmostEqual([2.0, 1.0], concs)

    def test_infusate_name_equal(self):
        # self.INFUSATE1.name: ti {C16:0-[5,6-13C2,17O2][2];glucose-[2,3-13C2,4-17O1][1]}
        # name_and_concentrations returns a name without the concentrations, and a list of same-ordered concentrations
        self.INFUSATE1.name_and_concentrations()
        # Should be equal even though the order is reversed
        self.assertTrue(
            self.INFUSATE1.infusate_name_equal(
                "ti {glucose-[2,3-13C2,4-17O1];C16:0-[5,6-13C2,17O2]}", [1.0, 2.0]
            )
        )
        # Should not be equal if only the tracer names are reversed (not the concentrations)
        self.assertFalse(
            self.INFUSATE1.infusate_name_equal(
                "ti {glucose-[2,3-13C2,4-17O1];C16:0-[5,6-13C2,17O2]}", [2.0, 1.0]
            )
        )
        # Should not be equal if the number of concentrations does not match
        self.assertFalse(
            self.INFUSATE1.infusate_name_equal(
                "ti {glucose-[2,3-13C2,4-17O1];C16:0-[5,6-13C2,17O2]}", [1.0]
            )
        )
        # Should be equal even though the order is reversed and the float concentrations are very slightly off
        self.assertTrue(
            self.INFUSATE1.infusate_name_equal(
                "ti {glucose-[2,3-13C2,4-17O1];C16:0-[5,6-13C2,17O2]}",
                [1.00000000001, 2.0],
            )
        )

    def test_tracer_labeled_elements(self):
        expected = ["C", "O"]
        output = self.INFUSATE1.tracer_labeled_elements
        self.assertEqual(expected, output)

    def test_name_from_data(self):
        # TODO: Implement test
        pass
