from datetime import datetime

from django.test import TestCase

from .models import Compound, MSRun, Protocol, Sample


class CompoundTests(TestCase):
    def setUp(self):
        Compound.objects.create(
            name="alanine", formula="C3H7NO2", hmdb_id="HMDB0000161"
        )

    def test_compound_name(self):
        """Compound lookup by name"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.name, "alanine")

    def test_compound_hmdb_url(self):
        """Compound hmdb url"""
        alanine = Compound.objects.get(name="alanine")
        self.assertEqual(alanine.hmdb_url, f"{Compound.HMDB_CPD_URL}/{alanine.hmdb_id}")


class ProtocolTests(TestCase):
    def setUp(self):
        Protocol.objects.create(name="p1", description="p1desc")

    def test_protocol_name(self):
        """Protocol lookup by name"""
        ptcl = Protocol.objects.get(name="p1")
        self.assertEqual(ptcl.description, "p1desc")


class MSRunTests(TestCase):
    def setUp(self):
        p1 = Protocol.objects.create(name="p1", description="p1desc")
        s1 = Sample.objects.create()
        MSRun.objects.create(name="msr1", date=datetime.now(), protocol=p1, sample=s1)

    def test_msrun_protocol(self):
        """MSRun lookup by name"""
        msr = MSRun.objects.get(name="msr1")
        self.assertEqual(msr.protocol.id, 1)
