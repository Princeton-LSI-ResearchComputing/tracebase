from datetime import datetime

import pandas as pd
from django.test import TestCase

from .models import Animal, Compound, MSRun, Protocol, Sample, Study, Tissue


class ExampleDataConsumer:
    def get_sample_test_dataframe(self):

        # making this a dataframe, if more rows are need for future tests, or we
        # switch to a file based test
        test_df = pd.DataFrame(
            {
                "Sample Name": ["bat-xz969"],
                "Date Collected": ["2020-11-18"],
                "Researcher Name": ["Xianfeng Zhang"],
                "Tissue": ["BAT"],
                "Animal ID": ["969"],
                "Animal Genotype": ["WT"],
                "Animal Body Weight": ["27.2"],
                "Tracer Compound": ["C16:0"],
                "Tracer Labeled Atom": ["C"],
                "Tracer Label Atom Count": ["16.00"],
                "Tracer Infusion Rate": ["0.55"],
                "Tracer Concentration": ["8.00"],
                "Animal State": ["Fasted"],
                "Study Name": ["obob_fasted"],
            }
        )
        return test_df


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


class StudyTests(TestCase, ExampleDataConsumer):
    def setUp(self):
        # Get test data
        self.testdata = self.get_sample_test_dataframe()
        first = self.testdata.iloc[0]

        # Create animal with tracer
        self.tracer = Compound.objects.create(name=first["Tracer Compound"])
        self.animal = Animal.objects.create(
            name=first["Animal ID"],
            state=first["Animal State"],
            body_weight=first["Animal Body Weight"],
            genotype=first["Animal Genotype"],
            tracer_compound=self.tracer,
            tracer_labeled_atom=first["Tracer Labeled Atom"],
            tracer_labeled_count=int(float(first["Tracer Label Atom Count"])),
            tracer_infusion_rate=first["Tracer Infusion Rate"],
            tracer_infusion_concentration=first["Tracer Concentration"],
        )

        # Create a sample from the animal
        self.tissue = Tissue.objects.create(name=first["Tissue"])
        self.sample = Sample.objects.create(
            name=first["Sample Name"],
            tissue=self.tissue,
            animal=self.animal,
            researcher=first["Researcher Name"],
            date=first["Date Collected"],
        )

        self.protocol = Protocol.objects.create(name="p1", description="p1desc")
        MSRun.objects.create(
            name="msr1", date=datetime.now(), protocol=self.protocol, sample=self.sample
        )

        self.first = first

    def test_tracer(self):
        self.assertEqual(self.tracer.name, self.first["Tracer Compound"])

    def test_tissue(self):
        self.assertEqual(self.tissue.name, self.first["Tissue"])

    def test_animal(self):
        self.assertEqual(self.animal.name, self.first["Animal ID"])
        self.assertEqual(
            self.animal.tracer_compound.name, self.first["Tracer Compound"]
        )

    def test_study(self):
        """create study and associate animal"""
        # Create a study
        study = Study.objects.create(name=self.first["Study Name"])
        self.assertEqual(study.name, self.first["Study Name"])

        # add the animal to the study
        study.animals.add(self.animal)
        self.assertEqual(study.animals.get().name, self.animal.name)

    def test_sample(self):
        # Test sample relations
        self.assertEqual(self.sample.name, self.first["Sample Name"])
        self.assertEqual(self.sample.tissue.name, self.first["Tissue"])
        self.assertEqual(self.sample.animal.name, self.first["Animal ID"])

    def test_msrun_protocol(self):
        """MSRun lookup by name"""
        msr = MSRun.objects.get(name="msr1")
        self.assertEqual(msr.protocol.name, "p1")
