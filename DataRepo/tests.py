from datetime import datetime

from django.test import TestCase

import pandas as pd

from .models import Compound, Study, Animal, Sample, Tissue, MSRun, Protocol

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


class StudyTests(TestCase):
    def get_test_dataframe(self):

        # making this a dataframe, if more rows are need for future tests, or we
        # switch to a file based test
        test_df = pd.DataFrame(
            {
                "Sample Name": ["bat-xz969"],
                "Date Collected": ["11/18/2020"],
                "Researcher Name": ["Xianfeng Zhang"],
                "Tissue": ["BAT"],
                "Animal ID": ["969"],
                "Animal Genotype": ["WT"],
                "Animal Body Weight": ["27.2"],
                "Tracer Compound": ["Palmitic acid"],
                "Tracer Labeled Atom": ["Carbon"],
                "Tracer Label Atom Count": ["16.00"],
                "Tracer Infusion Rate": ["0.55"],
                "Tracer Concentration": ["8.00"],
                "Animal State": ["Fasted"],
                "Study Name": ["obob_fasted"],
            }
        )
        return test_df

    def setUp(self):

        # may want to just read from file;  NOTE: values have been change from
        # the example file [datetime format, Tracer Compound name]
        self.testdata = pd.DataFrame(
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

    def test_create_studied_sample(self):
        """create studied sample"""
        first = self.testdata.iloc[0]
        # create our animals foreign keys
        tracer, tracer_created = Compound.objects.get_or_create(
            name=first["Tracer Compound"]
        )
        tissue, tissue_created = Tissue.objects.get_or_create(name=first["Tissue"])
        self.assertEqual(tissue.name, first["Tissue"])

        study = Study.objects.create(
            name=first["Study Name"],
        )
        self.assertEqual(study.name, first["Study Name"])

        # create the animal; using get_or_create in case this becomes a
        # file-based test
        animal, animal_created = Animal.objects.get_or_create(
            name=first["Animal ID"],
            state=first["Animal State"],
            body_weight=first["Animal Body Weight"],
            genotype=first["Animal Genotype"],
            tracer_compound=tracer,
            tracer_labeled_atom=first["Tracer Labeled Atom"],
            tracer_labeled_count=int(float(first["Tracer Label Atom Count"])),
            tracer_infusion_rate=first["Tracer Infusion Rate"],
            tracer_infusion_concentration=first["Tracer Concentration"],
        )
        self.assertEqual(animal.name, first["Animal ID"])

        # add the animal to the study
        study.animals.add(animal)

        # add the animals sample(s)
        sample = Sample.objects.create(
            name=first["Sample Name"],
            tissue=tissue,
            animal=animal,
            researcher=first["Researcher Name"],
            date=first["Date Collected"],
        )
        self.assertEqual(sample.name, first["Sample Name"])

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