from typing import Type

from django.db.models import Model

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.study_loader import StudyLoader
from DataRepo.models import (  # FCirc,
    Animal,
    ArchiveFile,
    Compound,
    CompoundSynonym,
    Infusate,
    InfusateTracer,
    LCMethod,
    MSRunSample,
    MSRunSequence,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupLabel,
    Protocol,
    Sample,
    Study,
    Tissue,
    Tracer,
    TracerLabel,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase

PeakGroupCompound: Type[Model] = PeakGroup.compounds.through


class StudyLoaderTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

    def test_study_loader_load_data_success(self):
        sl = StudyLoader(
            file="DataRepo/data/tests/submission_v3/multitracer_v3/study.xlsx"
        )
        sl.load_data()
        self.assertEqual(1, Animal.objects.count())
        self.assertEqual(1, ArchiveFile.objects.count())
        self.assertEqual(2, Compound.objects.count())
        self.assertEqual(8, CompoundSynonym.objects.count())

        # TODO: Add FCirc loading to the SamplesLoader
        # self.assertEqual(3, FCirc.objects.count())

        self.assertEqual(1, Infusate.objects.count())
        self.assertEqual(2, InfusateTracer.objects.count())
        self.assertEqual(1, LCMethod.objects.count())
        self.assertEqual(1, MSRunSample.objects.count())
        self.assertEqual(1, MSRunSequence.objects.count())
        self.assertEqual(15, PeakData.objects.count())

        # PeakDataLabel: 13 non-parent rows, 7 of which have nitrogen AND carbon
        self.assertEqual(20, PeakDataLabel.objects.count())

        self.assertEqual(2, PeakGroup.objects.count())
        self.assertEqual(3, PeakGroupLabel.objects.count())
        self.assertEqual(1, Protocol.objects.count())
        self.assertEqual(1, Sample.objects.count())
        self.assertEqual(1, Study.objects.count())
        self.assertEqual(1, Tissue.objects.count())
        self.assertEqual(2, Tracer.objects.count())
        self.assertEqual(3, TracerLabel.objects.count())
        self.assertEqual(2, PeakGroupCompound.objects.count())

    def test_study_loader_get_class_dtypes(self):
        sl = StudyLoader()
        dt = sl.get_loader_class_dtypes(AnimalsLoader)
        self.assertDictEqual(
            {
                "Age": int,
                "Animal Name": str,
                "Diet": str,
                "Feeding Status": str,
                "Genotype": str,
                "Infusate": str,
                "Infusion Rate": float,
                "Sex": str,
                "Study": str,
                "Treatment": str,
                "Weight": float,
            },
            dt,
        )
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

    def test_study_loader_get_sheet_names_tuple(self):
        sl = StudyLoader()
        snt = sl.get_sheet_names_tuple()
        self.assertDictEqual(
            {
                "ANIMALS": "Animals",
                "COMPOUNDS": "Compounds",
                "FILES": "Peak Annotation Files",
                "HEADERS": "Peak Annotation Details",
                "INFUSATES": "Infusates",
                "LCPROTOCOLS": "LC Protocols",
                "SAMPLES": "Samples",
                "SEQUENCES": "Sequences",
                "STUDY": "Study",
                "TISSUES": "Tissues",
                "TRACERS": "Tracers",
                "TREATMENTS": "Treatments",
            },
            snt._asdict(),
        )
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

    def test_study_loader_package_group_exceptions(self):
        # TODO: Implement test
        pass

    def test_study_loader_create_grouped_exceptions(self):
        # TODO: Implement test
        pass
