from typing import Type

from django.db.models import Model

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.study_loader import StudyLoader
from DataRepo.models import (
    Animal,
    ArchiveFile,
    Compound,
    CompoundSynonym,
    FCirc,
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
from DataRepo.utils.exceptions import (
    AggregatedErrorsSet,
    AllMissingTissues,
    MissingRecords,
    MissingTissues,
    MissingTreatments,
    RecordDoesNotExist,
)

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
        self.assertEqual(3, FCirc.objects.count())
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
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

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
        sl = StudyLoader(
            file="DataRepo/data/tests/submission_v3/multitracer_v3/study_missing_data.xlsx"
        )
        with self.assertRaises(AggregatedErrorsSet) as ar:
            sl.load_data()

        # Make sure nothing was loaded
        self.assertEqual(0, Animal.objects.count())
        self.assertEqual(0, ArchiveFile.objects.count())
        self.assertEqual(0, Compound.objects.count())
        self.assertEqual(0, CompoundSynonym.objects.count())
        self.assertEqual(0, FCirc.objects.count())
        self.assertEqual(0, Infusate.objects.count())
        self.assertEqual(0, InfusateTracer.objects.count())
        self.assertEqual(0, LCMethod.objects.count())
        self.assertEqual(0, MSRunSample.objects.count())
        self.assertEqual(0, MSRunSequence.objects.count())
        self.assertEqual(0, PeakData.objects.count())
        self.assertEqual(0, PeakDataLabel.objects.count())
        self.assertEqual(0, PeakGroup.objects.count())
        self.assertEqual(0, PeakGroupLabel.objects.count())
        self.assertEqual(0, Protocol.objects.count())
        self.assertEqual(0, Sample.objects.count())
        self.assertEqual(0, Study.objects.count())
        self.assertEqual(0, Tissue.objects.count())
        self.assertEqual(0, Tracer.objects.count())
        self.assertEqual(0, TracerLabel.objects.count())
        self.assertEqual(0, PeakGroupCompound.objects.count())

        aess = ar.exception

        # Make sure all the exceptions are categorized correctly per sheet and special category
        # NOTE: Only temporarily commented out due to a rebase after a cherry-pick. This totally changes by the end.
        # self.assertEqual(7, len(aess.aggregated_errors_dict.keys()))
        self.assertEqual(1, len(aess.aggregated_errors_dict["Animals"].exceptions))
        self.assertTrue(
            aess.aggregated_errors_dict["Animals"].exception_type_exists(
                MissingTreatments
            )
        )
        self.assertEqual(1, len(aess.aggregated_errors_dict["Samples"].exceptions))
        self.assertTrue(
            aess.aggregated_errors_dict["Samples"].exception_type_exists(MissingTissues)
        )
        self.assertEqual(
            1, len(aess.aggregated_errors_dict["Peak Annotation Details"].exceptions)
        )

        # TODO: Change this to MissingSamples
        self.assertTrue(
            aess.aggregated_errors_dict[
                "Peak Annotation Details"
            ].exception_type_exists(MissingRecords)
        )

        # TODO: It would be nice if every file got its own category
        self.assertEqual(
            1, len(aess.aggregated_errors_dict["Peak Annotation Files"].exceptions)
        )
        # NOTE: Only temporarily commented out due to a rebase after a cherry-pick. This totally changes by the end.
        # self.assertTrue(
        #     aess.aggregated_errors_dict["Peak Annotation Files"].exception_type_exists(
        #         NoSamples
        #     )
        # )

        self.assertEqual(
            1,
            len(
                aess.aggregated_errors_dict[
                    "All Tissues Exist in the Database"
                ].exceptions
            ),
        )
        self.assertTrue(
            aess.aggregated_errors_dict[
                "All Tissues Exist in the Database"
            ].exception_type_exists(AllMissingTissues)
        )

        # NOTE: Only temporarily commented out due to a rebase after a cherry-pick. This totally changes by the end.
        # self.assertEqual(
        #     1,
        #     len(
        #         aess.aggregated_errors_dict[
        #             "No Files are Missing All Samples"
        #         ].exceptions
        #     ),
        # )
        # self.assertTrue(
        #     aess.aggregated_errors_dict[
        #         "No Files are Missing All Samples"
        #     ].exception_type_exists(AllMissingSamples)
        # )

    def test_study_loader_create_grouped_exceptions(self):
        sl = StudyLoader(
            file="DataRepo/data/tests/submission_v3/multitracer_v3/study_missing_data.xlsx"
        )
        sl.missing_sample_record_exceptions = [
            RecordDoesNotExist(Sample, {"name": "s1"})
        ]
        sl.missing_compound_record_exceptions = [
            RecordDoesNotExist(Compound, {"name": "titanium"})
        ]
        sl.create_grouped_exceptions()
        self.assertEqual(2, len(sl.load_statuses.statuses.keys()))
        self.assertIn(
            "All Samples Exist in the Database", sl.load_statuses.statuses.keys()
        )
        self.assertIn(
            "All Compounds Exist in the Database", sl.load_statuses.statuses.keys()
        )
