from typing import Dict, Type

from django.db.models import Model

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.protocols_loader import ProtocolsLoader
from DataRepo.loaders.study_loader import StudyLoader, StudyV3Loader
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
    AllMissingSamples,
    AllMissingTissues,
    MissingRecords,
    MissingTissues,
    MissingTreatments,
    NoSamples,
    RecordDoesNotExist,
)
from DataRepo.utils.file_utils import read_from_file

PeakGroupCompound: Type[Model] = PeakGroup.compounds.through


class StudyLoaderTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml"]

    def test_study_loader_load_data_success(self):
        file = "DataRepo/data/tests/submission_v3/multitracer_v3/study.xlsx"
        sl = StudyV3Loader(
            df=read_from_file(file, sheet=None),
            file=file,
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
        sl = StudyV3Loader()
        dt = sl.get_loader_class_dtypes(AnimalsLoader)
        self.assertDictEqual(
            {
                "Animal Name": str,
                "Diet": str,
                "Feeding Status": str,
                "Genotype": str,
                "Infusate": str,
                "Sex": str,
                "Study": str,
                "Treatment": str,
            },
            dt,
        )
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

    def test_study_loader_get_sheet_names_tuple(self):
        sl = StudyV3Loader()
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
                "DEFAULTS": "Defaults",
                "ERRORS": "Errors",
            },
            snt._asdict(),
        )
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

    def test_study_loader_package_group_exceptions(self):
        file = (
            "DataRepo/data/tests/submission_v3/multitracer_v3/study_missing_data.xlsx"
        )
        sl = StudyV3Loader(
            df=read_from_file(file, sheet=None),
            file=file,
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
        self.assertEqual(
            5,
            len(aess.aggregated_errors_dict.keys()),
            msg=f"AES keys: {list(aess.aggregated_errors_dict.keys())}",
        )
        self.assertEqual(
            3,
            len(aess.aggregated_errors_dict["study_missing_data.xlsx"].exceptions),
            msg=f"Exceptions: {list(aess.aggregated_errors_dict['study_missing_data.xlsx'].get_exception_types())}",
        )
        self.assertTrue(
            aess.aggregated_errors_dict[
                "study_missing_data.xlsx"
            ].exception_type_exists(MissingTreatments)
        )
        self.assertTrue(
            aess.aggregated_errors_dict[
                "study_missing_data.xlsx"
            ].exception_type_exists(MissingTissues)
        )
        self.assertTrue(
            aess.aggregated_errors_dict[
                "study_missing_data.xlsx"
            ].exception_type_exists(MissingRecords)
        )

        self.assertEqual(
            1, len(aess.aggregated_errors_dict["alaglu_cor.xlsx"].exceptions)
        )
        self.assertTrue(
            aess.aggregated_errors_dict["alaglu_cor.xlsx"].exception_type_exists(
                NoSamples
            )
        )

        self.assertEqual(
            1,
            len(aess.aggregated_errors_dict["Tissues Check"].exceptions),
        )
        self.assertTrue(
            aess.aggregated_errors_dict["Tissues Check"].exception_type_exists(
                AllMissingTissues
            )
        )

        self.assertEqual(
            1,
            len(aess.aggregated_errors_dict["Peak Annotation Files Check"].exceptions),
        )
        self.assertTrue(
            aess.aggregated_errors_dict[
                "Peak Annotation Files Check"
            ].exception_type_exists(AllMissingSamples)
        )

    def test_study_loader_create_grouped_exceptions(self):
        file = (
            "DataRepo/data/tests/submission_v3/multitracer_v3/study_missing_data.xlsx"
        )
        sl = StudyV3Loader(
            df=read_from_file(file, sheet=None),
            file=file,
        )
        sl.missing_sample_record_exceptions = [
            RecordDoesNotExist(Sample, {"name": "s1"})
        ]
        sl.missing_compound_record_exceptions = [
            RecordDoesNotExist(Compound, {"name": "titanium"})
        ]
        sl.create_grouped_exceptions()
        self.assertEqual(
            7,
            len(sl.load_statuses.statuses.keys()),
            msg=f"Load status keys: {list(sl.load_statuses.statuses.keys())}",
        )
        self.assertIn("Samples Check", sl.load_statuses.statuses.keys())
        self.assertIn("Compounds Check", sl.load_statuses.statuses.keys())
        self.assertIn("study_missing_data.xlsx", sl.load_statuses.statuses.keys())

    def test_get_loader_instances(self):
        sl = StudyV3Loader(dry_run=True)
        loaders: Dict[str, TableLoader] = sl.get_loader_instances()
        self.assertIsInstance(loaders, dict)
        self.assertEqual(
            set(StudyV3Loader.DataHeaders._fields),
            set(list(loaders.keys()) + ["DEFAULTS", "ERRORS"]),
        )
        # Make sure that the loaders were instantiated using the common args
        self.assertTrue(loaders["STUDY"].dry_run)
        # Make sure that the loaders were instantiated using the custom args
        self.assertEqual(
            ProtocolsLoader.DataHeadersExcel, loaders["TREATMENTS"].get_headers()
        )

    def test_determine_matching_versions_v2(self):
        df = read_from_file(
            "DataRepo/data/tests/study_doc_versions/study_v2.xlsx", sheet=None
        )
        version_list, _ = StudyLoader.determine_matching_versions(df)
        self.assertEqual(["2.0"], version_list)

    def test_determine_matching_versions_v3(self):
        df = read_from_file(
            "DataRepo/data/tests/study_doc_versions/study_v3.xlsx", sheet=None
        )
        version_list, _ = StudyLoader.determine_matching_versions(df)
        self.assertEqual(["3.0"], version_list)


class StudyV3LoaderTests(TracebaseTestCase):
    def test_convert_df_v3(self):
        # TODO: Implement test
        pass


class StudyV2LoaderTests(TracebaseTestCase):
    def test_convert_df_v2(self):
        # TODO: Implement test
        pass
