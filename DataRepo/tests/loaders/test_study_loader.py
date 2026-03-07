from datetime import datetime, timedelta
from typing import Dict, Type

import pandas as pd
from django.core.management import call_command
from django.db.models import Model

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.msruns_loader import MSRunsLoader
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
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AggregatedErrorsSet,
    AllMissingCompounds,
    AllMissingSamples,
    AllMissingTissues,
    AllUnskippedBlanks,
    AnimalsWithoutSamples,
    AnimalsWithoutSerumSamples,
    FatalStudyLoadWarning,
    FileFromInputNotFound,
    MissingTissues,
    MissingTreatments,
    MultiLoadStatus,
    NoSamples,
    RecordDoesNotExist,
)
from DataRepo.utils.file_utils import read_from_file
from DataRepo.utils.infusate_name_parser import parse_infusate_name_with_concs

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

        # PeakDataLabel: 15 rows, 8 of which have nitrogen AND carbon
        self.assertEqual(23, PeakDataLabel.objects.count())

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
                "PGCONFLICTS": "Peak Group Conflicts",
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
            ].exception_type_exists(NoSamples)
        )

        # This essentially tests that failed sample loads of serum samples prevent AnimalsWithoutSerumSamples warnings,
        # as the file has a serum sample that failed to load.
        self.assertFalse(
            aess.aggregated_errors_dict[
                "study_missing_data.xlsx"
            ].exception_type_exists(AnimalsWithoutSerumSamples)
        )
        # Likewise for the AnimalsWithoutSamples warning
        self.assertFalse(
            aess.aggregated_errors_dict[
                "study_missing_data.xlsx"
            ].exception_type_exists(AnimalsWithoutSamples)
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
            len(
                aess.aggregated_errors_dict["Peak Annotation Samples Check"].exceptions
            ),
        )
        self.assertTrue(
            aess.aggregated_errors_dict[
                "Peak Annotation Samples Check"
            ].exception_type_exists(AllMissingSamples)
        )

    def test_study_loader_create_grouped_exceptions(self):
        file = (
            "DataRepo/data/tests/submission_v3/multitracer_v3/study_missing_data.xlsx"
        )
        stdy_ldr = StudyV3Loader(
            df=read_from_file(file, sheet=None),
            file=file,
        )
        stdy_ldr.missing_sample_record_exceptions = [
            RecordDoesNotExist(Sample, {"name": "s1"})
        ]
        stdy_ldr.missing_compound_record_exceptions = [
            RecordDoesNotExist(Compound, {"name": "titanium"})
        ]
        # Create a RecordDoesNotExist exception for a blank sample that is a warning
        blank_exc = RecordDoesNotExist(Sample, {"name": "blank6"})
        blank_exc.is_error = False
        blank_exc.is_fatal = True
        stdy_ldr.unskipped_blank_record_exceptions = [blank_exc]

        stdy_ldr.create_grouped_exceptions()

        expected_status_keys = set(
            [
                "Studies Check",
                "Samples Check",
                "Peak Annotation Samples Check",
                "Peak Annotation Blanks Check",
                "Tissues Check",
                "Treatments Check",
                "Compounds Check",
                "Peak Groups Check",
                "Contamination Check",
                "study_missing_data.xlsx",
            ]
        )

        self.assertEqual(
            expected_status_keys,
            set(stdy_ldr.load_statuses.statuses.keys()),
        )
        self.assertTrue(
            stdy_ldr.load_statuses.statuses["Samples Check"][
                "aggregated_errors"
            ].exception_type_exists(AllMissingSamples)
        )
        self.assertTrue(
            stdy_ldr.load_statuses.statuses["Compounds Check"][
                "aggregated_errors"
            ].exception_type_exists(AllMissingCompounds)
        )
        self.assertTrue(
            stdy_ldr.load_statuses.statuses["Peak Annotation Blanks Check"][
                "aggregated_errors"
            ].exception_type_exists(AllUnskippedBlanks)
        )

    def test_get_loader_instances(self):
        sl = StudyV3Loader(_validate=True)
        loaders: Dict[str, TableLoader] = sl.get_loader_instances()
        self.assertIsInstance(loaders, dict)
        self.assertEqual(
            set(StudyV3Loader.DataHeaders._fields),
            set(list(loaders.keys()) + ["DEFAULTS", "ERRORS"]),
        )
        # Make sure that the loaders were instantiated using the common args
        self.assertTrue(loaders["STUDY"].validate)
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

    def test_get_loader_classes(self):
        self.assertEqual(
            [
                "StudiesLoader",
                "CompoundsLoader",
                "TracersLoader",
                "InfusatesLoader",
                "ProtocolsLoader",
                "AnimalsLoader",
                "TissuesLoader",
                "SamplesLoader",
                "LCProtocolsLoader",
                "SequencesLoader",
                "MSRunsLoader",
                "PeakAnnotationFilesLoader",
                "PeakGroupConflicts",
            ],
            [lc.__name__ for lc in StudyLoader.get_loader_classes()],
        )

    def test_check_exclude_sheets_valid(self):
        # No problem
        sl = StudyV3Loader()
        sl.exclude_sheets = ["Treatments"]
        sl.check_exclude_sheets()
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))

    def test_check_exclude_sheets_invalid(self):
        # Invalid sheet
        sl = StudyV3Loader()
        sl.exclude_sheets = ["Invalid"]
        sl.check_exclude_sheets()
        self.assertEqual(1, len(sl.aggregated_errors_object.exceptions))
        self.assertEqual(1, sl.aggregated_errors_object.num_errors)
        self.assertIsInstance(sl.aggregated_errors_object.exceptions[0], ValueError)
        self.assertIn("Invalid", str(sl.aggregated_errors_object.exceptions[0]))

    def test_check_exclude_sheets_called_by_constructor(self):
        # Called from the constructor
        sl = StudyV3Loader(exclude_sheets=["Bad"])
        self.assertEqual(1, len(sl.aggregated_errors_object.exceptions))
        self.assertEqual(1, sl.aggregated_errors_object.num_errors)
        self.assertIsInstance(sl.aggregated_errors_object.exceptions[0], ValueError)
        self.assertIn("Bad", str(sl.aggregated_errors_object.exceptions[0]))

    def test_exclude_sheets_excluded_from_load(self):
        file = (
            "DataRepo/data/tests/submission_v3/multitracer_v3/study_missing_data.xlsx"
        )
        sl = StudyV3Loader(
            df=read_from_file(file, sheet=None),
            file=file,
            exclude_sheets=[
                # All but Study sheet
                "Compounds",
                "Tracers",
                "Infusates",
                "Treatments",
                "Animals",
                "Tissues",
                "Samples",
                "LC Protocols",
                "Sequences",
                "Peak Annotation Details",
                "Peak Annotation Files",
                "Peak Group Conflicts",
            ],
        )
        sl.load_data()
        # Errors in the other loaders from the problematic study doc are all skipped
        self.assertEqual(0, len(sl.aggregated_errors_object.exceptions))
        # Only the one study record is loaded
        self.assertDictEqual(
            {
                "Study": {
                    "created": 1,
                    "deleted": 0,
                    "errored": 0,
                    "existed": 0,
                    "skipped": 0,
                    "updated": 0,
                    "warned": 0,
                },
            },
            sl.record_counts,
        )

    def create_some_study_recs(self):
        """Create underlying data for test test_study_loader_stops_when_fatal_study_load_warning_present which loads
        partial study doc DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx
        """
        call_command("loaddata", "lc_methods")
        Compound.objects.create(
            name="gluc",
            formula="C6H12O6",
            hmdb_id="HMDB0000122",
        )
        infobj = parse_infusate_name_with_concs("gluc-[13C6][10]")
        inf, _ = Infusate.objects.get_or_create_infusate(infobj)
        inf.save()
        anml = Animal.objects.create(
            name="test_animal",
            age=timedelta(weeks=int(13)),
            sex="M",
            genotype="WT",
            body_weight=200,
            diet="normal",
            feeding_status="fed",
            infusate=inf,
        )
        tsu = Tissue.objects.create(name="Brain")
        Sample.objects.create(
            name="Sample Name",
            tissue=tsu,
            animal=anml,
            researcher="John Doe",
            date=datetime.now(),
        )

    # This decorator makes the test faster by disabling autoupdates (not needed for the test)
    @MaintainedModel.no_autoupdates()
    def test_study_loader_stops_when_fatal_study_load_warning_present(self):
        """When a loader buffers a FatalStudyLoadWarning exception, StudyLoader should not load subsequent sheets."""
        # Create an StudyV3Loader object
        # Set its df to a dataframe dict with incorrect mzXML paths

        # Create the necessary records for the partial study doc: mzxml_study_doc_same_seq.xlsx
        self.create_some_study_recs()

        file = "DataRepo/data/tests/same_name_mzxmls/mzxml_study_doc_same_seq.xlsx"

        # Create a dataframe dict
        df = read_from_file(file, sheet=None)

        # Modify the peak annotation details sheet to provide a bad mzXML (which should cause FatalStudyLoadWarning to
        # be buffered)
        df["Peak Annotation Details"] = pd.DataFrame.from_dict(
            {
                MSRunsLoader.DataHeaders.SAMPLENAME: [
                    "BAT-xz971",
                    "BAT-xz972",
                ],
                MSRunsLoader.DataHeaders.SAMPLEHEADER: [
                    "BAT_xz971",
                    "BAT_xz971",
                ],
                MSRunsLoader.DataHeaders.MZXMLNAME: [
                    None,
                    "some_unknown_sample.mzXML",  # <-- This should cause a FatalStudyLoadWarning and stop accucor loads
                ],
                MSRunsLoader.DataHeaders.ANNOTNAME: [
                    "accucor1.xlsx",
                    "accucor2.xlsx",
                ],
                MSRunsLoader.DataHeaders.SEQNAME: [
                    "John Doe, polar-HILIC-25-min, QE, 1991-5-7",
                    "John Doe, polar-HILIC-25-min, QE, 1991-5-7",
                ],
                MSRunsLoader.DataHeaders.SKIP: [
                    None,
                    None,
                ],
            }
        )

        study_loader = StudyV3Loader(
            df=df,
            file=file,
            debug=True,
        )

        with self.assertRaises(AggregatedErrorsSet) as ar:
            study_loader.load_data()
        aes = ar.exception

        # We cannot check the DB for no PeakGroups to confirm that the PeakAnnotationFilesLoader did not run, because
        # everything would have been rolled back, so we assert that there's a warning about the skipped
        # PeakAnnotationFilesLoader.  And we assert all the exception types.
        self.assertEqual(
            3,
            len(aes.aggregated_errors_dict["mzxml_study_doc_same_seq.xlsx"].exceptions),
        )
        self.assertIsInstance(
            aes.aggregated_errors_dict["mzxml_study_doc_same_seq.xlsx"].exceptions[0],
            FileFromInputNotFound,
        )
        self.assertIsInstance(
            aes.aggregated_errors_dict["mzxml_study_doc_same_seq.xlsx"].exceptions[1],
            FatalStudyLoadWarning,
        )
        self.assertIsInstance(
            aes.aggregated_errors_dict["mzxml_study_doc_same_seq.xlsx"].exceptions[2],
            FatalStudyLoadWarning,
        )
        self.assertIn(
            "Skipping loader PeakAnnotationFilesLoader",
            str(
                aes.aggregated_errors_dict["mzxml_study_doc_same_seq.xlsx"].exceptions[
                    2
                ]
            ),
        )
        self.assertEqual(1, len(aes.aggregated_errors_dict))

    def test_no_samples_no_serum_warnings(self):
        file = "DataRepo/data/tests/animal_without_samples/study.xlsx"

        # Load prerequisite data - entire study containing animal with no
        call_command(
            "load_study",
            infile="DataRepo/data/tests/no_serum_samples/study.xlsx",
        )

        # Create a loader instance
        sl = StudyV3Loader(
            df=read_from_file(file, sheet=None),
            file=file,
            # _validate=True causes raise of MultiLoadStatus when there is a warning, of which there should be 2
            _validate=True,
        )

        # It should raise, since _validate is True
        with self.assertRaises(MultiLoadStatus) as ar:
            sl.load_data()

        self.assertIn("study.xlsx", ar.exception.statuses.keys())
        study_doc_aes: AggregatedErrors = ar.exception.statuses["study.xlsx"][
            "aggregated_errors"
        ]
        # There should be 1 exception (AnimalsWithoutSerumSamples) that came from the SamplesLoader, evidenced by the
        # fact that it is under the study doc load key
        self.assertEqual(1, len(study_doc_aes.exceptions))
        self.assertIsInstance(study_doc_aes.exceptions[0], AnimalsWithoutSerumSamples)
        # The exception should be a warning
        self.assertFalse(study_doc_aes.exceptions[0].is_error)
        # And it should contain all of the animals' names
        self.assertEqual(["xz971"], study_doc_aes.exceptions[0].animals)

        self.assertIn("Animals Check", ar.exception.statuses.keys())
        animal_check_aes: AggregatedErrors = ar.exception.statuses["Animals Check"][
            "aggregated_errors"
        ]
        # There should be 1 exception (AnimalsWithoutSamples).  The serum exception should not happen, because it should
        # be filtered by the presence of the samples sheet exception.
        self.assertEqual(1, len(animal_check_aes.exceptions))
        self.assertIsInstance(animal_check_aes.exceptions[0], AnimalsWithoutSamples)
        # The exception should be a warning
        self.assertFalse(animal_check_aes.exceptions[0].is_error)
        # And it should contain all of the animals' names
        self.assertEqual(["xz972"], animal_check_aes.exceptions[0].animals)


class StudyV3LoaderTests(TracebaseTestCase):
    def test_convert_df_v3(self):
        # TODO: Implement test
        pass


class StudyV2LoaderTests(TracebaseTestCase):
    def test_convert_df_v2(self):
        # TODO: Implement test
        pass
