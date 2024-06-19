# import pandas as pd

# from DataRepo.loaders.study_loader import StudyLoader
# from DataRepo.models import (
#     Animal,
#     ArchiveFile,
#     Compound,
#     Infusate,
#     LCMethod,
#     MSRunSample,
#     MSRunSequence,
#     PeakData,
#     PeakDataLabel,
#     PeakGroup,
#     PeakGroupLabel,
#     Sample,
#     Tissue,
# )
from DataRepo.tests.tracebase_test_case import TracebaseTestCase

# PeakGroupCompound = PeakGroup.compounds.through


class StudyLoaderTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    def test_study_loader_constructor(self):
        # TODO: Implement test
        pass

    def test_study_loader_load_data_success(self):
        # TODO: Implement test
        pass

    def test_study_loader_get_class_dtypes(self):
        # TODO: Implement test
        pass

    def test_study_loader_get_sheet_names_tuple(self):
        # TODO: Implement test
        pass

    def test_study_loader_package_group_exceptions(self):
        # TODO: Implement test
        pass

    def test_study_loader_create_grouped_exceptions(self):
        # TODO: Implement test
        pass
