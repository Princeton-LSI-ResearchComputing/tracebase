# import pandas as pd

# from DataRepo.loaders.tracers_loader import TracersLoader
# from DataRepo.models import Tracer, TracerLabel
from DataRepo.tests.tracebase_test_case import TracebaseTestCase

# from DataRepo.utils.file_utils import read_from_file


class SequencesLoaderTests(TracebaseTestCase):
    fixtures = ["lc_methods.yaml", "data_types.yaml", "data_formats.yaml"]

    # TEST_DF = pd.DataFrame.from_dict(
    #     {
    #         "Sequence Number": [1],
    #         "Operator": ["Xianfeng Zeng"],
    #         "Date": ["2021-10-19 00:00:00"],
    #         "Instrument": ["HILIC"],
    #         "LC Protocol": ["polar-HILIC"],
    #         "LC Run Length": [25],
    #         "LC Description": [""],
    #         "Notes": [""],
    #     },
    # )

    def test_init_load(self):
        # TODO: Implement test
        pass

    def test_load_data(self):
        # TODO: Implement test
        pass

    def test_build_tracer_dict(self):
        # TODO: Implement test
        pass

    def test_load_tracer_dict(self):
        # TODO: Implement test
        pass

    def test_get_row_data(self):
        # TODO: Implement test
        pass

    def test_check_extract_name_data(self):
        # TODO: Implement test
        pass

    def test_get_or_create_tracer(self):
        # TODO: Implement test
        pass

    def test_get_tracer(self):
        # TODO: Implement test
        pass

    def test_get_compound(self):
        # TODO: Implement test
        pass

    def test_create_tracer(self):
        # TODO: Implement test
        pass

    def test_get_or_create_tracer_label(self):
        # TODO: Implement test
        pass

    def test_parse_label_positions(self):
        # TODO: Implement test
        pass

    def test_check_data_is_consistent(self):
        # TODO: Implement test
        pass

    def test_buffer_consistency_issues(self):
        # TODO: Implement test
        pass

    def test_check_tracer_name_consistent(self):
        # TODO: Implement test
        pass
