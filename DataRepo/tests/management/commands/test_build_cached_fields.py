from django.core.management import call_command

from DataRepo.models import MaintainedModel
from DataRepo.models.hier_cached_model import (
    delete_all_caches,
    disable_caching_updates,
    dump_cache_table_keys,
    enable_caching_retrievals,
    enable_caching_updates,
    get_cache_table_size,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@MaintainedModel.no_autoupdates()
def load_minimum_data():
    disable_caching_updates()
    call_command(
        "load_study",
        infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_no_newsample.xlsx",
        exclude_sheets=["Peak Annotation Files"],
    )
    call_command(
        "load_study",
        infile="DataRepo/data/tests/small_obob/small_obob_animal_and_sample_table_no_newsample_2ndstudy.xlsx",
        exclude_sheets=["Peak Annotation Files"],
    )
    call_command(
        "load_peak_annotations",
        infile="DataRepo/data/tests/small_obob/small_obob_maven_6eaas_inf.xlsx",
        lc_protocol_name="polar-HILIC-25-min",
        instrument="unknown",
        date="2021-06-03",
        operator="Michael Neinast",
    )
    call_command(
        "load_peak_annotations",
        infile=(
            "DataRepo/data/tests/small_obob/small_obob_maven_6eaas_serum/"
            "small_obob_maven_6eaas_serum.xlsx"
        ),
        lc_protocol_name="polar-HILIC-25-min",
        instrument="unknown",
        date="2021-06-03",
        operator="Michael Neinast",
    )


class BuildCachedFieldsTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    def test_build_cached_fields(self):
        load_minimum_data()

        enable_caching_retrievals()
        enable_caching_updates()
        delete_all_caches()

        self.assertEqual(
            0,
            get_cache_table_size(),
            msg=f"Cache Table Settings & Content:\n{dump_cache_table_keys()}",
        )

        call_command("build_cached_fields")

        self.assertEqual(
            779,
            get_cache_table_size(),
            msg=f"Cache Table Settings & Content:\n{dump_cache_table_keys()}",
        )
