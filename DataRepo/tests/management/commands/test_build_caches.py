from django.core.management import call_command

from DataRepo.management.commands.build_caches import cached_function_call
from DataRepo.models import Animal, MaintainedModel
from DataRepo.models.hier_cached_model import (
    delete_all_caches,
    disable_caching_retrievals,
    enable_caching_retrievals,
    enable_caching_updates,
    get_cache,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@MaintainedModel.no_autoupdates()
def load_data():
    load_minimum_data()
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


@MaintainedModel.no_autoupdates()
def load_minimum_data():
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


class BuildCachesTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        load_minimum_data()
        super().setUpTestData()

    def test_cached_function_call(self):
        c = Animal
        f = "tracers"
        a = Animal.objects.all().first()
        la = Animal.objects.all().last()
        disable_caching_retrievals()
        # Get the first and last uncached value
        uv = getattr(a, f)
        lv = getattr(la, f)

        enable_caching_retrievals()
        enable_caching_updates()
        delete_all_caches()

        # Call cached_function_call to populate all cached values for f
        cached_function_call(c, f)

        # Try to retrieve those cached values
        v, s = get_cache(a, f)
        lv, ls = get_cache(la, f)

        # Ensure the value was cached for both the first and last record
        # Results are querysets, which never equate, but are equatable as lists
        self.assertEqual(list(v), list(uv))
        self.assertTrue(s)
        self.assertEqual(list(lv), list(uv))
        self.assertTrue(ls)
