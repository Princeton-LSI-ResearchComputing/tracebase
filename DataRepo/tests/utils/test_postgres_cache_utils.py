import datetime

from django.core.management import call_command

from DataRepo.models import MaintainedModel, PeakGroup
from DataRepo.models.hier_cached_model import (
    delete_all_caches,
    enable_caching_retrievals,
    enable_caching_updates,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.postgres_cache_utils import (
    dump_cache_table_keys,
    get_cache_table_size,
)


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


class PostgresCacheUtilsTests(TracebaseTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    @classmethod
    def setUpTestData(cls):
        load_data()
        super().setUpTestData()

    def test_get_cache_table_size(self):
        enable_caching_retrievals()
        enable_caching_updates()

        # The cache table should already be empty, but just in case
        delete_all_caches()

        # Assert that we're starting empty
        self.assertEqual(0, get_cache_table_size())

        pgl = PeakGroup.objects.first().labels.first()
        f = "enrichment_fraction"

        # Trigger caching via decorator.  This builds a few values because they are used in the calculation.
        getattr(pgl, f)

        # Calling PeakGroupLabel.enrichment_fraction also sets Animal.tracers, hence the 2
        self.assertEqual(2, get_cache_table_size())

    def test_dump_cache_table_keys(self):
        call_command("build_cached_fields")
        expected_cache_settings = {
            "default": {
                "BACKEND": "django.core.cache.backends.db.DatabaseCache",
                "KEY_PREFIX": "PROD",
                "LOCATION": "tracebase_cache_table",
                "OPTIONS": {"MAX_ENTRIES": 1500000},
                "TIMEOUT": None,
            }
        }
        cache_settings, cache_data = dump_cache_table_keys()
        self.assertEqual(expected_cache_settings, cache_settings)
        self.assertEqual(779, len(cache_data))
        self.assertIn("PROD:1:Animal.", cache_data[0][0])
        self.assertIn(".tracers", cache_data[0][0])
        self.assertEqual(
            datetime.datetime(9999, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc),
            cache_data[0][1],
        )
