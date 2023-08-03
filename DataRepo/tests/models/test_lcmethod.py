from datetime import timedelta

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.test import override_settings, tag

from DataRepo.models import LCMethod
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


@override_settings(CACHES=settings.TEST_CACHES)
@tag("lcmethod")
class LCMethodTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()

        self.default_t_chromatographic_technique = "HILIC"
        self.default_t_description = "Description"
        self.default_t_minutes = 25
        self.default_t_run_length = timedelta(minutes=self.default_t_minutes)

        self.setup_lcmethod = LCMethod.objects.create(
            chromatographic_technique=self.default_t_chromatographic_technique,
            description=self.default_t_description,
            run_length=self.default_t_run_length,
        )

    def test_valid_prior(self):
        """Tests retrieval of pre-existing LCMethod"""
        _, created = LCMethod.objects.get_or_create(
            chromatographic_technique=self.default_t_chromatographic_technique,
            description=self.default_t_description,
            run_length=self.default_t_run_length,
        )

        # this unique record was already created during setup
        self.assertFalse(created)

    def test_null_run_length(self):
        """Tests insert and string of poorly defined LCMethod"""
        unknown_method = LCMethod.objects.create(
            chromatographic_technique="unknown",
            description="This is a poorly defined LCMethod",
        )
        self.assertEqual(str(unknown_method), "unknown")

    def test_null_description(self):
        """Tests insert of an invalid methodology; description required"""
        with self.assertRaisesRegexp(IntegrityError, "lcmethod_description_not_empty"):
            _ = LCMethod.objects.create(chromatographic_technique="unknown")

    def test_null_description_again(self):
        """Tests insert of an invalid methodology; description required"""
        bad = LCMethod(chromatographic_technique="unknown")
        with self.assertRaisesRegexp(ValidationError, "field cannot be blank"):
            bad.full_clean()

    def test_lcmethods_record_unique(self):
        """Tests LCMethod Unique constraint"""
        with self.assertRaisesRegexp(IntegrityError, "lcmethod_record_unique"):
            _ = LCMethod.objects.create(
                chromatographic_technique=self.default_t_chromatographic_technique,
                description=self.default_t_description,
                run_length=self.default_t_run_length,
            )

    def test_instance_string(self):
        self.assertEqual(
            str(self.setup_lcmethod), f"HILIC-0:{self.default_t_minutes}:00"
        )

    def test_null_run_length_instance_string(self):
        """Tests insert of an invalid methodology; description required"""
        ct = "unknown"
        test = LCMethod(
            chromatographic_technique=ct,
            description="Unknown methodology.",
        )
        self.assertEqual(str(test), ct)
