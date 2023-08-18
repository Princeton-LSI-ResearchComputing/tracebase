from datetime import timedelta

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.test import override_settings, tag

from DataRepo.models import LCMethod
from DataRepo.tests.tracebase_test_case import TracebaseTestCase

override_settings(CACHES=settings.TEST_CACHES)


@tag("lcmethod")
class LCMethodTests(TracebaseTestCase):
    def setUp(self):
        super().setUp()

        self.default_t_type = "HILIC"
        self.default_t_description = "Description"
        self.default_t_minutes = 25
        self.default_t_run_length = timedelta(minutes=self.default_t_minutes)
        self.default_t_name = f"{self.default_t_type}-{self.default_t_run_length}"
        self.unknown_string = "unknown test"

        self.setup_lcmethod = LCMethod.objects.create(
            name=self.default_t_name,
            type=self.default_t_type,
            description=self.default_t_description,
            run_length=self.default_t_run_length,
        )

    def test_valid_prior(self):
        """Tests retrieval of pre-existing LCMethod"""
        _, created = LCMethod.objects.get_or_create(
            name=self.default_t_name,
            type=self.default_t_type,
            description=self.default_t_description,
            run_length=self.default_t_run_length,
        )

        # this unique record was already created during setup
        self.assertFalse(created)

    def test_null_run_length(self):
        """Tests insert and string of a poorly defined LCMethod"""
        unknown_method = LCMethod.objects.create(
            name=self.unknown_string,
            type=self.unknown_string,
            description="This is a poorly defined LCMethod",
        )
        self.assertEqual(str(unknown_method), self.unknown_string)

    def test_null_description_create(self):
        """Tests insert of an invalid methodology; description required"""
        with self.assertRaisesRegexp(IntegrityError, "lcmethod_description_not_empty"):
            LCMethod.objects.create(name=self.unknown_string, type=self.unknown_string)

    def test_null_description_full_clean(self):
        """Tests insert of an invalid methodology; description required"""
        bad = LCMethod(name=self.unknown_string, type=self.unknown_string)
        with self.assertRaisesRegexp(ValidationError, "description.* cannot be blank"):
            bad.full_clean()

    def test_null_name_create(self):
        """Tests insert of an invalid methodology; name required"""
        with self.assertRaisesRegexp(IntegrityError, "lcmethod_name_not_empty"):
            LCMethod.objects.create(
                description=self.unknown_string, type=self.unknown_string
            )

    def test_null_name_full_clean(self):
        """Tests insert of an invalid methodology; name required"""
        bad = LCMethod(description=self.unknown_string, type=self.unknown_string)
        with self.assertRaisesRegexp(ValidationError, "name.* cannot be blank"):
            bad.full_clean()

    def test_null_type_create(self):
        """Tests insert of an invalid methodology; type required"""
        with self.assertRaisesRegexp(IntegrityError, "lcmethod_type_not_empty"):
            LCMethod.objects.create(
                description=self.unknown_string, name=self.unknown_string
            )

    def test_null_type_full_clean(self):
        """Tests insert of an invalid methodology; type required"""
        bad = LCMethod(description=self.unknown_string, name=self.unknown_string)
        with self.assertRaisesRegexp(ValidationError, "type.* cannot be blank"):
            bad.full_clean()

    def test_lcmethods_name_unique(self):
        """Tests LCMethod Unique constraint"""
        with self.assertRaisesRegexp(IntegrityError, "DataRepo_lcmethod_name_key"):
            LCMethod.objects.create(
                name=self.default_t_name,
                type=self.default_t_type,
                description=self.default_t_description + "different",
                run_length=self.default_t_run_length,
            )

    def test_lcmethods_description_unique(self):
        """Tests LCMethod Unique constraint"""
        with self.assertRaisesRegexp(
            IntegrityError, "DataRepo_lcmethod_description_key"
        ):
            LCMethod.objects.create(
                name=self.default_t_name + "different",
                type=self.default_t_type,
                description=self.default_t_description,
                run_length=self.default_t_run_length,
            )

    def test_instance_string(self):
        self.assertEqual(
            str(self.setup_lcmethod), f"HILIC-0:{self.default_t_minutes}:00"
        )
