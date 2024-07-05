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

    def test_create_name(self):
        # Anything is accepted - because its result can be used to create records
        new_lcm_name = LCMethod.create_name(type="some_type", run_length=30)
        self.assertEqual(
            "some_type-30-min",
            new_lcm_name,
            msg="Type, run length, and 'min' are joined with '-'",
        )

        new_lcm_name = LCMethod.create_name(type="some_type")
        self.assertEqual(
            "some_type", new_lcm_name, msg="A 'None' run length results in name=type"
        )

        new_lcm_name = LCMethod.create_name(run_length=10)
        self.assertEqual(
            f"{LCMethod.DEFAULT_TYPE}-10-min",
            new_lcm_name,
            msg="A 'None' type results in the default type",
        )

    def test_get_name(self):
        rec = LCMethod.objects.create(
            name="some-stale-name-30-min",
            type="actual-type",
            description="n/a",
            run_length=timedelta(minutes=25),
        )
        new_lcm_name = rec.get_name()
        self.assertEqual(
            "actual-type-25-min",
            new_lcm_name,
            msg="Name returned should be based on the type and run length field values, not the value in the field.",
        )

    def test_parse_lc_protocol_name(self):
        # TODO: Implement test
        pass
