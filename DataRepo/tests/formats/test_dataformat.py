from django.db.models import F
from django.db.models.functions import Lower, Upper

from DataRepo.formats.dataformat import (
    ConditionallyRequiredArgumentError,
    FieldPathError,
    Format,
    MutuallyExclusiveArgumentsError,
    TypeUnitsMismatch,
    UnknownComparison,
)
from DataRepo.tests.formats.formats_test_base import FormatsTestCase
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class DataformatMainTests(TracebaseTestCase):
    """Test class for DataRepo.formats.dataformat.__main__"""

    def test_conditionally_required_argument_error(self):
        """Test __main__.ConditionallyRequiredArgumentError - no exception = successful test"""
        ConditionallyRequiredArgumentError()

    def test_field_path_error(self):
        """Test __main__.FieldPathError - no exception = successful test"""
        FieldPathError()

    def test_mutually_exclusive_arguments_error(self):
        """Test __main__.MutuallyExclusiveArgumentsError - no exception = successful test"""
        MutuallyExclusiveArgumentsError()

    def test_type_units_mismatch(self):
        """Test __main__.TypeUnitsMismatch"""
        tum = TypeUnitsMismatch("atype")
        self.assertIn("atype", str(tum))

    def test_unknown_comparison(self):
        """Test __main__.UnknownComparison - no exception = successful test"""
        UnknownComparison()

    def test_order_by_field_to_name(self):
        fld = Format.order_by_field_to_name(Upper(Lower(F("testfieldname"))).desc())
        self.assertEqual("testfieldname", fld)


class FormatTests(FormatsTestCase):

    def test_get_all_comparison_choices(self):
        fmt = Format()

        all_ncmp_choices = (
            ("exact", "is"),
            ("not_exact", "is not"),
            ("lt", "<"),
            ("lte", "<="),
            ("gt", ">"),
            ("gte", ">="),
            ("not_isnull", "has a value (ie. is not None)"),
            ("isnull", "does not have a value (ie. is None)"),
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("icontains", "contains"),
            ("not_icontains", "does not contain"),
            ("istartswith", "starts with"),
            ("not_istartswith", "does not start with"),
            ("iendswith", "ends with"),
            ("not_iendswith", "does not end with"),
        )
        self.assertEqual(all_ncmp_choices, fmt.get_all_comparison_choices())
