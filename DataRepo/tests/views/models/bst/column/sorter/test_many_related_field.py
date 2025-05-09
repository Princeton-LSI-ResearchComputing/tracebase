from django.db.models import (
    CASCADE,
    CharField,
    F,
    ForeignKey,
    IntegerField,
    ManyToManyField,
)
from django.db.models.aggregates import Count
from django.db.models.functions import Length, Lower, Upper
from django.test import override_settings

from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    create_test_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.sorter.many_related_field import (
    BSTManyRelatedSorter,
)

BSTMRSManyTestModel = create_test_model(
    "BSTMRSManyTestModel",
    {
        "name": CharField(max_length=255),
        "value": IntegerField(),
    },
)

BSTMRSMiddleTestModel = create_test_model(
    "BSTMRSMiddleTestModel",
    {
        "name": CharField(max_length=255),
        "mms": ManyToManyField(to="loader.BSTMRSManyTestModel", related_name="fms"),
    },
)

BSTMRSChildTestModel = create_test_model(
    "BSTMRSChildTestModel",
    {
        "name": CharField(max_length=255),
        "parent": ForeignKey(
            to="loader.BSTMRSMiddleTestModel",
            related_name="children",
            on_delete=CASCADE,
        ),
    },
)


@override_settings(DEBUG=True)
class BSTManyRelatedSorterTests(TracebaseTestCase):
    def test_not_many_related(self):
        with self.assertRaises(ValueError) as ar:
            BSTManyRelatedSorter(
                BSTMRSManyTestModel.name.field,  # pylint: disable=no-member
                BSTMRSManyTestModel,
                asc=True,
            )
        self.assertIn(
            "field_path 'name' must be many-related with the model 'BSTMRSManyTestModel'",
            str(ar.exception),
        )

    def test_init_str_char_asc(self):
        s = BSTManyRelatedSorter("mms__name", BSTMRSMiddleTestModel, asc=True)
        self.assertEqual("Min(Lower(F(mms__name)))", str(s.expression))

    def test_init_str_char_desc(self):
        s = BSTManyRelatedSorter("mms__name", BSTMRSMiddleTestModel, asc=False)
        self.assertEqual("Max(Lower(F(mms__name)))", str(s.expression))

    def test_init_str_int_asc(self):
        s = BSTManyRelatedSorter("mms__value", BSTMRSMiddleTestModel, asc=True)
        self.assertEqual("Min(F(mms__value))", str(s.expression))

    def test_init_str_int_desc(self):
        s = BSTManyRelatedSorter("mms__value", BSTMRSMiddleTestModel, asc=False)
        self.assertEqual("Max(F(mms__value))", str(s.expression))

    def test_init_lower_char_asc(self):
        s = BSTManyRelatedSorter(Lower("mms__name"), BSTMRSMiddleTestModel, asc=True)
        self.assertEqual("Min(Lower(F(mms__name)))", str(s.expression))

    def test_init_lower_char_desc(self):
        s = BSTManyRelatedSorter(Lower("mms__name"), BSTMRSMiddleTestModel, asc=False)
        self.assertEqual("Max(Lower(F(mms__name)))", str(s.expression))

    def test_init_f_int_asc(self):
        s = BSTManyRelatedSorter(F("mms__value"), BSTMRSMiddleTestModel, asc=True)
        self.assertEqual("Min(F(mms__value))", str(s.expression))

    def test_init_f_int_desc(self):
        s = BSTManyRelatedSorter(F("mms__value"), BSTMRSMiddleTestModel, asc=False)
        self.assertEqual("Max(F(mms__value))", str(s.expression))

    def test_init_upper_char_asc(self):
        with self.assertWarns(DeveloperWarning) as aw:
            s = BSTManyRelatedSorter(
                Upper("mms__name"), BSTMRSMiddleTestModel, asc=True
            )
        self.assertEqual("Min(Upper(F(mms__name)))", str(s.expression))
        self.assertEqual(1, len(aw.warnings))
        self.assertIn("no output_field set", str(aw.warnings[0].message))

    def test_init_upper_char_desc(self):
        with self.assertWarns(DeveloperWarning) as aw:
            s = BSTManyRelatedSorter(
                Upper("mms__name"), BSTMRSMiddleTestModel, asc=False
            )
        self.assertEqual("Max(Upper(F(mms__name)))", str(s.expression))
        self.assertEqual(1, len(aw.warnings))
        self.assertIn("no output_field set", str(aw.warnings[0].message))

    def test_init_length_char_asc(self):
        # Length() has a default output_field, so no warning is generated:
        # In [2]: l = Length("name")
        # In [4]: l.output_field
        # Out[4]: <django.db.models.fields.IntegerField>
        s = BSTManyRelatedSorter(Length("mms__name"), BSTMRSMiddleTestModel, asc=True)
        self.assertEqual("Min(Length(F(mms__name)))", str(s.expression))

    def test_init_length_char_desc(self):
        # Length() has a default output_field, so no warning is generated:
        s = BSTManyRelatedSorter(Length("mms__name"), BSTMRSMiddleTestModel, asc=False)
        self.assertEqual("Max(Length(F(mms__name)))", str(s.expression))

    def test_init_count_char_asc(self):
        with self.assertWarns(DeveloperWarning) as aw:
            s = BSTManyRelatedSorter(
                Count("mms__name", distinct=True), BSTMRSMiddleTestModel, asc=True
            )
        self.assertEqual("Count(F(mms__name), distinct=True)", str(s.expression))
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Unable to apply aggregate function 'Min'", str(aw.warnings[0].message)
        )
        self.assertIn("sorter for column 'mms__name'", str(aw.warnings[0].message))
        self.assertIn(
            "already has an aggregate function 'Count", str(aw.warnings[0].message)
        )
        self.assertIn(
            "In order for the delimited values to be sorted",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "row sort to be based on either the first or last delimited value",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "must not already be wrapped in an aggregate", str(aw.warnings[0].message)
        )
        self.assertIn(
            "Sorting on this column will not base row position on the min/max",
            str(aw.warnings[0].message),
        )
        self.assertIn(
            "sort of the delimited values will be static and appear unordered",
            str(aw.warnings[0].message),
        )
        self.assertIn("use BSTAnnotColumn", str(aw.warnings[0].message))

    def test_init_count_char_desc(self):
        with self.assertWarns(DeveloperWarning) as aw:
            s = BSTManyRelatedSorter(
                Count("mms__name", distinct=True), BSTMRSMiddleTestModel, asc=False
            )
        self.assertEqual("Count(F(mms__name), distinct=True)", str(s.expression))
        self.assertEqual(1, len(aw.warnings))
        self.assertIn(
            "Unable to apply aggregate function 'Max'", str(aw.warnings[0].message)
        )

    def test_init_reverse_relation(self):
        s = BSTManyRelatedSorter(F("children__name"), BSTMRSMiddleTestModel, asc=False)
        self.assertEqual("Max(Lower(F(children__name)))", str(s.expression))

    @TracebaseTestCase.assertNotWarns()
    def test_order_by(self):
        self.assertEqual(
            "OrderBy(Max(Lower(F(children__name))), descending=True)",
            str(
                BSTManyRelatedSorter(
                    F("children__name"), BSTMRSMiddleTestModel, asc=False
                ).order_by
            ),
        )

    @TracebaseTestCase.assertNotWarns()
    def test_many_order_by(self):
        s = BSTManyRelatedSorter(F("children__name"), BSTMRSMiddleTestModel, asc=False)
        self.assertEqual(
            "OrderBy(Lower(F(children__name)), descending=True)", str(s.many_order_by)
        )
        s = BSTManyRelatedSorter(
            Upper("children__name", output_field=CharField()), BSTMRSMiddleTestModel
        )
        self.assertEqual(
            "OrderBy(Upper(F(children__name)), descending=False)", str(s.many_order_by)
        )

    def test_get_server_sorter_matching_expression(self):
        # TODO: Implement test
        pass
