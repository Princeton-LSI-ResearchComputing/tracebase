from datetime import timedelta

from django.apps import apps
from django.db import ProgrammingError
from django.db.models import (
    CASCADE,
    CharField,
    Count,
    F,
    Field,
    FloatField,
    ForeignKey,
    Func,
    ManyToManyField,
    Min,
    Value,
)
from django.db.models.functions import Concat, Extract, Lower, Upper
from django.db.models.query_utils import DeferredAttribute
from django.forms import ValidationError, model_to_dict
from django.test import override_settings

from DataRepo.models import (
    Animal,
    ArchiveFile,
    DataFormat,
    LCMethod,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    Study,
    Tracer,
    TracerLabel,
)
from DataRepo.models.utilities import (
    MultipleFields,
    NoFields,
    _get_field_val_by_iteration_helper,
    _get_field_val_by_iteration_manyrelated_helper,
    _get_field_val_by_iteration_onerelated_helper,
    _last_many_rec_iterator,
    _lower,
    _recursive_many_rec_iterator,
    dereference_field,
    field_path_to_field,
    field_path_to_model_path,
    get_all_models,
    get_distinct_fields,
    get_many_related_field_val_by_subquery,
    get_model_by_name,
    get_next_model,
    is_many_related,
    is_many_related_to_parent,
    is_many_related_to_root,
    is_number_field,
    is_related,
    is_string_field,
    is_unique_field,
    model_path_to_model,
    model_title,
    model_title_plural,
    resolve_field,
    resolve_field_path,
    select_representative_field,
    update_rec,
)
from DataRepo.tests.tracebase_test_case import (
    TracebaseTestCase,
    TracebaseTransactionTestCase,
    create_test_model,
)


class ModelUtilitiesTests(TracebaseTransactionTestCase):
    fixtures = ["data_types.yaml", "data_formats.yaml", "lc_methods.yaml"]

    def test_get_all_models(self):
        """Test that we return all models in the right order"""
        all_models = set(apps.get_app_config("DataRepo").get_models())
        test_all_models = set(get_all_models())
        # Test for duplicates
        self.assertEqual(len(test_all_models), len(get_all_models()))
        # Test that the sets contain the same things
        missing_models = all_models - test_all_models
        extra_models = test_all_models - all_models
        self.assertEqual(
            missing_models,
            set(),
            msg="Models returned by DataRepo.models.utilities.get_all_models() are missing these.",
        )
        self.assertEqual(
            extra_models,
            set(),
            msg="Models returned by DataRepo.models.utilities.get_all_models() includes these non-existant models.",
        )

        # Generally, child tables are at the top and parent tables are at the bottom
        ordered_model_name_list = [
            "Compound",
            "CompoundSynonym",
            "LCMethod",
            "Tissue",
            "PeakDataLabel",
            "PeakData",
            "PeakGroup",
            "PeakGroupLabel",
            "MSRunSample",
            "MSRunSequence",
            "ArchiveFile",
            "DataType",
            "DataFormat",
            "FCirc",
            "Sample",
            "Animal",
            "AnimalLabel",
            "TracerLabel",
            "Tracer",
            "Infusate",
            "InfusateTracer",
            "Protocol",
            "Study",
        ]
        self.assertEqual(
            ordered_model_name_list,
            [model.__name__ for model in get_all_models()],
            msg=(
                "Models returned by DataRepo.models.utilities.get_all_models() must be returned in this safe deletion "
                f"order: {', '.join(ordered_model_name_list)}."
            ),
        )

    def test_dereference_field(self):
        fld_input = "peak_group"
        mdl_input = "PeakData"
        expected_field = "peak_group__pk"
        fld_output = dereference_field(fld_input, mdl_input)
        self.assertEqual(expected_field, fld_output)

    def test_get_model_by_name(self):
        mdl_input = "PeakData"
        model_output = get_model_by_name(mdl_input)
        self.assertEqual(model_output.__class__.__name__, "ModelBase")
        self.assertEqual(mdl_input, model_output.__name__)

    def test_update_rec(self):
        lcm = LCMethod.objects.get(name__exact="polar-HILIC-25-min")
        lcm_id = lcm.id
        update_dict = model_to_dict(lcm)
        update_dict["name"] = "polar-HILIC-20-min"
        update_dict["run_length"] = timedelta(minutes=20)
        update_dict["description"] = "Edited description"
        update_rec(lcm, update_dict)
        updated_lcm = LCMethod.objects.get(name__exact="polar-HILIC-20-min")
        self.assertEqual(lcm_id, updated_lcm.id)
        self.assertEqual(timedelta(minutes=20), updated_lcm.run_length)
        self.assertEqual("Edited description", updated_lcm.description)

        # Test that it calls full clean by making it raise a ValidationError
        # The clean method assures that the name matches the type and run length, so change run length and not the name
        update_dict["run_length"] = timedelta(minutes=25)
        with self.assertRaises(ValidationError):
            update_rec(lcm, update_dict)

    def test_resolve_field(self):
        self.assertIsInstance(ArchiveFile.filename, DeferredAttribute)
        self.assertIsInstance(resolve_field(ArchiveFile.filename), Field)

    def test_is_related(self):
        self.assertFalse(is_related("filename", ArchiveFile))
        self.assertTrue(is_related("data_format", ArchiveFile))
        self.assertTrue(is_related("peak_groups", ArchiveFile))
        self.assertTrue(
            is_related("msrun_sample__sample__animal__studies__name", PeakGroup)
        )
        self.assertTrue(is_related("msrun_sample__sample__animal__name", PeakGroup))
        self.assertTrue(is_related("msrun_sample", PeakGroup))
        self.assertFalse(is_related("name", PeakGroup))

    def test_is_many_related(self):
        # "many related" means many to many or many to one
        # ArchiveFile.filename is not a foreign key field, so it can't be many related
        self.assertFalse(
            is_many_related(ArchiveFile.filename.field)  # pylint: disable=no-member
        )
        # The default is based on where the foreign key is defined (i.e. in ArchiveFile), so this is one to many, not
        # many to ...
        self.assertFalse(
            is_many_related(ArchiveFile.data_format.field)  # pylint: disable=no-member
        )
        # From the perspective of DataFormat, it is many-related to ArchiveFile.  ArchiveFile.data_format describes its
        # field as one_to_many, so from the perspective of the DataFormat model, this relationship is a many_to_one.
        self.assertTrue(
            is_many_related(
                ArchiveFile.data_format.field, DataFormat  # pylint: disable=no-member
            )
        )
        # The link from PeakGroup to ArchiveFile is one to many, but this asks if the link from the source model
        # (PeakGroup) to ArchiveFile is many to ...
        self.assertFalse(
            is_many_related(
                ArchiveFile.peak_groups.field, PeakGroup  # pylint: disable=no-member
            )
        )
        # The default is based on where the foreign key is defined (i.e. in PeakGroup)
        self.assertFalse(
            is_many_related(ArchiveFile.peak_groups.field)  # pylint: disable=no-member
        )
        # But if we're asking from the perspective of ArchiveFile, it is many to ...
        self.assertTrue(
            is_many_related(
                ArchiveFile.peak_groups.field, ArchiveFile  # pylint: disable=no-member
            )
        )

    def test_is_many_related_to_root(self):
        self.assertFalse(is_many_related_to_root("filename", ArchiveFile))
        self.assertFalse(is_many_related_to_root("data_format__code", ArchiveFile))
        self.assertTrue(is_many_related_to_root("peak_groups__name", ArchiveFile))
        self.assertTrue(
            is_many_related_to_root(
                "msrun_sample__sample__animal__studies__name", PeakGroup
            )
        )
        self.assertFalse(
            is_many_related_to_root("msrun_sample__sample__animal__name", PeakGroup)
        )
        self.assertFalse(is_many_related_to_root("description", Study))

    def test_is_many_related_to_parent(self):
        self.assertFalse(is_many_related_to_parent("data_format", ArchiveFile))
        self.assertTrue(is_many_related_to_parent("peak_groups", ArchiveFile))
        self.assertTrue(
            is_many_related_to_parent(
                "msrun_sample__sample__animal__studies", PeakGroup
            )
        )
        self.assertFalse(
            is_many_related_to_parent(
                "peak_groups__msrun_sample__sample__animal", ArchiveFile
            )
        )
        # Works when ends in non-relation
        self.assertFalse(
            is_many_related_to_parent(
                "peak_groups__msrun_sample__sample__animal__name", ArchiveFile
            )
        )
        self.assertTrue(
            is_many_related_to_parent(
                "peak_groups__msrun_sample__sample__animal__studies", ArchiveFile
            )
        )
        # Works when ends in non-relation
        self.assertTrue(
            is_many_related_to_parent(
                "peak_groups__msrun_sample__sample__animal__studies__name", ArchiveFile
            )
        )

    def test_field_path_to_field(self):
        ra_field = field_path_to_field(
            ArchiveFile, "peak_groups__peak_data__raw_abundance"
        )
        self.assertIsInstance(ra_field, FloatField)
        self.assertEqual("raw_abundance", ra_field.name)
        # Many to many Foreign Key
        studies_field = field_path_to_field(
            ArchiveFile, "peak_groups__msrun_sample__sample__animal__studies"
        )
        self.assertIsInstance(studies_field, ManyToManyField)
        self.assertEqual("studies", studies_field.name)
        # Many to many field
        study_name_field = field_path_to_field(
            ArchiveFile, "peak_groups__msrun_sample__sample__animal__studies__name"
        )
        self.assertIsInstance(study_name_field, CharField)
        self.assertEqual("name", study_name_field.name)

    def test_get_next_model(self):
        self.assertEqual(PeakGroup, get_next_model(ArchiveFile, "peak_groups"))
        self.assertEqual(Study, get_next_model(Animal, "studies"))

    def test_field_path_to_model_path(self):
        self.assertEqual(
            "peak_groups__peak_data",
            field_path_to_model_path(
                ArchiveFile, "peak_groups__peak_data__raw_abundance"
            ),
        )
        self.assertEqual(
            "peak_groups__peak_data",
            field_path_to_model_path(ArchiveFile, "peak_groups__peak_data"),
        )

    def test_model_path_to_model(self):
        # Reverse relation
        self.assertEqual(
            PeakData, model_path_to_model(ArchiveFile, "peak_groups__peak_data")
        )
        # Mixed Reverse/Forward relations
        self.assertEqual(
            Animal,
            model_path_to_model(
                ArchiveFile, "peak_groups__msrun_sample__sample__animal"
            ),
        )
        # Many to many relation
        self.assertEqual(
            Study,
            model_path_to_model(
                ArchiveFile, "peak_groups__msrun_sample__sample__animal__studies"
            ),
        )

    def test_is_string_field(self):
        self.assertFalse(is_string_field(PeakData.raw_abundance))
        self.assertFalse(
            is_string_field(PeakData.raw_abundance.field)  # pylint: disable=no-member
        )
        self.assertTrue(is_string_field(PeakGroup.name))
        self.assertTrue(
            is_string_field(PeakGroup.name.field)  # pylint: disable=no-member
        )

    def test_is_number_field(self):
        self.assertTrue(is_number_field(PeakData.raw_abundance))
        self.assertTrue(
            is_number_field(PeakData.raw_abundance.field)  # pylint: disable=no-member
        )
        self.assertFalse(is_number_field(PeakGroup.name))
        self.assertFalse(
            is_number_field(PeakGroup.name.field)  # pylint: disable=no-member
        )

    def test_is_unique_field(self):
        self.assertFalse(is_unique_field(ArchiveFile.filename))
        self.assertFalse(
            is_unique_field(ArchiveFile.filename.field)  # pylint: disable=no-member
        )
        self.assertTrue(is_unique_field(ArchiveFile.checksum))
        self.assertTrue(
            is_unique_field(ArchiveFile.checksum.field)  # pylint: disable=no-member
        )

    def test_resolve_field_path(self):
        # Assumed base model = Sample
        self.assertEqual("animal__sex", resolve_field_path("animal__sex"))
        self.assertEqual("animal__sex", resolve_field_path(Lower("animal__sex")))
        self.assertEqual("animal__sex", resolve_field_path(Lower(F("animal__sex"))))
        self.assertEqual("animal__sex", resolve_field_path(Upper(Lower("animal__sex"))))
        self.assertEqual("animal__sex", resolve_field_path(F("animal__sex")))
        self.assertEqual("animal__sex", resolve_field_path(Count(F("animal__sex"))))
        self.assertEqual("animal__sex", resolve_field_path(Min(F("animal__sex"))))
        # Assumed base model = Animal
        self.assertEqual("sex", resolve_field_path(Animal.sex))
        self.assertEqual("sex", resolve_field_path(CharField(name="sex")))
        # Unsupported
        with self.assertRaises(NoFields) as ar:
            resolve_field_path(Value(0))
        self.assertEqual(
            "No field name in field representation.",
            str(ar.exception),
        )
        with self.assertRaises(MultipleFields) as ar:
            resolve_field_path(Concat(F("animal__sex"), F("animal__body_weight")))
        self.assertEqual(
            "Multiple field names in field representation ['animal__sex', 'animal__body_weight'].",
            str(ar.exception),
        )
        with self.assertRaises(ProgrammingError) as ar:
            resolve_field_path(1)
        self.assertEqual(
            "Unexpected field_or_expression type: 'int'.",
            str(ar.exception),
        )

        # Test deeper field_path expression and filtering out expressions without field_paths
        self.assertEqual(
            "time_collected",
            resolve_field_path(
                Extract(
                    F("time_collected"),
                    "epoch",
                )
                / Value(60),
            ),
        )
        self.assertEqual(
            "date",
            resolve_field_path(
                Func(
                    F("date"),
                    Value("YYYY-MM-DD"),
                    output_field=CharField(),
                    function="to_char",
                ),
            ),
        )

    def test_get_distinct_fields_nonkeyfield(self):
        with self.assertRaises(ValueError) as ar:
            self.assertEqual(["name"], get_distinct_fields(Tracer, "name"))
        self.assertEqual(
            "The path provided must have at least 1 foreign key to extract the related model path.",
            str(ar.exception),
        )

    def test_get_distinct_fields_keyfield_with_expression(self):
        # Tracer has 'Lower' in its meta ordering
        self.assertEqual(["tracer__name"], get_distinct_fields(TracerLabel, "tracer"))

    def test_get_distinct_fields_recursive_keyfield_with_negation(self):
        # PeakData has "-corrected_abundance" - ensure the "-" is stripped
        # PeakDataLabel links to PeakData, which links to PeakGroup via their orderings
        self.assertEqual(
            ["peak_data__peak_group__name", "peak_data__corrected_abundance"],
            get_distinct_fields(PeakDataLabel, "peak_data"),
        )

    def test_select_representative_field(self):
        self.assertEqual("name", select_representative_field(Animal))
        self.assertEqual("checksum", select_representative_field(ArchiveFile))
        self.assertEqual("code", select_representative_field(DataFormat))
        self.assertEqual("name", select_representative_field(LCMethod))
        self.assertEqual("name", select_representative_field(PeakGroup))
        self.assertEqual("name", select_representative_field(Study))
        self.assertEqual("name", select_representative_field(Tracer))

        # No suitable field in these models (no single order-by and no unique that are not key fields whose values are
        # not guaranteed from load-to-load)
        self.assertIsNone(select_representative_field(PeakData))
        self.assertIsNone(select_representative_field(PeakDataLabel))
        self.assertIsNone(select_representative_field(TracerLabel))

        # When there's no suitable field, you can force it...
        self.assertEqual(
            "raw_abundance", select_representative_field(PeakData, force=True)
        )
        self.assertEqual(
            "element", select_representative_field(PeakDataLabel, force=True)
        )
        self.assertEqual("name", select_representative_field(TracerLabel, force=True))

        # Test when a subset is supplied to select from - and no ideal choice, uses first non-relation, non-id of
        # supplied order
        self.assertEqual(
            "med_mz",
            select_representative_field(
                PeakData,
                force=True,
                subset=[
                    "id",
                    "peak_group",
                    "med_mz",
                    "corrected_abundance",
                    "med_rt",
                ],
            ),
        )

    def test_model_title(self):
        self.assertEqual("Peak Data Label", model_title(PeakDataLabel))

    def test_model_title_plural(self):
        self.assertEqual("Peak Data Labels", model_title_plural(PeakDataLabel))


MUQStudyTestModel = create_test_model(
    "MUQStudyTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": [Lower("name").desc()]},
        ),
    },
)

MUQInfusateTestModel = create_test_model(
    "MUQInfusateTestModel",
    {"name": CharField(max_length=255, unique=True)},
)

MUQTracerTestModel = create_test_model(
    "MUQTracerTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "infusate": ForeignKey(
            to="loader.MUQInfusateTestModel",
            related_name="tracers",
            on_delete=CASCADE,
        ),
    },
)

MUQAnimalTestModel = create_test_model(
    "MUQAnimalTestModel",
    {
        "name": CharField(max_length=255, unique=True),
        "desc": CharField(max_length=255),
        "studies": ManyToManyField(
            to="loader.MUQStudyTestModel", related_name="animals"
        ),
        "treatment": ForeignKey(
            to="loader.MUQTreatmentTestModel",
            related_name="animals",
            on_delete=CASCADE,
        ),
        "infusate": ForeignKey(
            to="loader.MUQInfusateTestModel",
            null=True,
            related_name="infused_animals",
            on_delete=CASCADE,
        ),
    },
    attrs={
        "Meta": type(
            "Meta",
            (),
            {"app_label": "loader", "ordering": ["-name"]},
        ),
    },
)

MUQTreatmentTestModel = create_test_model(
    "MUQTreatmentTestModel",
    {"name": CharField(unique=True), "desc": CharField()},
)


@override_settings(DEBUG=True)
class ModelUtilityQueryTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        cls.t1 = MUQTreatmentTestModel.objects.create(name="T1", desc="t1")
        cls.t2 = MUQTreatmentTestModel.objects.create(name="oddball", desc="t2")
        cls.s1 = MUQStudyTestModel.objects.create(name="S1", desc="s1")
        cls.s2 = MUQStudyTestModel.objects.create(name="S2", desc="s2")
        cls.a1 = MUQAnimalTestModel.objects.create(
            name="A1", desc="a1", treatment=cls.t1
        )
        cls.a1.studies.add(cls.s1)
        cls.a2 = MUQAnimalTestModel.objects.create(
            name="A2", desc="a2", treatment=cls.t2
        )
        cls.a2.studies.add(cls.s1)
        cls.a2.studies.add(cls.s2)
        super().setUpTestData()

    @TracebaseTestCase.assertNotWarns()
    def test_get_field_val_by_iteration_helper_basic(self):
        with self.assertNumQueries(0):
            # NOTE: Not sure yet why this performs no queries
            val, sval, id = _get_field_val_by_iteration_helper(
                self.a1,
                ["treatment"],
                sort_field_path=["treatment", "name"],
            )
        self.assertEqual(self.t1, val)
        # Whether this is lower-cased or not, it does not matter.  The sort value and unique value are only present to
        # be compatible with the many-related companion recursive path.  It might not even be necessary, since I split
        # up the methods... so I should look into the possibility of remove it.
        self.assertEqual("T1", sval)
        self.assertIsInstance(id, int)

        with self.assertNumQueries(1):
            vals = _get_field_val_by_iteration_helper(
                self.a2,
                ["studies"],
                related_limit=2,
                sort_field_path=["studies", "name"],
            )
        expected1 = set([self.s2, self.s1])
        expected2 = set(["s1", "s2"])
        self.assertIsInstance(vals, list)
        self.assertIsInstance(vals[0], tuple)
        self.assertIsInstance(vals[1], tuple)
        self.assertEqual(2, len(vals))
        # Tests above assures that the unsubscriptable-object errors from pylint are false positives
        vals1 = set([v[0] for v in vals])  # pylint: disable=unsubscriptable-object
        vals2 = set([v[1] for v in vals])  # pylint: disable=unsubscriptable-object
        vals3 = set([v[2] for v in vals])  # pylint: disable=unsubscriptable-object
        self.assertEqual(expected1, vals1)
        self.assertEqual(expected2, vals2)
        self.assertTrue(all(isinstance(v3, int) for v3 in vals3))

    @TracebaseTestCase.assertNotWarns()
    def test_get_field_val_by_iteration_helper_manycheck(self):
        """This test ensures that an empty list instead of None is returned when the field path doesn't make it to the
        many-related portion of the path before hitting a None value.  E.g. Animal's field path:
        infusate__tracers__name should return an empty list instead of None when no animal has an infusate.
        """
        vals = _get_field_val_by_iteration_helper(
            self.a2,
            ["infusate", "tracers", "name"],
            related_limit=2,
            sort_field_path=["name"],
        )
        self.assertEqual([], vals)

    @TracebaseTestCase.assertNotWarns()
    def test__get_field_val_by_iteration_onerelated_helper(self):
        with self.assertNumQueries(0):
            # NOTE: Not sure yet why this performs no queries
            val, sval, id = _get_field_val_by_iteration_onerelated_helper(
                self.a1,
                ["treatment"],
                sort_field_path=["treatment", "name"],
            )
        self.assertEqual(self.t1, val)
        # Whether this is lower-cased or not, it does not matter.  The sort value and unique value are only present to
        # be compatible with the many-related companion recursive path.  It might not even be necessary, since I split
        # up the methods... so I should look into the possibility of remove it.
        self.assertEqual("T1", sval)
        self.assertIsInstance(id, int)

    @TracebaseTestCase.assertNotWarns()
    def test__get_field_val_by_iteration_manyrelated_helper(self):
        with self.assertNumQueries(1):
            vals = _get_field_val_by_iteration_manyrelated_helper(
                self.a2,
                ["studies"],
                related_limit=2,
                sort_field_path=["studies", "name"],
            )
        expected2 = set(["s1", "s2"])
        expected1 = set([self.s2, self.s1])
        vals1 = set([v[0] for v in vals])
        vals2 = set([v[1] for v in vals])
        vals3 = set([v[2] for v in vals])
        self.assertEqual(expected2, vals2)
        self.assertEqual(expected1, vals1)
        self.assertTrue(all(isinstance(v3, int) for v3 in vals3))

    @TracebaseTestCase.assertNotWarns()
    def test__last_many_rec_iterator(self):
        mr_qs = MUQStudyTestModel.objects.all()
        iterator = iter(_last_many_rec_iterator(mr_qs, ["name"]))
        expected1 = set([self.s2, self.s1])
        # The names are lower-cased
        expected2 = set(["s1", "s2"])
        with self.assertNumQueries(1):
            val1 = next(iterator)
        with self.assertNumQueries(0):
            # NOTE: I don't understand yet why this performs no query
            val2 = next(iterator)
        vals1 = set([val1[0], val2[0]])
        vals2 = set([val1[1], val2[1]])
        vals3 = set([val1[2], val2[2]])
        self.assertEqual(expected1, vals1)
        self.assertEqual(expected2, vals2)
        self.assertTrue(all(isinstance(v3, int) for v3 in vals3))
        with self.assertRaises(StopIteration):
            next(iterator)

    @TracebaseTestCase.assertNotWarns()
    def test__recursive_many_rec_iterator(self):
        mr_qs = MUQStudyTestModel.objects.all()
        iterator = iter(
            _recursive_many_rec_iterator(mr_qs, ["name"], ["name"], 2, None)
        )
        expected = set([("S2", "S2", "S2"), ("S1", "S1", "S1")])
        with self.assertNumQueries(1):
            r1 = next(iterator)
        with self.assertNumQueries(0):
            # NOTE: I don't understand yet why this performs no query
            r2 = next(iterator)
        vals = set([r1, r2])
        self.assertEqual(expected, vals)
        with self.assertRaises(StopIteration):
            next(iterator)

    @TracebaseTestCase.assertNotWarns()
    def test_get_many_related_field_val_by_subquery(self):
        """This test is the same as test_get_many_related_rec_val_by_subquery, only it adds the count keyword arg."""
        qs = MUQAnimalTestModel.objects.all()
        rec = qs.first()

        val = get_many_related_field_val_by_subquery(
            rec,
            "studies__name",
            related_limit=2,
            annotations={"studies_name_bstcellsort": Lower("studies__name")},
            order_bys=[F("studies_name_bstcellsort").asc(nulls_first=True)],
            distincts=["studies_name_bstcellsort"],
        )

        self.assertEqual(["S1", "S2"], val)

    @TracebaseTestCase.assertNotWarns()
    def test__lower(self):
        self.assertEqual("test string", _lower("Test String"))
        self.assertEqual(5, _lower(5))
        self.assertIsNone(_lower(None))
