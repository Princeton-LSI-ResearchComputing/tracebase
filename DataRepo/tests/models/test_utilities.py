from datetime import timedelta

from django.apps import apps
from django.db.models import CharField, Field, FloatField, ManyToManyField
from django.db.models.query_utils import DeferredAttribute
from django.forms import ValidationError, model_to_dict

from DataRepo.models import ArchiveFile, LCMethod
from DataRepo.models.animal import Animal
from DataRepo.models.archive_file import DataFormat
from DataRepo.models.peak_data import PeakData
from DataRepo.models.peak_group import PeakGroup
from DataRepo.models.study import Study
from DataRepo.models.utilities import (
    dereference_field,
    field_path_to_field,
    field_path_to_model_path,
    get_all_models,
    get_model_by_name,
    get_next_model,
    is_many_related,
    is_many_related_to_root,
    is_number_field,
    is_string_field,
    is_unique_field,
    model_path_to_model,
    resolve_field,
    update_rec,
)
from DataRepo.tests.tracebase_test_case import TracebaseTransactionTestCase


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

    def test_is_many_related(self):
        # "many related" means many to many or many to one
        # ArchiveFile.filename is not a foreign key field, so it can't be many related
        self.assertFalse(is_many_related(ArchiveFile.filename.field))
        # The default is based on where the foreign key is defined (i.e. in ArchiveFile), so this is one to many, not
        # many to ...
        self.assertFalse(is_many_related(ArchiveFile.data_format.field))
        # From the perspective of DataFormat, it is a many to ... relationship
        self.assertTrue(is_many_related(ArchiveFile.data_format.field, DataFormat))
        # The link from PeakGroup to ArchiveFile is one to many, but this asks if the link from the source model
        # (PeakGroup) to ArchiveFile is many to ...
        self.assertFalse(is_many_related(ArchiveFile.peak_groups.field, PeakGroup))
        # The default is based on where the foreign key is defined (i.e. in PeakGroup)
        self.assertFalse(is_many_related(ArchiveFile.peak_groups.field))
        # But if we're asking from the perspective of ArchiveFile, it is many to ...
        self.assertTrue(is_many_related(ArchiveFile.peak_groups.field, ArchiveFile))

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
        self.assertFalse(is_string_field(PeakData.raw_abundance.field))
        self.assertTrue(is_string_field(PeakGroup.name))
        self.assertTrue(is_string_field(PeakGroup.name.field))

    def test_is_number_field(self):
        self.assertTrue(is_number_field(PeakData.raw_abundance))
        self.assertTrue(is_number_field(PeakData.raw_abundance.field))
        self.assertFalse(is_number_field(PeakGroup.name))
        self.assertFalse(is_number_field(PeakGroup.name.field))

    def test_is_unique_field(self):
        self.assertFalse(is_unique_field(ArchiveFile.filename))
        self.assertFalse(is_unique_field(ArchiveFile.filename.field))
        self.assertTrue(is_unique_field(ArchiveFile.checksum))
        self.assertTrue(is_unique_field(ArchiveFile.checksum.field))
