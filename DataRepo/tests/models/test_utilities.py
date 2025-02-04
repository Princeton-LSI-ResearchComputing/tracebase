from datetime import timedelta

from django.apps import apps
from django.forms import ValidationError, model_to_dict

from DataRepo.models import LCMethod
from DataRepo.models.utilities import (
    dereference_field,
    get_all_models,
    get_model_by_name,
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
