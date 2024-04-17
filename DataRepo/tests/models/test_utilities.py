from datetime import datetime, timedelta

from django.apps import apps
from django.core.management import call_command
from django.db import IntegrityError
from django.db.models import Q
from django.forms import ValidationError, model_to_dict

from DataRepo.models import Animal, LCMethod, MSRunSequence, Study
from DataRepo.models.utilities import (
    check_for_inconsistencies,
    dereference_field,
    get_all_models,
    get_enumerated_fields,
    get_model_by_name,
    get_non_auto_model_fields,
    get_unique_constraint_fields,
    get_unique_fields,
    handle_load_db_errors,
    update_rec,
)
from DataRepo.tests.tracebase_test_case import TracebaseTransactionTestCase
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    InfileDatabaseError,
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

    # TODO: When the SampleTableLoader inherits from TableLoader, remove this test already copied to loader.py
    def test_get_unique_constraint_fields(self):
        mdl_name = "MSRunSample"
        model = get_model_by_name(mdl_name)
        unique_field_sets = get_unique_constraint_fields(model)
        self.assertEqual(2, len(unique_field_sets))
        self.assertEqual(
            ["msrun_sequence", "sample", Q(**{"ms_data_file__isnull": True})],
            unique_field_sets[0],
        )
        self.assertEqual(["ms_data_file"], unique_field_sets[1])

    # TODO: When the SampleTableLoader inherits from TableLoader, remove this test already copied to loader.py
    def test_get_non_auto_model_fields(self):
        expected = [
            "labels",
            "samples",
            "name",
            "infusate",
            "infusion_rate",
            "genotype",
            "body_weight",
            "age",
            "sex",
            "diet",
            "feeding_status",
            "treatment",
            "last_serum_sample",
            "studies",
        ]
        field_names = [
            f.name if hasattr(f, "name") else f.field_name
            for f in get_non_auto_model_fields(Animal)
        ]
        self.assertEqual(expected, field_names)

    # TODO: When the SampleTableLoader inherits from TableLoader, remove this test already copied to loader.py
    def test_get_enumerated_fields(self):
        field_names = get_enumerated_fields(Animal)
        self.assertEqual(["sex"], field_names)

    # TODO: When the SampleTableLoader inherits from TableLoader, remove this test already copied to loader.py
    def test_get_unique_fields(self):
        field_names = get_unique_fields(Animal)
        self.assertEqual(["name"], field_names)

    # TODO: When the SampleTableLoader inherits from TableLoader, remove this test already copied to loader.py
    def test_check_for_inconsistencies(self):
        call_command(
            "load_study_table",
            infile="DataRepo/data/tests/small_obob/small_obob_study.xlsx",
        )
        rec = Study.objects.first()
        rec_dict = {
            "code": "obf",
            "name": "ob/ob Fasted",
            "description": "Inconsistent description",
        }
        incs = check_for_inconsistencies(rec, rec_dict)
        self.assertEqual(1, len(incs))
        self.assertEqual(ConflictingValueError, type(incs[0]))
        self.assertEqual(rec, incs[0].rec)
        self.assertIn("description", incs[0].differences.keys())
        self.assertEqual(
            "Inconsistent description", incs[0].differences["description"]["new"]
        )
        self.assertEqual(
            "ob/ob and wildtype littermates were fasted 7 hours and infused with tracers",
            incs[0].differences["description"]["orig"],
        )

    # TODO: When the SampleTableLoader inherits from TableLoader, remove this test already copied to loader.py
    def test_handle_load_db_errors_integrityerror(self):
        """
        Tests handle_load_db_errors's handling of unique constraint violations (i.e. IntegrityErrors).
        """
        call_command(
            "load_study_table",
            infile="DataRepo/data/tests/small_obob/small_obob_study.xlsx",
        )
        conflicts = []
        rec_dict = {
            "code": "obf",
            "name": "ob/ob Fasted",
            "description": "Inconsistent description",
        }
        try:
            Study.objects.create(**rec_dict)
        except Exception as e:
            handle_load_db_errors(
                e,
                Study,
                rec_dict,
                conflicts_list=conflicts,
            )
        self.assertEqual(1, len(conflicts))
        self.assertEqual(ConflictingValueError, type(conflicts[0]))
        self.assertEqual(Study.objects.first(), conflicts[0].rec)
        self.assertIn("description", conflicts[0].differences.keys())
        self.assertEqual(
            "Inconsistent description", conflicts[0].differences["description"]["new"]
        )
        self.assertEqual(
            "ob/ob and wildtype littermates were fasted 7 hours and infused with tracers",
            conflicts[0].differences["description"]["orig"],
        )

    # TODO: When the SampleTableLoader inherits from TableLoader, remove this test already copied to loader.py
    def test_handle_load_db_errors_otherintegrityerror(self):
        """
        Tests handle_load_db_errors's handling of other IntegrityErrors.
        """
        rec_dict = {
            "code": "obf",
            "name": "ob/ob Fasted",
            "description": "Inconsistent description",
        }
        aes = AggregatedErrors()
        try:
            raise IntegrityError("Some other error")
        except Exception as e:
            handle_load_db_errors(
                e,
                Study,
                rec_dict,
                aes=aes,
            )
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(InfileDatabaseError, type(aes.exceptions[0]))

    # TODO: When the SampleTableLoader inherits from TableLoader, remove this test already copied to loader.py
    def test_handle_load_db_errors_validationerror(self):
        """
        Tests handle_load_db_errors's handling of ValidationErrors raised by invalid enumeration choices.
        """
        rec_dict = {
            "researcher": "George",
            "date": datetime.now(),
            "instrument": "invalid",
            "lc_method": LCMethod.objects.get(name__exact="polar-HILIC-25-min"),
        }
        aes = AggregatedErrors()
        try:
            rec = MSRunSequence(**rec_dict)
            rec.full_clean()
            rec.save()
        except Exception as e:
            if not handle_load_db_errors(
                e,
                MSRunSequence,
                rec_dict,
                aes=aes,
            ):
                raise e
        self.assertEqual(1, len(aes.exceptions))
        self.assertEqual(InfileDatabaseError, type(aes.exceptions[0]))

    # TODO: When the SampleTableLoader inherits from TableLoader, remove this test already copied to loader.py
    def test_handle_load_db_errors_unsupportederror(self):
        """
        Tests handle_load_db_errors's handling of unsupported error types (should raise)
        """
        rec_dict = {
            "name": "anything",
        }
        aes = AggregatedErrors()
        with self.assertRaises(ValueError):
            try:
                raise ValueError("Not supported")
            except Exception as e:
                if not handle_load_db_errors(
                    e,
                    Animal,
                    rec_dict,
                    aes=aes,
                ):
                    raise e
