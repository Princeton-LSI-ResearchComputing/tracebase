import importlib
import warnings

import pandas as pd
from chempy import Substance
from chempy.util.periodic import atomic_number
from django.conf import settings
from django.core.exceptions import ValidationError

ALL_MODELS_IN_SAFE_DELETION_ORDER = [
    "Compound",
    "CompoundSynonym",
    "Tissue",
    "PeakData",
    "PeakGroup",
    "PeakGroupSet",
    "MSRun",
    "Sample",
    "Animal",
    "Protocol",
    "Study",
]


def value_from_choices_label(label, choices):
    """
    Return the choices value for a given label
    """
    # Search choices by label
    result = None
    for choices_value, choices_label in choices:
        if label == choices_label:
            result = choices_value
    # If search by label failed, check if we already have a valid value
    if result is None:
        if label in dict(choices):
            result = label
    # If we didn't fine anything, but something was provided it's invalid
    if label is not None and result is None:
        raise ValidationError(
            f"'{label}' is not a valid selection, must be one of {choices}"
        )
    return result


def atom_count_in_formula(formula, atom):
    """
    Return the number of specified atom in the compound.
    Returns None if atom is not a recognized symbol
    Returns 0 if the atom is recognized, but not found in the compound
    """
    substance = Substance.from_formula(formula)
    try:
        count = substance.composition.get(atomic_number(atom))
    except (ValueError, AttributeError):
        warnings.warn(f"{atom} not found in list of elements")
        count = None
    else:
        if count is None:
            # Valid atom, but not in formula
            count = 0
    return count


def get_all_models():
    """
    Retrieves all models (that were explicitly defined, i.e. no hidden related models) from DataRepo and returns them
    as a list ordered in a way that they can all have their contents deleted without running afould of "restrict"
    constraints
    """
    module = importlib.import_module("DataRepo.models")
    mdls = [
        getattr(module, class_name) for class_name in ALL_MODELS_IN_SAFE_DELETION_ORDER
    ]
    return mdls


def get_model_fields(model):
    """
    Retrieves all non-auto- and non-relation- fields from the supplied model and returns as a list
    """
    return list(
        filter(
            lambda x: x.get_internal_type() != "AutoField"
            and not getattr(x, "is_relation"),
            model._meta.get_fields(),
        )
    )


def get_all_fields_named(target_field):
    """
    Dynamically retrieves all fields from any model with a specific name
    """
    models = get_all_models()
    found_fields = []
    for model in models:
        fields = list(get_model_fields(model))
        for field in fields:
            if field.name == target_field:
                found_fields.append([model, field])
    return found_fields


def get_researchers(database=settings.TRACEBASE_DB):
    """
    Get a list of distinct researcher names that is the union of values in researcher fields from any model
    """
    target_field = "researcher"
    researchers = []
    # Get researcher names from any model containing a "researcher" field
    fields = get_all_fields_named(target_field)
    for field_info in fields:
        model = field_info[0]
        researchers += list(
            map(
                lambda x: x[target_field],
                model.objects.using(database).values(target_field).distinct(),
            )
        )
    unique_researchers = list(pd.unique(researchers))
    return unique_researchers
