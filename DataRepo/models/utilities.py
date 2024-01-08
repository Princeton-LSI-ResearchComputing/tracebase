import importlib
import warnings

from chempy import Substance
from chempy.util.periodic import atomic_number
from django.apps import apps
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import IntegrityError
from django.db.models import Q
from django.forms.models import model_to_dict

# Generally, child tables are at the top and parent tables are at the bottom
ALL_MODELS_IN_SAFE_DELETION_ORDER = [
    "Compound",
    "CompoundSynonym",
    "LCMethod",
    "Tissue",
    "PeakDataLabel",
    "PeakData",
    "PeakGroup",
    "PeakGroupLabel",
    "PeakGroupSet",
    "MSRun",
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
    Return the count of supplied atom in the compound.
    Returns None if atom is not a recognized symbol.
    Returns 0 if the atom is recognized, but not found in the compound.
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
    return [
        f
        for f in model._meta.get_fields()
        if f.get_internal_type() != "AutoField" and not getattr(f, "is_relation")
    ]


def get_unique_constraint_fields(model):
    """
    Returns a list of lists of fields involved in UniqueConstraints in a given model.
    """
    uflds = []
    if hasattr(model._meta, "constraints"):
        for constraint in model._meta.constraints:
            if type(constraint).__name__ == "UniqueConstraint":
                uflds.append(constraint.fields)
    return uflds


def get_unique_fields(model, fields=None):
    """
    Returns a list of non-auto-fields where unique is True.

    If fields (list of field names) is provided, the returned fields are limited to the list of field names provided.
    """
    return [
        f
        for f in get_non_auto_model_fields(model)
        if (fields is None or f.name in fields) and hasattr(f, "unique") and f.unique
    ]


def get_enumerated_fields(model, fields=None):
    """
    Returns a list of non-auto-fields where choices is populated.

    If fields (list of field names) is provided, the returned fields are limited to the list of field names provided.
    """
    return [
        f
        for f in get_non_auto_model_fields(model)
        if (fields is None or f.name in fields) and hasattr(f, "choices") and f.choices
    ]


def get_non_auto_model_fields(model):
    """
    Retrieves all non-auto-fields from the supplied model and returns as a list
    """
    return [f for f in model._meta.get_fields() if f.get_internal_type() != "AutoField"]


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


def dereference_field(field_name, model_name):
    mdl = get_model_by_name(model_name)
    fld = getattr(mdl, field_name)
    deref_field = field_name
    # If this is a foreign key (i.e. it's a model reference, not an actual DB field)
    if fld.field.__class__.__name__ == "ForeignKey":
        deref_field += "__pk"
    return deref_field


def get_model_by_name(model_name):
    return apps.get_model("DataRepo", model_name)


def create_is_null_field(field_with_null):
    """
    The Django ORM's order_by always puts null fields at the end, which can be a problem if you want the last record
    ordered by a date field.  This method will return a dict that contains the `select` and `order_by arguments for the
    .extra(method, along with the name of the added "is null field that you can use as an argument to `order_by`)`.

    Example: extra_args, is_null_field = create_is_null_field("msrun_sequence__date")
    MSRunSequence.objects.extra(**extra_args).order_by(f"-{is_null_field}", "msrun_sequence__date")

    Note, adding .annotate() doesn't seem to work, or rather I couldn't get it to work. See:
    https://stackoverflow.com/questions/7749216/django-order-by-date-but-have-none-at-end

    See the Django documentation for `extra()` and note that it is currently deprecated, and will go away in version 5.
    TODO: Attempt again to solve the problem this function works around (see docstring)
    """
    is_null_field_name = f"{field_with_null}_is_null"

    # Build an SQL field name from the ORM field name
    field_with_null_path_list = field_with_null.split("__")
    if len(field_with_null_path_list) > 1:
        field = field_with_null_path_list.pop()
        table = field_with_null_path_list.pop()
        field_with_null_ref = '"DataRepo_' + table + '".'
        field_with_null_ref += f'"{field}"'
    else:
        field_with_null_ref = field_with_null

    select_val = {is_null_field_name: f"{field_with_null_ref} IS NULL"}
    order_by_val = [is_null_field_name, field_with_null]

    return (
        {
            "select": select_val,
            "order_by": order_by_val,
        },
        is_null_field_name,
    )


def model_as_dict(obj):
    return model_to_dict(obj)


def exists_in_db(mdl_obj):
    """
    This takes a model object and returns a boolean to indicate whether the object exists in the database.

    Note, it does not assert that the values of the fields are the same.
    """
    if not hasattr(mdl_obj, "pk"):
        return False
    try:
        type(mdl_obj).objects.get(pk__exact=mdl_obj.pk)
    except Exception as e:
        if issubclass(type(e), ObjectDoesNotExist):
            return False
        raise e
    return True


def check_for_inconsistencies(rec, rec_dict, rownum=None, sheet=None, file=None):
    # This was moved here (from its prior global location at the top) to avoid circular import
    from DataRepo.utils.exceptions import ConflictingValueError

    conflicting_value_errors = []
    for field, new_value in rec_dict.items():
        orig_value = getattr(rec, field)
        if orig_value != new_value:
            conflicting_value_errors.append(
                ConflictingValueError(
                    rec,
                    field,
                    orig_value,
                    new_value,
                    rownum=rownum,
                    sheet=sheet,
                    file=file,
                )
            )
    return conflicting_value_errors


def handle_load_db_errors(
    exception,
    model,
    rec_dict,
    aes=None,
    conflicts_list=None,
    rownum=None,
    sheet=None,
    file=None,
):
    """
    This function evaluates whether the supplied exception is the result of either a field value conflict or a
    validation error (triggered by the clean code that runs from a full_save).  It will either buffer a
    ConflictingValue error in the supplied conflicts_list, buffer a validation error in the supplied
    AggregatedErrors object, or not buffer anything.  It returns a boolean indicating whether an error was handled
    (/buffered).
    """
    # This was moved here (from its prior global location at the top) to avoid circular import
    from DataRepo.utils.exceptions import InfileDatabaseError

    if aes is None and conflicts_list is None:
        raise ValueError(
            "Either aes and/or conflicts_list is required by handle_load_db_errors()."
        )
    estr = str(exception)
    if isinstance(exception, IntegrityError):
        if "duplicate key value violates unique constraint" in estr:
            # Create a list of lists of unique fields and unique combos of fields
            # This first one is forced into a list of lists so that we only need to loop once
            unique_combos = [
                [f] for f in get_unique_fields(model, fields=rec_dict.keys())
            ]
            # Now get the actual unique combos
            unique_combos.extend(get_unique_constraint_fields(model))
            field_set = set(rec_dict.keys())
            for combo_fields in unique_combos:
                combo_set = set(combo_fields)
                # Only proceed if we have all the values
                if not combo_set.issubset(field_set):
                    continue
                # Retrieve the record with the conflicting value(s) that caused the unique constraint error
                q = Q()
                for uf in combo_fields:
                    q &= Q(**{f"{uf}__exact": rec_dict[uf]})
                qs = model.objects.filter(q)
                if qs.count() == 1:
                    rec = qs.first()
                    errs = check_for_inconsistencies(
                        rec, rec_dict, rownum=rownum, sheet=sheet, file=file
                    )
                    if conflicts_list:
                        conflicts_list.extend(errs)
                        return True
                    elif aes:
                        for err in errs:
                            aes.buffer_error(err)
                        return True
        elif aes is not None:
            aes.buffer_error(InfileDatabaseError(exception, rec_dict, rownum=rownum))
            return True
        # Raise Integrity errors that are not handled above
        raise InfileDatabaseError(exception, rec_dict, rownum=rownum)
    elif isinstance(exception, ValidationError):
        if "is not a valid choice" in estr:
            choice_fields = get_enumerated_fields(model, fields=rec_dict.keys())
            for choice_field in choice_fields:
                if choice_field in estr and rec_dict[choice_field] is not None:
                    if aes is not None:
                        # Only include error once
                        if not aes.exception_type_exists():
                            aes.buffer_error(
                                InfileDatabaseError(exception, rec_dict, rownum=rownum)
                            )
                        else:
                            for existing_exc in aes.get_exception_type(
                                InfileDatabaseError
                            ):
                                if existing_exc.rec_dict != rec_dict:
                                    aes.buffer_error(
                                        InfileDatabaseError(
                                            exception, rec_dict, rownum=rownum
                                        )
                                    )
                        # Whether we buffered or not, the error was identified and handled
                        return True
                    raise InfileDatabaseError(exception, rec_dict, rownum=rownum)
    # If we get here, we did not identify the error as one we knew what to do with
    return False
