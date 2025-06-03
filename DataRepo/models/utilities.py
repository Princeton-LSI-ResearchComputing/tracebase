import importlib
import warnings
from typing import List, Optional, Type, Union

from chempy import Substance
from chempy.util.periodic import atomic_number
from django.apps import apps
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import ProgrammingError
from django.db.models import Expression, F, Field, Model
from django.db.models.expressions import Combinable
from django.db.models.fields.related_descriptors import (
    ForwardManyToOneDescriptor,
    ManyToManyDescriptor,
    ReverseManyToOneDescriptor,
)
from django.db.models.query_utils import DeferredAttribute
from django.urls import resolve

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


def resolve_field(
    field: Union[
        Field,
        DeferredAttribute,
        ForwardManyToOneDescriptor,
        ManyToManyDescriptor,
        ReverseManyToOneDescriptor,
    ]
) -> Field:
    """A field at the end of a model path can be a deferred attribute or any of a series of 4 descriptors.  This method
    takes the field and returns the actual field (if it is deferred or a descriptor, otherwise, the supplied field).
    """
    field_containers = [
        DeferredAttribute,
        ForwardManyToOneDescriptor,
        ManyToManyDescriptor,
        ReverseManyToOneDescriptor,
    ]
    return (
        field.field if any(isinstance(field, fc) for fc in field_containers) else field
    )


def resolve_field_path(
    field_or_expression: Union[str, Combinable, DeferredAttribute, Field]
) -> str:
    """Takes a representation of a field, e.g. from Model._meta.ordering, which can have transform functions applied
    (like Lower('field_name')) and returns the field path.

    Examples:
        Assumed base model = Sample
            resolve_field_path("animal__sex")  # Outputs: "animal__sex"
            resolve_field_path(Lower("animal__sex"))  # Outputs: "animal__sex"
            resolve_field_path(F("animal__sex"))  # Outputs: "animal__sex"
        Assumed base model = Animal
            resolve_field_path(Animal.sex)  # Outputs: "sex"
            resolve_field_path(CharField(name="sex"))  # Outputs: "sex"
        Unsupported:
            resolve_field_path(CONCAT(F("animal__sex"), F("animal__body_weight")))  # ERROR
    Assumptions:
        1. In the case of field_or_expression being a Field, the "field path" returned assumes that the context of the
            field path is the immediate model that the Field belongs to.
    Limitations:
        1. Only supports a single source field path.
    Args:
        field_or_expression (Union[str, Combinable]): A str (e.g. a field path), Combinable (e.g. an F, Transform [like
            Lower("name")], Expression, etc object), or a Model Field.
    Exceptions:
        ValueError when the field_or_expression contains multiple field paths.
    Returns
        field_path (str): The field path that either was the field_or_expression or its wrapper
    """
    if isinstance(field_or_expression, str):
        return (
            field_or_expression
            if not field_or_expression.startswith("-")
            else field_or_expression[1:]
        )
    elif isinstance(field_or_expression, Field):
        return field_or_expression.name
    elif isinstance(field_or_expression, DeferredAttribute):
        return field_or_expression.field.name
    elif isinstance(field_or_expression, Expression):
        field_reps = field_or_expression.get_source_expressions()

        if len(field_reps) == 0:
            raise NoFields("No field name in field representation.")
        elif len(field_reps) > 1:
            raise MultipleFields(
                f"Multiple field names in field representation {[f.name for f in field_reps]}."
            )

        if isinstance(field_reps[0], Expression):
            return resolve_field_path(field_reps[0])
        elif isinstance(field_reps[0], F):
            return field_reps[0].name
        else:
            raise ProgrammingError(
                f"Unexpected field_or_expression type: '{type(field_or_expression).__name__}'."
            )
    elif isinstance(field_or_expression, F):
        return field_or_expression.name
    else:
        raise ProgrammingError(
            f"Unexpected field_or_expression type: '{type(field_or_expression).__name__}'."
        )


def is_many_related(field: Field, source_model: Optional[Model] = None):
    """Takes a field (and optional source model) and returns whether that field is many-related relative to the source
    model, (which is assumed to the the model where the field was defined, if source_model is None).
    """
    return (
        field.many_to_many
        or (
            source_model is not None
            and (
                (source_model == field.related_model and field.many_to_one)
                or (source_model != field.related_model and field.one_to_many)
            )
        )
        or (source_model is None and field.one_to_many)
    )


def is_many_related_to_root(field_path: Union[str, List[str]], source_model: Model):
    """Takes a field path and source model and returns whether the leaf field is many-related relative to the first
    source model.

    Args:
        field_path (Union[str, List[str]]): A dunderscore delimited field path (or list).
        source_model (Model): The model that the first field in the field path belongs to.
    Exceptions:
        ValueError when the field path is invalid
    Returns:
        (bool): Whether the leaf field in the field path is many-related with the root model.
    """
    if len(field_path) == 0:
        raise ValueError("field_path string/list must have a non-zero length.")
    if isinstance(field_path, str):
        return is_many_related_to_root(field_path.split("__"), source_model)
    field = resolve_field(getattr(source_model, field_path[0]))
    if is_many_related(field, source_model=source_model):
        return True
    if len(field_path) == 1:
        return False
    return is_many_related_to_root(
        field_path[1:], get_next_model(source_model, field_path[0])
    )


def field_path_to_field(model: Type[Model], path: Union[str, List[str]]) -> Field:
    """Recursive method to take a root model and a dunderscore-delimited path and return the Field class at the end of
    the path.  The intention is so that the Field can be interrogated as to type or retrieve choices, etc.
    """
    if model is None:
        raise ValueError("model must not be None.")
    if path is None or len(path) == 0:
        raise ValueError("path string/list must have a non-zero length.")
    if isinstance(path, str):
        return field_path_to_field(model, path.split("__"))
    if len(path) == 1:
        if hasattr(model, path[0]):
            return resolve_field(getattr(model, path[0]))
        raise AttributeError(
            f"Model: {model.__name__} does not have a field attribute named: '{path[0]}'."
        )
    return field_path_to_field(get_next_model(model, path[0]), path[1:])


def get_next_model(current_model: Model, field_name: str) -> Type[Model]:
    """Given a current model and a foreign key field name from a field path, return the model associated with the
    field.
    """
    field = resolve_field(getattr(current_model, field_name))
    return field.model if current_model != field.model else field.related_model


def field_path_to_model_path(
    model: Type[Model], path: Union[str, List[str]], _output: str = ""
) -> Optional[str]:
    """Recursive method to take a root model and a dunderscore-delimited path and return the path to the model at the
    end of the path (excluding the field).  The utility here is to be able to supply all related models to
    prefetch_related."""
    if len(path) == 0:
        raise ValueError("path string/list must have a non-zero length.")
    if isinstance(path, str):
        return field_path_to_model_path(model, path.split("__"))
    new_output = path[0] if _output == "" else f"{_output}__{path[0]}"
    if len(path) == 1:
        if hasattr(model, path[0]):
            tail = resolve_field(getattr(model, path[0]))
            if tail.is_relation:
                return new_output
            else:
                return _output if _output != "" else None
        raise ValueError(
            f"Model: '{model.__name__}' does not have a field attribute named: '{path[0]}'."
        )
    return field_path_to_model_path(
        get_next_model(model, path[0]), path[1:], new_output
    )


def model_path_to_model(model: Type[Model], path: Union[str, List[str]]) -> Type[Model]:
    """Recursive method to take a root model and a dunderscore-delimited path and return the model class at the end of
    the path."""
    if len(path) == 0:
        raise ValueError("path string/list must have a non-zero length.")
    if isinstance(path, str):
        return model_path_to_model(model, path.split("__"))
    if len(path) == 1:
        if hasattr(model, path[0]):
            return get_next_model(model, path[0])
        raise ValueError(
            f"Model: '{model.__name__}' does not have a field attribute named: '{path[0]}'."
        )
    return model_path_to_model(get_next_model(model, path[0]), path[1:])


def get_distinct_fields(model: Type[Model], field_path: str):
    """Collects all of the order-by fields associated with self.field_path.  For non-foreign-key fields, it just
    returns a single-member list containing self.field_path.  Otherwise, it returns all of the fields in the related
    model's Meta.ordering.  It calls a recursive helper method in case any related model's meta ordering also contains a
    foreign key field.

    Args:
        model (Type[Model])
        field_path (str)
    Exceptions:
        None
    Returns:
        distinct_fields (List[str]): A list of field_paths from the provided field_path that are either just the
            field_path supplied or a series of field_paths from the related model's ordering, if the field at the end of
            the path is a foreign key.
    """
    return _get_distinct_fields_helper(model, field_path)


def _get_distinct_fields_helper(model: Type[Model], field_path: str):
    """Collects all of the order-by fields associated with field_path.  For non-foreign-key fields, it just returns
    a single-member list containing field_path.  Otherwise, it returns all of the fields in the related model's
    Meta.ordering.  It recursively retrieves any other related model's meta ordering if the current ordering also
    contains a foreign key field.

    Args:
        model (Type[Model])
        field_path (str)
    Exceptions:
        None
    Returns:
        distinct_fields (List[str]): A list of field_paths from the provided field_path that are either just the
            field_path supplied or a series of field_paths from the related model's ordering, if the field at the end of
            the path is a foreign key.
    """
    related_model_path = field_path_to_model_path(model, field_path)
    distinct_fields = []
    if field_path != related_model_path:
        distinct_fields.append(field_path)
    else:
        related_model = model_path_to_model(model, related_model_path)
        # To use .distinct(), you need the ordering fields from the related model, otherwise you get an exception
        # about the order_by and distinct fields being different
        for obf_exp in related_model._meta.ordering:
            obf = resolve_field_path(obf_exp)
            obf_path = f"{field_path}__{obf}"
            obf_related_model_path = field_path_to_model_path(model, obf_path)
            if obf_path != obf_related_model_path:
                distinct_fields.append(obf_path)
            else:
                distinct_fields.extend(_get_distinct_fields_helper(model, obf_path))
    return distinct_fields


def is_string_field(
    field: Optional[
        Union[
            Field,
            DeferredAttribute,
            ForwardManyToOneDescriptor,
            ManyToManyDescriptor,
            ReverseManyToOneDescriptor,
        ]
    ],
    default: bool = False,
) -> bool:
    str_field_names = [
        "CharField",
        "EmailField",
        "FilePathField",
        "GenericIPAddressField",
        "TextField",
        "URLField",
        "SlugField",
        "UUIDField",
    ]
    if field is not None:
        field = resolve_field(field)
        return field.__class__.__name__ in str_field_names
    return default


def is_number_field(
    field: Optional[
        Union[
            Field,
            DeferredAttribute,
            ForwardManyToOneDescriptor,
            ManyToManyDescriptor,
            ReverseManyToOneDescriptor,
        ]
    ],
    default: bool = False,
) -> bool:
    num_field_names = [
        "AutoField",
        "BigAutoField",
        "BigIntegerField",
        "DecimalField",
        "FloatField",
        "IntegerField",
        "PositiveBigIntegerField",
        "PositiveIntegerField",
        "PositiveSmallIntegerField",
        "SmallAutoField",
        "SmallIntegerField",
    ]
    if field is not None:
        field = resolve_field(field)
        return field.__class__.__name__ in num_field_names
    return default


def is_unique_field(
    field: Optional[
        Union[
            Field,
            DeferredAttribute,
            ForwardManyToOneDescriptor,
            ManyToManyDescriptor,
            ReverseManyToOneDescriptor,
        ]
    ],
) -> bool:
    if field is not None:
        field = resolve_field(field)
        return field.unique
    return False


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


def update_rec(rec: Model, rec_dict: dict):
    """Update the supplied model record using the fields and values in the supplied rec_dict.
    This could be accomplished using a queryset.update() call, but if it changes a field that was used in the original
    query, and that query no longer matches, you cannot iterate through the records of the queryset to save the changes
    you've made, thus the need for this method.
    Args:
        rec (Model)
        rec_dict (dict): field values keyed on field name
    Exceptions:
        None
    Returns:
        None
    """
    for fld, val in rec_dict.items():
        setattr(rec, fld, val)
    rec.full_clean()
    rec.save()


def get_detail_url_name(model_object: Model):
    return resolve(model_object.get_absolute_url()).url_name


class NoFields(Exception):
    pass


class MultipleFields(Exception):
    pass
