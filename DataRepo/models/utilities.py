import importlib
from typing import List, Optional, Type, Union
from warnings import warn

from chempy import Substance
from chempy.util.periodic import atomic_number
from django.apps import apps
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import ProgrammingError
from django.db.models import Expression, F, Field, Model, QuerySet
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
    """Return the choices value for a given label"""
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
        warn(f"{atom} not found in list of elements")
        count = None
    else:
        if count is None:
            # Valid atom, but not in formula
            count = 0
    return count


def get_all_models():
    """Retrieves all models (that were explicitly defined, i.e. no hidden related models) from DataRepo and returns them
    as a list ordered in a way that they can all have their contents deleted without running afould of "restrict"
    constraints"""
    module = importlib.import_module("DataRepo.models")
    mdls = [
        getattr(module, class_name) for class_name in ALL_MODELS_IN_SAFE_DELETION_ORDER
    ]
    return mdls


def get_model_fields(model: Type[Model]):
    """Retrieves all non-auto- and non-relation- fields from the supplied model and returns as a list"""
    return [
        f
        for f in model._meta.get_fields()
        if f.get_internal_type() != "AutoField" and not getattr(f, "is_relation")
    ]


def get_all_fields_named(target_field):
    """Dynamically retrieves all fields from any model with a specific name"""
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

    Args:
        field (Union[Field, DeferredAttribute, ForwardManyToOneDescriptor, ManyToManyDescriptor,
            ReverseManyToOneDescriptor]): A representation of a Field.
    Exceptions:
        None
    Returns:
        (Field): A Field instance, resolved from one of a number of representations of a Field.
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
            return resolve_field_path(field_reps[0].name)
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


def is_related(field_path: str, model: Type[Model]):
    """Takes a field path and root model and returns whether the first field in the path is a foreign key.

    Args:
        field_path (Union[str, List[str]]): A dunderscore delimited field path.
        model (Model): The model that the first field in the field path belongs to.
    Exceptions:
        None
    Returns:
        (bool): Whether the first field in the field path is a foreign key.
    """
    first_field_name = field_path.split("__")
    first_field = field_path_to_field(model, first_field_name)
    return "__" in field_path or first_field.is_relation


def is_many_related(field: Field, source_model: Optional[Model] = None):
    """Takes a field (and optional source model) and returns whether that field is many-related relative to the source
    model.

    Example:
        is_many_related(ArchiveFile.data_format.field, DataFormat) -> True (many to one)
        # ArchiveFile.data_format describes its field as a one_to_many relationship with DataFormat, so from the
        # perspective of the DataFormat model, the relationship from DataFormat to ArchiveFile is many_to_one.
        is_many_related(ArchiveFile.data_format.field, ArchiveFile) -> False (one to many)
        is_many_related(Animal.studies.field) -> True (many to many)
    Assumptions:
        1. source_model is assumed to be the model where the field was defined, if source_model is None.
    Args:
        field (Field): A Field instance.
        source_model (Optional[Model]): The model where field is being accessed.
            NOTE: A related field has 2 associated models, one of which may not be many-related with the other that is,
            so the result depends on the source_model from which you are accessing the field.
    Exceptions:
        None
    Returns:
        (bool): Whether the field is many-related from the perspective of source_model.
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


def is_many_related_to_root(
    field_path: Union[str, List[str]], source_model: Type[Model]
):
    """Takes a field path and source model and returns whether the leaf field is many-related relative to the first
    source model.

    Args:
        field_path (Union[str, List[str]]): A dunderscore delimited field path (or list).
        source_model (Type[Model]): The model that the first field in the field path belongs to.
    Exceptions:
        ValueError when the field path is invalid.
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


def is_many_related_to_parent(
    field_path: Union[str, List[str]], source_model: Type[Model]
):
    """Takes a field path and source model and returns whether the leaf field is many-related to its immediate parent
    model.

    Args:
        field_path (Union[str, List[str]]): A dunderscore delimited field path (or list)
        source_model (Type[Model]): The model that the first field in the field path belongs to
    Exceptions:
        ValueError when the field path is invalid
    Returns:
        (bool): Whether the leaf field in the field path is many-related with its parent model.
    """
    if len(field_path) == 0:
        raise ValueError("field_path string/list must have a non-zero length.")
    if isinstance(field_path, str):
        return is_many_related_to_parent(field_path.split("__"), source_model)
    if len(field_path) == 1:
        field = resolve_field(getattr(source_model, field_path[0]))
        return is_many_related(field, source_model=source_model)
    elif len(field_path) == 2:
        field = resolve_field(getattr(source_model, field_path[0]))
        next_model = get_next_model(source_model, field_path[0])
        # if the last field in the field_path is not a foreign key
        if not resolve_field(getattr(next_model, field_path[1])).is_relation:
            # Return whether the last foreign key is many-related to its parent
            return is_many_related(field, source_model=source_model)
    return is_many_related_to_parent(
        field_path[1:], get_next_model(source_model, field_path[0])
    )


def field_path_to_field(model: Type[Model], path: Union[str, List[str]]) -> Field:
    """Recursive method to take a root model and a dunderscore-delimited path and return the Field class at the end of
    the path.  The intention is so that the Field can be interrogated as to type or retrieve choices, etc.

    Args:
        model (Type[Model]): Model class at the root of the path.
        path (Union[str, List[str]]): Dunderscore-delimited field path string or list of dunderscore-split fields.
    Exceptions:
        ValueError when an argument is invalid.
        AttributeError when any field in the path is not present on the associated model.
    Returns:
        (Field): A Field instance for the field at the end of the path
    """
    if model is None:
        raise ValueError("model must not be None.")
    if path is None or len(path) == 0:
        raise ValueError("path string/list must have a non-zero length.")
    if isinstance(path, str):
        return field_path_to_field(model, path.split("__"))
    if len(path) == 1:
        name = resolve_field_path(path[0])
        if hasattr(model, name):
            return resolve_field(getattr(model, name))
        raise AttributeError(
            f"Model: {model.__name__} does not have a field attribute named: '{name}'."
        )
    return field_path_to_field(get_next_model(model, path[0]), path[1:])


def get_next_model(current_model: Model, field_name: str) -> Type[Model]:
    """Given a current model and a foreign key field name from a field path, return the model associated with the
    field.

    Args:
        current_model (Model): The model referencing field_name.  In a field path, this is the model associated with the
            foreign key just before the supplied field_name.
        field_name (str): A field name accessible from a model.  This can be a forward or reverse relation, which is why
            current_model is necessary, because the field has a 'model' attribute that is the model where the field is
            defined, and a 'related_model' attribute which is the model the field references.  The supplied field name
            however can be either of those models.  current_model thus indicates the direction and the returned model is
            the 'other' one.
    Exceptions:
        None
    Returns:
        (Type[Model]): The model class that does not match the current_model, i.e. the "next" model.
    """
    field = resolve_field(getattr(current_model, field_name))
    return field.model if current_model != field.model else field.related_model


def field_path_to_model_path(
    model: Type[Model],
    path: Union[str, List[str]],
    many_related: bool = False,
    _output: str = "",
    _mr_output: str = "",
) -> str:
    """Recursive method to take a root model and a dunderscore-delimited path and return the path to the last foreign
    key (treated here as "a model", because a queryset constructed using this field path results in a model object (or
    related manager, if it is many-related)).  If the last element in the field path is not a foreign key, it is
    ignored, but if it *is* a foreign key, it is retained.  This can be used to later infer if the field_path ends in a
    foreign key/model or not.  The original intention of this method is its utility in being able to supply all related
    models to prefetch_related.

    Args:
        model (Type[Model]): The Django Model class upon which the path can be used in filtering, etc.
        path (Union[str, List[str]]): A dunderscore-delimited lookup string (or list of the dunderscore-split) field
            path, which can be used in filtering, etc. off the model.
        many_related (bool) [False]: Return the path to the last many-related model in the supplied path, instead of the
            last model.
        _output (str) [""]: Used in recursion to build up the resulting model path.
        _mr_output (str) [""]: Used in recursion to build up the resulting many-related model path (only used if
            many_related is True).
    Exceptions:
        ValueError if the field path is too short, a model is missing a field in the field path, or if a many-related
            model was requested and none was found.
    Returns:
        _output (str): The path to the last foreign key ("model") in the supplied path or None if there are no foreign
            keys in the path (i.e. it's just a field name).
    """
    if len(path) == 0:
        raise ValueError("path string/list must have a non-zero length.")
    if isinstance(path, str):
        return field_path_to_model_path(
            model, path.split("__"), many_related=many_related
        )

    new_output = path[0] if _output == "" else f"{_output}__{path[0]}"

    # If we only want the last many-related model, update _mr_output
    if many_related:
        if hasattr(model, path[0]):
            fld = resolve_field(getattr(model, path[0]))
            if fld.is_relation and is_many_related(fld, model):
                _mr_output = new_output
        else:
            raise ValueError(
                f"Model: '{model.__name__}' does not have a field attribute named: '{path[0]}'."
            )

    # If we're at the end of the path - no more recursion - return the result
    if len(path) == 1:
        if many_related:
            if _mr_output == "":
                raise ValueError(
                    f"No many-related model was found in the path '{new_output}'."
                )
            return _mr_output
        elif hasattr(model, path[0]):
            tail = resolve_field(getattr(model, path[0]))
            if tail.is_relation:
                return new_output
            elif _output == "":
                raise ValueError(
                    "The path provided must have at least 1 foreign key to extract the related model path."
                )
            else:
                return _output
        raise ValueError(
            f"Model: '{model.__name__}' does not have a field attribute named: '{path[0]}'."
        )

    # Recurse
    return field_path_to_model_path(
        get_next_model(model, path[0]),
        path[1:],
        many_related=many_related,
        _output=new_output,
        _mr_output=_mr_output,
    )


def select_representative_field(
    model: Type[Model], force=False, include_expression=False
) -> Optional[Union[str, Expression]]:
    """(Arbitrarily) select the best single field to represent a model.

    A field is chosen based on the following criteria, in order of precedence:

    1. The model's _meta.ordering field is chosen, if only 1 exists.
    2. The first non-ID field that is unique is chosen, if one exists.
    3. The first non-ID field is chosen, if only 1 exists.
    4. The first non-ID field, if any exist and force=True, and a warning is issued^.
    5. The first non-foreign key field, if any exist and force=True, and a warning is issued^.
    6. If force=True, pk, and a warning is issued^.
    7. Otherwise None

    ^ Warnings are only issued in DEBUG mode.

    Args:
        model (Type[Model])
        force (bool) [False]: Force a field to be selected as a representative when there is no single ordering field
            and there are no unique fields that are not the arbitrary primary key or foreign keys.
        include_expression (bool) [False]: If a suitable single field is selected from the model's ordering, return it
            as-is, instead of just returning a string version of the field name.
    Exceptions:
        None
    Returns:
        (Optional[str]): The name of the selected field to represent the model.
    """
    if len(model._meta.ordering) == 1:
        # If there's only 1 ordering field, use it
        if include_expression:
            return model._meta.ordering[0]
        return resolve_field_path(model._meta.ordering[0])

    all_fields = model._meta.get_fields()

    # Grab the first non-ID field from the related model that is unique, if one exists
    f: Field
    non_relations: List[Field] = []
    for f in all_fields:
        related_field = resolve_field(f)
        if not related_field.is_relation and related_field.name != "id":
            if related_field.unique:
                return related_field.name
            else:
                non_relations.append(related_field)

    if len(non_relations) == 1:
        return non_relations[0].name

    if not force:
        return None

    fldname = "pk"
    if len(non_relations) > 0:
        fldname = non_relations[0].name
    elif "id" in all_fields:
        fldname = "id"

    if settings.DEBUG:
        warn(
            f"Unable to select a representative field for model '{model.__name__}'.  Defaulting to '{fldname}'."
        )

    return fldname


def is_key_field(
    path: Union[
        str,
        List[str],
        Field,
        DeferredAttribute,
        ForwardManyToOneDescriptor,
        ManyToManyDescriptor,
        ReverseManyToOneDescriptor,
    ],
    model: Optional[Type[Model]] = None,
) -> bool:
    """Takes a path (or field representation) and a model and returns whether or not the field at the end of the path is
    a foreign key.

    Args:
        path (Union[str, List[str], Field, DeferredAttribute, ForwardManyToOneDescriptor, ManyToManyDescriptor,
            ReverseManyToOneDescriptor]): A field or dunderscore-delimited field path (i.e. a Django lookup).
        model (Optional[Type[Model]]): A Model.  Must not be None if path is a str or list.
    Exceptions:
        ValueError when model is None and field is a str or list, or when path is None
        TypeError when field is an unsupported type
    Returns:
        (bool): Whether the field at the end of the path is a foreign key.
    """
    if path is None:
        raise ValueError("path must not be None")
    if not isinstance(path, str) and not isinstance(path, list):
        return resolve_field(path).is_relation
    if isinstance(path, str) or isinstance(path, list):
        if model is not None and issubclass(model, Model):
            field_path = path
            if isinstance(path, list):
                field_path = "__".join(path)
            model_path = field_path_to_model_path(model, path)
            return field_path == model_path
        raise ValueError("model is required when path is a str or list.")
    raise TypeError(
        f"Invalid path type: '{type(path).__name__}'.  Must be one of: [str, list, Field, DeferredAttribute, "
        "ForwardManyToOneDescriptor, ManyToManyDescriptor, or ReverseManyToOneDescriptor]."
    )


def model_path_to_model(model: Type[Model], path: Union[str, List[str]]) -> Type[Model]:
    """Recursive method to take a root model and a dunderscore-delimited path and return the model class at the end of
    the path.

    Args:
        model (Type[Model]): Model class at the root of the path.
        path (Union[str, List[str]]): Dunderscore-delimited field path string or list of dunderscore-split fields.
    Exceptions:
        ValueError when an argument is invalid.
        AttributeError when any field in the path is not present on the associated model.
    Returns:
        (Type[Model]): The model class associated with the last foreign key field in the path.
    """
    if len(path) == 0:
        raise ValueError("path string/list must have a non-zero length.")
    if isinstance(path, str):
        return model_path_to_model(model, path.split("__"))
    if len(path) == 1:
        if hasattr(model, path[0]):
            return get_next_model(model, path[0])
        raise AttributeError(
            f"Model: '{model.__name__}' does not have a field attribute named: '{path[0]}'."
        )
    return model_path_to_model(get_next_model(model, path[0]), path[1:])


def get_distinct_fields(model: Type[Model], field_path: Optional[str] = None):
    """Collects all of the order-by fields associated with the model.

    If a field_path is supplied, it does one of 2 things, depending on the type of field at the end of the field_path.

    1. For non-foreign-key fields, it just returns a single-member list containing field_path.
    2. For foreign keys, it returns all of the fields in the related model's Meta.ordering.  It calls a recursive helper
       method in order to traverse the field_path to get to the related model/field at the end.

    Args:
        model (Type[Model])
        field_path (Optional[str]): Restricts the returned fields to the specific field at the end of the field_path
    Exceptions:
        None
    Returns:
        distinct_fields (List[str]): A list of field_paths from the provided model (or optional field_path) that are
            either just the field_path supplied or a series of field_paths from the related model's ordering,
            (if the field at the end of the path is a foreign key).
    """
    if field_path is None:
        distinct_fields = []
        if "ordering" in model._meta.ordering:
            for obf_exp in model._meta.ordering:
                obf = resolve_field_path(obf_exp)
                obf_path = f"{field_path}__{obf}"
                distinct_fields.extend(_get_distinct_fields_helper(model, obf_path))
        else:
            distinct_fields.append("pk")
        return distinct_fields
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
    """Returns whether the supplied field is a string/text-type field.  Intended for use in deciding whether to apply
    case-insensitive sorting and/or substring searching.

    Args:
        field (Optional[Union[Field, DeferredAttribute, ForwardManyToOneDescriptor, ManyToManyDescriptor,
            ReverseManyToOneDescriptor]): A field or field representation.
        default (bool) [False]: What to return if field is None.
    Exceptions:
        None
    Returns:
        (bool): Whether the supplied field is a string/text-type field.
    """
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
    """Returns whether the supplied field is a numeric-type field.  Intended for use in deciding whether to apply
    case-insensitive sorting and/or substring searching.

    Args:
        field (Optional[Union[Field, DeferredAttribute, ForwardManyToOneDescriptor, ManyToManyDescriptor,
            ReverseManyToOneDescriptor]): A field or field representation.
        default (bool) [False]: What to return if field is None.
    Exceptions:
        None
    Returns:
        (bool): Whether the supplied field is a numeric-type field.
    """
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
    default: bool = False,
) -> bool:
    """Returns whether the supplied field is unique.  Intended for use in deciding whether a field is appropriate to be
    linked to a detail record by default.

    Args:
        field (Optional[Union[Field, DeferredAttribute, ForwardManyToOneDescriptor, ManyToManyDescriptor,
            ReverseManyToOneDescriptor]): A field or field representation.
        default (bool) [False]: What to return if field is None.
    Exceptions:
        None
    Returns:
        (bool): Whether the supplied field is unique.
    """
    if field is not None:
        field = resolve_field(field)
        return field.unique
    return default


def dereference_field(field_name: str, model_name: str) -> str:
    """Takes a model name and a field name and returns a dunderscore-delimited path to a non-foreign-key field.  If a
    field is a foreign key, "__pk" is appended.  The intended purpose of this method is to aid in compiling a list of
    distinct fields, where order is unimportant.  The reason this is useful is that fields supplied to .distinct() must
    also be supplied to .order_by(), and .order_by() does something not entirely transparent when given a foreign key -
    it expands the foreign key to all of the related models' ordering fields defined in its Meta.

    Args:
        field_name (str): Name of a model field.
        model_name (str): Name of a model.
    Exceptions:
        None
    Returns:
        (Field): A Field instance for the field at the end of the path
    """
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
    """The Django ORM's order_by always puts null fields at the end, which can be a problem if you want the last record
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
    """This takes a model object and returns a boolean to indicate whether the object exists in the database.

    NOTE: it does not assert that the values of the fields are the same.
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


def model_title(model: Type[Model]) -> str:
    """Creates a title-case string from the supplied model, accounting for potentially set verbose settings.  Pays
    particular attention to pre-capitalized values in the model name, and ignores the potentially poorly automated
    title-casing in existing verbose values of the model so as to not lower-case acronyms in the model name, e.g.
    MSRunSample (which automatically gets converted to Msrun Sample instead of the preferred MS Run Sample).

    Args:
        model (Type[Model])
    Exceptions:
        None
    Returns:
        (str): The title-case version of the model name.
    """
    from DataRepo.utils.text_utils import camel_to_title, underscored_to_title

    try:
        vname: str = model._meta.__dict__["verbose_name"]
        sanitized = vname.replace(" ", "")
        sanitized = sanitized.replace("_", "")
        if any([c.isupper() for c in vname]) and model.__name__.lower() == sanitized:
            return underscored_to_title(vname)
        else:
            return camel_to_title(model.__name__)
    except Exception:
        return camel_to_title(model.__name__)


def model_title_plural(model: Type[Model]) -> str:
    """Creates a title-case string from self.model, accounting for potentially set verbose settings.  Pays
    particular attention to pre-capitalized values in the model name, and ignores the potentially poorly automated
    title-casing in existing verbose values of the model so as to not lower-case acronyms in the model name, e.g.
    MSRunSample (which automatically gets converted to Msrun Sample instead of the preferred MS Run Sample).

    Args:
        model (Type[Model])
    Exceptions:
        None
    Returns:
        (str): The title-case version of the model name.
    """
    from DataRepo.utils.text_utils import camel_to_title, underscored_to_title

    try:
        vname: str = model._meta.__dict__["verbose_name_plural"]
        if any([c.isupper() for c in vname]):
            return underscored_to_title(vname)
        else:
            return f"{camel_to_title(model.__name__)}s"
    except Exception:
        return f"{camel_to_title(model.__name__)}s"


def get_field_val_by_iteration(
    rec: Model,
    field_path: List[str],
    related_limit: int = 5,
    sort_field_path: Optional[List[str]] = None,
    _sort_val: Optional[List[str]] = None,
):
    """Private recursive method that takes a record and a path and traverses the record along the path to return
    whatever value is at the end of the path.  If it traverses through a many-related model, it returns a list of
    values.

    NOTE: If the end of the field_path is a foreign key itself, the value (or values) returned is not the key value
    (i.e. not an integer), but a model object (or objects).

    NOTE: The recursive calls are made via the supporting methods:
    - _get_field_val_by_iteration_onerelated_helper - Handles passing through singly-related foreign keys along the
        field_path
    - _get_field_val_by_iteration_manyrelated_helper - Handles passing through many-related foreign keys along the
        field_path

    The way this works is, _get_field_val_by_iteration_manyrelated_helper is called at any point along the
    field_path (possibly multiple points), where the foreign key being passed through is many-related.  Anytime a
    foreign key (of the end-value) along the field path is 1-related to its parent, it calls
    _get_field_val_by_iteration_onerelated_helper.  _get_field_val_by_iteration_onerelated_helper returns a 3-member
    tuple.  As those values are being passed back through the call stack, when they pass through the many-related
    step, those tuples are collected into a list of tuples.  The end result will either be a tuple (if there are no
    many-related relations along the path), or a list of tuples.  Each tuple is the value itself, a sort value, and
    a primary key.  In the case of there being no many-related component in the field_path, the second 2 values in
    the tuple are meaningless.

    Assumptions:
        1. The sort_field_path value will be a field under the associated column's related_model_path
    Args:
        rec (Model): A Model object.
        field_path (List[str]): A path from the rec object to a field/column value, that has been split by
            dunderscores.
        related_limit (int) [5]: Truncate/stop at this many (many-related) records.
        sort_field_path (Optional[List[str]]): A path from the rec object to a sort field, that has been split by
            dunderscores.  Only relevant if you know the field path to traverse through a many-related model.
        _sort_val (Optional[List[str]]): Do not supply.  This holds the sort value if the field path is longer than
            the sort field path.
        NOTE: We don't need to know if sorting is forward or reverse.  We are only returning tuples containing the
        sort value.  The sort must be done later, by the caller.
    Exceptions:
        ValueError when the sort field returns more than 1 value.
    Returns:
        (Union[List[Tuple[Any, Any, Any]]], Tuple[Any, Any, Any]): A list of 3-membered tuples or a 3-membered
            tuple.  Each tuple contains the value, a sort value, and a unique value.
    """
    if len(field_path) == 0 or rec is None:
        return None

    if is_many_related_to_parent(field_path[0], type(rec)):
        # This method handles only fields that are many-related to their immediate parent
        return _get_field_val_by_iteration_manyrelated_helper(
            rec,
            field_path,
            related_limit=related_limit,
            sort_field_path=sort_field_path,
            _sort_val=_sort_val,
        )

    # This method handles only fields that are singly related to their immediate parent
    return _get_field_val_by_iteration_onerelated_helper(
        rec,
        field_path,
        related_limit=related_limit,
        sort_field_path=sort_field_path,
        _sort_val=_sort_val,
    )


def _get_field_val_by_iteration_onerelated_helper(
    rec: Model,
    field_path: List[str],
    related_limit: int = 5,
    sort_field_path: Optional[List[str]] = None,
    _sort_val: Optional[List[str]] = None,
):
    """Private recursive method that takes a field_path and a record (that is 1:1 related with the first element in
    the remaining field_path) and traverses the record along the path to return whatever ORM object's field value is
    at the end of the path.

    NOTE: Recursive calls go to get_field_val_by_iteration, which calls this method or the companion method
    (_get_field_val_by_iteration_manyrelated_helper) for many-related portions of the field_path.

    Assumptions:
        1. The related_sort_fld value will be a field under the related_model_path
    Args:
        rec (Model): A Model object.
        field_path (List[str]): A path from the rec object to a field/column value, that has been split by
            dunderscores.
        related_limit (int) [5]: Truncate/stop at this many (many-related) records.
        sort_field_path (Optional[List[str]]): A path from the rec object to a sort field, that has been split by
            dunderscores.  Only relevant if you know the field path to traverse through a many-related model.
        _sort_val (Optional[List[str]]): Do not supply.  This holds the sort value if the field path is longer than
            the sort field path.
    Exceptions:
        ValueError when the sort field returns more than 1 value.
    Returns:
        (Tuple[Any, Any, Any]): A tuple containing the value, a sort value, and a unique value.
    """
    if is_many_related_to_parent(field_path[0], type(rec)):
        raise TypeError(
            "_get_field_val_by_iteration_onerelated_helper called with a many-related field"
        )

    val_or_rec = getattr(rec, field_path[0])

    next_sort_field_path = sort_field_path[1:] if sort_field_path is not None else None
    # If we're at the end of the field path, we need to issue a separate recursive call to get the sort value
    if (
        sort_field_path is not None
        and len(sort_field_path) > 0
        and _sort_val is None
        and (
            sort_field_path[0] != field_path[0]
            or len(sort_field_path) == 1
            or len(field_path) == 1
        )
    ):
        # NOTE: Limiting to 2, because we only expect 1 and will raise ProgrammingError if multiple returned
        sort_val, _, _ = get_field_val_by_iteration(
            rec, sort_field_path, related_limit=2
        )
        if isinstance(sort_val, list):
            raise ProgrammingError(
                "The sort value must not be many-related with the value for the column"
            )
        next_sort_field_path = None
        _sort_val = sort_val

    if len(field_path) == 1:
        uniq_val = val_or_rec
        if isinstance(val_or_rec, Model):
            uniq_val = val_or_rec.pk
        # NOTE: Returning the value, a value to sort by, and a value that makes it unique per record (or field)
        return val_or_rec, _sort_val, uniq_val

    return get_field_val_by_iteration(
        val_or_rec,
        field_path[1:],
        related_limit=related_limit,
        sort_field_path=next_sort_field_path,
        _sort_val=_sort_val,
    )


def _get_field_val_by_iteration_manyrelated_helper(
    rec: Model,
    field_path: List[str],
    related_limit: int = 5,
    sort_field_path: Optional[List[str]] = None,
    _sort_val: Optional[List[str]] = None,
):
    """Private recursive method that takes a field_path and a record (that is many:1_or_many related with the first
    element in the remaining field_path) and traverses the record along the path to return values found at the end
    of the field_path.

    NOTE: Recursive calls go to get_field_val_by_iteration, which calls this method or the companion method
    (_get_field_val_by_iteration_single_helper) for singly-related portions of the field_path.

    NOTE: The recursive calls to get_field_val_by_iteration come from the 2 supporting methods:
    - _last_many_rec_iterator
    - _recursive_many_rec_iterator

    Assumptions:
        1. The sort_field_path value starts with the field_path
    Args:
        rec (Model): A Model object.
        field_path (List[str]): A path from the rec object to a field/column value, that has been split by
            dunderscores.
        related_limit (int) [5]: Truncate/stop at this many (many-related) records.
        sort_field_path (Optional[List[str]]): A path from the rec object to a sort field, that has been split by
            dunderscores.  Only relevant if you know the field path to traverse through a many-related model.
        _sort_val (Optional[List[str]]): Do not supply.  This holds the sort value if the field path is longer than
            the sort field path.
    Exceptions:
        ValueError when the sort field returns more than 1 value.
    Returns:
        (List[Tuple[Any, Any, Any]]): A list of tuples, the size of which corresponds to the number of unique
            records.
    """
    from DataRepo.utils.func_utils import reduceuntil

    if rec is None:
        return []

    if not is_many_related_to_parent([field_path[0]], type(rec)):
        raise TypeError(
            "_get_field_val_by_iteration_many_related_helper called without a many-related field"
        )

    mr_qs: QuerySet = getattr(rec, field_path[0])

    next_sort_field_path = sort_field_path[1:] if sort_field_path is not None else []
    # If the sort_field_path has diverged from the field_path, retrieve its value
    if sort_field_path is not None and sort_field_path[0] != field_path[0]:
        sort_val, _, _ = get_field_val_by_iteration(
            rec,
            sort_field_path,
            # We only expect 1 value and are going to assume that the sort field was properly checked/generated to
            # not go through another many-related relationship.  Still, we specify the limit to be safe.
            related_limit=1,
        )
        if isinstance(sort_val, list):
            raise ProgrammingError(
                "The sort value must not be many-related with the value for the column"
            )
        next_sort_field_path = []
        _sort_val = sort_val

    if len(field_path) == 1:

        uniq_vals = reduceuntil(
            lambda ulst, val: ulst + [val] if val not in ulst else ulst,
            lambda val: related_limit is not None and len(val) >= related_limit,
            _last_many_rec_iterator(mr_qs, next_sort_field_path),
            [],
        )

        return uniq_vals

    uniq_vals = reduceuntil(
        lambda ulst, val: ulst + [val] if val not in ulst else ulst,
        lambda val: related_limit is not None and len(val) >= related_limit,
        _recursive_many_rec_iterator(
            mr_qs,
            field_path[1:],
            next_sort_field_path,
            related_limit,
            _sort_val,
        ),
        [],
    )

    return uniq_vals


def _last_many_rec_iterator(
    mr_qs: QuerySet,
    next_sort_field_path: List[str],
):
    """Private iterator to help _get_field_val_by_iteration_many_related_helper.  It iterates through the queryset,
    converting the many-related records to tuples of the record, the sort value, and the primary key.  This allows
    the caller to stop when it reaches its goal.  This is called when we're at the end of the field_path.  I.e. the
    end of the field_path is a foreign key to a many-related model.  It will make a recursive call if the
    sort_field_path is deeper than the field_path.

    NOTE: This lower-cases the sort value (if it is a str).

    Args:
        mr_qs: (QuerySet): A queryset of values that are many-related to self.model.
        next_sort_field_path (Optional[List[str]]): The next sort_field_path that can be supplied directly to
            recursive calls to get_field_val_by_iteration without slicing it.
    Exceptions:
        None
    Returns:
        (Tuple[Any, Any, Any]): The value, sort-value, and primary key of the many-related model
    """
    mr_rec: Model
    for mr_rec in mr_qs.all():
        yield (
            # Model object is the value returned
            mr_rec,
            # Each rec gets its own sort value.
            (
                # TODO: See if this loop always causes a query.  If it does, then this iteration strategy may not be
                # as efficient as I'd hoped and should be entirely removed, as should the _lower() method
                _lower(get_field_val_by_iteration(mr_rec, next_sort_field_path)[0])
                if len(next_sort_field_path) > 0
                # Lower-case the string version of the many-related model object
                else str(mr_rec).lower()
            ),
            # We don't need pk for uniqueness when including model objects, but callers expect it
            mr_rec.pk,
        )


def _recursive_many_rec_iterator(
    mr_qs: QuerySet,
    next_field_path: List[str],
    next_sort_field_path: List[str],
    related_limit: int,
    _sort_val,
):
    """Private iterator to help _get_field_val_by_iteration_many_related_helper.  It iterates through the queryset,
    retrieving the values at the end of the path using recursive calls.  This allows the caller to stop when it
    reaches its goal.  This is called when a many-related model is encountered before we're at the end of the
    field_path.

    Args:
        mr_qs: (QuerySet): A queryset of values that are many-related to the model the queryset comes from.
        field_path (List[str])
        next_sort_field_path (List[str]): In order to simplify this method, instead of taking sort_field_path (which
            needs to be checked and converted to the next path, because it can diverge or be a different length from
            the field_path), that work must be done before calling this method.
        related_limit (int)
        _sort_val (Any)
    Exceptions:
        None
    Returns:
        (Tuple[Any, Any, Any]): The value, sort-value, and primary key of the many-related model
    """
    mr_rec: Model
    for mr_rec in mr_qs.all():
        val = get_field_val_by_iteration(
            mr_rec,
            next_field_path,
            related_limit=related_limit,
            sort_field_path=next_sort_field_path,
            _sort_val=_sort_val,
        )
        if isinstance(val, tuple):
            yield val
        else:
            # Must be a list
            for tpl in val:
                yield tpl


def get_many_related_field_val_by_subquery(
    rec: Model,
    field_path: str,
    related_limit: int = 5,
    annotations: dict = {},
    order_bys: list = [],
    distincts: list = [],
) -> list:
    """

    Args:
        rec (Model): A Model object whose Model corresponds to the start of the field_path.
        field_path (str): A dunderscore delimited field path whose starting field is a field of the rec's model.
        related_limit (int) [5]: Limit the size of the list returned to this many field values.
        annotations (dict) [{}]: Any annotations that need to be created.  Primarily, this is to support what is
            provided in the order_by list.
        order_bys (List[Union[str, OrderBy]]) [[]]: A list of OrderBy values (object or str).  This will be
            automatically appended to with the field_path and the primary key of the last many-related model foreign
            key in the field_path.
        distincts (List[str]) [[]]: A list of field paths.  Must match the fields contained in the order_bys list.
            This will be prepended with the field_path and appended with the primary key of the last many-related
            model foreign key in the field_path.
    Exceptions:
        None
    Returns:
        vals_list (list): A unique list of values from the many-related model at the end of the field path.
    """
    # This is used to ensure that the last many-related model is what is made distinct (not any other one-related
    # model that could be later in the field_path after the many-related model)
    many_related_model_path = field_path_to_model_path(
        rec.__class__, field_path, many_related=True
    )

    # If the field in the field_path is a foreign key (whether it is to the many-related model or another model that
    # is one-related with the many-related model), the primary key value returned is converted into model objects.
    related_model_path = field_path_to_model_path(rec.__class__, field_path)
    related_model = model_path_to_model(rec.__class__, related_model_path)
    is_fk = related_model_path == field_path

    # Append the actual field from the column (which may differ from the sort's field [c.i.p. as happens in column
    # groups])
    order_bys.append(field_path)
    # Append the many-related model's primary key in order to force a value for each distinct record
    order_bys.append(f"{many_related_model_path}__pk")

    # Prepend the actual field from the column (which may differ from the sort).  We put it first so we can easily
    # extract it in the values_list.  This is necessary because Django's combination of annotate, distinct, and
    # values_list have some quirks that prevent an intuitive usage of just flattening with the one value you want.
    # I tried many different versions of this before figuring this out.
    distincts.insert(0, field_path)
    # Append the many-related model's primary key in order to force a value for each distinct record
    distincts.append(f"{many_related_model_path}__pk")

    # We re-perform (essentially) the same query that generated the table, but for one root-table record, and with
    # all of the many-related values joined in to "split" the row, but we're only going to keep those many-related
    # values.  We can/will get repeated values if the field is not unique in the many-related model, but they each
    # will represent a unique many-related model record.  Furthermore, if the column is in a column group, the
    # order-bys will by based on the same field expression, because BSTColumnGroup modifies col.sorter.
    qs = (
        rec.__class__.objects
        # Filter for the current root model record
        .filter(pk=rec.pk)
        # The annotation is for use in the order_by, since it needs to be supplied to .distinct()
        .annotate(**annotations)
        # The order_by should be the annotation (e.g. the lower-cased version of the sort field) and 2 fields
        # necessary to be able to supply to distinct and the values_list: the primary key of the many-related model
        # (distinct) and the field_path of the field we want for the column (values_list)
        .order_by(*order_bys)
        # NOTE: This makes it distinct per the target many-related model record, even if there exist multiple many-
        # related models in the field path.
        .distinct(*distincts)
    )

    vals_list = [
        # Return an object, like an actual queryset does, if val is a foreign key field
        related_model.objects.get(pk=val) if is_fk else val
        for val in list(v[0] for v in qs.values_list(*distincts)[0:related_limit])
    ]

    return vals_list


# TODO: This should be removed and the BSTManyRelatedColumn class should have a python sort method to take its place
def _lower(val):
    """Intended for use in list comprehensions to lower-case the sort value, IF IT IS A STRING.
    Otherwise it returns the unmodified value."""
    if isinstance(val, str):
        return val.lower()
    return val


# TODO: Figure out a way to move these exception classes to exceptions.py without hitting a circular import
class NoFields(Exception):
    pass


class MultipleFields(Exception):
    pass
