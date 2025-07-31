import importlib
from typing import Any, List, Optional, Type, Union
from warnings import warn

from chempy import Substance
from chempy.util.periodic import atomic_number
from django.apps import apps
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import ProgrammingError
from django.db.models import Expression, F, Field, Model, Q, QuerySet
from django.db.models.expressions import Combinable
from django.db.models.fields.related_descriptors import (
    ForwardManyToOneDescriptor,
    ManyToManyDescriptor,
    ReverseManyToOneDescriptor,
    ReverseOneToOneDescriptor,
)
from django.db.models.fields.reverse_related import ForeignObjectRel
from django.db.models.query_utils import DeferredAttribute
from django.urls import resolve

# Postgres-specific function values for annotations
DATE_FORMAT = "YYYY-MM-DD"  # Postgres date format syntax
DATETIME_FORMAT = "YYYY-MM-DD HH24:MI:SS"  # Postgres date format syntax
DBSTRING_FUNCTION = "to_char"  # Postgres function
DURATION_SECONDS_ATTRIBUTE = "epoch"  # Postgres interval specific

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

DJANGO_LOOKUPS = [
    "exact",
    "iexact",
    "contains",
    "icontains",
    "in",
    "gt",
    "gte",
    "lt",
    "lte",
    "startswith",
    "istartswith",
    "endswith",
    "iendswith",
    "range",
    "year",
    "month",
    "day",
    "week_day",
    "isnull",
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
        str,
        Field,
        DeferredAttribute,
        ForwardManyToOneDescriptor,
        ManyToManyDescriptor,
        ReverseManyToOneDescriptor,
        ReverseOneToOneDescriptor,
    ],
    model: Optional[Type[Model]] = None,
    real: bool = True,
) -> Field:
    """A field at the end of a model path can be a deferred attribute or a descriptor.  If a model is not supplied, this
    method takes the field and returns the actual field (if it is deferred or a descriptor, otherwise, the supplied
    field).  If a model is supplied, it returns the field directly associated with the model, whether it is real or
    not, which includes reverse relations.

    Args:
        field (Union[str, Field, DeferredAttribute, ForwardManyToOneDescriptor, ManyToManyDescriptor,
            ReverseManyToOneDescriptor]): A representation of a Field.
        model (Optional[Type[Model]]): The model associated with the field.  Required is 'real' is False or if field is
            a string.
        real (bool) [True]: Supplying 'real' as True tells this method that you want the actual real Field instance
            (i.e. not an automatically generated reverse relation field).  Reverse relation fields don't have attributes
            like 'help_text' or 'choices' that are usually associated with a real field.  Real fields are associated
            with the models where they were defined.  But when foreign keys are created, a hidden "reverse relation"
            field is generated and associated with the remote model to complement the real field as a "reverse relation"
            - a way of getting from that remote model back to the model where the foreign key was defined.  Supplying
            'real' as False indicates that you want the field directly associated with the supplied model, even if it is
            a reverse relation and doesn't have all the attributes of a normal field instance.  Use this if you need the
            field that is an attribute of a specific model.
    Exceptions:
        None
    Returns:
        (Field): A Field instance, resolved from one of a number of representations of a Field.
    """
    from DataRepo.utils.exceptions import RequiredArgument

    if not real and model is None:
        raise RequiredArgument(
            "model",
            message="The 'model' argument must not be None if the 'real' argument is False.",
        )

    if isinstance(field, str):
        if model is None:
            raise RequiredArgument(
                "model",
                message="The 'model' argument must not be None if the 'field' argument is a string.",
            )
        try:
            field = getattr(model, field)
        except AttributeError as ae:
            try:
                field = getattr(model, f"{field}_set")
            except Exception:
                raise ae
        resolve_field(field, model=model, real=real)

    field_containers = (
        DeferredAttribute,
        ForwardManyToOneDescriptor,
        ManyToManyDescriptor,
        ReverseManyToOneDescriptor,
    )
    if isinstance(field, field_containers):
        # To get the field from the model where it was defined (i.e. a real field)
        if real:
            if is_reverse_related_field(field.field):
                return field.rel
            # Otherwise return a reverse related field (not a real field):
            return field.field
        if not hasattr(model, field.field.name):
            return field.rel
        # Otherwise return a reverse related field (not a real field):
        return field.field
    elif isinstance(field, ReverseOneToOneDescriptor):
        return field.related
    return field


def is_model_field(model: Type[Model], field_name: str) -> bool:
    """Determines if the supplied field name is a field of the supplied model.

    Takes reverse relations without a related_name into account.

    Args:
        model (Type[Model])
        field_name (str)
    Exceptions:
        None
    Returns:
        (bool)
    """
    try:
        resolve_field(field_name, model=model)
        return True
    except AttributeError:
        pass
    return False


def resolve_field_path(
    field_or_expression: Union[str, Combinable, DeferredAttribute, Field, Q, List[str]],
    _level=0,
    all=False,
) -> Union[str, List[str]]:
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
    Args:
        field_or_expression (Union[str, Combinable, DeferredAttribute, Field, Q, List[str]]): A str (e.g. a field path),
            Combinable (e.g. an F, Transform [like Lower("name")], Expression, etc object), a Model Field (or a
            DeferredAttribute representing a Model Field), a Q object (e.g. an expression inside a SubQuery Combinable),
            or a str list (because extracting field paths from a Q object can return multiple paths).
        _level (int): Recursion level, used to determine whether to raise an exception or return ""
        all (bool) [False]: Return all extracted field paths in a list.  If none, an empty list is returned.  Default
            behavior is only 1.
    Exceptions:
        MultipleFields when all is False and multiple field paths extracted
        NoFields when no field paths could be extracted
        TypeError when field_or_expression is an unsupported type (could be encountered via recursion)
    Returns
        field_path (Union[str, List[str]]): The field path that either was the field_or_expression or its wrapper.  The
            initial call always returns a str if all is False and always returns a List[str] if all is True.  You may
            need to cast the return value to satisfy mypy.
    """
    if isinstance(field_or_expression, str):
        fld_pth = (
            field_or_expression
            if not field_or_expression.startswith("-")
            else field_or_expression[1:]
        )
        return fld_pth if not all else [fld_pth]
    elif isinstance(field_or_expression, Field):
        return field_or_expression.name if not all else [field_or_expression.name]
    elif isinstance(field_or_expression, Q):
        return resolve_field_path(
            extract_field_paths_from_q(field_or_expression), _level=_level + 1, all=all
        )
    elif isinstance(field_or_expression, list):
        if len(field_or_expression) == 0:
            if _level == 0 and all:
                return []
            elif _level > 0:
                # Return an empty string (to stay type-consistent) to indicate no fields detected in the expression(s)
                return "" if not all else []
            else:
                # If we're back to the original caller and there are still no expressions, raise an exception.
                raise NoFields("No field name in field representation.")
        elif len(field_or_expression) > 1:
            if all:
                return field_or_expression
            raise MultipleFields(
                f"Multiple field names in field representation {field_or_expression}."
            )
        return field_or_expression[0] if not all else field_or_expression
    elif isinstance(field_or_expression, DeferredAttribute):
        return (
            field_or_expression.field.name
            if not all
            else [field_or_expression.field.name]
        )
    elif isinstance(field_or_expression, Expression):
        field_reps: List[str] = []
        for fld in field_or_expression.get_source_expressions():
            if isinstance(fld, str) and fld != "":
                field_reps.append(fld)
            else:
                tmp_field_reps = resolve_field_path(fld, _level=_level + 1, all=all)
                if isinstance(tmp_field_reps, str):
                    field_reps.append(tmp_field_reps)
                else:  # Assumes list
                    field_reps.extend(tmp_field_reps)
        field_reps = [f for f in field_reps if f != ""]

        if len(field_reps) == 0:
            if _level == 0 and all:
                return []
            elif _level > 0:
                # Return an empty string (to stay type-consistent) to indicate no fields detected in the expression(s)
                return "" if not all else []
            else:
                # If we're back to the original caller and there are still no expressions, raise an exception.
                raise NoFields("No field name in field representation.")
        elif len(field_reps) > 1:
            if all:
                return field_reps
            raise MultipleFields(
                f"Multiple field names in field representation {field_reps}."
            )
        else:  # Assumes field_reps[0] is a str based on the above code
            # Strings need processing too, in case this comes from an order-by expression with a leading dash (for
            # reversing the order)
            return resolve_field_path(field_reps[0], _level=_level + 1, all=all)
    elif isinstance(field_or_expression, F):
        return field_or_expression.name if not all else [field_or_expression.name]
    else:
        raise TypeError(
            f"Unsupported field_or_expression type: '{type(field_or_expression).__name__}' for expression: "
            f"{field_or_expression}."
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


def is_many_related(field: Field, source_model: Optional[Type[Model]] = None):
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
        source_model (Optional[Type[Model]]): The model where field is being accessed.
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


def is_reverse_related_field(field: Field) -> bool:
    # NOTE: remote_field being defined is not always a good indicator that a field is a reverse relation.
    # E.g. ReverseOneToOneDescriptor
    return (hasattr(field, "auto_created") and field.auto_created) or isinstance(
        field, ForeignObjectRel
    )


def field_to_manager_name(field: Field) -> str:
    """Returns the field's name that can be used in a manager path.  This is a field name if the field is not a foreign
    key.  When it's a foreign key that is a reverse relation, it will be the field name as long as field.related_name
    has a non-None value.  Otherwise it is the field name with "_set" appended.

    Args:
        field (Field)
    Exceptions:
        None
    Returns:
        (str) field (when not a reverse relation) or manager name
    """
    is_revrel = is_reverse_related_field(field)
    if (
        # Don't return an appended "_set" if related_name is defined
        not is_revrel
        or (
            hasattr(field, "related_name")
            and getattr(field, "related_name") is not None
        )
    ):
        return field.name
    return f"{field.name}_set"


def field_name_to_manager_name(field: str, model: Type[Model]) -> str:
    """Returns the field name that can be used in a field path.  This is not always the actual field name.  When it's a
    reverse relation, it will be the field name as long as field.related_name has a non-None value.  Otherwise it is the
    field name with "_set" appended.

    Args:
        field (str)
        model (Type[Model])
    Exceptions:
        None
    Returns:
        (str) field (when not a reverse relation) or manager name
    """
    return field_to_manager_name(resolve_field(field, model=model, real=False))


def field_path_to_manager_path(
    model: Type[Model], field_path: Union[str, List[str]]
) -> str:
    """Field paths are used for query filters and sorting, but for operations that involve managing many-related model-
    objects, what is needed is a manager path.  Instead of involing database fields, it involves python manager objects.
    Such operations include things like prefetching many-related model objects or counting many-related model objects in
    an annotation.

    Example:
        # LCMethod.msrunsequence is a reverse related field (because MSRunSequence links to LCMethod.
        # It defines no related_name, so the default is to use the lower-cased model name with "_set" appended.
        # So to prefetch all MSRunSequence records associated with LCMethod records, you would do the following:
        LCMethod.objects.prefetch_related('msrunsequence_set')
        # But to filter on MSRunSequence.researcher being "Mark Hamil", you use the field name:
        LCMethod.objects.prefetch_related('msrunsequence_set').filter(msrunsequence__researcher='Mark Hamil')
    Args:
        model (Type[Model]): The root model of the field_path
        field_path (Union[str, List[str]]): The non-manager field path
    Exceptions:
        None
    Returns:
        manager_path (str)
    """
    if isinstance(field_path, str):
        return field_path_to_manager_path(model, field_path.split("__"))
    if len(field_path) == 0:
        return ""
    manager_name = field_to_manager_name(
        resolve_field(field_path[0], model=model, real=False)
    )
    if len(field_path) == 1:
        return manager_name
    else:
        remainder = field_path_to_manager_path(
            get_next_model(model, field_path[0]), field_path[1:]
        )
        return f"{manager_name}__{remainder}"


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
    field = resolve_field(field_path[0], model=source_model)
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
        try:
            field = resolve_field(field_path[0], model=source_model)
        except AttributeError:
            # Annotations are not attributes of the model class (only the instance), so field_path[0] can only be
            # many_related if the model class has it as an attribute.
            # Assume it is an annotation
            return False
        return is_many_related(field, source_model=source_model)
    elif len(field_path) == 2:
        field = resolve_field(field_path[0], model=source_model)
        next_model = get_next_model(source_model, field_path[0])
        # if the last field in the field_path is not a foreign key
        if not resolve_field(field_path[1], model=next_model).is_relation:
            # Return whether the last foreign key is many-related to its parent
            return is_many_related(field, source_model=source_model)
    return is_many_related_to_parent(
        field_path[1:], get_next_model(source_model, field_path[0])
    )


def field_path_to_field(
    model: Type[Model], path: Union[str, List[str]], real: bool = True
) -> Field:
    """Recursive method to take a root model and a dunderscore-delimited path and return the Field class at the end of
    the path.  The intention is so that the Field can be interrogated as to type or retrieve choices, etc.

    Args:
        model (Type[Model]): Model class at the root of the path.
        path (Union[str, List[str]]): Dunderscore-delimited field path string or list of dunderscore-split fields.
        real (bool): Only return actual fields, not reverse related fields.
            NOTE: Reverse related fields don't have help_text, choices, etc.
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
        return field_path_to_field(model, path.split("__"), real=real)
    if len(path) == 1:
        name = resolve_field_path(path[0])
        if isinstance(name, str) and is_model_field(model, name):
            return resolve_field(name, model=model, real=real)
        elif not isinstance(name, str):
            raise TypeError(f"Expected str. Got {type(name).__name__}")
        raise AttributeError(
            f"Model: {model.__name__} does not have a field attribute named: '{name}'."
        )
    return field_path_to_field(
        get_next_model(model, path[0]),
        path[1:],
        real=real,
    )


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
    field = resolve_field(field_name, model=current_model, real=False)
    # return field.model if current_model != field.model else field.related_model
    return field.related_model


def field_path_to_model_path(
    model: Type[Model],
    path: Union[str, List[str]],
    last_many_related: bool = False,
    first_many_related: bool = False,
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
        last_many_related (bool) [False]: Return the path to the last many-related model in the supplied path, instead
            of the last model.  Mutually exclusive with first_many_related (i.e. both cannot be True).
        first_many_related (bool) [False]: Return the path to the first many-related model in the supplied path, instead
            of the last model.  Mutually exclusive with last_many_related (i.e. both cannot be True).
        _output (str) [""]: Used in recursion to build up the resulting model path.
        _mr_output (str) [""]: Used in recursion to build up the resulting many-related model path (only used if
            many_related is True).
    Exceptions:
        ValueError if the field path is too short, a model is missing a field in the field path, or if a many-related
            model was requested and none was found.
    Returns:
        _output (str): The path to the last foreign key ("model") in the supplied path or "" if there are no foreign
            keys in the path (i.e. it's just a field name).
    """
    from DataRepo.utils.exceptions import MutuallyExclusiveMethodArgs

    if last_many_related and first_many_related:
        raise MutuallyExclusiveMethodArgs(
            "last_many_related and first_many_related cannot both be True."
        )
    if len(path) == 0:
        raise ValueError("path string/list must have a non-zero length.")
    if isinstance(path, str):
        return field_path_to_model_path(
            model,
            path.split("__"),
            last_many_related=last_many_related,
            first_many_related=first_many_related,
        )

    new_output = path[0] if _output == "" else f"{_output}__{path[0]}"

    # If we only want the last many-related model, update _mr_output
    if last_many_related or first_many_related:
        fld = resolve_field(path[0], model=model)
        if fld.is_relation and is_many_related(fld, model):
            _mr_output = new_output
        if first_many_related:
            return _mr_output

    # If we're at the end of the path - no more recursion - return the result
    if len(path) == 1:
        if last_many_related or first_many_related:
            if _mr_output == "":
                raise ValueError(
                    f"No many-related model was found in the path '{new_output}'."
                )
            return _mr_output
        elif is_model_field(model, path[0]):
            tail = resolve_field(path[0], model=model)
            if tail.is_relation:
                return new_output
            else:
                # A model path of "" indicates the root model
                return _output
        raise ValueError(
            f"Model: '{model.__name__}' does not have a field attribute named: '{path[0]}'."
        )

    # Recurse
    return field_path_to_model_path(
        get_next_model(model, path[0]),
        path[1:],
        last_many_related=last_many_related,
        first_many_related=first_many_related,
        _output=new_output,
        _mr_output=_mr_output,
    )


def select_representative_field(
    model: Type[Model],
    force=False,
    include_expression=False,
    subset: Optional[List[str]] = None,
) -> Optional[Union[str, Expression]]:
    """(Arbitrarily) select the best single field to represent a model, optionally from a subset of fields.

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
        subset (Optional[List[str]]): A subset of field names/paths to select from.  Use this to restrict the
            representative field selection to only the provided fields.  Field paths to related fields can be supplied,
            but they will be ignored and cannot be selected as a representiative.  Related fields can still be selected
            as representatives, but only the foreign keys with a path length of 1 are considered (i.e. no field path
            containing "__" is considered).  This will also ignore annotation fields.  Only fields that are attributes
            of the model are considered.
    Exceptions:
        ProgrammingError when a supplied subset of fields has no suitable representative field (i.e. when all supplied
            fields are from a many-related model).
    Returns:
        (Optional[str]): The name of the selected field to represent the model.
    """
    if subset is not None and len(subset) > 0:
        all_fields = [
            field_path_to_field(model, fld)
            for fld in subset
            if "__" not in fld and hasattr(model, fld)
        ]
        all_names = subset.copy()
    else:
        all_fields = model._meta.get_fields()
        all_names = [f.name for f in all_fields]

    if len(all_fields) == 0:
        return None

    if (
        len(model._meta.ordering) == 1
        and resolve_field_path(model._meta.ordering[0]) in all_names
    ):
        # If there's only 1 ordering field, use it
        if include_expression:
            return model._meta.ordering[0]
        return resolve_field_path(model._meta.ordering[0])

    # Grab the first non-ID field from the related model that is unique, if one exists
    f: Field
    non_relations: List[Field] = []
    one_relations: List[Field] = []
    for f in all_fields:
        related_field = resolve_field(f)
        if (
            not related_field.is_relation
            and related_field.name != "id"
            and related_field.null is False
        ):
            if related_field.unique:
                return related_field.name
            else:
                non_relations.append(related_field)
        elif (
            related_field.name != "id"
            and related_field.is_relation
            and not is_many_related_to_parent(related_field.name, model)
        ):
            one_relations.append(related_field)

    if len(non_relations) == 1:
        return non_relations[0].name

    if not force:
        return None

    fldname = "pk" if subset is None or len(subset) == 0 else subset[0]
    if len(non_relations) > 0:
        fldname = non_relations[0].name
    elif "id" in all_fields:
        fldname = "id"
    elif len(one_relations) > 0:
        fldname = one_relations[0].name
    elif subset is not None or (isinstance(subset, list) and len(subset) > 0):
        raise ProgrammingError(
            f"The subset of '{model.__name__}' fields supplied: {subset} has no suitable representative field.  "
            f"Include a field name that is not many-related to model '{model.__name__}'."
        )

    if settings.DEBUG:
        from DataRepo.utils.exceptions import DeveloperWarning

        warn(
            f"Unable to select a representative field for model '{model.__name__}'.  Defaulting to '{fldname}'.",
            DeveloperWarning,
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
    the path (or if the path is empty, the root model).

    Args:
        model (Type[Model]): Model class at the root of the path.
        path (Union[str, List[str]]): Dunderscore-delimited field path string or list of dunderscore-split fields.
    Exceptions:
        AttributeError when any field in the path is not present on the associated model.
    Returns:
        (Type[Model]): The model class associated with the last foreign key field in the path.
    """
    if len(path) == 0:
        # If the path is empty, return the root model
        return model
    if isinstance(path, str):
        return model_path_to_model(model, path.split("__"))
    if len(path) == 1:
        if is_model_field(model, path[0]):
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


def is_long_field(
    field: Optional[
        Union[
            Field,
            DeferredAttribute,
            ForwardManyToOneDescriptor,
            ManyToManyDescriptor,
            ReverseManyToOneDescriptor,
        ]
    ],
    short_len: int = 256,
    default: bool = False,
) -> bool:
    """Returns whether the supplied field can contain a value longer than short_len.  Intended for use in deciding
    whether to allow field values to soft-wrap.

    Args:
        field (Optional[Union[Field, DeferredAttribute, ForwardManyToOneDescriptor, ManyToManyDescriptor,
            ReverseManyToOneDescriptor]): A field or field representation.
        short_len (int) [256]: The longest length of a "short" field.  If the number of characters the field allows (its
            max_length) is longer than this number, returns True.
        default (bool) [False]: What to return if field is None.
    Exceptions:
        None
    Returns:
        (bool): Whether the supplied field is a string/text-type field.
    """
    if field is not None:
        nomax_field_names = [
            "TextField",
        ]

        field = resolve_field(field)
        return field.__class__.__name__ in nomax_field_names or (
            hasattr(field, "max_length")
            and isinstance(field.max_length, int)
            and field.max_length > short_len
        )
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


def get_model_by_name(model_name: str):
    if "." in model_name:
        # This is mainly for the tests which supply "loader.ModelName"
        return apps.get_model(*list(model_name.split(".")))
    model_name = model_name.replace("DataRepo.", "")
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
        vname: str = model._meta.verbose_name
        sanitized = vname.replace(" ", "")
        sanitized = sanitized.replace("_", "")
        if any([c.isupper() for c in vname]) or model.__name__.lower() != sanitized:
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

    plural_name = ""
    try:
        if model is not None:
            # If the model has a plural verbose name different from name (because it's automatically filled in with
            # name), use it
            if model.__name__.lower() != model._meta.verbose_name_plural.lower():
                if any(c.isupper() for c in model._meta.verbose_name_plural):
                    # If the field has a verbose name with caps, use it as-is
                    plural_name = model._meta.verbose_name_plural
                else:
                    # Otherwise convert it to a title
                    plural_name = underscored_to_title(model._meta.verbose_name_plural)
            if model._meta.verbose_name_plural:
                plural_name = underscored_to_title(model._meta.verbose_name_plural)
        else:
            plural_name = f"{camel_to_title(model.__name__)}s"
    except Exception:
        plural_name = f"{camel_to_title(model.__name__)}s"
    return plural_name


def get_field_val_by_iteration(
    rec: Model,
    field_path: List[str],
    related_limit: int = 5,
    sort_field_path: Optional[List[str]] = None,
    asc: bool = True,
    value_unique: bool = False,
):
    """User-facing interface to _get_field_val_by_iteration_helper, which returns either a tuple or list of tuples,
    where each tuple contains the value of interest, the sort value, and a unique value (e.g. primary key).

    Args:
        rec (Model): A Model object.
        field_path (List[str]): A path from the rec object to a field/column value, that has been split by
            dunderscores.
        related_limit (int) [5]: Truncate/stop at this many (many-related) records.
        sort_field_path (Optional[List[str]]): A path from the rec object to a sort field, that has been split by
            dunderscores.  Only relevant if the field path traverses through a many-related model.
        asc (bool) [True]: Sort is ascending.  Only relevant if the field path traverses through a many-related model.
        value_unique (bool) [False]: For columns whose field_path passes through a many-related model, only list a
            unique set of values.  The opposite behavior (default behavior, i.e. False) is to show all values associated
            with a unique set of the last many-related model records in the field_path.
            Example:
                If the the field_path is mdla.mdlb__mdlc__mdld__name
                where mdla is the model of rec
                the relationships are M:M, M:M, and 1:M respectively
                there are 5 unique mdld records
                but 2 unique name values among them
                When value_unique = True, this method will return 2 names
                When value_unique = False, this method will return 5 names
    Exceptions:
        None
    Returns:
        (Any): Either a field value or a list of field values.
    """
    val = _get_field_val_by_iteration_helper(
        rec,
        field_path,
        related_limit=related_limit,
        sort_field_path=sort_field_path,
        value_unique=value_unique,
    )

    # Many-related columns should return lists
    if isinstance(val, list) and all(isinstance(v, tuple) and len(v) == 3 for v in val):
        return [
            # Returning the first value of the tuple - converting empty strings to None
            tpl[0] if not isinstance(tpl[0], str) or tpl[0] != "" else None
            # Sort based on the the sort value in the tuple (the second value at index 1)
            for tpl in sorted(
                val, key=lambda t: (t[1] is not None, t[1]), reverse=not asc
            )
        ]

    # Convert empty strings in the tuple's return value to None
    return val[0] if not isinstance(val[0], str) or val[0] != "" else None


def _get_field_val_by_iteration_helper(
    rec: Model,
    field_path: List[str],
    related_limit: int = 5,
    sort_field_path: Optional[List[str]] = None,
    _sort_val: Optional[Any] = None,
    _uniq_val: Optional[Any] = None,
    value_unique: bool = False,
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
        _sort_val (Optional[Any]): Do not supply.  This holds the sort value if the field path is longer than
            the sort field path.
        _uniq_val (Optional[Any]): Do not supply.  This holds the unique value if the field path is longer than
            the path to the last many-related model.  Only used when value_unique is False.
        value_unique (bool) [False]: For columns whose field_path passes through a many-related model, only list a
            unique set of values.  The opposite behavior (default behavior, i.e. False) is to show all values associated
            with a unique set of the last many-related model records in the field_path.  See get_field_val_by_iteration
            for an example.
        NOTE: We don't need to know if sorting is forward or reverse.  We are only returning tuples containing the
        sort value.  The sort must be done later, by the caller.
    Exceptions:
        ValueError when the sort field returns more than 1 value.
    Returns:
        (Union[List[Tuple[Any, Any, Any]]], Tuple[Any, Any, Any]): A list of 3-membered tuples or a 3-membered
            tuple.  Each tuple contains the value, a sort value, and a unique value.
    """
    if len(field_path) == 0:
        raise ProgrammingError("A non-zero length field_path is required")

    if rec is None:
        if is_many_related_to_root(field_path, type(rec)):
            return []
        else:
            return None, None, None

    if is_many_related_to_parent(field_path[0], type(rec)):
        # This method handles only fields that are many-related to their immediate parent
        retval = _get_field_val_by_iteration_manyrelated_helper(
            rec,
            field_path,
            related_limit=related_limit,
            sort_field_path=sort_field_path,
            _sort_val=_sort_val,
            value_unique=value_unique,
        )
    else:
        # This method handles only fields that are singly related to their immediate parent
        retval = _get_field_val_by_iteration_onerelated_helper(
            rec,
            field_path,
            related_limit=related_limit,
            sort_field_path=sort_field_path,
            _sort_val=_sort_val,
            _uniq_val=_uniq_val,
            value_unique=value_unique,
        )

    # A many-related field can return a single (tuple) value instead of a list if a None exists on the path before the
    # first many-related model is encountered.  E.g. animal__infusate__tracer_links__tracer is many-related to Sample,
    # thus we expect a list, but if the animal has no infusate, a tuple would be returned because we haven't gotten to
    # the many-related relationship with tracer_links yet.  The following catches that and performs type checking.
    # NOTE: hasattr assumes that if the model doesn't have the first field, then it is an annotation and is by
    # definition, not many-related
    if is_model_field(type(rec), field_path[0]) and is_many_related_to_root(
        field_path, type(rec)
    ):
        if (
            isinstance(retval, tuple) and (len(retval) != 3 or retval[0] is not None)
        ) and not isinstance(retval, list):
            raise ProgrammingError(
                f"Expected a list, but got '{type(retval).__name__}': '{retval}' when looking for many-related field "
                f"'{'__'.join(field_path)}' in a '{type(rec).__name__}' record: '{rec}'."
            )
        if isinstance(retval, tuple):
            return []
    else:
        if not isinstance(retval, tuple) or len(retval) != 3:
            raise ProgrammingError(
                f"Expected a 3-member tuple, but got '{type(retval).__name__}': '{retval}' when looking for one-"
                f"related field '{'__'.join(field_path)}' in a '{type(rec).__name__}' record: '{rec}'."
            )

    return retval


def _get_field_val_by_iteration_onerelated_helper(
    rec: Model,
    field_path: List[str],
    related_limit: int = 5,
    sort_field_path: Optional[List[str]] = None,
    _sort_val: Optional[Any] = None,
    _uniq_val: Optional[Any] = None,
    value_unique: bool = False,
):
    """Private recursive method that takes a field_path and a record (that is 1:1 related with the first element in
    the remaining field_path) and traverses the record along the path to return whatever ORM object's field value is
    at the end of the path.

    NOTE: Recursive calls go to _get_field_val_by_iteration_helper, which calls this method or the companion method
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
        _sort_val (Optional[Any]): Do not supply.  This holds the sort value if the field path is longer than
            the sort field path.
        _uniq_val (Optional[Any]): Do not supply.  This holds the unique value if the field path is longer than
            the path to the last many-related model.
        value_unique (bool) [False]: For columns whose field_path passes through a many-related model, only list a
            unique set of values.  The opposite behavior (default behavior, i.e. False) is to show all values associated
            with a unique set of the last many-related model records in the field_path.  See get_field_val_by_iteration
            for an example.
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
        sort_val, _, _ = _get_field_val_by_iteration_helper(
            rec, sort_field_path, related_limit=2
        )
        if isinstance(sort_val, list):
            raise ProgrammingError(
                "The sort value must not be many-related with the value for the column"
            )
        next_sort_field_path = None
        _sort_val = sort_val

    if len(field_path) == 1:
        if value_unique is True:
            _uniq_val = val_or_rec
            if isinstance(val_or_rec, Model):
                _uniq_val = val_or_rec.pk
        elif _uniq_val is None:
            _uniq_val = rec.pk
        # NOTE: Returning the value, a value to sort by, and a value that makes it unique per record (or field)
        return val_or_rec, _sort_val, _uniq_val

    # The foreign key is None and iterating deeper would cause an exception, so return a None tuple
    if val_or_rec is None:
        return None, None, None

    # The foreign key is None and iterating deeper would cause an exception, so return a None tuple
    if val_or_rec is None:
        return None, None, None

    return _get_field_val_by_iteration_helper(
        val_or_rec,
        field_path[1:],
        related_limit=related_limit,
        sort_field_path=next_sort_field_path,
        _sort_val=_sort_val,
        _uniq_val=_uniq_val,
        value_unique=value_unique,
    )


def _get_field_val_by_iteration_manyrelated_helper(
    rec: Model,
    field_path: List[str],
    related_limit: int = 5,
    sort_field_path: Optional[List[str]] = None,
    _sort_val: Optional[Any] = None,
    value_unique: bool = False,
):
    """Private recursive method that takes a field_path and a record (that is many:1_or_many related with the first
    element in the remaining field_path) and traverses the record along the path to return values found at the end
    of the field_path.

    NOTE: Recursive calls go to _get_field_val_by_iteration_helper, which calls this method or the companion method
    (_get_field_val_by_iteration_single_helper) for singly-related portions of the field_path.

    NOTE: The recursive calls to _get_field_val_by_iteration_helper come from the 2 supporting methods:
    - _last_many_rec_iterator
    - _recursive_many_rec_iterator

    NOTE: Being many-related, _uniq_val is not needed as an argument, because primary key in this model is the source of
    the _uniq_val.

    Assumptions:
        1. The sort_field_path value starts with the field_path
    Args:
        rec (Model): A Model object.
        field_path (List[str]): A path from the rec object to a field/column value, that has been split by
            dunderscores.
        related_limit (int) [5]: Truncate/stop at this many (many-related) records.
        sort_field_path (Optional[List[str]]): A path from the rec object to a sort field, that has been split by
            dunderscores.  Only relevant if you know the field path to traverse through a many-related model.
        _sort_val (Optional[Any]): Do not supply.  This holds the sort value if the field path is longer than the sort
            field path.
        value_unique (bool) [False]: For columns whose field_path passes through a many-related model, only list a
            unique set of values.  The opposite behavior (default behavior, i.e. False) is to show all values associated
            with a unique set of the last many-related model records in the field_path.  See get_field_val_by_iteration
            for an example.
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

    # This gets the "manager" object that handles the many-related model objects.  Its name can either be the name of
    # the field in the model where it was defined, the related_name defined in that field if we're on the opposite
    # model, or the lower-cased related model's name with "_set" appended when related_name is not set.
    mr_qs: QuerySet = getattr(rec, field_name_to_manager_name(field_path[0], type(rec)))

    next_sort_field_path = sort_field_path[1:] if sort_field_path is not None else []
    # If the sort_field_path has diverged from the field_path, retrieve its value
    if sort_field_path is not None and sort_field_path[0] != field_path[0]:
        sort_val, _, _ = _get_field_val_by_iteration_helper(
            rec,
            sort_field_path,
            # We only expect 1 value and are going to assume that the sort field was properly checked/generated to
            # not go through another many-related relationship.  Still, we specify the limit to be safe.
            related_limit=1,
            value_unique=value_unique,
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
            value_unique=value_unique,
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

    NOTE: Being many-related, _uniq_val is not needed as an argument, because primary key in this model is the source of
    the _uniq_val.

    Args:
        mr_qs: (QuerySet): A queryset of values that are many-related to self.model.
        next_sort_field_path (Optional[List[str]]): The next sort_field_path that can be supplied directly to
            recursive calls to _get_field_val_by_iteration_helper without slicing it.
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
                _lower(
                    _get_field_val_by_iteration_helper(mr_rec, next_sort_field_path)[0]
                )
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
    value_unique: bool = False,
):
    """Private iterator to help _get_field_val_by_iteration_many_related_helper.  It iterates through the queryset,
    retrieving the values at the end of the path using recursive calls.  This allows the caller to stop when it
    reaches its goal.  This is called when a many-related model is encountered before we're at the end of the
    field_path.

    NOTE: Being many-related, _uniq_val is not needed as an argument, because primary key in this model updates the
    _uniq_val.

    Args:
        mr_qs: (QuerySet): A queryset of values that are many-related to the model the queryset comes from.
        field_path (List[str])
        next_sort_field_path (List[str]): In order to simplify this method, instead of taking sort_field_path (which
            needs to be checked and converted to the next path, because it can diverge or be a different length from
            the field_path), that work must be done before calling this method.
        related_limit (int)
        _sort_val (Any)
        value_unique (bool) [False]: For columns whose field_path passes through a many-related model, only list a
            unique set of values.  The opposite behavior (default behavior, i.e. False) is to show all values associated
            with a unique set of the last many-related model records in the field_path.  See get_field_val_by_iteration
            for an example.
    Exceptions:
        None
    Returns:
        (Tuple[Any, Any, Any]): The value, sort-value, and primary key of the many-related model
    """
    mr_rec: Model
    # TODO: The prototype uses .distinct() here.  See if that improves performance...
    for mr_rec in mr_qs.all():
        val = _get_field_val_by_iteration_helper(
            mr_rec,
            next_field_path,
            related_limit=related_limit,
            sort_field_path=next_sort_field_path,
            _sort_val=_sort_val,
            # At every point in the field_path that is many-related to its parent, update the unique val
            _uniq_val=mr_rec.pk,
            value_unique=value_unique,
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
    value_unique: bool = False,
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
        value_unique (bool) [False]: For columns whose field_path passes through a many-related model, only list a
            unique set of values.  The opposite behavior (default behavior, i.e. False) is to show all values associated
            with a unique set of the last many-related model records in the field_path.  See get_field_val_by_iteration
            for an example.
    Exceptions:
        ProgrammingError
    Returns:
        vals_list (list): A unique list of values from the many-related model at the end of the field path.
    """
    # This is used to ensure that the last many-related model is what is made distinct (not any other one-related
    # model that could be later in the field_path after the many-related model)
    many_related_model_path = field_path_to_model_path(
        rec.__class__, field_path, last_many_related=True
    )

    # If the field in the field_path is a foreign key (whether it is to the many-related model or another model that
    # is one-related with the many-related model), the primary key value returned is converted into model objects.
    related_model_path = field_path_to_model_path(rec.__class__, field_path)
    related_model = model_path_to_model(rec.__class__, related_model_path)
    is_fk = related_model_path == field_path

    # TODO: Add the ability to handle foreign keys in the order_bys and distincts arguments.  RN, the developer has to
    # supply the pre-determined values that account for model ordering fields.  In other words, the supplied order_bys
    # and distincts are put directly into the query without ensuring there aren't other fields that Django will
    # automatically add from the model ordering, which is what causes exceptions about order-bys having to match
    # distincts.

    # Append the actual field from the column (which may differ from the sort's field [c.i.p. as happens in column
    # groups])
    if is_fk:
        # If this is a foreign key, django incorporates the related model's ordering fields.  We must incorporate them
        # to avoid a ProgrammingError arisen from Django's core code
        ob_fields = get_distinct_fields(type(rec), field_path)
        order_bys.extend(ob_fields)
    else:
        ob_fields = [field_path]
        order_bys.append(field_path)

    if value_unique is False:
        # Append the many-related model's primary key in order to force a value for each distinct record
        order_bys.append(f"{many_related_model_path}__pk")
        order_bys.append(f"{related_model_path}__pk")

    # Prepend the actual field (or fields) from the column (which may differ from the sort).  We put it first so we can
    # easily extract it in the values_list.  This is necessary because Django's combination of annotate, distinct, and
    # values_list have some quirks that prevent an intuitive usage of just flattening with the one value you want.
    # I tried many different versions of this before figuring this out.
    if is_fk:
        distincts = [f"{related_model_path}__pk"] + ob_fields + distincts
    else:
        distincts = ob_fields + distincts

    if value_unique is False:
        # Append the many-related model's primary key in order to force a value for each distinct record
        distincts.append(f"{many_related_model_path}__pk")
        distincts.append(f"{related_model_path}__pk")

    # We re-perform (essentially) the same query that generated the table, but for one root-table record, and with
    # all of the many-related values joined in to "split" the row, but we're only going to keep those many-related
    # values.  We can/will get repeated values if the field is not unique in the many-related model, but they each
    # will represent a unique many-related model record.  Furthermore, if the column is in a column group, the
    # order-bys will by based on the same field expression, because BSTColumnGroup modifies col.sorter.
    qs = (
        type(rec)
        .objects
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
        .values_list(*distincts)
    )

    try:
        vals_list = [
            # Return an object, like an actual queryset does, if val is a foreign key field
            related_model.objects.get(pk=val) if is_fk and val is not None else val
            for val in list(v[0] for v in qs[0:related_limit])
        ]
    except (ProgrammingError, ObjectDoesNotExist) as pe:
        raise ProgrammingError(
            f"Error executing (0-{related_limit} sliced) query:\n\n"
            f"QUERY: {qs.query}\n\n"
            f"ERROR: {pe}\n"
            "using:\n\n"
            f"\tMODEL: {type(rec).__name__}\n"
            f"\tFILTER: pk={rec.pk}\n"
            f"\tANNOTATIONS: {annotations}\n"
            f"\tORDER_BYS: {order_bys}\n"
            f"\tDISTINCTS: {distincts}\n"
            f"\tVALUES_LIST: {distincts}\n"
        )

    return vals_list


def extract_field_paths_from_q(q_obj: Q) -> List[str]:
    """Recursively extracts all field paths from a Django Q object.

    Example:
        extract_field_paths_from_q(Q(animal__name__icontains="test") | Q(animal__study__pk=5))
        # ["animal__name", "animal__study__pk"]
    Limitations:
        1. Only supports a simple lookup, where the first element of the tuple is assumed to be the field path
    Args:
        q_obj (Q)
    Exceptions:
        None
    Returns:
        field_paths (List[str]): A list of field_paths extracted from the Q expression(s) from the left hand side (with
            lookups removed).
    """
    field_paths = set()
    for child in q_obj.children:
        if isinstance(child, Q):
            field_paths.update(extract_field_paths_from_q(child))
        else:
            field_paths.add(remove_lookup(child[0]))
    return list(field_paths)


def remove_lookup(field_path: Union[List[str], str]):
    """Chops off the lookup (if present) from the end of a field_path by checking it against a list of known django
    lookups

    Examples:
        remove_lookup("sample__name__icontains")  # "sample__name"
    Args:
        field_path (Union[List[str], str])
    Exceptions:
        None
    Returns:
        (str)
    """
    if not isinstance(field_path, list):
        return remove_lookup(field_path.split("__"))
    if field_path[-1] in DJANGO_LOOKUPS:
        field_path.pop()
    return "__".join(field_path)


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
