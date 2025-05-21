from typing import Any
from warnings import warn

from django import template
from django.conf import settings
from django.db.models import Model

from DataRepo.models.utilities import get_field_val_by_iteration
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)

register = template.Library()


@register.filter
def is_model_obj(field):
    """Determine if a template variable is a model object."""
    return isinstance(field, Model)


@register.filter
def has_attr(object, attr: str):
    """This allows you to check if a template variable has an attribute."""
    return hasattr(object, attr)


@register.filter
def get_attr(object, attr, default=None):
    """This allows you to get an attribute of an object using a template variable."""
    try:
        v = getattr(object, attr, default)
    except (TypeError, KeyError) as e:
        if settings.DEBUG:
            warn(
                f"Attribute '{attr}' not found in {type(object).__name__} object: '{object}'.\n"
                f"Returning default '{default}'.\n"
                f"Original error: {type(e).__name__}: {str(e)}",
                category=DeveloperWarning,
                stacklevel=0,
            )
        v = default
    return v


@register.filter
def get_rec_val(rec: Model, column: BSTBaseColumn) -> Any:
    """Retrieves a field value from a model object using a column object.  The return will be a list of field values if
    the column is a BSTManyRelatedColumn.

    Agrs:
        rec (Model)
        column (BSTBaseColumn)
    Exceptions:
        None
    Returns:
        (Any): a field value or a list of field values (if the column is from a field in a many-related model)
    """
    if isinstance(column, BSTManyRelatedColumn):
        default: list = []
        try:
            vals = getattr(rec, column.list_attr_name)
        except AttributeError as ae:
            if settings.DEBUG:
                warn(
                    f"Attribute '{column.list_attr_name}' not found in '{type(rec).__name__}' record: '{rec}'.\n"
                    f"Returning default '{default}'.\n"
                    f"Original error: {type(ae).__name__}: {str(ae)}",
                    category=DeveloperWarning,
                    stacklevel=0,
                )
            vals = default
        return vals
    return get_field_val_by_iteration(rec, column.name.split("__"))


@register.filter
def get_absolute_url(model_object: Model):
    """Get a model object's detail URL."""
    url = model_object.get_absolute_url()
    if url is not None and url != "":
        return url
    return None
