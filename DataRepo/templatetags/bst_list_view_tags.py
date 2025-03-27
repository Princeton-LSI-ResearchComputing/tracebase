from django import template
from django.db.models import Model

register = template.Library()


@register.filter
def is_model_obj(field):
    """Determine if a template variable is a model object."""
    return isinstance(field, Model)


@register.filter
def has_detail_url(model_object_or_class):
    """Check if a model object or class has a get_absolute_url method."""
    return hasattr(model_object_or_class, "get_absolute_url")


@register.filter
def get_attr(object, attr, default=None):
    """This allows you to get an attribute of an object using a template variable."""
    try:
        v = getattr(object, attr, default)
    except (TypeError, KeyError) as e:
        print(
            f"WARNING: Lookup performed on object: '{object}' with attribute: '{attr}'. ",
            f"Caught error: [{type(e).__name__}: {str(e)}].  Returning default '{default}'.",
        )
        v = default
    return v
