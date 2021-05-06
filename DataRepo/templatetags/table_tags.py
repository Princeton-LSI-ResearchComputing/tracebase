from django import template
from django.template.defaultfilters import stringfilter

register = template.Library()

@register.filter
def value_from_model(model, field):
    return getattr(model, field)

@register.filter
@stringfilter
def template_exists(value):
    try:
        template.loader.get_template(value)
        return True
    except template.TemplateDoesNotExist:
        return False
