from django import template
from django.template.defaultfilters import floatformat

register = template.Library()


@register.filter
def getFormatName(qry, fmt):
    """
    Retrieves a format name, given a format
    """
    return qry["searches"][fmt]["name"]

@register.filter
def durationToWeeks(td):
    if td is None:
        return None
    return td.total_seconds() // 604800

@register.filter
def decimalPlaces(number, places):
    if number is None:
        return None
    return floatformat(number, places)

# This allows indexing a list or dict
@register.filter
def index(indexable, i):
    return indexable[i]


@register.simple_tag
def define(the_val):
    return the_val
