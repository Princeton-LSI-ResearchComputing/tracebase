from django import template

register = template.Library()


@register.filter
def getFormatName(qry, fmt):
    """
    Retrieves a format name, given a format
    """
    return qry["searches"][fmt]["name"]
