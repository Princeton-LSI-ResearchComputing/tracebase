from django import template
from django.template.defaultfilters import floatformat

from DataRepo.views import getDownloadQryList, manyToManyFilter

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
def durationToMins(td):
    if td is None:
        return None
    return td.total_seconds() // 60


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


@register.filter
def getClass(state):
    styleclass = None
    if state is None:
        styleclass = ""
    elif state == "FAILED":
        styleclass = "text-danger"
    elif state == "WARNING":
        styleclass = "text-warning"
    elif state == "PASSED":
        styleclass = "text-success"
    else:
        styleclass = "text-info"
    return styleclass


@register.filter
def count_tracer_groups(res):
    cnt = 0
    for pg in res.all():
        if pg.is_tracer_compound_group:
            cnt = cnt + 1
    return cnt


@register.simple_tag
def shouldKeepManyToMany(rootrec, mm_lookup, qry, refilter):
    """
    If refilter is true, this method calls the views.manyToManyFilter to filter out records that do not match search
    terms from a many-to-many related table.  Note, this can only handle composite views that contain a single many-to-
    many relationship.  If there are multiple many-to-many relationships in the composite view, a new method will have
    to be written.
    """
    return not refilter or manyToManyFilter(rootrec, mm_lookup, qry)


@register.simple_tag
def createDict():
    return {}


@register.simple_tag
def addToDict(theDict, theKey, theVal):
    theDict[theKey] = theVal
    # We don't need to return the dict, because the one created by createDict is still in memory and will reflect this
    # addition, but we don't want there to be a visible effect in the template either, so return an empty string
    return ""


@register.simple_tag
def createCounter():
    return {"count": 0}


@register.simple_tag
def incrementCounter(countdict):
    countdict["count"] += 1
    return ""


@register.simple_tag
def getCount(countdict):
    return countdict["count"]


@register.simple_tag
def getDownloadQrys():
    return getDownloadQryList()
