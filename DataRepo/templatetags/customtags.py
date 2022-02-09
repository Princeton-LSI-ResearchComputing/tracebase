from django import template
from django.template.defaultfilters import floatformat
from django.urls import reverse
from django.utils import dateparse
from django.utils.html import format_html_join

from DataRepo.views import getDownloadQryList

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


@register.filter
def obj_hyperlink(id_name_list, obj):
    """
    takes an object list and returns a comma-separated list of hyperlinks.
    Notes:
    works for three types of object_list with defined format in Pandas DataFrames:
        study, tracer, treatment
        each item of object_list contains id and name seprated by "||"
    examples:
        study list:  ['1||obob_fasted']
        tracer list: ['30||C16:0', '11||lysine']
        treatment list: [3||Ser/gly-free diet, 2||Control diet]
    For a study without treament data, value in DataFrame is [nan], which is [None] after converting
        to json record for rendering templates
    """
    if obj == "study":
        tmplt_name = "study_detail"
    elif obj == "tracer":
        tmplt_name = "compound_detail"
    elif obj == "treatment":
        tmplt_name = "protocol_detail"

    if id_name_list == [None]:
        return None
    else:
        id_name_dict = {}
        for x in id_name_list:
            if x is not None:
                k, v = x.split("||")
                id_name_dict[k] = v
        obj_format_html = format_html_join(
            ",",
            '<a href="{}">{}</a>',
            [
                (reverse(tmplt_name, args=[str(id)]), id_name_dict[id])
                for id in id_name_dict
            ],
        )
        return obj_format_html


@register.filter
def convert_iso_date(value):
    if value is None:
        return None
    return dateparse.parse_datetime(value).strftime("%Y-%m-%d")


@register.filter
def duration_iso_to_mins(value):
    if value is None:
        return None
    return dateparse.parse_duration(value).seconds // 60


@register.filter
def duration_iso_to_weeks(value):
    if value is None:
        return None
    return dateparse.parse_duration(value).days // 7


@register.filter
def hmdb_id_url(hmdb_id):
    if hmdb_id is None:
        return None
    else:
        return f"https://hmdb.ca/metabolites/{hmdb_id}"


@register.simple_tag
def get_case_insensitive_synonyms(case_qs):
    # Create a list of the names/synonyms from the queryset
    case_list = []
    for rec in list(case_qs.all()):
        case_list.append(rec.name)
    case_list.sort()
    # Sort so that the case returned is predictable
    case_insensitive_dict = {}
    for item in case_list:
        lcitem = item.lower()
        case_insensitive_dict[lcitem] = item
    return list(case_insensitive_dict.values())
