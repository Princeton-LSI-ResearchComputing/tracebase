from django import template
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.template.defaultfilters import floatformat
from django.urls import reverse
from django.utils import dateparse
from django.utils.html import format_html_join
from django.utils.safestring import mark_safe

from DataRepo.formats.search_group import SearchGroup
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df

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
    try:
        v = indexable[i]
    except (TypeError, KeyError) as e:
        print(
            f"Warning: Lookup performed on indexable variable with value: [{indexable}] with index/key: [{i}]. ",
            f"Caught error: [{str(e)}].  Returning None.",
        )
        v = None
    return v


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
    basv = SearchGroup()
    return basv.getDownloadQryList()


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
    elif obj == "compound":
        tmplt_name = "compound_detail"
    elif obj == "infusate":
        tmplt_name = "infusate_detail"
    elif obj == "treatment":
        tmplt_name = "protocol_detail"

    if id_name_list == [None] or id_name_list is None:
        return None
    else:
        id_name_dict = {}
        for x in id_name_list:
            if x is not None and x != qs2df.null_rpl_str:
                k, v = x.split("||")
                id_name_dict[k] = v
        obj_format_html1 = format_html_join(
            ", ",
            # use defined css for "white-space: nowrap;"
            '</div><div class="nobr"><a href="{}">{}</a>',
            [
                (reverse(tmplt_name, args=[str(id)]), id_name_dict[id])
                for id in id_name_dict
            ],
        )
        # remove first "</div>" in formatted hmtl string
        obj_format_html = mark_safe(obj_format_html1[6:])
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


@register.simple_tag(
    takes_context=True
)  # Prepends context to submitted args (do not explicitly supply)
def get_template_cookie(context, template_name, cookie_name, cookie_default):
    request = context["request"]
    full_cookie_name = ".".join([template_name, cookie_name])
    result = request.COOKIES.get(full_cookie_name, cookie_default)
    if result == "__default__":
        result = cookie_default
    return result


@register.simple_tag
def get_serum_tracer_peak_groups_first_searched(qry):
    """
    Looks at the qry object to see if the first search term in the fctemplate format is "is+last", and if so, what
    should be shown (preious, last, both, or neither)
    """
    shown = "both"
    if qry:
        if "is_last" == qry["searches"]["fctemplate"]["tree"]["queryGroup"][0]["fld"]:
            if (
                qry["searches"]["fctemplate"]["tree"]["queryGroup"][0]["ncmp"]
                == "isnull"
            ):
                shown = "neither"
            elif (
                qry["searches"]["fctemplate"]["tree"]["queryGroup"][0]["ncmp"]
                == "iexact"
            ):
                if (
                    qry["searches"]["fctemplate"]["tree"]["queryGroup"][0]["val"]
                    == "true"
                ):
                    shown = "last"
                else:
                    shown = "previous"
    return shown


@register.filter
def gt(x, y):
    """
    This is here to get around htmlhint's spec-char-escape error even though {% if x > y %} works.
    """
    return x > y


@register.simple_tag
def uniquify(retval, unused):
    """
    This is an htmlhint workaround so that the ID attribute appears unique to htmlhint when an HTML element is rendered
    differently in 2 parts of a conditional, but with the same ID.  Just supply a different value to unused.
    """
    return retval


@register.simple_tag
def get_many_related_rec(qs, pk):
    """
    Takes a queryset and a value of the related table's primary key (that can be associated with the root table record
    via an added annotation controlled by the dataformat's root_annot_fld value), and returns either the one many
    related table record (that matches the primary key) in a list of size 1 or (if there was no annotated primary key
    value and an empty string was supplied instead) a list of all the records contained in the queryset (what you would
    get from qs.all()).

    Further explanation...
    If a many related table is marked in the Format with split_rows=True, this method identifies the one related
    record that is associated with the current instance of the root record (which will be a duplicate instance if
    split_rows is True), as if this was a proper SQL left join.  While django always returns every related table record
    associated with every root table record on its key path, this method essentially allows the template to reconstruct
    a full SQL joined table result by providing the many related record that was associated with the original left-join
    query, even if it was a M:M related table.  It uses an annotated version of the related table record's primary key
    that was added to the root table record using getFullJoinAnnotations().

    It returns a list in each case so that full join can be turned off and on by simply toggling the `split_rows`
    boolean value in the Format class.
    """
    if pk != "":
        try:
            recs = [qs.get(pk__exact=pk)]
        except ObjectDoesNotExist:
            recs = None
        except MultipleObjectsReturned as mor:
            raise MultipleObjectsReturned(
                "Internal error: Primary key is not unique in M:M record list. Was "
                f"`.distinct()` removed from the Prefetch queryset parameter? {mor}"
            )
    else:
        recs = qs.all()

    return recs


@register.simple_tag
def compile_stats(stats, num_chars=160):
    """
    Takes stats, which is a sorted list of dicts that each contain a val and cnt.  It creates a comma-delimited string
    of the values (annotated with their count).  It also creates a short version of the string based on the supplied
    num_chars.  If the string is longer, it truncates the string and inserts an ellipsis.
    """
    more_str = "..."
    smry = ""
    for i, val in enumerate(stats):
        smry += f"{val['val']} ({val['cnt']})"
        if i != (len(stats) - 1):
            smry += ", "
    short = smry
    if len(smry) > num_chars:
        # black and flake8 disagree on how `[0 : (num_chars - 3)]` should be spaced, so...
        num_chars -= len(more_str)
        short = str(smry[0:num_chars])
        short += more_str
    return {"full": smry, "short": short}


@register.simple_tag
def display_filter(filter):
    """
    This method is an overly simplistic placeholder until we need a more complex filter to support.  It handles a
    single filtering condition only.
    """
    if (
        "type" not in filter
        or "queryGroup" not in filter
        or filter["type"] != "group"
        or len(filter["queryGroup"]) != 1
        or filter["queryGroup"][0]["type"] != "query"
    ):
        raise NotYetImplemented(
            "The display of filtering criterial currently only handles a single condition in a group of size 1.  "
            "`customtags.display_filter` needs to handle more cases."
        )
    ncmp = filter["queryGroup"][0]["ncmp"]
    val = filter["queryGroup"][0]["val"]
    return f"{ncmp} {val}"


class NotYetImplemented(Exception):
    pass
