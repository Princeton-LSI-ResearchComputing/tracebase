from datetime import datetime
from typing import List, Optional, Union

from django import template
from django.core.exceptions import (
    MultipleObjectsReturned,
    ObjectDoesNotExist,
    FieldError,
    ObjectDoesNotExist,
    ValidationError,
)
from django.db.models import Model
from django.http import Http404
from django.template.defaultfilters import floatformat
from django.urls import reverse
from django.utils import dateparse
from django.utils.html import format_html_join
from django.utils.safestring import mark_safe

from DataRepo.formats.search_group import SearchGroup
from DataRepo.models.utilities import get_model_by_name
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df
from DataRepo.widgets import ListViewRowsPerPageSelectWidget

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


@register.filter
def multiply(left, right):
    try:
        return float(left) * float(right)
    except (ValueError, TypeError) as e:
        print(
            f"WARNING: multiplication of '{left}' * '{right}' failed. ",
            f"Caught error: [{str(e)}].  Returning '{left}'.",
        )
        return left


@register.filter
def intmultiply(left, right):
    try:
        return int(float(left) * float(right))
    except (ValueError, TypeError) as e:
        print(
            f"WARNING: multiplication of '{left}' * '{right}' failed. ",
            f"Caught error: [{str(e)}].  Returning '{left}'.",
        )
        return int(left)


# This allows indexing a list or dict
@register.filter
def index(indexable, i):
    try:
        v = indexable[i]
    except (TypeError, KeyError) as e:
        print(
            f"WARNING: Lookup performed on indexable variable with value: [{indexable}] with index/key: [{i}]. ",
            f"Caught error: [{str(e)}].  Returning None.",
        )
        v = None
    return v


# This allows indexing an attribute from an object with a variable
@register.filter
def get_attr(object, attr, default=None):
    try:
        v = getattr(object, attr, default)
    except (TypeError, KeyError) as e:
        print(f"{type(e).__name__}: {str(e)}")
        v = default
    return v


@register.filter
def has_detail_url(model_object_or_class):
    return hasattr(model_object_or_class, "get_absolute_url")


@register.filter
def get_detail_url(model_object: Model):
    url = model_object.get_absolute_url()
    if url is not None and url != "":
        return url
    return None


@register.filter
def is_model_obj(field):
    # Based on
    return isinstance(field, Model)


@register.simple_tag
def define(the_val):
    """Use this in a template to define variables. For example, use `{% define 1 as my_num_var %}` to create a variable
    named `my_num_var` whose value is 1.  You can change the value later using the same mechanism:
    `{% define 3 as my_num_var %}`, with 1 caveat...

    A value set above a `for` loop can be changed inside the loop and the code inside the loop will show that it is
    changed, but AFTER the end of the loop, the value reverts to what it was before the loop.  For example, this will
    not work as expected:
        {% define False as file_exists %}  <!-- WHEN YOU DO THIS -->
        {% for container in rec.containers.all %}  <!-- BECAUSE FOR LOOPS HAVE THEIR OWN "NAMESPACE" -->
            {% if file_exists %}  <!-- THIS WORKS -->
                <br>
            {% endif %}
            {% if container.file %}
                {% define True as file_exists %}  <!-- THIS WORKS TOO -->
            {% endif %}
        {% endfor %}
        {% if not file_exists %}  <!-- BUT THIS DOES NOT WORK AS EXPECTED - IT WILL ALWAYS BE False -->
            No file
        {% endif %}
    There is no workaround for this.  If you need to change a value inside a loop, you will not be able to do it.
    """
    return the_val


@register.filter
def getClass(state):
    styleclass = None
    if state is None:
        styleclass = ""
    elif state == "FAILED" or state == "ERROR":
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
def addToDict(theDict: dict, theKey, theVal, overwrite=False):
    if overwrite or (overwrite is False and theKey not in theDict.keys()):
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


@register.simple_tag
def obj_hyperlink(id_name_list, obj, newline=False):
    """
    returns a comma-separated list of hyperlinks.
    Notes:
    works for object_lists with defined format in Pandas DataFrames:
        obj types include study, tracer, compound, infusate, treatment
        each item of object_list contains id and name seprated by "||"
    examples:
        study list:  ['1||obob_fasted']
        tracer list: ['30||C16:0', '11||lysine']
        treatment list: [3||Ser/gly-free diet, 2||Control diet]
    For a study without treament data, value in DataFrame is [nan], which is [None] after converting
        to json record for rendering templates
    If newline=True, return html string with <div> tag and newline.
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
    else:
        return f"HTML format error: undefined object type: {obj}"

    if id_name_list == [None] or id_name_list is None:
        return None

    id_name_dict = {}
    for x in id_name_list:
        if x is not None and x != qs2df.null_rpl_str:
            k, v = x.split("||")
            if k != qs2df.na_str:
                id_name_dict[k] = v

    if newline is True:
        obj_format_html = (
            '<div class="newlines">'
            + format_html_join(
                ",\n",
                '<a href="{}">{}</a>',
                [
                    (reverse(tmplt_name, args=[str(id)]), id_name_dict[id])
                    for id in id_name_dict
                ],
            )
            + "</div>"
        )
    else:
        obj_format_html = format_html_join(
            ", ",
            '<a href="{}">{}</a>',
            [
                (reverse(tmplt_name, args=[str(id)]), id_name_dict[id])
                for id in id_name_dict
            ],
        )
    return mark_safe(obj_format_html)


@register.simple_tag
def filter_page_size_list(
    lst_of_rows_per_page_nums: List[Union[str, int]],
    num_rows_in_result: Optional[int] = None,
    selected: Optional[int] = None,
):
    """Takes a list of strings or ints (representing the number of rows to show in a page) and a number of rows in the
    results (and an optional "selected" value - of "rows per page" options) and returns a list of ints representing all
    values less than the number of rows in the results.

    Limitations:
        1. This does not ensure a non-empty list returned
    Assumptions:
        1. A value of 0 is assumed to represent "all" rows on 1 page, which takes the place of a number option larger
           than the number of results.  E.g. If there are 22 results and the options include 10, 25, and 0, 10 an 0 are
           returned.  25 is excluded.  Some implementations don't include the "all" option and instead include 1 option
           higher than the number of results.
        2. The rows per page options are sorted from smallest to largest number of rows per page (except 0, aka "ALL")
    Args:
        lst_of_rows_per_page_nums (List[str|int]): A list of numbers of rows poer page to choose from.
        num_rows_in_result (int): The actual number of rows in the results being paginated.
        selected (Optional[int]): The number of rows per page that should be selected.  (Note, this is only to add that
            option if not present in the list.)
    Exceptions:
        None
    Returns:
        relevant_page_sizes (List[int])
    """
    relevant_page_sizes = []
    # Cast every value to an int
    rows_per_page_opts = [int(rpp) for rpp in lst_of_rows_per_page_nums]

    # Assume present.  All we want to do is not add an extra for the specified rows per page.
    selected_present = True
    if selected is not None:
        selected = int(selected)
        first_greater_rpp_index = -1

        # Django won't send the page object to the template if paginate_by is 0, but we want users to be able to pare
        # down the rows per page, so when the django view sees a 0 "limit" sent in in the URL params, it just changes it
        # to the number of results in the queryset, which gives us the page object.  Here, we then change it to 0 so
        # that the select list shows "ALL" instead of the number of results per page.
        if num_rows_in_result is not None and selected == num_rows_in_result:
            selected = 0

        selected_present = selected in rows_per_page_opts

        # If the selected rows per page is not among the list of options, let's select the best place in the list to
        # insert it
        if not selected_present:
            try:
                # Get the index of the first element larger than the selected value
                first_greater_rpp_index = list(
                    rpp > selected for rpp in rows_per_page_opts
                ).index(True)
            except ValueError:
                # If 0 is at the end of the list
                if (
                    0 in rows_per_page_opts
                    and rows_per_page_opts.index(0) == len(rows_per_page_opts) - 1
                ):
                    # Set the index to the location of the zero (the last index)
                    first_greater_rpp_index = len(rows_per_page_opts) - 1
                else:
                    # We will assume 0 is at the beginning and just append since selected is larger than everything
                    rows_per_page_opts.append(selected)
                    selected_present = True

    for i, page_size in enumerate(rows_per_page_opts):
        # If the selected rows per page recorded in the page object (assuming that's what was sent in)
        if not selected_present and i == first_greater_rpp_index:
            # Add it at the index just before the first value that's larger
            relevant_page_sizes.append(selected)
        if num_rows_in_result is None or page_size <= num_rows_in_result:
            relevant_page_sizes.append(page_size)

    return relevant_page_sizes


@register.simple_tag
def get_rows_per_page_select_list(
    page_sizes: List[int], name: str, selected: int, all_label: str = "ALL"
):
    page_sizes_tuples = (
        (size, ("ALL" if int(size) == 0 else size)) for size in page_sizes
    )
    widget = ListViewRowsPerPageSelectWidget(choices=page_sizes_tuples)
    selected_label = all_label if selected == 0 else selected
    return mark_safe(widget.render(name, selected_label))


@register.filter
def convert_iso_date(value):
    if value is None:
        return None
    return dateparse.parse_datetime(value).strftime("%Y-%m-%d")


@register.filter
def format_date(date: Union[datetime, str], fmt: str):
    return dateparse.parse_datetime(str(date)).strftime(fmt) if date is not None else None


@register.simple_tag
def append_unique(lst: list, val):
    if val is not None and val not in lst:
        lst.append(val)
    return ""


@register.simple_tag
def append(lst: list, val):
    lst.append(val)
    return ""


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


# Prepends context to submitted args (do not explicitly supply)
@register.simple_tag(takes_context=True)
def get_template_cookie(context, template_name, cookie_name, cookie_default):
    request = context["request"]
    full_cookie_name = ".".join([template_name, cookie_name])
    result = request.COOKIES.get(full_cookie_name, cookie_default)
    if result == "__default__":
        result = cookie_default
    return result


# Prepends context to submitted args (do not explicitly supply)
@register.simple_tag(takes_context=True)
def get_cookie(context, name, default):
    return context["request"].COOKIES.get(name, default)


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


@register.filter
def lte(x, y):
    """
    This is here to get around htmlhint's spec-char-escape error even though {% if x < y %} works.
    """
    return x <= y


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
def get_model_rec_by_id(mdl_name, pk):
    mdl = get_model_by_name(mdl_name)
    return mdl.objects.get(pk__exact=pk)


@register.filter
def polarity_name_to_sign(name:str):
    if name is None:
        return None
    elif name.lower().startswith("p"):
        return "+"
    elif name.lower().startswith("n"):
        return "-"
    return "error"


@register.filter
def sigdig(num, digits=3) -> str:
    """Return the supplied num with the significant number of digits."""
    return f"{num:.{digits}g}"


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


@register.simple_tag
def get_basic_search_count(mdl: str, fld: str, cmp: str, val: str, fmt: str, units: Optional[str] = None):
    """Retrieve the number of records that match a basic search.

    Args:
        mdl (str): The model instance to search.  Note, this isn't necessarily (but often is) the target model name.
            This should match an outer key in the 'model_instances' attribute of the derived 'Format' class (e.g. the
            'FluxCircFormat' derived Format class).
        fld (str): The Model field name to search
        cmp (str): The search comparator (e.g. 'iexact')
        val (str): The search term
        fmt (str): search result format (key or name)
        units (Optional[str]) [None]
    Exceptions:
        Http404: When the arguments are invalid.
    Returns:
        tot (int): A count of the number of results from the search.
    """
    basv_metadata = SearchGroup()

    # Turns a format key or format name into a format key, so you can send in either (the format key or name)
    fmtkey = basv_metadata.formatNameOrKeyToKey(fmt)

    try:
        qry = basv_metadata.createNewBasicQuery(mdl, fld, cmp, val, fmtkey, units)
    except (KeyError, ObjectDoesNotExist, ValidationError, FieldError) as e:
        raise Http404(
            f"Invalid basic search - mdl: '{mdl}', fld: '{fld}', cmp: '{cmp}', val: '{val}', fmtkey: '{fmtkey}', "
            f"units: '{units}'.  Error: {type(e).__name__}: {e}"
        )

    _, tot, _ = basv_metadata.performQuery(qry)

    return tot


@register.simple_tag
def log(*args, timestamp: bool = True):
    """Print the args to the server log.  Intended for debugging template performance."""
    if timestamp:
        print(f"{datetime.now()}: ", end="")
    print(*args)


class NotYetImplemented(Exception):
    pass
