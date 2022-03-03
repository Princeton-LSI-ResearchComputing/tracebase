import json

from django.apps import apps
from django.db.models import Prefetch, Q
from django.http import Http404

from DataRepo.compositeviews import BaseAdvancedSearchView


def getAllBrowseData(
    format, basv, limit=None, offset=0, order_by=None, order_direction=None
):
    """
    Grabs all data without a filtering match for browsing.
    """
    return performQuery(None, format, basv, limit, offset, order_by, order_direction)


def performQuery(
    qry=None,
    fmt=None,
    basv=None,
    limit=None,
    offset=0,
    order_by=None,
    order_direction=None,
):
    """
    Executes an advanced search query.  The only required input is either a qry object or a format (fmt).
    """
    results = None
    cnt = 0
    q_exp = None

    if qry is not None:
        q_exp = constructAdvancedQuery(qry)
        if fmt is not None and fmt != qry["selectedtemplate"]:
            raise Exception(
                f"The selected format in the qry object: [{qry['selectedtemplate']}] does not match the supplied "
                f"format: [{fmt}]"
            )
        else:
            fmt = qry["selectedtemplate"]
    elif fmt is None:
        raise Exception(
            "Neither a qry object nor a format was supplied.  1 of the 2 is required."
        )

    if basv is None:
        basv = BaseAdvancedSearchView()

    if fmt in basv.getFormatNames().keys():

        # If the Q expression is None, get all, otherwise filter
        if q_exp is None:
            results = basv.getRootQuerySet(fmt).distinct()
        else:
            results = basv.getRootQuerySet(fmt).filter(q_exp).distinct()

        # Count the total results.  Limit/offset are only used for paging.
        cnt = results.count()

        # Order by
        if order_by is not None:
            if order_direction is not None:
                if order_direction == "desc":
                    order_by = f"-{order_by}"
                elif order_direction and order_direction != "asc":
                    raise Exception(
                        f"Invalid order direction: {order_direction}.  Must be 'asc' or 'desc'."
                    )
            results = results.order_by(order_by)

        # Limit
        if limit is not None:
            start_index = offset
            end_index = offset + limit
            results = results[start_index:end_index]

        # If prefetches have been defined in the base advanced search view
        if qry is None:
            prefetches = basv.getPrefetches(fmt)
        else:
            # Retrieve the prefetch data
            prefetch_qrys = basv.getTrueJoinPrefetchPathsAndQrys(qry, fmt)

            # Build the prefetches, including subqueries for M:M related tables to produce a "true join" if a search
            # term is from a M:M related model
            prefetches = []
            for pfq in prefetch_qrys:
                # Rerooted subquery prefetches are in a list whereas regular prefetches that get everything are just a
                # string
                if isinstance(pfq, list):
                    pf_path = pfq[0]
                    pf_qry = pfq[1]
                    pf_mdl = pfq[2]

                    # Construct a new Q expression using the rerooted query
                    pf_q_exp = constructAdvancedQuery(pf_qry)

                    # grab the model using its name
                    mdl = apps.get_model("DataRepo", pf_mdl)

                    # Create the subquery queryset
                    pf_qs = mdl.objects.filter(pf_q_exp).distinct()

                    # Append a prefetch object with the subquery queryset
                    prefetches.append(Prefetch(pf_path, queryset=pf_qs))
                else:
                    prefetches.append(pfq)

        if prefetches is not None:
            results = results.prefetch_related(*prefetches)

    else:
        # Log a warning
        print("WARNING: Invalid selected format:", fmt)

    return results, cnt


def isQryObjValid(qry, form_class_list):
    """
    Determines if an advanced search qry object was properly constructed/populated (only at the root).
    """

    if (
        type(qry) is dict
        and "selectedtemplate" in qry
        and "searches" in qry
        and len(form_class_list) == len(qry["searches"].keys())
    ):
        for key in form_class_list:
            if (
                key not in qry["searches"]
                or type(qry["searches"][key]) is not dict
                or "tree" not in qry["searches"][key]
                or "name" not in qry["searches"][key]
            ):
                return False
        return True
    else:
        return False


def isValidQryObjPopulated(qry):
    """
    Checks whether a query object is fully populated with at least 1 search term.
    """
    selfmt = qry["selectedtemplate"]
    if len(qry["searches"][selfmt]["tree"]["queryGroup"]) == 0:
        return False
    else:
        return isValidQryObjPopulatedHelper(
            qry["searches"][selfmt]["tree"]["queryGroup"]
        )


def isValidQryObjPopulatedHelper(group):
    for query in group:
        if query["type"] == "query":
            if not query["val"] or query["val"] == "":
                return False
        elif query["type"] == "group":
            if len(query["queryGroup"]) == 0:
                return False
            else:
                tmp_populated = isValidQryObjPopulatedHelper(query["queryGroup"])
                if not tmp_populated:
                    return False
    return True


def constructAdvancedQuery(qryRoot):
    """
    Turns a qry object into a complex Q object by calling its helper and supplying the selected format's tree.
    """

    return constructAdvancedQueryHelper(
        qryRoot["searches"][qryRoot["selectedtemplate"]]["tree"]
    )


def constructAdvancedQueryHelper(qry):
    """
    Recursively build a complex Q object based on a hierarchical tree defining the search terms.
    """

    if "type" not in qry:
        print("ERROR: type missing from qry object: ", qry)
    if qry["type"] == "query":
        cmp = qry["ncmp"].replace("not_", "", 1)
        negate = cmp != qry["ncmp"]

        # Special case for isnull (ignores qry['val'])
        if cmp == "isnull":
            if negate:
                negate = False
                qry["val"] = False
            else:
                qry["val"] = True

        criteria = {"{0}__{1}".format(qry["fld"], cmp): qry["val"]}
        if negate is False:
            return Q(**criteria)
        else:
            return ~Q(**criteria)

    elif qry["type"] == "group":
        q = Q()
        gotone = False
        for elem in qry["queryGroup"]:
            gotone = True
            if qry["val"] == "all":
                nq = constructAdvancedQueryHelper(elem)
                if nq is None:
                    return None
                else:
                    q &= nq
            elif qry["val"] == "any":
                nq = constructAdvancedQueryHelper(elem)
                if nq is None:
                    return None
                else:
                    q |= nq
            else:
                return None
        if not gotone or q is None:
            return None
        else:
            return q
    return None


def createNewBasicQuery(basv_metadata, mdl, fld, cmp, val, fmt):
    """
    Constructs a new qry object for an advanced search from basic search input.
    """

    qry = basv_metadata.getRootGroup(fmt)

    try:
        mdl = basv_metadata.getModelInstance(fmt, mdl)
    except KeyError as ke:
        raise Http404(ke)

    sfields = basv_metadata.getSearchFields(fmt, mdl)

    if fld not in sfields:
        raise Http404(
            f"Field [{fld}] is not searchable.  Must be one of [{','.join(sfields.keys())}]."
        )

    num_empties = basv_metadata.getNumEmptyQueries(qry["searches"][fmt]["tree"])
    if num_empties != 1:
        raise Http404(
            f"The static filter for format {fmt} is improperly configured. It must contain exactly 1 empty query."
        )

    empty_qry = getFirstEmptyQuery(qry["searches"][fmt]["tree"])

    empty_qry["type"] = "query"
    empty_qry["pos"] = ""
    empty_qry["static"] = False
    empty_qry["fld"] = sfields[fld]
    empty_qry["ncmp"] = cmp
    empty_qry["val"] = val

    dfld, dval = searchFieldToDisplayField(basv_metadata, mdl, fld, val, qry)

    if dfld != fld:
        # Set the field path for the display field
        empty_qry["fld"] = sfields[dfld]
        empty_qry["val"] = dval

    return qry


def getFirstEmptyQuery(qry_ref):
    """
    This method takes the "tree" from a qry object (i.e. what you get from basv_metadata.getRootGroup(fmt)) and returns
    a reference to the single empty item of type query that should be present in a new rootGroup.
    """
    if qry_ref["type"] and qry_ref["type"] == "query":
        if qry_ref["val"] == "":
            return qry_ref
        return None
    elif qry_ref["type"] and qry_ref["type"] == "group":
        immutable = qry_ref["static"]
        if len(qry_ref["queryGroup"]) > 0:
            for qry in qry_ref["queryGroup"]:
                emptyqry = getFirstEmptyQuery(qry)
                if emptyqry:
                    if immutable:
                        raise Http404(
                            "Group containing empty query must not be static."
                        )
                    return emptyqry
        return None
    raise Http404("Type not found.")


def searchFieldToDisplayField(basv_metadata, mdl, fld, val, qry):
    """
    Takes a field from a basic search and converts it to a non-hidden field for an advanced search select list.
    """

    dfld = fld
    dval = val
    fmt = qry["selectedtemplate"]
    dfields = basv_metadata.getDisplayFields(fmt, mdl)
    if fld in dfields.keys() and dfields[fld] != fld:
        # If fld is not a displayed field, perform a query to convert the undisplayed field query to a displayed query
        recs, tot = performQuery(qry, fmt, basv_metadata)
        if tot == 0:
            print(
                f"WARNING: Failed basic/advanced {fmt} search conversion: {qry}. No records found matching {mdl}."
                f"{fld}='{val}'."
            )
            raise Http404(f"No records found matching [{mdl}.{fld}={val}].")
        # Set the field path for the display field
        dfld = dfields[fld]
        dval = getJoinedRecFieldValue(
            recs, basv_metadata, fmt, mdl, dfields[fld], fld, val
        )

    return dfld, dval


# Warning, the code in this method would potentially not work in cases where multiple search terms (including a term
# from a m:m related table) were or'ed together.  This cannot happen currently because this is only utilized for
# handoff fields from search_basic, so the first record is guaranteed to have a matching value from the search term.
def getJoinedRecFieldValue(recs, basv_metadata, fmt, mdl, dfld, sfld, sval):
    """
    Takes a queryset object and a model.field and returns its value.
    """

    if len(recs) == 0:
        raise Http404("Records not found.")

    kpl = basv_metadata.getKeyPathList(fmt, mdl)
    ptr = recs[0]
    # This loop climbs through each key in the key path, maintaining a pointer to the current model
    for key in kpl:
        # If this is a many-to-many model
        if ptr.__class__.__name__ == "ManyRelatedManager":
            tmprecs = ptr.all()
            ptr = getattr(tmprecs[0], key)
        else:
            ptr = getattr(ptr, key)

    # Now find the value of the display field that corresponds to the value of the search field
    gotit = True
    if ptr.__class__.__name__ == "ManyRelatedManager":
        tmprecs = ptr.all()
        gotit = False
        for tmprec in tmprecs:
            # If the value of this record for the searched field matches the search term
            tsval = getattr(tmprec, sfld)
            if str(tsval) == str(sval):
                # Return the value of the display field
                dval = getattr(tmprec, dfld)
                gotit = True
    else:
        dval = getattr(ptr, dfld)

    if not gotit:
        print(
            f"ERROR: Values retrieved for search field {mdl}.{sfld} using search term: {sval} did not match."
        )
        raise Http404(
            f"ERROR: Unable to find a value for [{mdl}.{sfld}] that matches the search term.  Unable to "
            f"convert to the handoff field {dfld}."
        )

    return dval


def formsetsToDict(rawformset, form_classes):
    """
    Takes a series of forms and a list of form fields and uses the pos field to construct a hierarchical qry tree.
    """

    # All forms of each type are all submitted together in a single submission and are duplicated in the rawformset
    # dict.  We only need 1 copy to get all the data, so we will arbitrarily use the first one

    # Figure out which form class processed the forms (inferred by the presence of 'saved_data' - this is also the
    # selected format)
    processed_formkey = None
    for key in rawformset.keys():
        # We need to identify the form class that processed the form to infer the selected output format.  We do that
        # by checking the dictionary of each form class's first form for evidence that it processed the forms, i.e. the
        # presence of the "saved_data" class data member which is created upon processing.
        if "saved_data" in rawformset[key][0].__dict__:
            processed_formkey = key
            break

    # If we were unable to locate the selected output format (i.e. the copy of the formsets that were processed)
    if processed_formkey is None:
        raise Http404(
            f"Unable to find the saved form-processed data among formats: {','.join(rawformset.keys())}."
        )

    return formsetToDict(rawformset[processed_formkey], form_classes)


def formsetToDict(rawformset, form_classes):
    """
    Helper for formsetsToDict that handles only the forms belonging to the selected output format.
    """

    search = {"selectedtemplate": "", "searches": {}}

    # We take a raw form instead of cleaned_data so that form_invalid will repopulate the bad form as-is
    isRaw = False
    try:
        formset = rawformset.cleaned_data
    except AttributeError:
        isRaw = True
        formset = rawformset

    for rawform in formset:

        if isRaw:
            form = rawform.saved_data
        else:
            form = rawform

        path = form["pos"].split(".")

        [format, formatName, selected] = rootToFormatInfo(path.pop(0))
        rootinfo = path.pop(0)

        # If this format has not yet been initialized
        if format not in search["searches"]:
            search["searches"][format] = {}
            search["searches"][format]["tree"] = {}
            search["searches"][format]["name"] = formatName

            # Initialize the root of the tree
            [pos, gtype, static] = pathStepToPosGroupType(rootinfo)
            aroot = search["searches"][format]["tree"]
            aroot["pos"] = ""
            aroot["type"] = "group"
            aroot["val"] = gtype
            aroot["static"] = static
            aroot["queryGroup"] = []
            curqry = aroot["queryGroup"]
        else:
            # The root already exists, so go directly to its child list
            curqry = search["searches"][format]["tree"]["queryGroup"]

        if selected is True:
            search["selectedtemplate"] = format

        for spot in path:
            [pos, gtype, static] = pathStepToPosGroupType(spot)
            while len(curqry) <= pos:
                curqry.append({})
            if gtype is not None:
                # This is a group
                # If the inner node was not already set
                if not curqry[pos]:
                    curqry[pos]["pos"] = ""
                    curqry[pos]["type"] = "group"
                    curqry[pos]["val"] = gtype
                    curqry[pos]["static"] = static
                    curqry[pos]["queryGroup"] = []
                # Move on to the next node in the path
                curqry = curqry[pos]["queryGroup"]
            else:
                # This is a query

                # Keep track of keys encountered
                keys_seen = {}
                for key in form_classes[format].form.base_fields.keys():
                    keys_seen[key] = 0
                cmpnts = []

                curqry[pos]["type"] = "query"

                # Set the form values in the query based on the form elements
                for key in form.keys():
                    # Remove "form-#-" from the form element ID
                    cmpnts = key.split("-")
                    keyname = cmpnts[-1]
                    keys_seen[key] = 1
                    if keyname == "pos":
                        curqry[pos][key] = ""
                    elif keyname == "static":
                        if form[key] == "true":
                            curqry[pos][key] = True
                        else:
                            curqry[pos][key] = False
                    elif key not in curqry[pos]:
                        curqry[pos][key] = form[key]
                    else:
                        # Log a warning
                        print(
                            f"WARNING: Unrecognized form element not set at pos {pos}: {key} to {form[key]}"
                        )

                # Now initialize anything missing a value to an empty string
                # This is used to correctly reconstruct the user's query upon form_invalid
                for key in form_classes[format].form.base_fields.keys():
                    if keys_seen[key] == 0:
                        curqry[pos][key] = ""
    return search


def pathStepToPosGroupType(spot):
    """
    Takes a substring from a pos field defining a single tree node and returns its position and group type (if it's an
    inner node).  E.g. "0-all"
    """

    pos_gtype_stc = spot.split("-")
    if len(pos_gtype_stc) == 3:
        pos = pos_gtype_stc[0]
        gtype = pos_gtype_stc[1]
        if pos_gtype_stc[2] == "true":
            static = True
        else:
            static = False
    elif len(pos_gtype_stc) == 2:
        pos = pos_gtype_stc[0]
        gtype = pos_gtype_stc[1]
        static = False
    else:
        pos = spot
        gtype = None
        static = False
    pos = int(pos)
    return [pos, gtype, static]


def rootToFormatInfo(rootInfo):
    """
    Takes the first substring from a pos field defining the root node and returns the format code, format name, and
    whether it is the selected format.
    """

    val_name_sel = rootInfo.split("-")
    sel = False
    name = ""
    if len(val_name_sel) == 3:
        val = val_name_sel[0]
        name = val_name_sel[1]
        if val_name_sel[2] == "selected":
            sel = True
    elif len(val_name_sel) == 2:
        val = val_name_sel[0]
        name = val_name_sel[1]
    else:
        print("WARNING: Unable to parse format name from submitted form data.")
        val = val_name_sel
        name = val_name_sel
    return [val, name, sel]


def getDownloadQryList():
    """
    Returns a list of dicts where the keys are name and json and the values are the format name and the json-
    stringified qry object with the target format selected
    """
    basv_metadata = BaseAdvancedSearchView()
    qry_list = []
    for format, name in basv_metadata.getFormatNames().items():
        qry_list.append(
            {"name": name, "json": json.dumps(basv_metadata.getRootGroup(format))}
        )
    return qry_list
