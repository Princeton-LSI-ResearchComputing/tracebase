from copy import deepcopy

from django.db.models import Q
from django.http import Http404


def extractFldPaths(qry):
    """
    Returns the fld values under the tree of the selectedtemplate.
    """
    unique_fld_paths = []
    fld_paths = extractFldPathsHelper(qry["searches"][qry["selectedtemplate"]]["tree"])

    for fld_path in fld_paths:
        if fld_path not in unique_fld_paths:
            unique_fld_paths.append(fld_path)

    return unique_fld_paths


def extractFldPathsHelper(subtree):
    """
    Recursive helper to extractFldPaths
    """
    fld_paths = []
    if subtree["type"] == "group":
        for child in subtree["queryGroup"]:
            tmp_fld_paths = extractFldPathsHelper(child)
            for fld_path in tmp_fld_paths:
                fld_paths.append(fld_path)
    elif subtree["type"] == "query":
        fld_path_name = subtree["fld"]
        fld_path, fld_name = splitPathName(fld_path_name)
        return [fld_path]
    else:
        raise Exception(
            f"Qry type: [{subtree['type']}] must be either 'group' or 'query'."
        )

    return fld_paths


def splitPathName(fld):
    """
    Removes the field name from the end of a key path.  The last __ delimited string is assumed to be a field name.
    """
    fld_path_list = fld.split("__")
    fld_name = fld_path_list.pop()
    fld_path = "__".join(fld_path_list)
    return fld_path, fld_name


def splitCommon(fld_path, reroot_path):
    """
    Returns 2 strings: the beginning portion of fld_path that it has in common with the reroot_path and the remainder
    of the fld_path.
    """
    # Initialize the list versions of the supplied paths
    fld_path_list = []
    if fld_path != "":
        fld_path_list = fld_path.split("__")
    reroot_path_list = []
    if reroot_path != "":
        reroot_path_list = reroot_path.split("__")

    # Set loop length to the length of the shorter list
    length = 0
    if len(fld_path_list) < len(reroot_path_list):
        length = len(fld_path_list)
    else:
        length = len(reroot_path_list)

    # Initialize the list versions of the paths to return and assume they start out the same
    command_path_list = []
    remaining_path_list = []
    same = True

    # For the common path positions
    for i in range(0, length):
        # If these nodes are the same
        if fld_path_list[i] == reroot_path_list[i]:
            command_path_list.append(fld_path_list[i])
        else:
            same = False
            for node in fld_path_list[i:]:
                remaining_path_list.append(node)
            break

    # If the paths were the same, and the field path was longer, finish off the remainder
    if same and len(fld_path_list) > len(reroot_path_list):
        for i in range(len(reroot_path_list), len(fld_path_list)):
            remaining_path_list.append(fld_path_list[i])

    return "__".join(command_path_list), "__".join(remaining_path_list)


def createFilterGroup(all=True, static=False):
    """
    This returns a 1-query portion of what is usually under qry[searches][<template>][tree]
    """
    val = "any"
    if all:
        val = "all"
    return {
        "type": "group",
        "val": val,
        "static": static,
        "queryGroup": [],
    }


def createFilterCondition(fld, ncmp, val, units, static=False):
    """
    This returns a 1-query portion of what is usually under qry[searches][<template>][tree]
    """
    return {
        "type": "query",
        "pos": "",
        "static": static,
        "fld": fld,
        "ncmp": ncmp,
        "val": val,
        "units": units,
    }


def appendFilterToGroup(parent, filter):
    """
    This returns a 1-query portion of what is usually under qry[searches][<template>][tree]
    """
    output_filter = deepcopy(parent)
    output_filter["queryGroup"].append(filter)
    return output_filter


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


def getFirstEmptyQuery(qry_ref):
    """
    This method takes the "tree" from a qry object (i.e. what you get from basv_metadata.getRootGroup(fmt)) and
    returns a reference to the single empty item of type query that should be present in a new rootGroup.
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


def setFirstEmptyQuery(qry_ref, fmt, fld, cmp, val, units):
    """
    This method takes the "tree" from a qry object (i.e. what you get from basv_metadata.getRootGroup(fmt)) and
    returns a reference to the single empty item of type query that should be present in a new rootGroup.
    """
    empty_qry = getFirstEmptyQuery(qry_ref["searches"][fmt]["tree"])

    if empty_qry is None:
        return None

    empty_qry["type"] = "query"
    empty_qry["pos"] = ""
    empty_qry["static"] = False
    empty_qry["fld"] = fld
    empty_qry["ncmp"] = cmp
    empty_qry["val"] = val
    empty_qry["units"] = units


def getNumEmptyQueries(qry, fmt):
    """
    Takes a qry object and a format and counts the number of empty queries for that format's search tree.
    """
    return getNumEmptyQueriesHelper(qry["searches"][fmt]["tree"])


def getNumEmptyQueriesHelper(filter):
    """
    Takes a "tree" value of 1 format from the rootGroup query object and recursively counts the number of empty
    queries.
    """
    if filter["type"] == "query":
        # If empty (i.e. val holds an empty string), return 1
        if not filter["static"] and filter["val"] == "":
            return 1
        return 0
    elif filter["type"] == "group":
        total_empty = 0
        for query in filter["queryGroup"]:
            total_empty += getNumEmptyQueriesHelper(query)
        return total_empty
    else:
        raise Exception(
            f"Invalid query type {filter['type']}.  Must be either 'query' or 'group'."
        )


def getSelectedFormat(qry):
    return qry["selectedtemplate"]


def setSelectedFormat(qry, fmt):
    qry["selectedtemplate"] = fmt
    return qry


def getSearchTree(qry, fmt):
    return qry["searches"][fmt]["tree"]


def isQuery(filter):
    return filter["type"] == "query"


def isQueryGroup(filter):
    return filter["type"] == "group"


def isAllGroup(filter):
    return filter["val"] == "all"


def isAnyGroup(filter):
    return filter["val"] == "any"


def getFilterType(filter):
    return filter["type"]


def getChildren(filter):
    return filter["queryGroup"]


def getValue(filter):
    return filter["val"]


def getUnits(filter):
    return filter["units"]


def getComparison(filter):
    return filter["ncmp"]


def getField(filter):
    return filter["fld"]


def setField(filter, fld):
    filter["fld"] = fld
    return filter


def constructAdvancedQuery(qryRoot, units_lookup=None):
    """
    Turns a qry object into a complex Q object by calling its helper and supplying the selected format's tree.
    """
    return constructAdvancedQueryHelper(
        qryRoot["searches"][qryRoot["selectedtemplate"]]["tree"],
        units_lookup,
    )


def constructAdvancedQueryHelper(qry, units_lookup=None):
    """
    Recursively build a complex Q object based on a hierarchical tree defining the search terms.
    """

    if "type" not in qry:
        print("ERROR: type missing from qry object: ", qry)

    if qry["type"] == "query":
        cmp = qry["ncmp"].replace("not_", "", 1)
        negate = cmp != qry["ncmp"]
        val = qry["val"]
        fld = qry["fld"]
        units = qry["units"]

        # Special case for isnull (ignores qry['val'])
        if cmp == "isnull":
            if negate:
                negate = False
                val = False
            else:
                val = True
        elif units_lookup:
            # If different units options exist for this field, convert the val entered into the database's native units
            if fld in units_lookup.keys() and units_lookup[fld]:
                if units not in units_lookup[fld].keys():
                    raise KeyError(
                        f"Specified units [{units}] not in the units group for field {fld}."
                    )
                elif "convert" not in units_lookup[fld][units].keys():
                    raise KeyError(
                        f"No 'convert' function set in the units referenced by field {fld}."
                    )
                elif type(units_lookup[fld][units]["convert"]).__name__ != "function":
                    raise KeyError(
                        f"No 'convert' function set in the units referenced by field {fld}."
                    )
                val = units_lookup[fld][units]["convert"](val)

        criteria = {"{0}__{1}".format(fld, cmp): val}
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
                nq = constructAdvancedQueryHelper(elem, units_lookup)
                if nq is None:
                    return None
                else:
                    q &= nq
            elif qry["val"] == "any":
                nq = constructAdvancedQueryHelper(elem, units_lookup)
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
