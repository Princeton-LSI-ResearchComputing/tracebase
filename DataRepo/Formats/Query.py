from copy import deepcopy


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


def createFilterCondition(fld, ncmp, val, static=False):
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
    }


def appendFilterToGroup(parent, filter):
    """
    This returns a 1-query portion of what is usually under qry[searches][<template>][tree]
    """
    output_filter = deepcopy(parent)
    output_filter["queryGroup"].append(filter)
    return output_filter
