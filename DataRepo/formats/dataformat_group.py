import json
from copy import deepcopy
from typing import Dict

from django.db.models import Prefetch
from django.db.utils import ProgrammingError
from django.http import Http404

from DataRepo.formats.dataformat import Format
from DataRepo.formats.dataformat_group_query import (
    constructAdvancedQuery,
    getNumEmptyQueries,
    getSelectedFormat,
    setFirstEmptyQuery,
)
from DataRepo.models.utilities import get_model_by_name


class FormatGroup:
    """
    This class groups all search output formats in a single class and adds metadata that applies to all search output
    formats as a whole.  It includes all derived classes of Format.
    """

    modes = ["search", "browse"]
    default_mode = "search"
    default_format = None
    modeldata: Dict[int, Format] = {}

    def addFormats(self, format_classes):
        """
        Add formats and set the default to the first class, unless default already set.
        """
        for cls in format_classes:
            self.modeldata[cls.id] = cls
        if self.default_format is None:
            self.default_format = format_classes[0].id

    def setDefaultFormat(self, format_class):
        self.default_format = format_class.id

    def setDefaultMode(self, mode):
        if mode not in self.modes:
            raise Exception(
                f"Invalid mode: {mode}.  Must be one of: [{', '.join(self.modes)}]"
            )
        self.default_mode = mode

    def getRootGroup(self, selfmt=None):
        """
        This method builds a fresh qry object (aka "rootGroup"). This is the object that defines an advanced search and
        is modified via javascript as the user composes a search.  It is passed back and forth via json.  The starting
        structure is:
            rootGroup = {
              selectedtemplate: 'pgtemplate',
              searches: {
                pgtemplate: {
                  name: 'PeakGroups',
                  tree: {
                    type: 'group',
                    val: 'all',
                    static: false,
                    queryGroup: [...]
                  }
                },
                ...
              }
            }
        The value of the tree item is defined by the static_filter in each derived class and that is what must contain
        an empty query.  At least 1 empty query is required in every item of type group, because the starting search
        form must contain a spot for the user to enter their search.
        """
        if selfmt is None:
            selfmt = self.default_format
        if selfmt not in self.modeldata.keys():
            print(
                f"WARNING: Unknown format: [{selfmt}]. Falling back to default format: [{self.default_format}]"
            )
            selfmt = self.default_format
        rootGroup = {
            "selectedtemplate": selfmt,
            "searches": {},
        }
        for format in self.modeldata.keys():
            rootGroup["searches"][format] = {}
            rootGroup["searches"][format]["name"] = self.modeldata[format].name
            if self.staticFilterIsValid(self.modeldata[format].static_filter):
                rootGroup["searches"][format]["tree"] = deepcopy(
                    self.modeldata[format].static_filter
                )
            else:
                print(
                    f"ERROR: No empty queries in format {format}: ",
                    self.modeldata[format].static_filter,
                )
                raise Exception(
                    f"Static filter for format {format} must contain at least 1 non-static empty query."
                )
        return rootGroup

    def staticFilterIsValid(self, filter):
        """
        Takes a "tree" value of 1 format from the rootGroup query object and raises an exception for missing keys or
        invalid values in the root query and calls staticFilterIsValidHelper to recursively validate the treee.
        """

        if (
            "type" not in filter
            or filter["type"] != "group"
            or "queryGroup" not in filter
            or len(filter["queryGroup"]) == 0
        ):
            raise Exception(
                "Invalid root query group.  Must be of type 'group' and contain a populated queryGroup array."
            )
        else:
            num_nonstatic = self.getNumNonStaticGroups(filter)
            if num_nonstatic == 0:
                raise Exception(
                    "Invalid root query group.  There must exist at least 1 non-static query group."
                )
            return self.staticFilterIsValidHelper(filter)

    def staticFilterIsValidHelper(self, filter):
        """
        Raises an exception for missing keys or invalid values and returns true if at least 1 empty query exists among
        all recursively checked objects of type query.
        """
        # Validate the keys present in both query and group types
        if "type" not in filter or "val" not in filter or "static" not in filter:
            raise Exception(
                "Static filter is missing 1 or more required keys: [type, val, static]."
            )
        elif filter["type"] == "query":
            # Validate the keys of the query
            if "ncmp" not in filter or "pos" not in filter or "fld" not in filter:
                raise Exception(
                    "Missing keys in query.  At least 1 of the following keys is missing: [fld, ncmp, pos]."
                )
            # If empty (i.e. val holds an empty string), return true
            if not filter["static"] and filter["val"] == "":
                return True
            return False
        elif filter["type"] == "group":
            # Validate the keys & values of the group
            if (
                "queryGroup" not in filter
                or len(filter["queryGroup"]) == 0
                or (filter["val"] != "all" and filter["val"] != "any")
            ):
                raise Exception(
                    "Invalid group.  Must contain a queryGroup key with a populated array and val must be either "
                    "'all' or 'any'."
                )
            empty_exists = False
            for query in filter["queryGroup"]:
                if self.staticFilterIsValidHelper(query):
                    empty_exists = True
            return empty_exists
        else:
            raise Exception(
                f"Invalid query type {filter['type']}.  Must be either 'query' or 'group'."
            )

    def getNumNonStaticGroups(self, filter):
        """
        Takes a "tree" value of 1 format from the rootGroup query object and recursively counts the number of nonstatic
        groups.
        """
        if filter["type"] == "query":
            return 0
        elif filter["type"] == "group":
            total_nonstatic = 0
            if not filter["static"]:
                total_nonstatic = 1
            for query in filter["queryGroup"]:
                total_nonstatic += self.getNumNonStaticGroups(query)
            return total_nonstatic
        else:
            raise Exception(
                f"Invalid query type {filter['type']}.  Must be either 'query' or 'group'."
            )

    def getRootQuerySet(self, format):
        """
        Calls getRootQuerySet of the supplied format.
        """
        return self.modeldata[format].getRootQuerySet()

    def getPrefetches(self, format):
        """
        Calls getPrefetches of the supplied ID of the search output format class.
        """
        return self.modeldata[format].getPrefetches()

    def getTrueJoinPrefetchPathsAndQrys(self, qry, format=None):
        """
        Calls getTrueJoinPrefetchPathsAndQrys of the supplied ID of the search output format class.
        """
        selfmt = getSelectedFormat(qry)
        if format is not None and format != selfmt:
            raise Exception(
                f"Supplied format: [{format}] does not match the qry selected format: [{selfmt}]"
            )
        elif format is None:
            format = selfmt
        return self.modeldata[format].getTrueJoinPrefetchPathsAndQrys(qry)

    def getSearchFieldChoices(self, format):
        """
        Calls getSearchFieldChoices of the supplied ID of the search output format class.
        """
        return self.modeldata[format].getSearchFieldChoices()

    def getSearchFieldChoicesDict(self):
        """
        Creates a format-keyed dict of the fld choices tuples.  Used to populate the fld select list to
        address issue #229 in the same way ncmp_choices is populated and pared down by javascript.
        """
        fld_choices = {}
        for fmtid in self.modeldata.keys():
            fld_choices[fmtid] = self.modeldata[fmtid].getSearchFieldChoices()
        return fld_choices

    def getAllSearchFieldChoices(self):
        """
        Creates a flat tuple of every field in every model of every derived class.  Used to initially populate the fld
        select list that is updated by javascript based on the selected format.  This initial population in the form
        class is important for form validation.  Otherwise, nothing but the selected format will validate, causing
        issues like the one described in #229.
        """
        all_fld_choices = ()
        seen = []
        for fmtid in self.modeldata.keys():
            for (fld_val, fld_name) in self.getSearchFieldChoices(fmtid):
                if fld_val not in seen:
                    seen.append(fld_val)
                    all_fld_choices = all_fld_choices + ((fld_val, fld_name),)
        return all_fld_choices

    def getFieldUnitsLookup(self, fmt):
        return self.modeldata[fmt].getFieldUnitsLookup()

    def getFieldUnitsDict(self):
        """
        Creates a format-keyed dict of the unit choices data, including the tuples used to create a select list.  This
        is used to populate the units select list based on the selected field.

        The returned data structure looks like this:

        format: {
            path__field: {
                "units": unit_options_key,  # (identity, postgres_interval)
                "default": field_default,
                "choices": list of tuples,  # to be used in populating a select list
                "metadata": {
                    units_sel_list_value: {
                        "example": example,
                        "about": about,
                    },
                },
            },
        }
        """

        fld_units = {}
        for fmtid in self.modeldata.keys():
            fld_units[fmtid] = self.modeldata[fmtid].getFieldUnitsDict()

        return fld_units

    def getAllFieldUnitsChoices(self):
        """
        Calls getAllFieldUnitsChoices of the default output format class.

        All units options are the same for every Format class contained in this class, so we only need to call one.
        """
        return self.modeldata[self.default_format].getAllFieldUnitsChoices()

    def getComparisonChoices(self):
        """
        Calls getComparisonChoices of the default output format class.

        All ncmp_choices are the same for every Format class contained in this class, so it doesn't matter which one we
        use.
        """
        return self.modeldata[self.default_format].getComparisonChoices()

    def getAllComparisonChoices(self):
        """
        Calls getAllComparisonChoices of the default output format class.

        All ncmp_choices are the same for every Format class contained in this class, so we only need to call one.
        """
        return self.modeldata[self.default_format].getAllComparisonChoices()

    def getModelInstances(self, format):
        """
        Calls getModelInstances of the supplied ID of the search output format class.
        """
        return self.modeldata[format].getModelInstances()

    def getModelInstance(self, format, mdl):
        """
        Calls getModelInstance of the supplied ID of the search output format class.
        """
        return self.modeldata[format].getModelInstance(mdl)

    def getFormatNames(self):
        """
        Returns a dict of search output format class IDs to their user-facing names.
        """
        namedict = {}
        for fmtid in self.modeldata.keys():
            namedict[fmtid] = str(self.modeldata[fmtid].name)
        return namedict

    def getFieldTypes(self):
        """
        Returns a dict of fmt -> path__field -> {type -> field_type (number, string, enumeration), choices -> list of
        tuples}.
        """
        typedict = {}
        for fmtid in self.modeldata.keys():
            typedict[fmtid] = self.modeldata[fmtid].getFieldTypes()
        return typedict

    def getSearchFields(self, fmt, mdl):
        """
        Takes a format key and model and returns a dict of searchable field name -> field key path
        """
        return self.modeldata[fmt].getSearchFields(mdl)

    def getDisplayFields(self, fmt, mdl):
        """
        Takes a format key and model and returns a dict of field name -> display field name (if there exists a handoff
        from a non-displayed field to a displayed one)
        """
        return self.modeldata[fmt].getDisplayFields(mdl)

    def getKeyPathList(self, fmt, mdl):
        """
        Calls getKeyPathList of the supplied ID of the search output format class.
        """
        return self.modeldata[fmt].getKeyPathList(mdl)

    def formatNameOrKeyToKey(self, fmtsubmitted):
        """
        Takes a search output format ID or name and returns the corresponding search output format
        ID.  This method exists to facilitate the usage of (case-insensitive) format names in search_basic URLs.
        """

        fmtkey = fmtsubmitted
        names = self.getFormatNames()
        foundit = False
        if fmtsubmitted in names:
            foundit = True
        else:
            for fmtid, fmtnm in names.items():
                if fmtnm.lower() == fmtsubmitted.lower():
                    fmtkey = fmtid
                    foundit = True
                    break
        if foundit is False:
            return None
        return fmtkey

    def reRootQry(self, fmt, qry, new_root_model_instance_name):
        return self.modeldata[fmt].reRootQry(qry, new_root_model_instance_name)

    def getDistinctFields(
        self, fmt, order_by=None, assume_distinct=True, split_all=False
    ):
        return self.modeldata[fmt].getDistinctFields(
            order_by, assume_distinct, split_all
        )

    def getFullJoinAnnotations(self, fmt):
        return self.modeldata[fmt].getFullJoinAnnotations()

    def getStatsParams(self, fmt):
        return self.modeldata[fmt].getStatsParams()

    def statsAvailable(self, fmt):
        return self.modeldata[fmt].statsAvailable()

    def meetsAllConditionsByValList(self, fmt, rootrec, query, field_order):
        """
        This is a python-code version of a complex Q expression, necessary for checking filters in aggregate count
        annotations, because the Django ORM does not support .distinct(fields).annotate(Count) when duplicate root
        table records exist.
        """
        return self.modeldata[fmt].meetsAllConditionsByValList(
            rootrec, query, field_order
        )

    def searchFieldToDisplayField(self, mdl, fld, val, qry):
        """
        Takes a field from a basic search and converts it to a non-hidden field for an advanced search select list.
        """

        dfld = fld
        dval = val
        fmt = getSelectedFormat(qry)
        dfields = self.getDisplayFields(fmt, mdl)
        if fld in dfields.keys() and dfields[fld] != fld:
            # If fld is not a displayed field, perform a query to convert the undisplayed field query to a displayed
            # query
            recs, tot, stats = self.performQuery(qry, fmt)
            if tot == 0:
                print(
                    f"WARNING: Failed search to display field {fmt} search conversion of query object for "
                    f"field=value: [{fld}='{val}']. No records found matching {mdl}."
                )
                raise Http404(f"No records found matching [{mdl}.{fld}={val}].")
            # Set the field path for the display field
            dfld = dfields[fld]
            dval = self.getJoinedRecFieldValue(recs, fmt, mdl, dfields[fld], fld, val)

        return dfld, dval

    # Warning, the code in this method would potentially not work in cases where multiple search terms (including a term
    # from a m:m related table) were or'ed together.  This cannot happen currently because this is only utilized for
    # handoff fields from search_basic, so the first record is guaranteed to have a matching value from the search term.
    def getJoinedRecFieldValue(self, recs, fmt, mdl, dfld, sfld, sval):
        """
        Takes a queryset object and a model.field and returns its value.
        """

        if len(recs) == 0:
            raise Http404("Records not found.")

        kpl = self.getKeyPathList(fmt, mdl)
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

    def getAllBrowseData(
        self,
        format,
        limit=None,
        offset=0,
        order_by=None,
        order_direction=None,
        generate_stats=False,
    ):
        """
        Grabs all data without a filtering match for browsing.
        """
        return self.performQuery(
            None, format, limit, offset, order_by, order_direction, generate_stats
        )

    def performQuery(
        self,
        qry=None,
        fmt=None,
        limit=None,
        offset=0,
        order_by=None,
        order_direction=None,
        generate_stats=False,
    ):
        """
        Executes an advanced search query.  The only required input is either a qry object or a format (fmt).
        """
        results = None
        cnt = 0
        q_exp = None

        if qry is not None:
            selfmt = getSelectedFormat(qry)
            units_lookup = self.getFieldUnitsLookup(selfmt)
            q_exp = constructAdvancedQuery(qry, units_lookup)
            if fmt is not None and fmt != selfmt:
                raise Exception(
                    f"The selected format in the qry object: [{selfmt}] does not match the supplied format: [{fmt}]"
                )
            else:
                fmt = selfmt
        elif fmt is None:
            raise Exception(
                "Neither a qry object nor a format was supplied.  1 of the 2 is required."
            )

        if fmt not in self.getFormatNames().keys():
            raise KeyError("Invalid selected format: {fmt}")

        # If the Q expression is None, get all, otherwise filter
        if q_exp is None:
            results = self.getRootQuerySet(fmt)
        else:
            results = self.getRootQuerySet(fmt).filter(q_exp)

        # Get stats before applying order by and distinct so that unsplit rows can be accurately counted by making all
        # M:M related tables distinct
        stats = {
            "data": {},
            "populated": generate_stats,
            "show": False,
            "available": self.statsAvailable(fmt),
        }
        if generate_stats:
            stats["data"] = self.getQueryStats(results, fmt)
            stats["show"] = True

        # Order by
        if order_by is not None:
            order_by_arg = order_by
            if order_direction is not None:
                if order_direction == "desc":
                    order_by_arg = f"-{order_by}"
                elif order_direction and order_direction != "asc":
                    raise Exception(
                        f"Invalid order direction: {order_direction}.  Must be 'asc' or 'desc'."
                    )
            results = results.order_by(order_by_arg)

        # This ensures the number of records matches the number of rows desired in the html table based on the
        # split_rows values configured in each format in SearchGroup
        distinct_fields = self.getDistinctFields(fmt, order_by)
        results = results.distinct(*distinct_fields)

        # Count the total results after employing distinct.  Limit/offset are only used for paging.
        cnt = results.count()

        # Limit
        if limit is not None:
            start_index = offset
            end_index = offset + limit
            results = results[start_index:end_index]

        # If prefetches have been defined in the base advanced search view
        if qry is None:
            prefetches = self.getPrefetches(fmt)
        else:
            # Retrieve the prefetch data
            prefetch_qrys = self.getTrueJoinPrefetchPathsAndQrys(qry, fmt)

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
                    pf_units_lookup = pfq[3]

                    # Construct a new Q expression using the rerooted query
                    pf_q_exp = constructAdvancedQuery(pf_qry, pf_units_lookup)

                    # grab the model using its name
                    mdl = get_model_by_name(pf_mdl)

                    # Create the subquery queryset
                    pf_qs = mdl.objects.filter(pf_q_exp).distinct()

                    # Append a prefetch object with the subquery queryset
                    prefetches.append(Prefetch(pf_path, queryset=pf_qs))
                else:
                    prefetches.append(pfq)

        if prefetches is not None:
            results = results.prefetch_related(*prefetches)

        split_row_annotations = self.getFullJoinAnnotations(fmt)
        for annotation in split_row_annotations:
            results = results.annotate(**annotation)

        return results, cnt, stats

    def getQueryStats(self, res, fmt):
        """
        This method takes a queryset (produced by performQuery) and a format (e.g. "pgtemplate") and returns a stats
        dict keyed on the stat name and containing the counts of the number of unique values for the fields defined in
        the basic advanced search view object for the supplied template.  E.g. The results contain 5 distinct tissues.
        """
        # Obtain the metadata about what stats we will display
        params_arrays = self.getStatsParams(fmt)
        if params_arrays is None:
            return None

        # Since `results.values(*distinct_fields).annotate(cnt=Count("pk"))` throws an exception if duplicate records
        # result from the possible use of distinct(fields) in the res sent in, we must mix in the distinct fields that
        # uniquely identify records.
        stats_fields = [fld for d in params_arrays for fld in d["distincts"]]

        # These are the distinct fields that that dictate the number of rows in the view's output table
        fmt_distinct_fields = self.getDistinctFields(fmt, assume_distinct=False)
        # These are the distinct fields necessary to get an accurate count of unique values
        all_distinct_fields = self.getDistinctFields(
            fmt, assume_distinct=False, split_all=True
        )
        all_fields = all_distinct_fields + stats_fields
        stats = {}
        cnt_dict = {}

        try:
            # order_by(*all_distinct_fields) and distinct(*all_distinct_fields) are required to get accurate row counts,
            # otherwise, some duplicates will get counted
            for rec in (
                res.order_by(*all_distinct_fields)
                .distinct(*all_distinct_fields)
                .values_list(*all_fields)
            ):
                # This is a combination of field values whose unique count corresponds to the number of output rows of
                # this format
                reccombo = ";".join(
                    # values_list is fast, but can only be indexed by number...
                    list(str(rec[all_fields.index(fld)]) for fld in fmt_distinct_fields)
                )

                # For each stats category defined for this format
                for params in params_arrays:

                    if "delimiter" in params:
                        delim = params["delimiter"]
                    else:
                        delim = " "

                    # Get distinct fields for the count by taking the union of the record-identifying distinct fields
                    # and the distinct fields whose repeated values we want to count.
                    distinct_fields = params["distincts"]

                    statskey = params["displayname"]
                    valcombo = delim.join(
                        # values_list is fast, but can only be indexed by number...
                        list(str(rec[all_fields.index(fld)]) for fld in distinct_fields)
                    )

                    if statskey not in stats:
                        stats[statskey] = {
                            "count": 0,
                            "filter": params["filter"],
                        }
                        cnt_dict[statskey] = {}

                    # Update the stats
                    if params["filter"] is None:
                        # Count unique and duplicate values
                        if valcombo not in cnt_dict[statskey]:
                            cnt_dict[statskey][valcombo] = {}
                            cnt_dict[statskey][valcombo][reccombo] = 1
                            stats[statskey]["count"] += 1
                        else:
                            cnt_dict[statskey][valcombo][reccombo] = 1
                    else:
                        # Count values meeting a criteria/filter
                        if self.meetsAllConditionsByValList(
                            fmt, rec, params["filter"], all_fields
                        ):
                            stats[statskey]["count"] += 1
                            if valcombo not in cnt_dict[statskey]:
                                cnt_dict[statskey][valcombo] = {}
                                cnt_dict[statskey][valcombo][reccombo] = 1
                            else:
                                cnt_dict[statskey][valcombo][reccombo] = 1

            # For each stats category defined for this format
            for params in params_arrays:
                statskey = params["displayname"]

                # For the top 10 unique values (delimited-combos), in order of descending number of occurrences
                top10 = []
                for valcombo in sorted(
                    cnt_dict[statskey].keys(),
                    key=lambda vc: -len(cnt_dict[statskey][vc].keys()),
                )[0:10]:
                    top10.append(
                        {
                            "val": valcombo,
                            "cnt": len(cnt_dict[statskey][valcombo].keys()),
                        }
                    )

                stats[statskey]["sample"] = top10
        except ProgrammingError as pe:
            if len(
                all_distinct_fields
            ) > 1 and "SELECT DISTINCT ON expressions must match initial ORDER BY expressions" in str(
                pe
            ):
                raise UnsupportedDistinctCombo(all_distinct_fields) from pe
            else:
                raise pe

        return stats

    def getDownloadQryList(self):
        """
        Returns a list of dicts where the keys are name and json and the values are the format name and the json-
        stringified qry object with the target format selected
        """
        qry_list = []
        for format, name in self.getFormatNames().items():
            qry_list.append(
                {"name": name, "json": json.dumps(self.getRootGroup(format))}
            )
        return qry_list

    def createNewBasicQuery(self, mdl, fld, cmp, val, units, fmt):
        """
        Constructs a new qry object for an advanced search from basic search input.
        """

        qry = self.getRootGroup(fmt)

        try:
            mdl = self.getModelInstance(fmt, mdl)
        except KeyError as ke:
            raise Http404(ke)

        sfields = self.getSearchFields(fmt, mdl)

        if fld not in sfields:
            raise Http404(
                f"Field [{fld}] is not searchable.  Must be one of [{','.join(sfields.keys())}]."
            )

        num_empties = getNumEmptyQueries(qry, fmt)
        if num_empties != 1:
            raise Http404(
                f"The static filter for format {fmt} is improperly configured. It must contain exactly 1 empty query."
            )

        target_fld = sfields[fld]
        target_val = val

        setFirstEmptyQuery(qry, fmt, target_fld, cmp, target_val, units)

        dfld, dval = self.searchFieldToDisplayField(mdl, fld, val, qry)

        if dfld != fld:
            # Set the field path for the display field
            target_fld = sfields[dfld]
            target_val = dval

            # Re-create another empty copy of the qry
            qry = self.getRootGroup(fmt)
            # Note units cannot be transfered, so default should always be "identity"
            setFirstEmptyQuery(qry, fmt, target_fld, cmp, target_val, "identity")

        return qry


class UnsupportedDistinctCombo(Exception):
    def __init__(self, fields):
        message = (
            f"Unsupported combination of distinct fields: [{', '.join(fields)}].  The problem likely stems from the "
            "usage of field references that are not real database fields, used in both .distinct() and .order_by().  "
            "Those methods resolve foreign keys (to database fields) in models differently.  Be sure to supply actual "
            "database fields and not foreign key object references."
        )
        super().__init__(message)
        self.fields = fields
