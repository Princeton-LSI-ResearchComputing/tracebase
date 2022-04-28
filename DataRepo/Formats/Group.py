from copy import deepcopy
from typing import Dict

from DataRepo.Formats.Format import Format


class Group:
    """
    This class groups all search output formats in a single class and adds metadata that applies to all
    search output formats as a whole.  It includes all derived classes of BasSearchView.  BaseSearchView
    differs from this class in that BaseSearchView has an instance for each search output format class and
    this class has a single instance for all search view output format classes.
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

    def getNumEmptyQueries(self, filter):
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
                total_empty += self.getNumEmptyQueries(query)
            return total_empty
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
        if format is not None and format != qry["selectedtemplate"]:
            raise Exception(
                f"Supplied format: [{format}] does not match the qry selected format: [{qry['selectedtemplate']}]"
            )
        elif format is None:
            format = qry["selectedtemplate"]
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

    def getComparisonChoices(self):
        """
        Calls getComparisonChoices of the default output format class.
        """
        return self.modeldata[self.default_format].getComparisonChoices()

    def getAllComparisonChoices(self):
        """
        Calls getAllComparisonChoices of the default output format class.
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

    def getDistinctFields(self, fmt, order_by=None, assume_distinct=True):
        return self.modeldata[fmt].getDistinctFields(order_by, assume_distinct)

    def getFullJoinAnnotations(self, fmt):
        return self.modeldata[fmt].getFullJoinAnnotations()

    def getStatsParams(self, fmt):
        return self.modeldata[fmt].getStatsParams()

    def meetsAllConditionsByValList(self, fmt, rootrec, query, field_order):
        """
        This is a python-code version of a complex Q expression, necessary for checking filters in aggregate count
        annotations, because the Django ORM does not support .distinct(fields).annotate(Count) when duplicate root
        table records exist.
        """
        return self.modeldata[fmt].meetsAllConditionsByValList(
            rootrec, query, field_order
        )
