from copy import deepcopy
from typing import Dict, List, Optional

from django.apps import apps
from django.db.models import F, Model

from DataRepo.formats.dataformat_group_query import (
    appendFilterToGroup,
    createFilterCondition,
    createFilterGroup,
    extractFldPaths,
    getChildren,
    getComparison,
    getField,
    getFilterType,
    getSearchTree,
    getValue,
    isAllGroup,
    isQuery,
    isQueryGroup,
    setField,
    splitCommon,
    splitPathName,
)


class Format:
    """
    This class holds common data/functions for search output formats.

    Note that any comparison types added to ncmp_choices must also be implemented in meetsCondition().
    """

    id = ""
    name = ""
    model_instances: Dict[str, Dict] = {}
    rootmodel: Model = None
    stats: Optional[List[Dict]] = None
    ncmp_choices = {
        "number": [
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("lt", "<"),
            ("lte", "<="),
            ("gt", ">"),
            ("gte", ">="),
            ("not_isnull", "has a value (ie. is not None)"),
            ("isnull", "does not have a value (ie. is None)"),
        ],
        "string": [
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("icontains", "contains"),
            ("not_icontains", "does not contain"),
            ("istartswith", "starts with"),
            ("not_istartswith", "does not start with"),
            ("iendswith", "ends with"),
            ("not_iendswith", "does not end with"),
            ("lt", "is alphabetically less than"),
            ("lte", "is alphabetically less than or equal to"),
            ("gt", "is alphabetically greater than"),
            ("gte", "is alphabetically greater than or equal to"),
            ("not_isnull", "has a value (ie. is not None)"),
            ("isnull", "does not have a value (ie. is None)"),
        ],
        "enumeration": [
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("not_isnull", "has a value (ie. is not None)"),
            ("isnull", "does not have a value (ie. is None)"),
        ],
    }
    static_filter = appendFilterToGroup(
        createFilterGroup(),
        createFilterCondition("", "", ""),
    )  # Same as qry['tree']

    # static_filter example WITH static=True (below).  Note that a non-static empty query must be present in a non-
    # static queryGroup because that is where the user is prompted to add their search.  When static is True, the user
    # will not be allowed to edit that portion of the search form.
    #
    # static_filter = {
    #     "type": "group",
    #     "val": "all",
    #     "static": True,
    #     "queryGroup": [
    #         {
    #             'type': 'query',
    #             'pos': '',
    #             'ncmp': 'istartswith',
    #             'fld': 'msrun__sample__tissue__name',
    #             'val': Tissue.SERUM_TISSUE_PREFIX,
    #             'static': True,
    #         },
    #         {
    #             'type': 'group',
    #             'val': 'all',
    #             "queryGroup": [
    #                 {
    #                     'type': 'query',
    #                     'pos': '',
    #                     "static": False,
    #                     'ncmp': '',
    #                     'fld': '',
    #                     'val': '',
    #                 },
    #             ],
    #         },
    #     ]
    # }

    @classmethod
    def getSearchFieldChoices(self):
        """
        This generates the tuple to populate the select list choices for the AdvSearchForm fld field.
        """

        choices = ()
        for mkey in self.model_instances.keys():
            mpath = self.model_instances[mkey]["path"]
            for fkey in self.model_instances[mkey]["fields"].keys():
                # We only want it in the select list if it is both searchable and displayed
                if (
                    self.model_instances[mkey]["fields"][fkey]["searchable"] is True
                    and self.model_instances[mkey]["fields"][fkey]["displayed"] is True
                ):
                    fpath = ""
                    if mpath != "":
                        fpath = mpath + "__"
                    fpath += fkey
                    fname = self.model_instances[mkey]["fields"][fkey]["displayname"]
                    choices = choices + ((fpath, fname),)
        return tuple(sorted(choices, key=lambda x: x[1]))

    def getComparisonChoices(self):
        """
        Returns ncmp_choices (same for all derived classes)
        """
        return self.ncmp_choices

    def getAllComparisonChoices(self):
        """
        Returns the union of all ncmp_choices, ignoring differences in the second value. This is mainly only for form
        validation because it only validates known values (the first value in each tuple) regardless of the particular
        sub-population controlled by javascript in the advanced search form.
        """
        all_ncmp_choices = ()
        seen = []
        for fldtype in self.ncmp_choices.keys():
            for opt in self.ncmp_choices[fldtype]:
                if opt[0] not in seen:
                    seen.append(opt[0])
                    all_ncmp_choices = all_ncmp_choices + ((opt[0], opt[1]),)
        return all_ncmp_choices

    def getKeyPathList(self, mdl):
        """
        Returns a list of foreign key names for a composite view from the root table to the supplied table.
        """
        return self.model_instances[mdl]["path"].split("__")

    def getPrefetches(self):
        """
        Returns a list of prefetch strings for a composite view from the root table to the supplied table.  It includes
        a unique set of "foreign key paths" that encompass all tables.
        """
        desc_len_sorted_paths = sorted(
            map(
                lambda name: self.model_instances[name]["path"],
                self.getModelInstances(),
            ),
            key=len,
            reverse=True,
        )
        unique_paths = []
        for path in desc_len_sorted_paths:
            if path == "":
                continue
            contained = False
            for upath in unique_paths:
                if path in upath:
                    contained = True
                    break
            if not contained:
                unique_paths.append(path)
        return unique_paths

    def getTrueJoinPrefetchPathsAndQrys(self, qry):
        """
        Takes a qry object and returns a list of prefetch paths.  If a prefetch path contains models that are related
        M:M with the root model, that prefetch path will be split into multiple paths (all that end in a M:M model, and
        the remainder (if any of the path is left)).  Any path ending in a M:M model will be represented as a 3-member
        list containing the path, a re-rooted qry object, and the name of the new root model.
        """

        # Sort the paths so that multiple subquery prefetches on the same path are encountered hierarchically.
        # This is based on the assumption that prefetch filters work serially and are applied iteratively.  So if a
        # compound is filtered and then compound synonyms are filtered, the synonyms operate on the already filtered
        # compounds.  This migth be a false assumption.
        fld_paths = sorted(extractFldPaths(qry), key=len)

        # Identify the fld paths that need a subquery in its prefetch and collect those paths associated with their
        # rerooted qry objects
        subquery_paths = []
        for srch_path_str in fld_paths:
            srch_model_inst_name = self.pathToModelInstanceName(srch_path_str)
            if (
                self.model_instances[srch_model_inst_name]["manytomany"]["is"]
                and self.model_instances[srch_model_inst_name]["manytomany"][
                    "split_rows"
                ]
            ):
                new_qry = self.reRootQry(qry, srch_model_inst_name)
                subquery_paths.append(
                    [
                        srch_path_str,
                        new_qry,
                        self.model_instances[srch_model_inst_name]["model"],
                    ]
                )

        # If there are no subqueries necessary, just return all the prefetches
        if len(subquery_paths) == 0:
            return self.getPrefetches()

        prefetches = self.getPrefetches()

        # Create a dict to hold the more complex prefetch data so we know if we need queries on multiple nodes of a
        # path
        prefetch_dict = {}
        # Pre-populate the prefetch_dict with the subquery
        for subquery_path in subquery_paths:
            sq_path = subquery_path[0]
            sq_qry = subquery_path[1]
            sq_mdl = subquery_path[2]
            matched = False
            for pf_str in prefetches:
                if sq_path in pf_str:
                    if matched:
                        continue
                    if pf_str not in prefetch_dict.keys():
                        prefetch_dict[pf_str] = {}
                    prefetch_dict[pf_str][sq_path] = [sq_path, sq_qry, sq_mdl]
                    matched = True

        # Build the final prefetches, which is a list of items that can be either normal prefetch paths, or (possibly
        # multiple per prefetch path) sublist(s) containing a component of a prefetch path (ending in a model that has
        # a M:M relationship with the root model) together with a re-rooted qry object intended to filter the prefetch
        # records.  This only changes the output if a search term in a M:M related model excludes related records that
        # are linked to the root model.
        final_prefetches = []
        for pf_str in prefetches:
            if pf_str in prefetch_dict:
                full = False
                subqueries = prefetch_dict[pf_str]
                for path_str in subqueries.keys():
                    final_prefetches.append(subqueries[path_str])
                    if path_str == pf_str:
                        # Append the path and the re-rooted qry object
                        full = True
                if not full:
                    final_prefetches.append(pf_str)
            else:
                final_prefetches.append(pf_str)

        return final_prefetches

    def getFullJoinAnnotations(self):
        """
        This returns a list of dicts that, when expanded, can be supplied to .annotate().  It is intended to be used to
        distinguish between otherwise identical root table records that are returned because they link to many records
        of a related table, so that when looping through those subtable records, you can skip the ones that do not
        belong to that fully joined record.

        This is necessary because the Django ORM does not suppored many-to-many tables involved in a join.  You always
        get every M:M related record with every root table record even though Django returned the number of root table
        records that would have been used in a full join.
        """
        annotations = []
        for mdl_inst_nm in self.model_instances.keys():
            # If this model is many to many related, and we want a full join
            if (
                self.model_instances[mdl_inst_nm]["manytomany"]["is"]
                and self.model_instances[mdl_inst_nm]["manytomany"]["split_rows"]
            ):
                # If an root_annot_fld key exists in the "manytomany" dict, use it
                if (
                    "root_annot_fld"
                    in self.model_instances[mdl_inst_nm]["manytomany"].keys()
                ):
                    annot_fld = self.model_instances[mdl_inst_nm]["manytomany"][
                        "root_annot_fld"
                    ]
                else:
                    # Otherwise, come up with a reasonable default
                    annot_fld = self.model_instances[mdl_inst_nm]["model"].lower()

                    # Check to make sure it doesn't exist
                    if annot_fld in self.rootmodel.__dict__.keys():
                        raise Exception(
                            f"Many-to-many model {mdl_inst_nm} with split_rows=True in format {self.id} must have a "
                            "value for [root_annot_fld].  This is the name that will be used to associate a root "
                            "table record with its M:M field in a unique combination."
                        )

                # Append a dict that creates an annotation when supplied to .annotate()
                # E.g. .annotate(compound=compounds__pk)
                # ...where we're adding an annotation accessed as 'root_table_rec.compound' and its value is that
                # of essentially: root_table_rec.compounds.all()["pk"]
                annotations.append(
                    {annot_fld: F(self.model_instances[mdl_inst_nm]["path"] + "__pk")}
                )

        return annotations

    def getModelInstances(self):
        """
        Returns a list of all model instance names (keys of the model_instances datamember) containing fields that are
        in an output format.  This is generally the model name, but if a model has 2 different links in the composite
        view, it may have a name different from the model it contains (e.g. MeasuredCompound and TracerCompound in the
        PeakGroups format).
        """
        return list(self.model_instances.keys())

    def getModelInstance(self, mdl):
        """
        Given a string that is either a model instance name or a model name, return the corresponding model instance
        name or report an error if it is ambiguous or not found.
        """
        mdl_instance_names = self.getModelInstances()
        if mdl not in mdl_instance_names:
            # Look through the actual model names (instead of the instance names) to see if there's a unique match.
            inst_names = []
            for inst_name in mdl_instance_names:
                if self.model_instances[inst_name]["model"] == mdl:
                    inst_names.append(inst_name)
            if len(inst_names) == 1:
                return inst_names[0]
            elif len(inst_names) > 1:
                raise KeyError(
                    f"Ambiguous model instance [{mdl}].  Must specify one of [{','.join(inst_names)}]."
                )
            else:
                raise KeyError(
                    f"Invalid model instance [{mdl}].  Must be one of [{','.join(mdl_instance_names)}]."
                )
        return mdl

    def getFieldTypes(self):
        """
        Returns a dict of path__field -> {type -> field_type (number, string, enumeration), choices -> list of tuples}.
        """

        typedict = {}
        # For each model
        for mdl in self.model_instances.keys():
            # Grab the path
            path = self.model_instances[mdl]["path"]
            # If the path has a value (i.e. it's not the root table), append the Q object separator
            if path != "":
                path += "__"
            # For each field
            for fld in self.model_instances[mdl]["fields"].keys():
                # Create the field key (mimmicking the keys in the fld select list - but containing ALL fields)
                fldkey = path + fld
                typedict[fldkey] = {}
                # Save a dict with values for type and choices (if present)
                typedict[fldkey]["type"] = self.model_instances[mdl]["fields"][fld][
                    "type"
                ]
                if "choices" in self.model_instances[mdl]["fields"][fld].keys():
                    typedict[fldkey]["choices"] = self.model_instances[mdl]["fields"][
                        fld
                    ]["choices"]
                else:
                    typedict[fldkey]["choices"] = []
        return typedict

    def getSearchFields(self, mdl):
        """
        Returns a dict of searchable fields for a given model/table whose keys are the field names and whose values are
        strings of the full foreign key path (delimited by dunderscores).
        """

        fielddict = {}
        path = self.model_instances[mdl]["path"]
        if path != "":
            path += "__"
        for field in self.model_instances[mdl]["fields"].keys():
            if self.model_instances[mdl]["fields"][field]["searchable"] is True:
                fielddict[field] = path + field
        return fielddict

    def getDisplayFields(self, mdl):
        """
        Returns a dict of displayed fields for a given model/table whose keys are the field names and whose values are
        searchable field names in the same model/table that should be displayed in their stead.  The values of the
        fields in the dict values returned must have the same relation requirements as the field in the dict key.  E.g.
        If a non-displayed dict key field is unique, the displayed field dict value must also be unique.  I.e. A search
        using the dict key field and the dict value field must return the same records.
        """

        fielddict = {}
        for field in self.model_instances[mdl]["fields"].keys():
            if self.model_instances[mdl]["fields"][field]["displayed"] is False:
                if "handoff" in self.model_instances[mdl]["fields"][field].keys():
                    fielddict[field] = self.model_instances[mdl]["fields"][field][
                        "handoff"
                    ]
                # Nothing if a handoff is not present
            else:
                fielddict[field] = field
        return fielddict

    def getRootQuerySet(self):
        if self.rootmodel is not None:
            return self.rootmodel.objects.all()
        print("ERROR: rootmodel not set.")
        return None

    def pathToModelInstanceName(self, fld_path):
        """
        Takes a model instance name (i.e. a key to the model_instances dict) and returns its key path
        """
        for mdl in self.model_instances.keys():
            if fld_path == self.model_instances[mdl]["path"]:
                return mdl
        # This should raise an exception if we got here
        self.checkPath(fld_path)

    def reRootQry(self, qry, new_root_model_instance_name):
        """
        This takes a qry object and the name of a model instance in the composite view and re-roots the fld values,
        making all the field paths come from a different model root.  It is intended to be used for prefetch
        subqueries.
        """
        ret_qry = deepcopy(qry)
        self.reRootQryHelper(
            getSearchTree(ret_qry, self.id), new_root_model_instance_name
        )
        return ret_qry

    def reRootQryHelper(self, subtree, new_root_model_instance_name):
        """
        Recursive helper to reRootQry
        """
        if isQueryGroup(subtree):
            for child in getChildren(subtree):
                self.reRootQryHelper(child, new_root_model_instance_name)
        elif isQuery(subtree):
            setField(
                subtree,
                self.reRootFieldPath(getField(subtree), new_root_model_instance_name),
            )
        else:
            type = getFilterType(subtree)
            raise Exception(f"Qry type: [{type}] must be either 'group' or 'query'.")

    def reRootFieldPath(self, fld, reroot_instance_name):
        """
        Returns a modified fld path (derived from a qry object), re-rooted by using reroot_instance_name as the model
        instance name of the new root.  Essentially, it chops off the common path and prepends the reverse path of the
        fld's model.
        """
        fld_path, fld_name = splitPathName(fld)
        fld_instance_name = self.pathToModelInstanceName(fld_path)
        if fld_instance_name == reroot_instance_name:
            return fld_name
        else:
            reroot_path = self.model_instances[reroot_instance_name]["path"]
            reroot_rpath = self.model_instances[reroot_instance_name]["reverse_path"]

            # Determine the common path item(s) between reroot_path and fld_path (which could be none) and grab the
            # remainder of fld_path
            common_path, fld_path_rem = splitCommon(fld_path, reroot_path)
            common_path_list = []
            if common_path != "":
                common_path_list = common_path.split("__")

            # Pop off as many nodes off the end of the reverse reroot path as there were common nodes between the
            # forward reroot path and the fld path.
            reroot_rpath_list = []
            if reroot_rpath != "":
                reroot_rpath_list = reroot_rpath.split("__")
            for item in common_path_list:
                reroot_rpath_list.pop()
            reroot_rpath_rem = "__".join(reroot_rpath_list)

            # Then we just join the reverse reroot path remainder with the forward fld path remainder, e.g. if the orig
            # reroot Compound path was compounds and the fld path was compounds_synonyms, then the rerooted path for a
            # synonyms fields would be "synonyms"
            fld_new_path = ""
            if reroot_rpath_rem:
                fld_new_path += reroot_rpath_rem + "__"
            if fld_path_rem:
                fld_new_path += fld_path_rem + "__"

            # Append the field name
            fld_new_path += fld_name

            return fld_new_path

    def checkPath(self, path):
        """
        Simply raises an exception if the path doesn't exist
        """
        avail = list(
            map(lambda m: self.model_instances[m]["path"], self.model_instances.keys())
        )
        if path not in avail:
            raise Exception(
                f"Field path: [{path}] not found in model instances of format [{self.id}].  "
                f"Available paths are: [{', '.join(avail)}]."
            )

    def getOrderByFields(self, mdl_inst_nm=None, model_name=None):
        """
        Retrieves a model's default order by fields, given a model instance name.
        """
        # Determine the model name based on either the supplied model or model instance name
        mdl_nm = model_name
        if mdl_inst_nm is None and model_name is None:
            raise Exception("Either a model instance name or model name is required.")
        elif mdl_inst_nm is not None and model_name is not None:
            raise Exception(
                "mdl_inst_nm and model_name are mutually exclusive options."
            )
        elif model_name is None:
            mdl_nm = self.model_instances[mdl_inst_nm]["model"]

        # Get a model object
        mdl = apps.get_model("DataRepo", mdl_nm)

        # Retreive any custom ordering
        if "ordering" in mdl._meta.__dict__:
            # The fields defined in the model's meta ordering - potentially containing non-database field references
            ordering = mdl._meta.__dict__["ordering"]
            # We will save the db-only field names here (i.e. no non-DB field references to model objects)
            db_field_ordering = []
            # For each order-by field reference
            for ob_field_val in ordering:
                # if the field ordering is reversed
                if ob_field_val.startswith("-"):
                    # Chop off the negative sign to get the unmodified field name
                    ob_field = ob_field_val[1:]
                else:
                    ob_field = ob_field_val
                add_flds = []
                fld = getattr(mdl, ob_field)
                # If this is a foreign key (i.e. it's a model reference, not an actual DB field)
                if fld.field.__class__.__name__ == "ForeignKey":
                    # Get the model name of the linked model
                    linked_model = self.getFKModelName(mdl, ob_field)
                    # Recursively get that model's ordering fields
                    add_flds = self.getOrderByFields(model_name=linked_model)
                    if len(add_flds) == 0:
                        # Default when the linking model says to order by an FK that has no custom ordering is to order
                        # by that model's primary key
                        db_field_ordering.append(ob_field + "__pk")
                    else:
                        # Append each field to the list with the link name prepended
                        for add_fld in add_flds:
                            db_field_ordering.append(ob_field + "__" + add_fld)
                else:
                    # If it's not a foreign key, just append the field as-is
                    db_field_ordering.append(ob_field)
            return db_field_ordering
        return []

    def getFKModelName(self, mdl, field_ref_name):
        """
        Given a model class and the name of a foreign key field, this retrieves the name of the model the foreign key
        links to
        """
        return mdl._meta.get_field(field_ref_name).related_model._meta.model.__name__

    def getDistinctFields(self, order_by=None, assume_distinct=True, split_all=False):
        """
        Puts together fields required by queryset.distinct() based on the value of each model instance's split_rows
        state (or if split_all is True).

        split_rows=True in the format allows us to choose whether the output rows in the html results template will
        contain M:M related table values joined in one cell on one row ("merged"), or whether they will be split across
        multiple rows ("split"), and supplying these fields to distinct ensures that the queryset record count reflects
        that html table row (split or merged).  split_all overrides this.

        An order_by field (including its key path) is required if the queryset will be non-default ordered, because
        .distinct() requires them to be present.  Otherwise, you will encounter an exception when the queryset is made
        distinct on the returned fields.  Only a single order_by field is supported.

        assume_distinct - This assumes (when split_rows is False) that all records are distinct/not-identical.  In that
        case, this method returns an empty list (as the parameters to .distinct()).  This is the default behavior.  If
        that assumption is false, supply assume_distinct=False.
        """
        distinct_fields = []
        for mdl_inst_nm in self.model_instances:
            # We only need to include a field if we want to split
            if self.model_instances[mdl_inst_nm]["manytomany"]["split_rows"] or (
                self.model_instances[mdl_inst_nm]["manytomany"]["is"] and split_all
            ):
                # Django's ordering fields are required when any field is provided to .distinct().  Otherwise, you get
                # the error: `ProgrammingError: SELECT DISTINCT ON expressions must match initial ORDER BY expressions`
                tmp_distincts = self.getOrderByFields(mdl_inst_nm)
                for fld_nm in tmp_distincts:
                    fld = self.model_instances[mdl_inst_nm]["path"] + "__" + fld_nm
                    distinct_fields.append(fld)
                # Don't assume the ordering fields are populated/unique, so include the primary key.  Duplicate fields
                # should be OK (though I haven't tested it).
                # Note, this assumes that being here means we're in a related table and not the root table, so path is
                # not an empty string
                distinct_fields.append(
                    self.model_instances[mdl_inst_nm]["path"] + "__pk"
                )

        # If there are any split_rows manytomany related tables, we will need to prepend the ordering (and pk) fields of
        # the root model
        if len(distinct_fields) > 0:
            distinct_fields.insert(0, "pk")
            tmp_distincts = self.getOrderByFields(model_name=self.rootmodel.__name__)
            tmp_distincts.reverse()
            for fld_nm in tmp_distincts:
                distinct_fields.insert(0, fld_nm)

            if order_by is not None and order_by not in distinct_fields:
                distinct_fields.insert(0, order_by)

        if len(distinct_fields) == 0 and not assume_distinct:
            distinct_fields.append("pk")

        return distinct_fields

    def getStatsParams(self):
        """Stats getter"""
        return deepcopy(self.stats)

    def statsAvailable(self):
        return self.stats is not None

    def meetsAllConditionsByValList(self, rootrec, query, field_order):
        """
        This is a python-code version of a complex Q expression, necessary for checking filters in aggregate count
        annotations, because the Django ORM does not support .distinct(fields).annotate(Count) when duplicate root
        table records exist.
        """
        if isQuery(query):
            recval = rootrec[field_order.index(getField(query))]
            return self.meetsCondition(recval, getComparison(query), getValue(query))
        else:
            if isAllGroup(query):
                for subquery in getChildren(query):
                    if not self.meetsAllConditionsByValList(
                        rootrec, subquery, field_order
                    ):
                        return False
                return True
            else:
                for subquery in getChildren(query):
                    if self.meetsAllConditionsByValList(rootrec, subquery, field_order):
                        return True
                return False

    def meetsCondition(self, recval, condition, searchterm):
        """
        Determines whether the recval and search term match, given the matching condition.
        This is only useful for re-filtering records in a template when the qry includes a fld from a many-to-many
        related model relative to the root model.
        Note that any changes to ncmp_choices must also be implemented here.
        """
        if condition == "iexact":
            return recval.lower() == searchterm.lower()
        elif condition == "not_iexact":
            return recval.lower() != searchterm.lower()
        elif condition == "lt":
            return recval < searchterm
        elif condition == "lte":
            return recval <= searchterm
        elif condition == "gt":
            return recval > searchterm
        elif condition == "gte":
            return recval >= searchterm
        elif condition == "isnull":
            return recval is None
        elif condition == "not_isnull":
            return recval is not None
        elif condition == "icontains":
            return searchterm.lower() in recval.lower()
        elif condition == "not_icontains":
            return searchterm.lower() not in recval.lower()
        elif condition == "istartswith":
            return recval.lower().startswith(searchterm.lower())
        elif condition == "not_istartswith":
            return not recval.lower().startswith(searchterm.lower())
        elif condition == "iendswith":
            return recval.lower().endswith(searchterm.lower())
        elif condition == "not_iendswith":
            return not recval.lower().endswith(searchterm.lower())
        else:
            raise UnknownComparison(
                f"Unrecognized negatable comparison (ncmp) value: {condition}."
            )


class UnknownComparison(Exception):
    pass
