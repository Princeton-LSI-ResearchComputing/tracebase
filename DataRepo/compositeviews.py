from copy import deepcopy
from typing import Dict, List, Optional

from django.apps import apps
from django.db.models import F, Model

from DataRepo.models import (
    Animal,
    PeakData,
    PeakGroup,
    Tissue,
    TracerLabeledClass,
)


def getSimpleFilter(fld, ncmp, val, static=False):
    """
    This returns a 1-query portion of what is usually under qry[searches][<template>][tree]
    """
    return {
        "type": "group",
        "val": "all",
        "static": static,
        "queryGroup": [
            {
                "type": "query",
                "pos": "",
                "static": static,
                "fld": fld,
                "ncmp": ncmp,
                "val": val,
            },
        ],
    }


class BaseSearchView:
    """
    This class holds common data/functions for search output formats.
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
    static_filter = {  # Same as qry['tree']
        "type": "group",
        "val": "all",
        "static": False,
        "queryGroup": [
            {
                "type": "query",
                "pos": "",
                "static": False,
                "ncmp": "",
                "fld": "",
                "val": "",
            },
        ],
    }

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
            ret_qry["searches"][self.id]["tree"], new_root_model_instance_name
        )
        return ret_qry

    def reRootQryHelper(self, subtree, new_root_model_instance_name):
        """
        Recursive helper to reRootQry
        """
        if subtree["type"] == "group":
            for child in subtree["queryGroup"]:
                self.reRootQryHelper(child, new_root_model_instance_name)
        elif subtree["type"] == "query":
            subtree["fld"] = self.reRootFieldPath(
                subtree["fld"], new_root_model_instance_name
            )
        else:
            raise Exception(
                f"Qry type: [{subtree['type']}] must be either 'group' or 'query'."
            )

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

        if "ordering" in mdl._meta.__dict__:
            return mdl._meta.__dict__["ordering"]
        return []

    def getDistinctFields(self, order_by=None, assume_distinct=True):
        """
        Puts together fields required by queryset.distinct() based on the value of each model instance's split_rows
        state.  split_rows=True allows us to choose whether the output rows in the html results template will contain
        M:M related table values joined in one cell on one row ("merged"), or whether they will be split across
        multiple rows ("split"), and supplying these fields to distinct ensures that the queryset record count reflects
        that html table row (split or merged).

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
            if self.model_instances[mdl_inst_nm]["manytomany"]["split_rows"]:
                # Django's ordering fields are required when any field is provided to .distinct().  Otherwise, you get
                # the error: `ProgrammingError: SELECT DISTINCT ON expressions must match initial ORDER BY expressions`
                tmp_distincts = self.getOrderByFields(mdl_inst_nm)
                for fld_nm in tmp_distincts:
                    fld = self.model_instances[mdl_inst_nm]["path"] + "__" + fld_nm
                    distinct_fields.append(fld)
                # Don't assume the ordering fields are populated/unique, so include the primary key.  Duplicate fields
                # should be OK (though I haven't tested it).
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
        return deepcopy(self.stats)

    def meetsCondition(self, recval, condition, searchterm):
        """
        Determines whether the recval and search term match, given the matching condition.
        This is only useful for re-filtering records in a template when the qry includes a fld from a many-to-many
        related model relative to the root model.
        Note that any changes to ncmp_choices must also be implemented here.
        """
        if condition == "iexact":
            print("matches?: ", (recval.lower() == searchterm.lower()))
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
            raise UnknownComparison(f"Unrecognized negatable comparison (ncmp) value: {condition}.")


class PeakGroupsSearchView(BaseSearchView):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "pgtemplate"
    name = "PeakGroups"
    rootmodel = PeakGroup
    stats = [
        {
            "displayname": "Animals",
            "distincts": ["msrun__sample__animal__name"],
            "filter": None,
        },
        {
            "displayname": "Labeled Elements",
            "distincts": ["peak_data__labeled_element"],
            "filter": None,
        },
        {
            "displayname": "Measured Compounds",
            "distincts": ["compounds__name"],
            "filter": None,
        },
        {
            "displayname": "Samples",
            "distincts": ["msrun__sample__name"],
            "filter": None,
        },
        {
            "displayname": "Tissues",
            "distincts": ["msrun__sample__tissue__name"],
            "filter": None,
        },
        {
            "displayname": "Tracer Compounds",
            "distincts": ["msrun__sample__animal__tracer_compound__name"],
            "filter": None,
        },
        {
            "displayname": "Studies",
            "distincts": ["msrun__sample__animal__studies__name"],
            "filter": None,
        },
        {
            "displayname": "Feeding Statuses",
            "distincts": ["msrun__sample__animal__feeding_status"],
            "filter": None,
        },
        {
            "displayname": "Infusion Rates",
            "distincts": ["msrun__sample__animal__tracer_infusion_rate"],
            "filter": None,
        },
        {
            "displayname": "Infusion Concentrations",
            "distincts": ["msrun__sample__animal__tracer_infusion_concentration"],
            "filter": None,
        },
    ]
    model_instances = {
        "PeakGroupSet": {
            "model": "PeakGroupSet",
            "path": "peak_group_set",
            "reverse_path": "peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Set Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "filename",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "filename": {
                    "displayname": "Peak Group Set Filename",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "CompoundSynonym": {
            "model": "CompoundSynonym",
            "path": "compounds__synonyms",
            "reverse_path": "compound__peak_groups",
            "manytomany": {
                "is": True,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Compound Synonym Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Measured Compound (Any Synonym)",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "PeakGroup": {
            "model": "PeakGroup",
            "path": "",
            "reverse_path": "",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Peak Group",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "formula": {
                    "displayname": "Formula",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "enrichment_fraction": {
                    "displayname": "Enrichment Fraction",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "enrichment_abundance": {
                    "displayname": "Enrichment Abundance",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "total_abundance": {
                    "displayname": "Total Abundance",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "normalized_labeling": {
                    "displayname": "Normalized Labeling",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Protocol": {
            "model": "Protocol",
            "path": "msrun__sample__animal__treatment",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Protocol Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Treatment",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Sample": {
            "model": "Sample",
            "path": "msrun__sample",
            "reverse_path": "msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Sample Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Sample",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Tissue": {
            "model": "Tissue",
            "path": "msrun__sample__tissue",
            "reverse_path": "samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Tissue Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Tissue",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Animal": {
            "model": "Animal",
            "path": "msrun__sample__animal",
            "reverse_path": "samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Animal Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Animal",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "genotype": {
                    "displayname": "Genotype",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "body_weight": {
                    "displayname": "Body Weight (g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "age": {
                    "displayname": "Age (d-hh:mm:ss)",
                    "searchable": False,  # currently no data available for testing and a bug: issue #180
                    "displayed": True,
                    "type": "number",
                },
                "sex": {
                    "displayname": "Sex",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": Animal.SEX_CHOICES,
                },
                "diet": {
                    "displayname": "Diet",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "feeding_status": {
                    "displayname": "Feeding Status",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "tracer_labeled_atom": {
                    "displayname": "Tracer Labeled Element",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
                },
                "tracer_infusion_rate": {
                    "displayname": "Tracer Infusion Rate (ul/min/g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "tracer_infusion_concentration": {
                    "displayname": "Tracer Infusion Concentration (mM)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "TracerCompound": {
            "model": "Compound",
            "path": "msrun__sample__animal__tracer_compound",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Tracer Compound Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Tracer Compound (Primary Synonym)",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "MeasuredCompound": {
            "model": "Compound",
            "path": "compounds",
            "reverse_path": "peak_groups",
            "manytomany": {
                "is": True,
                "split_rows": False,
                "root_annot_fld": "compound",
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Measured Compound Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Measured Compound (Primary Synonym)",
                    "searchable": True,
                    "displayed": True,  # Will display due to the handoff
                    "type": "string",
                },
            },
        },
        "Study": {
            "model": "Study",
            "path": "msrun__sample__animal__studies",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manytomany": {
                "is": True,
                "split_rows": False,
                "root_annot_fld": "study",
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Study Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Study",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
    }


class PeakDataSearchView(BaseSearchView):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "pdtemplate"
    name = "PeakData"
    rootmodel = PeakData
    stats = [
        {
            "displayname": "Animals",
            "distincts": ["peak_group__msrun__sample__animal__name"],
            "filter": None,
        },
        {
            "displayname": "Labels",
            "distincts": [
                "labeled_element",
                "labeled_count",
            ],
            "filter": None,
            "delimiter": ":",
        },
        {
            "displayname": "Feeding Statuses",
            "distincts": ["peak_group__msrun__sample__animal__feeding_status"],
            "filter": None,
        },
        {
            "displayname": "Corrected Abundances",  # Append " > 0.1" based on filter
            "distincts": ["corrected_abundance"],
            "filter": getSimpleFilter("corrected_abundance", "gt", 0.1),
        },
        {
            "displayname": "Samples",
            "distincts": ["peak_group__msrun__sample__name"],
            "filter": None,
        },
        {
            "displayname": "Tissues",
            "distincts": ["peak_group__msrun__sample__tissue__name"],
            "filter": None,
        },
        {
            "displayname": "Tracer Compounds",
            "distincts": ["peak_group__msrun__sample__animal__tracer_compound__name"],
            "filter": None,
        },
        {
            "displayname": "Measured Compounds",
            "distincts": ["peak_group__compounds__name"],
            "filter": None,
        },
    ]
    model_instances = {
        "PeakData": {
            "model": "PeakData",
            "path": "",
            "reverse_path": "",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Data Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    # "handoff": "",
                    # Using in link will expose the internal index field in the search form because there's no
                    # searchable unique field for handoff
                    "type": "number",
                },
                "labeled_element": {
                    "displayname": "Labeled Element",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
                },
                "labeled_count": {
                    "displayname": "Labeled Count",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "raw_abundance": {
                    "displayname": "Raw Abundance",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "corrected_abundance": {
                    "displayname": "Corrected Abundance",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "fraction": {
                    "displayname": "Fraction",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "med_mz": {
                    "displayname": "Median M/Z",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "med_rt": {
                    "displayname": "Median RT",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "PeakGroup": {
            "model": "PeakGroup",
            "path": "peak_group",
            "reverse_path": "peak_data",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Peak Group",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "formula": {
                    "displayname": "Formula",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "MeasuredCompound": {
            "model": "Compound",
            "path": "peak_group__compounds",
            "reverse_path": "peak_groups__peak_data",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Measured Compound Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Measured Compound (Primary Synonym)",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "CompoundSynonym": {
            "model": "CompoundSynonym",
            "path": "peak_group__compounds__synonyms",
            "reverse_path": "compound__peak_groups__peak_data",
            "manytomany": {
                "is": True,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Compound Synonym Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Measured Compound (Any Synonym)",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "PeakGroupSet": {
            "model": "PeakGroupSet",
            "path": "peak_group__peak_group_set",
            "reverse_path": "peak_groups__peak_data",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Set Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
                    "handoff": "filename",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "filename": {
                    "displayname": "Peak Group Set Filename",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Sample": {
            "model": "Sample",
            "path": "peak_group__msrun__sample",
            "reverse_path": "msruns__peak_groups__peak_data",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Sample Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
                    "type": "number",
                    "handoff": "name",
                },
                "name": {
                    "displayname": "Sample",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Tissue": {
            "model": "Tissue",
            "path": "peak_group__msrun__sample__tissue",
            "reverse_path": "samples__msruns__peak_groups__peak_data",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "Tissue",
                    "searchable": True,
                    "displayed": False,
                    "type": "number",
                    "handoff": "name",
                },
                "name": {
                    "displayname": "Tissue",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Animal": {
            "model": "Animal",
            "path": "peak_group__msrun__sample__animal",
            "reverse_path": "samples__msruns__peak_groups__peak_data",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Animal Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Animal",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "body_weight": {
                    "displayname": "Body Weight (g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "genotype": {
                    "displayname": "Genotype",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "sex": {
                    "displayname": "Sex",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": Animal.SEX_CHOICES,
                },
                "age": {
                    "displayname": "Age (d-hh:mm:ss)",
                    "searchable": False,
                    "displayed": True,
                    "type": "number",
                },
                "feeding_status": {
                    "displayname": "Feeding Status",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "diet": {
                    "displayname": "Diet",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "tracer_infusion_concentration": {
                    "displayname": "Tracer Infusion Concentration (mM)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "tracer_infusion_rate": {
                    "displayname": "Tracer Infusion Rate (ul/min/g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Protocol": {
            "model": "Protocol",
            "path": "peak_group__msrun__sample__animal__treatment",
            "reverse_path": "animals__samples__msruns__peak_groups__peak_data",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Protocol Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Treatment",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "TracerCompound": {
            "model": "Compound",
            "path": "peak_group__msrun__sample__animal__tracer_compound",
            "reverse_path": "animals__samples__msruns__peak_groups__peak_data",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Tracer Compound Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Tracer Compound (Primary Synonym)",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Study": {
            "model": "Study",
            "path": "peak_group__msrun__sample__animal__studies",
            "reverse_path": "animals__samples__msruns__peak_groups__peak_data",
            "manytomany": {
                "is": True,
                "split_rows": False,
                "root_annot_fld": "study",
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Study Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Study",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
    }


class FluxCircSearchView(BaseSearchView):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "fctemplate"
    name = "Fcirc"
    rootmodel = PeakGroup
    stats = None
    model_instances = {
        "PeakGroup": {
            "model": "PeakGroup",
            "path": "",
            "reverse_path": "",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    # "handoff": "",
                    # Using in link will expose the internal index field in the search form because there's no
                    # searchable unique field for handoff
                    "type": "number",
                },
                "rate_disappearance_average_per_gram": {
                    "displayname": "Average Rd (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_appearance_average_per_gram": {
                    "displayname": "Average Ra (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_disappearance_average_weight_normalized": {
                    "displayname": "Average Rd (nmol/min)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_appearance_average_weight_normalized": {
                    "displayname": "Average Ra (nmol/min)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_disappearance_intact_per_gram": {
                    "displayname": "Intact Rd (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_appearance_intact_per_gram": {
                    "displayname": "Intact Ra (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_disappearance_intact_weight_normalized": {
                    "displayname": "Intact Rd (nmol/min)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_appearance_intact_weight_normalized": {
                    "displayname": "Intact Ra (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Animal": {
            "model": "Animal",
            "path": "msrun__sample__animal",
            "reverse_path": "samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Animal Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Animal",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "genotype": {
                    "displayname": "Genotype",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "body_weight": {
                    "displayname": "Body Weight (g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "age": {
                    "displayname": "Animal Age (d-hh:mm:ss)",
                    "searchable": False,  # currently no data available for testing and a bug: issue #180
                    "displayed": True,
                    "type": "number",
                },
                "sex": {
                    "displayname": "Sex",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": Animal.SEX_CHOICES,
                },
                "diet": {
                    "displayname": "Diet",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "feeding_status": {
                    "displayname": "Feeding Status",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "tracer_labeled_atom": {
                    "displayname": "Tracer Labeled Element",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
                },
                "tracer_infusion_rate": {
                    "displayname": "Tracer Infusion Rate (ul/min/g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "tracer_infusion_concentration": {
                    "displayname": "Tracer Infusion Concentration (mM)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Protocol": {
            "model": "Protocol",
            "path": "msrun__sample__animal__treatment",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Protocol Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Treatment",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Sample": {
            "model": "Sample",
            "path": "msrun__sample",
            "reverse_path": "msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Sample Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    # "handoff": "",
                    # Using in link will expose the internal index field in the search form because there's no
                    # searchable unique field for handoff
                    "type": "number",
                },
                "time_collected": {
                    "displayname": "Time Collected (hh:mm:ss since infusion)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Compound": {
            "model": "Compound",
            "path": "msrun__sample__animal__tracer_compound",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Tracer Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Tracer Compound",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Study": {
            "model": "Study",
            "path": "msrun__sample__animal__studies",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manytomany": {
                "is": True,
                "split_rows": False,
                "root_annot_fld": "study",
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Study Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Study",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
    }

    def getRootQuerySet(self):
        # https://stackoverflow.com/questions/3397437/manually-create-a-django-queryset-or-rather-manually-add-objects-to-a-queryset
        serum_tracer_peakgroups = set()
        for pg in self.rootmodel.objects.filter(
            msrun__sample__tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        ):
            if pg.is_tracer_compound_group:
                serum_tracer_peakgroups.add(pg.id)
        return self.rootmodel.objects.filter(id__in=serum_tracer_peakgroups)


class BaseAdvancedSearchView:
    """
    This class groups all search output formats in a single class and adds metadata that applies to all
    search output formats as a whole.  It includes all derived classes of BasSearchView.  BaseSearchView
    differs from this class in that BaseSearchView has an instance for each search output format class and
    this class has a single instance for all search view output format classes.
    """

    modes = ["search", "browse"]
    default_mode = "search"
    default_format = ""
    modeldata: Dict[int, BaseSearchView] = {}

    def __init__(self):
        """
        This is a constructor that adds all search output format classes to modeldata, keyed on their IDs.
        """

        for cls in (PeakGroupsSearchView(), PeakDataSearchView(), FluxCircSearchView()):
            self.modeldata[cls.id] = cls
        self.default_format = PeakGroupsSearchView.id

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


def extractFldPaths(qry):
    """
    Takes a qry object and returns the fld values under the tree of the selectedtemplate.
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


class UnknownComparison(Exception):
    pass
