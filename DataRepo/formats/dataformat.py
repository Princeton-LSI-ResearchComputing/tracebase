import warnings
from copy import deepcopy
from datetime import timedelta
from typing import Dict, List, Optional

from django.db.models import CharField, F, Model, Value
from pytimeparse.timeparse import timeparse

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
    getUnits,
    getValue,
    isAllGroup,
    isQuery,
    isQueryGroup,
    setField,
    splitCommon,
    splitPathName,
)
from DataRepo.models.utilities import dereference_field, get_model_by_name


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
            ("exact", "is"),
            ("not_exact", "is not"),
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
    unit_options = {
        # The following dicts are used to populate a units select list (for fields of type "number" only).
        # To use, set the field units in the model_instances attribute of derived class like this:
        #
        #     self.model_instances[instance_name]["fields"][field_name]["units"] = {
        #         "type": "postgres_interval",  # This is the key below
        #         "default": "weeks",  # This is the next key 1 level deeper that is to be the default units
        #         "subset": ["months", "weeks", "days", "hours"],  # A subset of keys to include in the select list
        #     }
        #
        # The values above are selected from what's below.  Note, the order of the subset is how the select list will
        # be populated.
        "identity": {
            "default": "identity",
            "entry_options": {
                "identity": {
                    "name": "identity",
                    "example": None,
                    "convert": lambda v: v,
                    "pyconvert": lambda v: v,
                },
            },
        },
        # TODO: Enforce that "identity" always exists as a key in entry_options, because it's hard-coded in places
        "postgres_interval": {
            "default": "native",  # Override: model_instances[instance_name]["fields"][field_name]["units"]["default"]
            # The following has only been tested to work with DurationField lookups and a postgres database
            # (e.g. Animal.objects.filter(age__gt=converted_value) where converted_value is the user's entry in val
            # with the convert method below has been applied to it)
            # Documentation: https://www.postgresql.org/docs/current/datatype-datetime.html
            "entry_options": {
                "native": {
                    # format: [nn.nn{units}{:|,}[ ]]+
                    "name": "n.n{units},...",
                    "example": "1w,1d,1:01:01.1",
                    "convert": lambda v: v,
                    "pyconvert": lambda v: timedelta(seconds=timeparse(v)),
                    "about": (
                        "Values can be entered using the following format pattern: `[n{units}{:|,}]*hh:mm:ss[.f]`, "
                        "where units can be:\n\n- c[enturies]\n- decades\n- y[ears]\n- months\n- w[eeks]\n- d[ays]\n- "
                        "h[ours]\n- m[inutes]\n- s[econds]\n- milliseconds\n- microseconds\n\nIf milli/micro-seconds "
                        "are not included, the last 3 units (hours, minutes, and seconds) do not need to be specified."
                        "\n\nExamples:\n\n- 1w,1d,1:01:01.1\n- 1 year, 3 months\n- 2:30\n- 2 days, 11:29:59.999"
                    ),
                },
                "calendartime": {
                    # format: [nn.nn{units},]+
                    "name": "ny,nm,nw,nd",
                    "example": "0y,1m,2w,3d",
                    "convert": lambda v: v,
                    # NOTE: this pyconvert will raise an exception on any unit greater than weeks, but it's currently
                    # only used for results stats, and we control that - not the user
                    "pyconvert": lambda v: timedelta(seconds=timeparse(v)),
                },
                "clocktime": {
                    # format: [hh:mm[:ss]]
                    "name": "clocktime (hh:mm[:ss])",
                    "example": "2:30:10.1",
                    "convert": lambda v: v,
                    # NOTE: this pyconvert will raise an exception on any unit greater than weeks, but it's currently
                    # only used for results stats, and we control that - not the user
                    "pyconvert": lambda v: timedelta(seconds=timeparse(v)),
                },
                "millennia": {
                    "name": "millennia",
                    "example": "1.0",
                    "convert": lambda v: f"{v}millenniums",
                    "pyconvert": lambda v: timedelta(days=float(v) * 1000 * 365.25),
                },
                "centuries": {
                    "name": "centuries",
                    "example": "1.0",
                    "convert": lambda v: f"{v}c",
                    "pyconvert": lambda v: timedelta(days=float(v) * 100 * 365.25),
                },
                "decades": {
                    "name": "decades",
                    "example": "1.0",
                    "convert": lambda v: f"{v}decades",
                    "pyconvert": lambda v: timedelta(days=float(v) * 10 * 365.25),
                },
                "years": {
                    "name": "years",
                    "example": "1.0",
                    "convert": lambda v: f"{v}y",
                    "pyconvert": lambda v: timedelta(days=float(v) * 365.25),
                },
                "months": {
                    "name": "months",
                    "example": "1.0",
                    "convert": lambda v: f"{v}months",
                    "pyconvert": lambda v: timedelta(days=float(v) * 30.437),
                },
                "weeks": {
                    "name": "weeks",
                    "example": "1.0",
                    "convert": lambda v: f"{v}w",
                    "pyconvert": lambda v: timedelta(weeks=float(v)),
                },
                "days": {
                    "name": "days",
                    "example": "1.0",
                    "convert": lambda v: f"{v}d",
                    "pyconvert": lambda v: timedelta(days=float(v)),
                },
                "hours": {
                    "name": "hours",
                    "example": "1.0",
                    "convert": lambda v: f"{v}h",
                    "pyconvert": lambda v: timedelta(hours=float(v)),
                },
                "minutes": {
                    "name": "minutes",
                    "example": "1.0",
                    "convert": lambda v: f"{v}m",
                    "pyconvert": lambda v: timedelta(minutes=float(v)),
                },
                "seconds": {
                    "name": "seconds",
                    "example": "1.0",
                    "convert": lambda v: f"{v}s",
                    "pyconvert": lambda v: timedelta(seconds=float(v)),
                },
                "milliseconds": {
                    "name": "milliseconds",
                    "example": "1.0",
                    "convert": lambda v: f"{v}milliseconds",
                    "pyconvert": lambda v: timedelta(milliseconds=float(v)),
                },
                "microseconds": {
                    "name": "microseconds",
                    "example": "1.0",
                    "convert": lambda v: f"{v}microseconds",
                    "pyconvert": lambda v: timedelta(microseconds=float(v)),
                },
            },
        },
    }

    static_filter = appendFilterToGroup(
        createFilterGroup(),
        createFilterCondition("", "", "", ""),
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

    def getFieldUnitsLookup(self):
        """
        This method is used in the backend to handle a user's search selections (i.e. it's main utility is to be used
        to call the correct convert function on the value the user entered in the val field to convert what they
        entered into the units/format that is recorded in the database, e.g. change "2" to "2 weeks").  This is
        separate from the getFieldUnitsDict method because that method's return is sent to the view/template in json
        format, and the convert function cannot be transmitted in that context.

        This creates a dict keyed on fld values (i.e. the path of each field included in a format, as indicated by the
        fld selected by the user in the qry object) mapped to that field's defined list of units options as recorded in
        self.unit_options, e.g.:

        - self.unit_options[units_key]["entry_options"]

        The value is a dict keyed on the values of the units select list and contains the selected units' name, example
        string, and convert function.  For example, for a key of `msrun__sample__animal__age`, a lookup of that key
        would look like:

        returned_dict["msrun__sample__animal__age"] -> {
            ...
            "months": {
                "name": "months",
                "example": "1.0",
                "convert": lambda v: f"{v}months",
                "pyconvert": lambda v: timedelta(days=float(v) * 30.437),
            },
            "weeks": {
                "name": "weeks",
                "example": "1.0",
                "convert": lambda v: f"{v}w",
                "pyconvert": lambda v: timedelta(weeks=float(v)),
            },
            ...
        }

        And the units value the user selected in the search form, corresponds to the keys in that dict (e.g. "months"
        or "weeks").
        """
        units_lookup = {}
        for mdl in self.model_instances.keys():
            for fld in self.model_instances[mdl]["fields"].keys():
                if (
                    self.model_instances[mdl]["path"]
                    and self.model_instances[mdl]["path"] != ""
                ):
                    path_fld = f"{self.model_instances[mdl]['path']}__{fld}"
                else:
                    path_fld = fld
                if "units" in self.model_instances[mdl]["fields"][fld].keys():
                    if (
                        "key"
                        not in self.model_instances[mdl]["fields"][fld]["units"].keys()
                    ):
                        raise KeyError(
                            f"Field 'key' is required in field {mdl}.{fld}'s units dict"
                        )
                    units_key = self.model_instances[mdl]["fields"][fld]["units"]["key"]
                    units_lookup[path_fld] = self.unit_options[units_key][
                        "entry_options"
                    ]
                else:
                    units_lookup[path_fld] = None
        return units_lookup

    def getFieldUnitsDict(self):
        """
        This method is used in the frontend to populate the search interface (i.e. it's main utility is to be used to
        create the units select list, update the val field's placeholder with a units example, and optionally provide
        an explanation of the units format in the form of a tooltip-linked info icon.  This is separate from the
        getFieldUnitsLookup method because this method's return is sent to the view/template in json format, and the
        convert function that is included in the getFieldUnitsLookup method's output cannot be transmitted in that
        context.

        Returns a dict of

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
        }

        path__field is used as the key so that the value of the selections in the fld select list can be directly used
        to update the units select list.
        """

        unitsdict = {}
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
                unitsdict[fldkey] = {}

                if "units" in self.model_instances[mdl]["fields"][fld].keys():
                    if self.model_instances[mdl]["fields"][fld]["type"] != "number":
                        raise TypeUnitsMismatch(
                            self.model_instances[mdl]["fields"][fld]["type"]
                        )
                    key = self.model_instances[mdl]["fields"][fld]["units"]["key"]
                    if (
                        "default"
                        in self.model_instances[mdl]["fields"][fld]["units"].keys()
                    ):
                        default = self.model_instances[mdl]["fields"][fld]["units"][
                            "default"
                        ]
                        if (
                            default
                            not in self.unit_options[key]["entry_options"].keys()
                        ):
                            raise KeyError(
                                f"Invalid default value: [{default}] for field {fld} in model instance {mdl}.  Must "
                                f"be one of: [{', '.join(self.unit_options[key]['entry_options'].keys())}]"
                            )
                    if "subset" in self.model_instances[mdl]["fields"][fld]["units"]:
                        opt_keys = self.model_instances[mdl]["fields"][fld]["units"][
                            "subset"
                        ]
                        bad_keys = []
                        for opt_key in opt_keys:
                            if (
                                opt_key
                                not in self.unit_options[key]["entry_options"].keys()
                            ):
                                bad_keys.append(opt_key)
                        if len(bad_keys) > 0:
                            raise ValueError(
                                f"Bad units subset key(s): [{', '.join(bad_keys)}]"
                            )
                    else:
                        opt_keys = self.unit_options[key]["entry_options"].keys()
                    print(
                        f"Setting default of {key} units to '{default}' for field {self.id}.{mdl}.{fld}"
                    )
                else:
                    print(
                        f"Setting default of 'identity' units to 'identity' for field {self.id}.{mdl}.{fld}"
                    )
                    key = "identity"
                    default = "identity"
                    opt_keys = ["identity"]

                unitsdict[fldkey]["units"] = key
                unitsdict[fldkey]["default"] = default
                unitsdict[fldkey]["choices"] = ()
                unitsdict[fldkey]["metadata"] = {}
                for unit_key in opt_keys:
                    # Populate the choices for dynamically changing the units select list
                    unitsdict[fldkey]["choices"] = unitsdict[fldkey]["choices"] + (
                        (
                            unit_key,
                            self.unit_options[key]["entry_options"][unit_key]["name"],
                        ),
                    )

                    # Record examples and "about" info.  Example strings will be included in the field placeholder
                    # Have to use the value as the metadatakey, because retrieval of the example/about strings will be
                    # based on the units select list's selected value
                    unitsdict[fldkey]["metadata"][unit_key] = {}
                    unitsdict[fldkey]["metadata"][unit_key][
                        "example"
                    ] = self.unit_options[key]["entry_options"][unit_key]["example"]
                    if (
                        "about"
                        in self.unit_options[key]["entry_options"][unit_key].keys()
                    ):
                        unitsdict[fldkey]["metadata"][unit_key][
                            "about"
                        ] = self.unit_options[key]["entry_options"][unit_key]["about"]
                    else:
                        unitsdict[fldkey]["metadata"][unit_key]["about"] = None

        return unitsdict

    def getAllFieldUnitsChoices(self):
        """
        Returns the union of all unit_options, ignoring differences in the second value. This is mainly only for form
        validation because it only validates known values (the first value in each tuple) regardless of the particular
        sub-population controlled by javascript in the advanced search form.
        """
        all_unit_choices = ()
        seen = []
        for fldtype in self.unit_options.keys():
            for opt_key in self.unit_options[fldtype]["entry_options"].keys():
                if opt_key not in seen:
                    seen.append(opt_key)
                    opt_name = self.unit_options[fldtype]["entry_options"][opt_key][
                        "name"
                    ]
                    all_unit_choices = all_unit_choices + ((opt_key, opt_name),)
        return all_unit_choices

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
        # This gets non-root model key paths (that are not "through" models) sorted in descending order of their length
        desc_len_sorted_paths = [
            self.model_instances[x]["path"]
            for x in self.getModelInstances()
            if (
                self.model_instances[x]["path"] != ""
                and (
                    "through" not in self.model_instances[x]["manyrelated"]
                    or not self.model_instances[x]["manyrelated"]["through"]
                )
            )
        ]
        # This filters out paths that are contained inside other paths
        unique_paths = []
        for path in sorted(
            desc_len_sorted_paths,
            key=len,
            reverse=True,
        ):
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
        Takes a qry object and a units lookup dict (that maps the path version of fld [e.g. msrun__sample__animal__age]
        to a dict that contains the units options, including most importantly, a convert function that is found via the
        selected units key recorded in the qry) and returns a list of prefetch paths.  If a prefetch path contains
        models that are related M:M with the root model, that prefetch path will be split into multiple paths (all that
        end in a M:M model, and the remainder (if any of the path is left)).  Any path ending in a M:M model will be
        represented as a 3-member list containing the path, a re-rooted qry object, and the name of the new root model.
        The returned prefetches will be a mixed list of strings (containing simple prefetch paths for 1:1 relations)
        and sublists containing:

        - The simple prefetch path of a rerooted M:M related model
        - A rerooted qry object in order to perform the same query using the M:M related model as the root table for an
          independent query using `Prefetch()`
        - The name of the new root model
        - A new units lookup where the path keys are rerooted
        """

        # Sort the paths so that multiple subquery prefetches on the same path are encountered hierarchically.
        # This is based on the assumption that prefetch filters work serially and are applied iteratively.  So if a
        # compound is filtered and then compound synonyms are filtered, the synonyms operate on the already filtered
        # compounds.  This migth be a false assumption.
        fld_paths = sorted(extractFldPaths(qry), key=len)

        new_units_lookup = deepcopy(self.getFieldUnitsLookup())

        # Identify the fld paths that need a subquery in its prefetch and collect those paths associated with their
        # rerooted qry objects
        subquery_paths = []
        for srch_path_str in fld_paths:
            srch_model_inst_name = self.pathToModelInstanceName(srch_path_str)
            if (
                self.model_instances[srch_model_inst_name]["manyrelated"]["manytomany"]
                and self.model_instances[srch_model_inst_name]["manyrelated"][
                    "split_rows"
                ]
            ):
                new_qry = self.reRootQry(qry, srch_model_inst_name, new_units_lookup)
                subquery_paths.append(
                    [
                        srch_path_str,
                        new_qry,
                        self.model_instances[srch_model_inst_name]["model"],
                        new_units_lookup,
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
            sq_units_lkup = subquery_path[3]
            matched = False
            for pf_str in prefetches:
                if sq_path in pf_str:
                    if matched:
                        continue
                    if pf_str not in prefetch_dict.keys():
                        prefetch_dict[pf_str] = {}
                    prefetch_dict[pf_str][sq_path] = [
                        sq_path,
                        sq_qry,
                        sq_mdl,
                        sq_units_lkup,
                    ]
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
            if (
                "root_annot_fld"
                in self.model_instances[mdl_inst_nm]["manyrelated"].keys()
                and self.model_instances[mdl_inst_nm]["manyrelated"]["root_annot_fld"]
                and self.model_instances[mdl_inst_nm]["manyrelated"]["root_annot_fld"]
                != ""
                and self.model_instances[mdl_inst_nm]["manyrelated"]["is"]
                and not self.model_instances[mdl_inst_nm]["manyrelated"]["split_rows"]
            ):
                annot_fld = self.model_instances[mdl_inst_nm]["manyrelated"][
                    "root_annot_fld"
                ]
                # This is a special case.  When this is a many-related model (WRT the root model where the annotations
                # are saved), and we are not splitting on this relation, setting the value to an empty string has
                # special meaning: get every such related record to put on a single row related to the linked root
                # record.  See the get_many_related_rec method inside customtags.py.
                annotations.append({annot_fld: Value("", output_field=CharField())})

            # If this model is many to many related, and we want a full join
            if (
                # See TODO above
                # self.model_instances[mdl_inst_nm]["manyrelated"]["manytomany"]
                # and
                self.model_instances[mdl_inst_nm]["manyrelated"]["split_rows"]
            ):
                # If a root_annot_fld key exists in the "manyrelated" dict, use it
                if (
                    "root_annot_fld"
                    in self.model_instances[mdl_inst_nm]["manyrelated"].keys()
                ):
                    annot_fld = self.model_instances[mdl_inst_nm]["manyrelated"][
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

            # If these fields are distinct, annotating their values (if the field has a root_annot_fld) makes the
            # template cleaner
            if (
                # See TODO above
                # "distinct" in self.model_instances[mdl_inst_nm]
                # and self.model_instances[mdl_inst_nm]["distinct"]
                # and
                self.model_instances[mdl_inst_nm]["manyrelated"]["split_rows"]
            ):
                for fld_nm in self.model_instances[mdl_inst_nm]["fields"].keys():
                    if (
                        "root_annot_fld"
                        in self.model_instances[mdl_inst_nm]["fields"][fld_nm].keys()
                    ):
                        distinct_annot_fld = self.model_instances[mdl_inst_nm][
                            "fields"
                        ][fld_nm]["root_annot_fld"]
                        try:
                            fld = dereference_field(
                                fld_nm, self.model_instances[mdl_inst_nm]["model"]
                            )
                            annotations.append(
                                {
                                    distinct_annot_fld: F(
                                        self.model_instances[mdl_inst_nm]["path"]
                                        + "__"
                                        + fld
                                    )
                                }
                            )
                        except AttributeError as ae:
                            # We cannot annotate with "fields" that are properties
                            if "'property' object" not in str(ae):
                                raise ae

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

        path__field is used as the key so that the value of the selections in the fld select list can be directly used
        to update the ncmp select list
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

    def reRootQry(self, qry, new_root_model_instance_name, units_lookup=None):
        """
        This takes a qry object and the name of a model instance in the composite view and re-roots the fld values,
        making all the field paths come from a different model root.  It is intended to be used for prefetch
        subqueries.
        """
        ret_qry = deepcopy(qry)
        self.reRootQryHelper(
            getSearchTree(ret_qry, self.id), new_root_model_instance_name, units_lookup
        )
        return ret_qry

    def reRootQryHelper(self, subtree, new_root_model_instance_name, units_lookup=None):
        """
        Recursive helper to reRootQry
        """
        if isQueryGroup(subtree):
            for child in getChildren(subtree):
                self.reRootQryHelper(child, new_root_model_instance_name, units_lookup)
        elif isQuery(subtree):
            old_fld = getField(subtree)
            setField(
                subtree,
                self.reRootFieldPath(old_fld, new_root_model_instance_name),
            )
            new_fld = getField(subtree)
            if units_lookup and old_fld != new_fld:
                units_lookup[new_fld] = units_lookup[old_fld]
                units_lookup.pop(old_fld)
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
        mdl = get_model_by_name(mdl_nm)

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
            custom_distinct_fields_exist = (
                "distinct" in self.model_instances[mdl_inst_nm].keys()
                and self.model_instances[mdl_inst_nm]["distinct"]
            )

            # The way to split root records into multiple records (as if it was a left join) is via the distinct
            # method.  So to do a full left join, you would call this method with split_all=True.  It is unnecessary to
            # do this for 1:M tables because django compiles those records automatically.  The only thing it does not
            # handle is M:M relationships.

            # If the split_all override in false and there exist custom distinct fields defined
            if not split_all and custom_distinct_fields_exist:

                for distinct_fld_nm in self.model_instances[mdl_inst_nm][
                    "fields"
                ].keys():
                    try:
                        fld = (
                            self.model_instances[mdl_inst_nm]["path"]
                            + "__"
                            + dereference_field(
                                distinct_fld_nm,
                                self.model_instances[mdl_inst_nm]["model"],
                            )
                        )
                        distinct_fields.append(fld)
                    except AttributeError as ae:
                        # We can ignore/skip "fields" that are properties
                        if "'property' object" not in str(ae):
                            raise ae

            elif (
                # TODO: See: https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/484
                # If the split_all override was supplied as true and this is a M:M model
                split_all
                and self.model_instances[mdl_inst_nm]["manyrelated"]["manytomany"]
            ) or (
                # Always split if split_rows is true and there aren't custom distinct fields
                self.model_instances[mdl_inst_nm]["manyrelated"]["split_rows"]
                and not custom_distinct_fields_exist
                # Note if there are custom distinct fields and split_all=True (but this model is not M:M), we are
                # intentionally returning nothing because we want to split records that are otherwise combined by the
                # custom fields
            ):
                # Django's ordering fields are required when any field is provided to .distinct().  Otherwise, you
                # get the error: `ProgrammingError: SELECT DISTINCT ON expressions must match initial ORDER BY
                # expressions`
                tmp_distincts = self.getOrderByFields(mdl_inst_nm)
                for fld_nm in tmp_distincts:

                    # Remove potential loop added to the path when ordering_fields are dereferenced
                    # E.g. This changes "peak_data__labels__peak_data__peak_group__name" to
                    # "peak_data__peak_group__name"
                    field_path_array = fld_nm.split("__")
                    model_path_array = self.model_instances[mdl_inst_nm]["path"].split(
                        "__"
                    )
                    if (
                        len(field_path_array) > 1
                        and field_path_array[0] in model_path_array
                    ):
                        path_array = []
                        first = model_path_array.index(field_path_array[0])
                        path_array = model_path_array[0:first]
                        path_array += field_path_array
                        fld = "__".join(path_array)
                    else:
                        fld = self.model_instances[mdl_inst_nm]["path"] + "__" + fld_nm

                    distinct_fields.append(fld)

                # Don't assume the ordering fields are populated/unique, so include the primary key.  Duplicate
                # fields should be OK (though I haven't tested it).
                # Note, this assumes that being here means we're in a related table and not the root table, so path
                # is not an empty string
                distinct_fields.append(
                    self.model_instances[mdl_inst_nm]["path"] + "__pk"
                )

        # If there are any split_rows manytomany related tables, we will need to prepend the ordering (and pk) fields
        # of the root model
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
            fld = getField(query)
            val = getValue(query)
            ncmp = getComparison(query)
            units = getUnits(query)
            recval = rootrec[field_order.index(fld)]
            searchterm = self.matchUnits(fld, val, units)
            return self.meetsCondition(recval, ncmp, searchterm)
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

    def matchUnits(self, fld, val, units):
        """
        This is a python code version of matching units of the search term with the value returned from a query.  It
        takes the field path, the search term value, and the units, and returns the search term value in the units/
        format that the database returns so that they can be compared in `self.meetsCondition()`.

        If
        """
        units_lookup = self.getFieldUnitsLookup()
        if fld in units_lookup.keys():
            if units in units_lookup[fld].keys():
                try:
                    return units_lookup[fld][units]["pyconvert"](val)
                except Exception as e:
                    # Gracefully fail and log the problem
                    warnings.warn(
                        f"Python conversion of units [{units}] for field [{fld}] value [{val}] in format "
                        f"[{self.name}] failed with an exception: {str(e)}.  Returning original value.  Fix the "
                        "[pyconvert] function in Format.unit_options to better handle this value."
                    )
            warnings.warn(
                f"Units [{units}] dict for field [{fld}] not found for format [{self.name}]."
            )
        warnings.warn(
            f"Units lookup is missing field key [{fld}] not found for format [{self.name}]."
        )
        return val

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
        elif condition == "exact":
            return recval == searchterm
        elif condition == "not_exact":
            return recval != searchterm
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


class TypeUnitsMismatch(Exception):
    def __init__(self, type):
        message = (
            f"Unsupported combination of field type {type} and units.  Only fields of type 'number' can have unit "
            "options."
        )
        super().__init__(message)
        self.type = type
