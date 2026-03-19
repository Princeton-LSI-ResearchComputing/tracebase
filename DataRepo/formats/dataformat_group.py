import json
import time
from copy import deepcopy
from typing import Dict, Optional

from django.conf import settings
from django.core.exceptions import (
    FieldError,
    ObjectDoesNotExist,
    ValidationError,
)
from django.db.models import Prefetch
from django.db.utils import ProgrammingError

from DataRepo.formats.dataformat import Format
from DataRepo.formats.dataformat_group_query import (
    construct_advanced_query,
    get_num_empty_queries,
    get_selected_format,
    set_first_empty_query,
)
from DataRepo.models.utilities import get_model_by_name

SAFE_TIMEOUT_SECS = max(settings.GATEWAY_TIMEOUT - 5, 0)


class FormatGroup:
    """
    This class groups all search output formats in a single class and adds metadata that applies to all search output
    formats as a whole.  It includes all derived classes of Format.
    """

    modes = ["search", "browse"]
    default_mode = "search"
    default_format = None
    modeldata: Dict[int, Format] = {}

    def add_formats(self, format_classes):
        """
        Add formats and set the default to the first class, unless default already set.
        """
        for cls in format_classes:
            self.modeldata[cls.id] = cls
        if self.default_format is None:
            self.default_format = format_classes[0].id

    def set_default_format(self, format_class):
        self.default_format = format_class.id

    def set_default_mode(self, mode):
        if mode not in self.modes:
            raise ValueError(
                f"Invalid mode: {mode}.  Must be one of: [{', '.join(self.modes)}]"
            )
        self.default_mode = mode

    def get_root_group(self, selfmt=None):
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
        root_group = {
            "selectedtemplate": selfmt,
            "searches": {},
        }
        for format in self.modeldata.keys():
            root_group["searches"][format] = {}
            root_group["searches"][format]["name"] = self.modeldata[format].name
            if self.static_filter_is_valid(self.modeldata[format].static_filter):
                root_group["searches"][format]["tree"] = deepcopy(
                    self.modeldata[format].static_filter
                )
            else:
                print(
                    f"ERROR: No empty queries in format {format}: ",
                    self.modeldata[format].static_filter,
                )
                raise ValueError(
                    f"Static filter for format {format} must contain at least 1 non-static empty query."
                )
        return root_group

    def static_filter_is_valid(self, filter):
        """
        Takes a "tree" value of 1 format from the rootGroup query object and raises an exception for missing keys or
        invalid values in the root query and calls static_filterIs_valid_helper to recursively validate the treee.
        """

        if (
            "type" not in filter
            or filter["type"] != "group"
            or "queryGroup" not in filter
            or len(filter["queryGroup"]) == 0
        ):
            raise ValueError(
                "Invalid root query group.  Must be of type 'group' and contain a populated queryGroup array."
            )
        else:
            num_nonstatic = self.get_num_non_static_groups(filter)
            if num_nonstatic == 0:
                raise ValueError(
                    "Invalid root query group.  There must exist at least 1 non-static query group."
                )
            return self.static_filter_is_valid_helper(filter)

    def static_filter_is_valid_helper(self, filter):
        """
        Raises an exception for missing keys or invalid values and returns true if at least 1 empty query exists among
        all recursively checked objects of type query.
        """
        # Validate the keys present in both query and group types
        if "type" not in filter or "val" not in filter or "static" not in filter:
            raise ValueError(
                "Static filter is missing 1 or more required keys: [type, val, static]."
            )
        elif filter["type"] == "query":
            # Validate the keys of the query
            if "ncmp" not in filter or "pos" not in filter or "fld" not in filter:
                raise ValueError(
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
                raise ValueError(
                    "Invalid group.  Must contain a queryGroup key with a populated array and val must be either "
                    "'all' or 'any'."
                )
            empty_exists = False
            for query in filter["queryGroup"]:
                if self.static_filter_is_valid_helper(query):
                    empty_exists = True
            return empty_exists
        else:
            raise ValueError(
                f"Invalid query type {filter['type']}.  Must be either 'query' or 'group'."
            )

    def get_num_non_static_groups(self, filter):
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
                total_nonstatic += self.get_num_non_static_groups(query)
            return total_nonstatic
        else:
            raise ValueError(
                f"Invalid query type {filter['type']}.  Must be either 'query' or 'group'."
            )

    def get_root_query_set(self, format):
        """
        Calls get_root_query_set of the supplied format.
        """
        return self.modeldata[format].get_root_query_set()

    def get_prefetches(self, format):
        """
        Calls get_prefetches of the supplied ID of the search output format class.
        """
        return self.modeldata[format].get_prefetches()

    def get_true_join_prefetch_paths_and_qrys(self, qry, format=None):
        """
        Calls get_true_join_prefetch_paths_and_qrys of the supplied ID of the search output format class.
        """
        selfmt = get_selected_format(qry)
        if format is not None and format != selfmt:
            raise ValueError(
                f"Supplied format: [{format}] does not match the qry selected format: [{selfmt}]"
            )
        elif format is None:
            format = selfmt
        return self.modeldata[format].get_true_join_prefetch_paths_and_qrys(qry)

    def get_search_field_choices(self, format):
        """
        Calls get_search_field_choices of the supplied ID of the search output format class.
        """
        return self.modeldata[format].get_search_field_choices()

    def get_search_field_choices_dict(self):
        """
        Creates a format-keyed dict of the fld choices tuples.  Used to populate the fld select list to
        address issue #229 in the same way ncmp_choices is populated and pared down by javascript.
        """
        fld_choices = {}
        for fmtid in self.modeldata.keys():
            fld_choices[fmtid] = self.modeldata[fmtid].get_search_field_choices()
        return fld_choices

    def get_all_search_field_choices(self):
        """
        Creates a flat tuple of every field in every model of every derived class.  Used to initially populate the fld
        select list that is updated by javascript based on the selected format.  This initial population in the form
        class is important for form validation.  Otherwise, nothing but the selected format will validate, causing
        issues like the one described in #229.
        """
        all_fld_choices = ()
        seen = []
        for fmtid in self.modeldata.keys():
            for fld_val, fld_name in self.get_search_field_choices(fmtid):
                if fld_val not in seen:
                    seen.append(fld_val)
                    all_fld_choices = all_fld_choices + ((fld_val, fld_name),)
        return all_fld_choices

    def get_field_units_lookup(self, fmt):
        return self.modeldata[fmt].get_field_units_lookup()

    def get_field_units_dict(self):
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
            fld_units[fmtid] = self.modeldata[fmtid].get_field_units_dict()

        return fld_units

    def get_all_field_units_choices(self):
        """
        Calls get_all_field_units_choices of the default output format class.

        All units options are the same for every Format class contained in this class, so we only need to call one.
        """
        return self.modeldata[self.default_format].get_all_field_units_choices()

    def get_comparison_choices(self):
        """
        Calls get_comparison_choices of the default output format class.

        All ncmp_choices are the same for every Format class contained in this class, so it doesn't matter which one we
        use.
        """
        return self.modeldata[self.default_format].get_comparison_choices()

    def get_all_comparison_choices(self):
        """
        Calls get_all_comparison_choices of the default output format class.

        All ncmp_choices are the same for every Format class contained in this class, so we only need to call one.
        """
        return self.modeldata[self.default_format].get_all_comparison_choices()

    def get_model_instances(self, format):
        """
        Calls get_model_instances of the supplied ID of the search output format class.
        """
        return self.modeldata[format].get_model_instances()

    def get_model_instance(self, format, mdl):
        """
        Calls get_model_instance of the supplied ID of the search output format class.
        """
        return self.modeldata[format].get_model_instance(mdl)

    def get_model_from_instance(self, format, mdl_inst):
        """
        Calls get_model_from_instance of the supplied ID of the search output format class.
        """
        return self.modeldata[format].get_model_from_instance(mdl_inst)

    def get_format_names(self):
        """
        Returns a dict of search output format class IDs to their user-facing names.
        """
        namedict = {}
        for fmtid in self.modeldata.keys():
            namedict[fmtid] = str(self.modeldata[fmtid].name)
        return namedict

    def get_field_types(self):
        """
        Returns a dict of fmt -> path__field -> {type -> field_type (number, string, enumeration), choices -> list of
        tuples}.
        """
        typedict = {}
        for fmtid in self.modeldata.keys():
            typedict[fmtid] = self.modeldata[fmtid].get_field_types()
        return typedict

    def get_search_fields(self, fmt, mdl):
        """
        Takes a format key and model and returns a dict of searchable field name -> field key path
        """
        return self.modeldata[fmt].get_search_fields(mdl)

    def get_display_fields(self, fmt, mdl):
        """
        Takes a format key and model and returns a dict of field name -> display field name (if there exists a handoff
        from a non-displayed field to a displayed one)
        """
        return self.modeldata[fmt].get_display_fields(mdl)

    def get_key_path_list(self, fmt, mdl):
        """
        Calls get_key_path_list of the supplied ID of the search output format class.
        """
        return self.modeldata[fmt].get_key_path_list(mdl)

    def format_name_or_key_to_key(self, fmtsubmitted):
        """
        Takes a search output format ID or name and returns the corresponding search output format
        ID.  This method exists to facilitate the usage of (case-insensitive) format names in search_basic URLs.
        """

        fmtkey = fmtsubmitted
        names = self.get_format_names()
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

    def re_root_qry(self, fmt, qry, new_root_model_instance_name):
        return self.modeldata[fmt].re_root_qry(qry, new_root_model_instance_name)

    def get_distinct_fields(
        self, fmt, order_by=None, assume_distinct=True, split_all=False
    ):
        return self.modeldata[fmt].get_distinct_fields(
            order_by, assume_distinct, split_all
        )

    def get_order_by_fields(self, fmt):
        return self.modeldata[fmt].get_order_by_fields(
            model_name=self.modeldata[fmt].rootmodel.__name__
        )

    def get_full_join_annotations(self, fmt):
        return self.modeldata[fmt].get_full_join_annotations()

    def get_stats_params(self, fmt):
        return self.modeldata[fmt].get_stats_params()

    def stats_available(self, fmt):
        return self.modeldata[fmt].stats_available()

    def meets_all_conditions_by_val_list(self, fmt, rootrec, query, field_order):
        """
        This is a python-code version of a complex Q expression, necessary for checking filters in aggregate count
        annotations, because the Django ORM does not support .distinct(fields).annotate(Count) when duplicate root
        table records exist.
        """
        return self.modeldata[fmt].meets_all_conditions_by_val_list(
            rootrec, query, field_order
        )

    def search_field_to_display_field(self, mdl_instance, fld, val, qry):
        """
        Takes a field from a basic search and converts it to a non-hidden field for an advanced search select list.

        Currently, this is only used for id fields, but is written to handle any field.  Warning: this will only work
        when the query does not involve "or" groups in the query.  This is not checked.  It is assumed.
        """

        dfld = fld
        dval = val
        fmt = get_selected_format(qry)
        dfields = self.get_display_fields(fmt, mdl_instance)
        mdl = self.get_model_from_instance(fmt, mdl_instance)

        # If fld is not a displayed field
        if fld in dfields.keys() and dfields[fld] != fld:
            # grab the model using its name
            mdl_cls = get_model_by_name(mdl)
            # Set the query parameters
            kv_field_value = {f"{fld}__exact": val}
            # Create the subquery queryset
            qs = mdl_cls.objects.filter(**kv_field_value)
            if qs.count() == 0:
                print(
                    f"WARNING: Failed search-field to display-field conversion in format {fmt} for: "
                    f"[{mdl}.{fld}='{val}'].  No matching {mdl} records found."
                )
            elif qs.count() > 1:
                # We only expect to get here if fld is not a unique field.

                # The value may be unique in the root queryset subset
                recs, tot, _ = self.perform_query(qry, fmt)

                # Note, "recs" is the root model records, not mdl records, so we except multiple results
                if tot == 0:
                    print(
                        f"WARNING: Failed search-field to display-field conversion in format {fmt} for: "
                        f"[{mdl}.{fld}='{val}'].  No matching {self.modeldata[fmt].rootmodel.__name__} "
                        "records found."
                    )
                else:
                    # Set the field path for the display field
                    dfld = dfields[fld]
                    dval = self.get_joined_rec_field_value(
                        recs, fmt, mdl_instance, dfields[fld], fld, val
                    )
            else:
                mdl_rec = qs.first()
                dfld = dfields[fld]
                dval = getattr(mdl_rec, dfld)

        return dfld, dval

    # Warning, the code in this method would potentially not work in cases where multiple search terms (including a term
    # from a m:m related table) were or'ed together.  This cannot happen currently because this is only utilized for
    # handoff fields from search_basic, so the first record is guaranteed to have a matching value from the search term.
    def get_joined_rec_field_value(self, recs, fmt, mdl, dfld, sfld, sval):
        """
        Takes a queryset object and a model.field and returns its value.
        """

        if len(recs) == 0:
            print(
                f"WARNING: Unable to get field value in format {fmt} for field [{dfld}] where: "
                f"[{mdl}.{sfld}='{sval}'].  No matching {self.modeldata[fmt].rootmodel.__name__} records found."
            )
            raise ObjectDoesNotExist("Records not found.")

        kpl = self.get_key_path_list(fmt, mdl)
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
            raise ObjectDoesNotExist(
                f"ERROR: Unable to find a value for [{mdl}.{sfld}] that matches the search term.  Unable to "
                f"convert to the handoff field {dfld}."
            )

        return dval

    def get_all_browse_data(
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
        return self.perform_query(
            None, format, limit, offset, order_by, order_direction, generate_stats
        )

    def perform_query(
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
            selfmt = get_selected_format(qry)
            units_lookup = self.get_field_units_lookup(selfmt)
            q_exp = construct_advanced_query(qry, units_lookup)
            if fmt is not None and fmt != selfmt:
                raise ValueError(
                    f"The selected format in the qry object: [{selfmt}] does not match the supplied format: [{fmt}]"
                )
            else:
                fmt = selfmt
        elif fmt is None:
            raise ConditionallyRequiredArgumentError(
                "Neither a qry object nor a format was supplied.  1 of the 2 is required."
            )

        if fmt not in self.get_format_names().keys():
            raise KeyError(f"Invalid selected format: {fmt}")

        # If the Q expression is None, get all, otherwise filter
        if q_exp is None:
            results = self.get_root_query_set(fmt)
        else:
            results = self.get_root_query_set(fmt).filter(q_exp)

        # Get stats before applying order by and distinct so that unsplit rows can be accurately counted by making all
        # M:M related tables distinct
        stats = {
            "data": {},
            "populated": generate_stats,
            "show": False,
            "available": self.stats_available(fmt),
        }
        if generate_stats:
            data, based_on = self.get_query_stats(
                results, fmt, time_limit_secs=SAFE_TIMEOUT_SECS
            )
            stats["data"] = data
            stats["based_on"] = based_on
            stats["show"] = True

        # Order by
        if order_by is not None:
            order_by_arg = order_by
            if order_direction is not None:
                if order_direction == "desc":
                    order_by_arg = f"-{order_by}"
                elif order_direction and order_direction != "asc":
                    raise ValueError(
                        f"Invalid order direction: {order_direction}.  Must be 'asc' or 'desc'."
                    )
            results = results.order_by(order_by_arg)

        # This ensures the number of records matches the number of rows desired in the html table based on the
        # split_rows values configured in each format in SearchGroup
        distinct_fields = self.get_distinct_fields(fmt, order_by)

        # If there are distinct fields, then django may require order-by fields
        if order_by is None and len(distinct_fields) > 0:
            # Get the default order-by fields for the root model
            orderby_fields = self.get_order_by_fields(fmt)
            if len(orderby_fields) > 0:
                results = results.order_by(*orderby_fields)

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
            prefetches = self.get_prefetches(fmt)
        else:
            # Retrieve the prefetch data
            prefetch_qrys = self.get_true_join_prefetch_paths_and_qrys(qry, fmt)

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
                    pf_q_exp = construct_advanced_query(pf_qry, pf_units_lookup)

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

        split_row_annotations = self.get_full_join_annotations(fmt)
        for annotation in split_row_annotations:
            results = results.annotate(**annotation)

        return results, cnt, stats

    def get_query_stats(self, res, fmt, time_limit_secs: Optional[int] = None):
        """
        This method takes a queryset (produced by perform_query) and a format (e.g. "pgtemplate") and returns a stats
        dict keyed on the stat name and containing the counts of the number of unique values for the fields defined in
        the basic advanced search view object for the supplied template.  E.g. The results contain 5 distinct tissues.
        """
        # Obtain the metadata about what stats we will display
        params_arrays = self.get_stats_params(fmt)
        if params_arrays is None:
            return None

        # Since `results.values(*distinct_fields).annotate(cnt=Count("pk"))` throws an exception if duplicate records
        # result from the possible use of distinct(fields) in the res sent in, we must mix in the distinct fields that
        # uniquely identify records.
        stats_fields = [fld for d in params_arrays for fld in d["distincts"]]

        # These are the distinct fields that that dictate the number of rows in the view's output table
        fmt_distinct_fields = self.get_distinct_fields(fmt, assume_distinct=False)
        # These are the distinct fields necessary to get an accurate count of unique values
        all_distinct_fields = self.get_distinct_fields(
            fmt, assume_distinct=False, split_all=True
        )
        all_fields = all_distinct_fields + stats_fields
        stats = {}
        cnt_dict: Dict[str, dict] = {}
        start_time = time.time()
        loop_count = 0
        based_on = None

        try:
            # order_by(*all_distinct_fields) and distinct(*all_distinct_fields) are required to be able to count values
            # occurring in "unsplit" rows.  E.g. If a field from another table has a M:1 relationship with the rows of
            # the table, if we didn't make them distinct here, we wouldn't be able to count their values accurately.
            # And since we are doing extra splitting, we need to be able to accurately count the actual rows of the
            # results table too, and we do that with the reccombo variable below (which is a unique combination value on
            # each row).
            resultsqs = (
                res.order_by(*all_distinct_fields)
                .distinct(*all_distinct_fields)
                .values_list(*all_fields)
            )
            for rec in resultsqs.all():
                loop_count += 1

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
                    if params[
                        "filter"
                    ] is None or self.meets_all_conditions_by_val_list(
                        fmt, rec, params["filter"], all_fields
                    ):
                        if valcombo not in cnt_dict[statskey].keys():
                            cnt_dict[statskey][valcombo] = {}
                            cnt_dict[statskey][valcombo][reccombo] = 1
                            stats[statskey]["count"] += 1
                        else:
                            cnt_dict[statskey][valcombo][reccombo] = 1

                elapsed_time = time.time() - start_time
                if time_limit_secs is not None and elapsed_time >= time_limit_secs:
                    if resultsqs.count() > loop_count:
                        based_on = (
                            f"* Based on {loop_count / resultsqs.count() * 100:.2f}% of the data (truncated for "
                            "time)"
                        )
                    break

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

        return stats, based_on

    def get_download_qry_list(self):
        """
        Returns a list of dicts where the keys are name and json and the values are the format name and the json-
        stringified qry object with the target format selected
        """
        qry_list = []
        for format, name in self.get_format_names().items():
            qry_list.append(
                {"name": name, "json": json.dumps(self.get_root_group(format))}
            )
        return qry_list

    def create_new_basic_query(
        self, mdl, fld, cmp, val, fmt, units="identity", search_again=True
    ):
        """
        Constructs a new qry object for an advanced search from basic search input.

        search_again - Set this to False if the search query interface will not be needed.  When this is true, search
        fields like 'id' are converted into display fields (i.e. fields that can be seen and selected in the field
        drop-down).  Sometimes this conversion can be problematic because there may not exist a field that is "unique".
        (Note, any field [non-unique included] can be se as the display field as long as it is unique in the base
        queryset.)
        """

        qry = self.get_root_group(fmt)

        try:
            mdl_inst = self.get_model_instance(fmt, mdl)
        except KeyError as ke:
            # Print error to the console
            print(
                f"Exception with format {fmt} and model {mdl}: {type(ke).__name__}: {ke}"
            )
            raise ke

        sfields = self.get_search_fields(fmt, mdl_inst)

        if fld not in sfields:
            raise FieldError(
                f"Field [{fld}] is not searchable.  Must be one of [{','.join(sfields.keys())}]."
            )

        num_empties = get_num_empty_queries(qry, fmt)
        if num_empties != 1:
            raise ValidationError(
                f"The static filter for format {fmt} is improperly configured. It must contain exactly 1 empty query."
            )

        target_fld = sfields[fld]
        target_val = val

        set_first_empty_query(qry, fmt, target_fld, cmp, target_val, units)

        if search_again:
            dfld, dval = self.search_field_to_display_field(mdl_inst, fld, val, qry)

            if dfld != fld:
                # Set the field path for the display field
                target_fld = sfields[dfld]
                target_val = dval

                # Re-create another empty copy of the qry
                qry = self.get_root_group(fmt)
                # Note units cannot be transfered, so default should always be "identity"
                set_first_empty_query(qry, fmt, target_fld, cmp, target_val, "identity")

        return qry


class UnsupportedDistinctCombo(Exception):
    def __init__(self, fields: list):
        message = (
            f"Unsupported combination of distinct fields: {fields}.  The problem likely stems from the usage of field "
            "references that are not real database fields, used in both .distinct() and .order_by().  Those methods "
            "resolve foreign keys (to database fields) in models differently.  Be sure to supply actual database "
            "fields and not foreign key object references."
        )
        super().__init__(message)
        self.fields = fields


class ConditionallyRequiredArgumentError(Exception):
    pass
