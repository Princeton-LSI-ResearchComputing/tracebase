from __future__ import annotations
from collections import defaultdict
from datetime import datetime
from functools import reduce
from io import BytesIO, StringIO
# import sys
import traceback
from typing import Dict, List, Optional, Tuple, Type, Union
import base64
import pandas as pd
import csv
import _csv

from django.core.exceptions import FieldError
from django.db import ProgrammingError
from django.db.models import F, Max, Min, Model, Q, QuerySet, Count, AutoField, CharField, Field, ForeignKey, Prefetch
from django.db.models.functions import Coalesce, Lower
from django.template import loader
from django.utils.functional import classproperty
from django.views.generic import ListView

from DataRepo.models.utilities import field_path_to_field, field_path_to_model_path, get_field_from_model_path, is_many_related, is_number_field, is_string_field, is_unique_field, model_path_to_related_model
from DataRepo.utils.exceptions import MutuallyExclusiveArgs
from DataRepo.utils.text_utils import camel_to_title, underscored_to_title
from DataRepo.views.utils import GracefulPaginator, get_cookie, reduceuntil

from DataRepo.views.models.base import BSTColumn, BSTColumnGroup


class BootstrapTableListView(ListView):
    """Generic class-based view for a Model record list to make pages load faster, using server-side behavior for
    pagination."""

    # 0 = "ALL"
    PER_PAGE_CHOICES = [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0]

    paginator_class = GracefulPaginator
    paginate_by = 15
    template_name = "DataRepo/widgets/bst_list_view.html"

    export_header_template = "DataRepo/downloads/export_metadata_header.txt"
    headtmplt = loader.get_template(export_header_template) if export_header_template is not None else None
    # include_through_models: bool = False
    exclude_fields: Optional[List[str]] = ["id"]  # You can set reverse relations in your derived class

    HEADER_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    FILENAME_TIME_FORMAT = "%Y.%m.%d.%H.%M.%S"

    @classproperty
    def view_name(cls):
        return cls.__name__

    @classproperty
    def cookie_prefix(cls):
        return f"{cls.view_name}-"

    @classproperty
    def verbose_model_name_plural(cls):
        try:
            return underscored_to_title(cls.model._meta.__dict__["verbose_name_plural"])
        except:
            print(f"WARNING: Model {cls.model.__name__} has no Meta.verbose_name_plural.")
            return f"{camel_to_title(cls.model.__name__)}s"

    @classproperty
    def verbose_model_name(cls):
        try:
            return underscored_to_title(cls.model._meta.__dict__["verbose_name"])
        except:
            print(f"WARNING: Model {cls.model.__name__} has no Meta.verbose_name.")
            return camel_to_title(cls.model.__name__)

    @classproperty
    def verbose_name(cls):
        return camel_to_title(cls.view_name)

    def __init__(self, *columns: Union[BSTColumn, BSTColumnGroup], custom: Optional[Dict[str, BSTColumn]] = None, **kwargs):
        """An override of the superclass constructor intended to initialize custom instance attributes.

        Args:
            columns (Union[BSTColumn, BSTColumnGroup])
            custom (Optional[Dict[str, BSTColumn]]): Dict of BSTColumn objects keyed on django Field name.
            kwargs (dict): Passed to superclass.
        Exceptions:
            KeyError when there are multiple groups with the same many-related model.
            ProgrammingError fallback in case the type hint is circumvented.
        Returns:
            (BootstrapTableListView)
        """

        super().__init__(**kwargs)

        self.columns: List[BSTColumn] = []
        self.groups: List[BSTColumnGroup] = []
        self.groups_dict = defaultdict(list)
        self.ordering = self.model._meta.ordering

        if custom is not None and len(columns) > 0:
            raise MutuallyExclusiveArgs(
                "'custom' and 'column' are mutually exclusive arguments.  Only use 'custom' if you want default "
                "columns defined by an inspection of the view's model fields, but a subset need custom settings, e.g. "
                "a custom header or a converter.  Add them to a dict that is keyed on the Field name."
            )

        if len(columns) == 0:
            self.set_default_columns(custom)

        for column in columns:
            if isinstance(column, BSTColumn):
                self.columns.append(column)
            elif isinstance(column, BSTColumnGroup):
                if column.model in self.groups_dict.keys():
                    raise KeyError(f"Group defined multiple times with the same model: '{column.model}'.")
                self.groups.append(column)
                for group_column in column.columns:
                    self.groups_dict[column.model].append(group_column)
                    self.columns.append(group_column)
            else:
                raise ProgrammingError(
                    "Invalid columns argument.  Must be either a BSTColumn or BSTColumnGroup "
                    f"object, not '{type(column).__name__}'."
                )

        now = datetime.now()
        self.fileheader_timestamp = now.strftime(self.HEADER_TIME_FORMAT)
        self.filename_timestamp = now.strftime(self.FILENAME_TIME_FORMAT)

        self.total = 0
        self.raw_total = 0
        self.warnings = []
        self.cookie_resets = []
        self.clear_cookies = False

    @classmethod
    def has_detail_url(cls):
        return hasattr(cls.model, "get_absolute_url")

    def set_default_columns(self, custom: Optional[Dict[str, Union[dict, BSTColumn]]] = None):
        related_columns: List[BSTColumn] = []
        related_model_paths: dict = {}
        # through_models: list = []
        fields = self.model._meta.get_fields()

        # Check the exclude fields
        bad_excludes = []
        field_names = [f.name for f in fields]
        for ef in self.exclude_fields:
            # 2 special cases (for many_related columns added below)
            first_ef = ef.replace("first_", "")
            count_ef = ef.replace("_count", "")
            if ef not in field_names and first_ef not in field_names and count_ef not in field_names:
                bad_excludes.append(ef)
        if len(bad_excludes) > 0:
            raise ValueError(f"Invalid exclude fields: {bad_excludes}.  Choices are: {[f.name for f in fields]}.")

        # Check custom columns
        if custom is None:
            custom = {}
        else:
            excluded_customs = []
            for cf in custom.keys():
                if cf in self.exclude_fields:
                    excluded_customs.append(cf)
            if len(excluded_customs) > 0:
                raise ValueError(f"A custom field cannot be in the exclude_fields: {excluded_customs}.")

        leftover_custom_columns = dict((k, k) for k in custom.keys())
        # Add a detail-linked column if the user did not specify one and the model has a detail URL defined
        add_detail_column = not any(
            [
                (isinstance(v, BSTColumn) and v.link_to_detail)
                or (isinstance(v, dict) and v.get("link_to_detail"))
                for v in custom.values()
            ]
        ) and self.has_detail_url()
        detail_link_added = False
        for fld in fields:
            # print(f"FIELD {fld.name} TYPE: {type(fld).__name__}")
            if self.exclude_fields is not None and fld.name in self.exclude_fields:
                continue
            case_insensitive = is_string_field(fld, default=True)
            make_detail_link = False
            if add_detail_column and not fld.is_relation and is_unique_field(fld):
                make_detail_link = True
                add_detail_column = False
            if fld.is_relation:
                many_related = fld.one_to_many or fld.many_to_many
                # DEBUG: Testing this block(/BSTColumn column addition) to see if I can get this to work quickly.
                # print(f"fld.one_to_many or fld.many_to_many: {fld.one_to_many} or {fld.many_to_many}")
                try:
                    related_name = fld.related_name
                except AttributeError as ae:
                    print(f"1 AttributeError: {ae}")
                    related_name = fld.name
                first_sort_fld = f"{related_name}__pk"
                # Initially not sortable (bec. it's a pk), unless we can get a sort field figured out below...
                first_sortable = False

                if many_related:
                    kwargs_key = f"first_{related_name}" if f"first_{related_name}" in custom.keys() else related_name
                    column_name = f"first_{related_name}"
                else:
                    kwargs_key = fld.name
                    column_name = fld.name

                # Try to get a field from the related model to sort by
                if (
                    kwargs_key in custom.keys()
                    and (isinstance(custom[kwargs_key], dict))
                    and "related_sort_fld" in custom[kwargs_key].keys()
                ):
                    first_sortable = True
                    obf = get_field_from_model_path(self.model, custom[kwargs_key]["related_sort_fld"])
                    case_insensitive = is_string_field(obf, default=True)
                    print(f"RELATED SORT FLD FROM MODEL CUSOMIZATION DICT: {custom[kwargs_key]['related_sort_fld']} INSENSITIVE: {case_insensitive} OBF: {obf}")
                elif len(fld.related_model._meta.ordering) == 1:
                    order_fld = self.get_field_name(fld.related_model._meta.ordering[0])
                    obf = get_field_from_model_path(fld.related_model, order_fld)
                    case_insensitive = is_string_field(obf, default=True)
                    first_sort_fld = f"{related_name}__{order_fld}"
                    first_sortable = True
                    print(f"RELATED SORT FLD FROM MODEL ORDERING: {first_sort_fld} INSENSITIVE: {case_insensitive} OBF: {obf}")
                else:
                    # Grab the first field that is unique, if it exists
                    f: Field
                    for f in fld.related_model._meta.get_fields():
                        try:
                            if not f.is_relation and f.unique and f.name != "id":
                                order_fld = self.get_field_name(f.name)
                                obf = get_field_from_model_path(fld.related_model, order_fld)
                                case_insensitive = is_string_field(obf, default=True)
                                first_sort_fld = f"{related_name}__{order_fld}"
                                first_sortable = True
                                print(f"MANYREL SORT FLD SET TO FIRST UNIQUE FIELD: {first_sort_fld}")
                                break
                        except AttributeError as ae:
                            print(f"2 AttributeError: {ae}")
                            raise AttributeError(
                                f"Error for Field '{fld.related_model.__name__}.{f.name}': {ae}."
                            ).with_traceback(ae.__traceback__)
                    if not first_sortable:
                        print(
                            f"WARNING: A {fld.related_model.__name__}._meta.ordering with more than 1 ordering field "
                            "is not supported.  Supply BSTColumn objects to the constructor to enable sorting of the "
                            f"{self.model.__name__} BST ListView."
                        )

                # Add a count column for many-related models
                if many_related and related_name not in self.exclude_fields and f"{related_name}_count" not in self.exclude_fields:
                    leftover_custom_columns.pop(f"{related_name}_count", None)
                    if (
                        f"{related_name}_count" in custom.keys()
                        and isinstance(custom[f"{related_name}_count"], BSTColumn)
                    ):
                        print(f"ADDING CUSTOM COLUMN {related_name}_count")
                        related_columns.append(custom[f"{related_name}_count"])
                    else:
                        kwargs = {
                            "field": fld.name,
                            "header": underscored_to_title(fld.name) + " Count",
                            "sorter": BSTColumn.SORTER_CHOICES.NUMERIC,
                            "converter": Count(fld.name, distinct=True),
                        }
                        if f"{related_name}_count" in custom.keys():
                            kwargs.update(custom[f"{related_name}_count"])

                        print(f"ADDING DEFAULT COLUMN {related_name}_count")
                        related_columns.append(BSTColumn(f"{column_name}_count", **kwargs))

                if related_name not in self.exclude_fields and f"first_{related_name}" not in self.exclude_fields:
                    leftover_custom_columns.pop(f"first_{related_name}", None)
                    leftover_custom_columns.pop(related_name, None)
                    if (
                        f"first_{related_name}" in custom.keys()
                        and isinstance(custom[f"first_{related_name}"], BSTColumn)
                    ):
                        print(f"ADDING CUSTOM COLUMN NAMED {column_name} FROM RELATED PATH {related_name}")
                        related_columns.append(custom[f"first_{related_name}"])
                    else:
                        kwargs = {
                            "field": related_name,
                            "header": underscored_to_title(related_name),
                            "is_fk": True,
                            "many_related": many_related,
                            "related_model_path": related_name,
                            "related_sort_fld": first_sort_fld,
                            "sort_nocase": case_insensitive,
                            "sortable": first_sortable,
                            "sorter": (
                                BSTColumn.SORTER_CHOICES.HTML
                                if many_related
                                else BSTColumn.SORTER_CHOICES.ALPHANUMERIC
                            ),
                            # TODO: Figure out how to sort by another field (preferrably one that matches the output of __str__)
                            "searchable": False,
                        }
                        if f"first_{related_name}" in custom.keys():
                            kwargs.update(custom[f"first_{related_name}"])
                        elif related_name in custom.keys():
                            # Allow them to specify just the field in the key so that it maps correctly from what you
                            # get from Model._meta.get_fields()
                            kwargs.update(custom[related_name])

                        if many_related:
                            print(f"ADDING DEFAULT REVERSE RELATED COLUMN NAMED {column_name} FROM RELATED PATH {related_name} kwargs_key: {kwargs_key} kwargs: {kwargs}")
                            related_columns.append(BSTColumn(column_name, **kwargs))
                        else:
                            print(f"ADDING DEFAULT RELATED COLUMN NAMED {column_name} FROM RELATED PATH {related_name} kwargs: {kwargs}")
                            self.columns.append(BSTColumn(column_name, **kwargs))

                # print(f"FIELD NAME: {fld.name}")
                # if fld.many_to_many and hasattr(fld, "through"):
                #     through_models.append(fld.through.__name__)
                related_model_paths[fld.name] = fld.related_model.__name__

            else:
                print(f"NON-RELATION: {fld.name} CHOICES?: {fld.choices}")
                if (
                    fld.name in custom.keys()
                    and isinstance(custom[fld.name], BSTColumn)
                ):
                    # If the user didn't explicitly set whether this column should be linked, set whether we determined
                    # it should be linked
                    if custom[fld.name].link_to_detail is None:
                        custom[fld.name].link_to_detail = make_detail_link
                    # Now, if the column is linked (whether we linked it or the derived class linked it), set that a
                    # detail link has been added, so that we don't link any remaining columns
                    if custom[fld.name].link_to_detail is True:
                        detail_link_added = True
                    elif add_detail_column:
                        # Default to the first non-relation column, if one exists.  Note, add_detail_column is only True
                        # if there are no unique columns and the user has not explicitly set a column to link.
                        custom[fld.name].link_to_detail = True
                        detail_link_added = True
                        make_detail_link = False
                        add_detail_column = False
                    # Rename any unique field named "name" to the verbose model name
                    if custom[fld.name].header_orig is None:
                        custom[fld.name].header = self.get_field_header(fld)
                    print(f"ADDING CUSTOMIZED COLUMN {fld.name}")
                    self.columns.append(custom[fld.name])
                else:
                    kwargs = {
                        "header": self.get_field_header(fld),
                        "sorter": BSTColumn.SORTER_CHOICES.ALPHANUMERIC,
                        "sort_nocase": case_insensitive,
                        "link_to_detail": make_detail_link,
                    }
                    if make_detail_link:
                        detail_link_added = True
                    elif add_detail_column:
                        # Default to the first non-relation column, if one exists.  Note, add_detail_column is only True
                        # if there are no unique columns and the user has not explicitly set a column to link.
                        kwargs["link_to_detail"] = True
                        detail_link_added = True
                        make_detail_link = False
                        add_detail_column = False

                    if fld.choices is not None:
                        kwargs["select_options"] = [c[0] for c in fld.choices]

                    if fld.name in custom.keys():
                        print(f"ADDING EDITED DEFAULT COLUMN {fld.name}")
                        kwargs.update(custom[fld.name])
                    else:
                        print(f"ADDING DEFAULT COLUMN {fld.name}")
                    self.columns.append(BSTColumn(fld.name, **kwargs))
                leftover_custom_columns.pop(fld.name, None)

        for custom_key in leftover_custom_columns.keys():
            if isinstance(custom[custom_key], BSTColumn):
                print(f"ADDING LEFTOVER COLUMN {custom_key} as BSTColumn")
                self.columns.append(custom[custom_key])
            else:
                print(f"ADDING LEFTOVER COLUMN {custom_key} as dict arguments {custom[custom_key]}")
                self.columns.append(BSTColumn(custom_key, **custom[custom_key]))

        # Add the reverse related columns at the end (i.e. those without an explicitly defined foreign key and those
        # that are M:M related)
        if len(related_columns) > 0:
            for related_column in related_columns:
                self.columns.append(related_column)
                # if self.include_through_models or related_model_paths[related_column.field] not in through_models:
                #     self.columns.append(related_column)

        if not detail_link_added:
            self.columns.append(
                BSTColumn(
                    "detail_link",
                    field=None,
                    header="Detail",
                    sortable=False,
                    searchable=False,
                    exported=False,
                )
            )

    @classmethod
    def get_field_header(cls, field: Field):
        if field.name == "name" and is_unique_field(field):
            return cls.verbose_model_name
        if any(c.isupper() for c in field.verbose_name):
            return field.verbose_name
        return underscored_to_title(field.name)

    @classmethod
    def get_field_name(cls, field_representation):
        """Takes a representation of a field, like from Model._meta.ordering, which can have functions applied (like
        Lower('field_name')) and returns the field name."""
        if isinstance(field_representation, str):
            return field_representation
        field_reps = field_representation.get_source_expressions()
        if len(field_reps) != 1:
            raise ValueError(f"Not one field name in field representation {[f.name for f in field_reps]}.")
        return field_representation.get_source_expressions()[0].name

    def get_queryset(self):
        """An override of the superclass method intended to only set total and raw_total instance attributes."""
        print("SANITY CHECK")
        # sys.__stdout__.write('SANITY CHECK 2')
        qs = super().get_queryset()
        self.total = qs.count()
        self.raw_total = self.total
        try:
            return self.get_manipulated_queryset(qs)
        except Exception as e:
            tb = "".join(traceback.format_tb(e.__traceback__))
            # sys.stderr.write(f"{tb}{type(e).__name__}: {e}\n")
            print(f"3 {tb}{type(e).__name__}: {e}\n")
            self.warnings.append(
                "There was an error processing your request.  Your cookies have been cleared just in case a bad cookie "
                "is the reason.  If the error recurs, please report it to the administrators."
            )
            self.clear_cookies = True
            return qs

    # THIS WORKS!!!
    def paginate_queryset(self, *args, **kwargs):
        """An override of the superclass method intended to create attributes on the base model containing a list of
        related objects."""
        print("CALLING PAGINATE_QUERYSET")
        paginator, page, object_list, is_paginated = super().paginate_queryset(*args, **kwargs)
        for rec in object_list:
            print(f"{datetime.now()}: COMPILING REC {rec.pk}")
            for column in self.columns:
                print(f"{datetime.now()}: COMPILING COL {column.name}")
                if column.many_related and column.converter is None:
                    # many-related record values depend on annotations and those annotation names sometimes come in via
                    # cookies (e.g. order-by).  So if the cookies are being cleared, we can assume that something has
                    # gone wrong, so we will skip compiling the related record values in the hopes that the cookie reset
                    # will have fixed the issue and the next request will be processed correctly.
                    if not self.clear_cookies:
                        subrecs = self.get_many_related_rec_val(rec, column, related_limit=21)
                        if len(subrecs) == 21:
                            subrecs[-1] = "..."
                    else:
                        subrecs = []
                    setattr(rec, column.mm_list, subrecs)
                    # print(
                    #     f"SETTING REC related_objects.{rec.pk}.{column.name} to FWD?: {column.related_sort_fwd} "
                    #     f"ORDERING OF: {context['related_objects'][rec.pk][column.name]}"
                    # )
        return paginator, page, object_list, is_paginated

    def get_manipulated_queryset(self, qs: QuerySet):
        """The superclass handles actual pagination, but the number of pages can be affected by applied filters, and the
        Content of those pages can be affected by sorting.  And both can be affected by annotated fields.  This method
        applies those criteria based on cookies.

        Limitations:
            1. Searching imported_timestamp is not supported if the database is not postgres
            2. Sorting based on study will only be based on the first or last related study, not the combined string of
               all related study names.
        Args:
            qs (QuerySet)
        Exceptions:
            None
        Returns:
            qs (QuerySet)
        """
        print(f"{datetime.now()}: get_manipulated_queryset START")
        # 1. Retrieve search and sort settings (needed for annotations too, due to different annotations based on sort)

        # Search and filter criteria will be stored in a Q expression
        q_exp = Q()

        # Check the cookies for search/sort/filter settings
        search_term: Optional[str] = self.get_cookie("search")
        order_by: Optional[str] = self.get_cookie("order-by")
        order_by_column: Optional[BSTColumn] = None
        order_by_field = None
        ordered = order_by != ""
        order_dir: Optional[str] = self.get_cookie("order-dir", "asc")
        descending = order_dir.lower().startswith("d")

        # This updates the many-related sort settings in self.columns, based on self.groups.  Side-note, this can work
        # even when order_by is not a column but is a field under the many-related model.
        if ordered:
            self.update_orderings(order_by, order_dir)
            order_by_column: Optional[BSTColumn] = self.get_column(order_by)
            order_by_field = F(order_by_column.name)

        # We need the column names (from the BST data-field attributes) to use in Q expressions
        filter_columns = []
        search_fields = []
        model_paths = []
        # prefetches = []
        column: BSTColumn
        for column in self.columns:
            # Put all fields' model paths into model_paths, to be evaluated for entry into prefetches
            if isinstance(column.field, list):
                for fld in column.field:
                    mdl = field_path_to_model_path(self.model, fld)
                    if mdl is not None and mdl not in model_paths:
                        print(f"ADDING MODEL {mdl} FROM COLUMN {column.name} AND FIELD {column.field}")
                        model_paths.append(mdl)
                    else:
                        print(f"NOT ADDING MODEL {mdl} FROM COLUMN {column.name} AND FIELD {column.field}")
            elif column.field is not None:
                mdl = field_path_to_model_path(self.model, column.field)
                if mdl is not None and mdl not in model_paths:
                    print(f"ADDING MODEL {mdl} FROM COLUMN {column.name} AND FIELD {column.field}")
                    model_paths.append(mdl)
                else:
                    print(f"NOT ADDING MODEL {mdl} FROM COLUMN {column.name} AND FIELD {column.field}")
                # # DEBUG: Testing if this speeds up ArchiveFileListView
                # if mdl is not None and column.many_related and len(mdl.split("__")) > 1:
                #     print(f"ADDING PREFETCH {mdl} FROM COLUMN {column.name} AND CREATING LIST IN ATTR {column.mm_list}")
                #     prefetches.append(Prefetch(mdl, to_attr=column.mm_list))
                # elif mdl is not None:
                #     prefetches.append(mdl)

            # Construct Q expressions for the filters (if any)
            filter_value: str = self.get_column_cookie(column, "filter")
            if column.searchable and filter_value != "":
                filter_columns.append(column.name)
                search_field = (
                    column.field
                    if column.field is not None and (
                        # There are multiple fields associated with the column
                        isinstance(column.field, list)
                        # or there is no converter
                        or column.converter is None
                        # # Do not search an annotation that is a Coalesce, because it's REALLY slow
                        # or isinstance(column.converter, Coalesce)
                    )
                    else column.name
                )
                if isinstance(column.converter, Coalesce):
                    print(
                        "WARNING: Filtering/searching is prohibited for Coalesce annotations due to performance.  The "
                        f"search for annotation '{column.name}' is falling back to a search on '{column.field}'.  Try "
                        "changing the converter to a difference function, such as 'Case'."
                    )
                print(f"FILTERING COLUMN '{column.name}' USING FIELD '{search_field}' AND TERM '{filter_value}'")
                if isinstance(search_field, list):
                    or_q_exp = Q()
                    for coalesced_search_field in column.field:
                        search_fields.append(coalesced_search_field)
                        if(not is_number_field(get_field_from_model_path(self.model, coalesced_search_field), default=False)):
                            or_q_exp |= Q(**{f"{coalesced_search_field}__icontains": filter_value})
                        else:
                            or_q_exp |= Q(**{coalesced_search_field: filter_value})
                    q_exp &= or_q_exp
                elif column.field is not None:
                    search_fields.append(search_field)
                    if(not is_number_field(get_field_from_model_path(self.model, search_field), default=False)):
                        q_exp &= Q(**{f"{search_field}__icontains": filter_value})
                    else:
                        q_exp |= Q(**{search_field: filter_value})
                else:
                    raise ValueError(f"Column {column.name} must not be searchable if field is None.")

        # Add a global search if one is defined
        if search_term != "":
            global_q_exp, all_search_fields = self.get_any_field_query(search_term)
            q_exp &= global_q_exp
            search_fields = all_search_fields

        # 2. Prefetch all required related fields to reduce the number of queries

        # DEBUG: Testing if the Prefetch object strategy above is faster
        prefetches = []
        model_path: str
        for model_path in sorted(
            model_paths,
            key=len,
            reverse=True,
        ):
            contained = False
            for upath in prefetches:
                if model_path in upath:
                    contained = True
                    break
            if not contained:
                prefetches.append(model_path)
        print(f"PREFETCHES: {prefetches} MODEL PATHS: {model_paths}")
        # print(f"PREFETCHES: {prefetches} MODEL PATHS: {model_paths} COUNT: {qs.count()}")
        qs = qs.prefetch_related(*prefetches)

        # 3. Add annotations (which can be used in search & sort)

        annotations_before_filter = {}
        annotations_after_filter = {}
        for column in self.columns:
            print(f"LOOKING FOR COLUMN {column.name}'s FIELD {column.field} IN MODEL: {self.model}")
            many_related = False
            if column.field is not None:
                field_class: Type[Field] = get_field_from_model_path(self.model, (column.field if isinstance(column.field, str) else column.field[0]))
                many_related = field_class.field.one_to_many or field_class.field.many_to_many
            try:
                print(f"CHECKING COLUMN {column.name}")
                # If a converter exists, the column is an annotation column, so annotate it
                if column.converter is not None:
                    print("ADDING A CUSTOM ANNOTATION")
                    if isinstance(column.converter, Coalesce) and column not in filter_columns:
                        if search_term != "" or column.name in filter_columns:
                            print(
                                f"WARNING: Excluding annotation {column.name} from search/filter because it has a "
                                "Coalesce converter, which is *really* inefficient/slow.  Searching the field instead."
                            )
                        annotations_after_filter[column.name] = column.converter
                    elif column.field is not None:
                        if column in filter_columns:
                            print(
                                f"WARNING: Including a Coalesce converter annotation {column.name} because it was "
                                "explicitly filtered by the user, which is *really* inefficient/slow.  Consider "
                                "changing the annotation to a Case."
                            )
                        annotations_before_filter[column.name] = column.converter
                    else:
                        annotations_before_filter[column.name] = column.converter
                        # raise ValueError(f"Column {column.name} must not have a converter if field is None.")
            except Exception as e:
                # The fallback is to have the template render the database values in the default manner.  Searching will
                # disabled.  Sorting will be a string sort (which is not ideal, e.g. if the value is a datetime).
                column.searchable = False
                print(
                    f"WARNING: {type(e).__name__}: {e}\nConverter for column '{column.name}' failed.  Falling back to "
                    "default.  The converter may be specific to postgres and must be rewritten."
                )
            finally:
                # If no annotation was created and this needs to be an annotated field (because there's either a
                # converter or it's a many-related field)
                if (
                    column.name not in annotations_before_filter.keys()
                    and column.name not in annotations_after_filter.keys()
                    and column.is_annotation
                ):
                    print("ADDING A DEFAULT ANNOTATION")
                    if isinstance(column.field, list):
                        print("...FOR A COALESCE")
                        # There are multiple fields that link to the reference model, so we use coalesce, assuming that
                        # the same reference model record is not linked to from multiple other model fields.
                        if order_by == "" or (
                            order_by == column.name
                            and not descending
                        ):
                            # Get the minimum value of the first populated many-related field
                            if many_related or column.many_related:
                                # Apply Min to prevent changing the number of resulting rows
                                annotations_after_filter[column.name] = Coalesce(*[Min(f) for f in column.field])
                            else:
                                annotations_after_filter[column.name] = Coalesce(*column.field)

                            if order_by_column is not None and order_by_column == column and column.field != column.related_sort_fld_orig:
                                order_by_field_name = f"{order_by_column.name}_sort"

                                if many_related or column.many_related:
                                    # Apply Min to prevent changing the number of resulting rows
                                    annotations_after_filter[order_by_field_name] = Coalesce(*[Min(Lower(f)) for f in column.related_sort_fld_orig])
                                else:
                                    annotations_after_filter[order_by_field_name] = Coalesce(*[Lower(f) for f in column.related_sort_fld_orig])
                                order_by_field = F(order_by_field_name)
                                print(f"ALSO ADDING AN ORDER BY ANNOTATION FOR COLUMN: {order_by_column.name} BEC IT MATCHES: {order_by} FIELD NAME: {order_by_field_name} FIELD: {order_by_field}")

                        else:
                            # Get the maximum value of the first populated many-related field
                            if many_related or column.many_related:
                                # Apply Max to prevent changing the number of resulting rows
                                annotations_after_filter[column.name] = Coalesce(*[Max(f) for f in column.field])
                            else:
                                annotations_after_filter[column.name] = Coalesce(*column.field)
                    elif many_related or column.many_related:
                        print("...FOR A MANY RELATED")
                        # The order_by_field could be a foreign key, which is sorted by the column object's
                        # related_sort_fld.  When sorting the rows, we son't want to sort by the value of the
                        # annotation (the foreign key number).  We need to give the user the annotation value they
                        # requested for column.name, AND sort by the value they explicitly said they want to sort on, so
                        # we need another (temporary) annotation for the sort value when sorting by this column.
                        if order_by_column == column:
                            order_by_field_name = f"{order_by_column.name}_sort"
                            order_by_field = F(order_by_field_name)
                            print(f"ALSO ADDING AN ORDER BY ANNOTATION FOR COLUMN: {order_by_column.name} BEC IT MATCHES: {order_by} FIELD NAME: {order_by_field_name} FIELD: {order_by_field}")
                        # This assumes column.field is not None
                        if order_by == "" or (
                            order_by == column
                            and not descending
                        ):
                            # Apply Min to prevent changing the number of resulting rows
                            # This is only ever be used when there is a single result. If creating your own template, be
                            # careful not to rely on the annotation for sorting rows if your related_sort_fld
                            # differs
                            annotations_before_filter[column.name] = Min(column.field)
                            if order_by_column == column:
                                # This one-off sorting annotation will only ever be used below if ordering by this
                                # column or group
                                annotations_before_filter[order_by_field_name] = Min(column.related_sort_fld_orig)
                                print(f"ORDERING MANY RELATED FIELD NAME: {order_by_field_name} ORIG SORT FIELD: {column.related_sort_fld_orig} VALUE: {annotations_before_filter[order_by_field_name]}")
                        else:
                            # Apply Max to prevent changing the number of resulting rows
                            # This is only ever be used when there is a single result. If creating your own template, be
                            # careful not to rely on the annotation for sorting rows if your related_sort_fld
                            # differs
                            annotations_before_filter[column.name] = Max(column.field)
                            if order_by_column == column:
                                # This one-off sorting annotation will only ever be used below if ordering by this
                                # column or group
                                annotations_before_filter[order_by_field_name] = Max(column.related_sort_fld_orig)
                    elif column.field is not None:
                        print("...FOR A FIELD")
                        # Add a sort field for any foreign keys (if it has a sort field that differs from the field
                        if field_class.field.is_relation and order_by_column == column and column.field != column.related_sort_fld_orig:
                            order_by_field_name = f"{order_by_column.name}_sort"
                            order_by_field = F(order_by_field_name)
                            print(f"ALSO ADDING AN ORDER BY ANNOTATION FOR COLUMN: {order_by_column.name} BEC IT MATCHES: {order_by} FIELD NAME: {order_by_field_name} FIELD: {order_by_field}")
                            # This assumes column.field is not None
                            annotations_before_filter[order_by_field_name] = column.related_sort_fld_orig
                        # This is in case a user-supplied custom converter failed in the try block above and the field
                        # is not many_related and there are not multiple other model fields linking to the reference
                        # model
                        annotations_before_filter[column.name] = F(column.field)

        if len(annotations_before_filter.keys()) > 0:
            print(f"BEFORE-ANNOTATIONS: {annotations_before_filter}")
            # print(f"COUNT: {qs.count()} BEFORE BEFORE-ANNOTATIONS: {annotations_before_filter}")
            qs = qs.annotate(**annotations_before_filter)

        # 4. Apply the search and filters

        if len(q_exp.children) > 0:
            try:
                print(f"FILTERS: {q_exp}")
                qs = qs.filter(q_exp)
            except FieldError as fe:
                fld_str = "\n\t".join(search_fields)
                fld_msg = f"One or more of {len(search_fields)} fields is misconfigured:\n\n\t{fld_str}"
                warning = (
                    f"Your search could not be executed.  {fld_msg}\n\n"
                    "Please report this error to the site administrators."
                )
                print(f"WARNING: {warning}\nException: {type(fe).__name__}: {fe}")
                self.warnings.append(warning)
                if search_term != "":
                    self.cookie_resets = [self.get_cookie_name("search")]
                else:
                    self.cookie_resets = [self.get_column_cookie_name(c, "filter") for c in filter_columns]

        # 5. Apply coalesce annotations AFTER the filter, due to the inefficiency of WHERE clauses interacting with
        # COALESCE

        if len(annotations_after_filter.keys()) > 0:
            print(f"AFTER-ANNOTATIONS: {annotations_after_filter}")
            # print(f"COUNT: {qs.count()} BEFORE AFTER-ANNOTATIONS: {annotations_after_filter}")
            qs = qs.annotate(**annotations_after_filter)
        else:
            print("NO AFTER-ANNOTATIONS")

        # 6. Apply the sort

        # Sort the results, if sort has a value
        if ordered:
            print(f"COUNT BEFORE ORDERBY: {qs.count()} ORDER BY: {order_by}")

            if order_by_column.is_fk and not order_by_column.many_related and order_by_column.field != order_by_column.related_sort_fld:
                order_by_field = F(order_by_column.related_sort_fld)

            if order_by_column.sort_nocase and not isinstance(order_by_column.field, list):
                order_by_field = Lower(order_by_field)

            if descending:
                print(f"ORDERING ROWS BY {order_by_field}, desc, nulls last")
                qs = qs.order_by(order_by_field.desc(nulls_last=True))
            else:
                print(f"ORDERING ROWS BY {order_by_field}, asc, nulls first")
                qs = qs.order_by(order_by_field.asc(nulls_first=True))

        # 7. Ensure distinct results (because annotations and/or sorting can cause the equivalent of a left join).

        print("DISTINCTING")
        # print(f"COUNT BEFORE DISTINCT: {qs.count()}")
        qs = qs.distinct()

        # 8. Update the count

        print("COUNTING")
        # Set the total after the search
        self.total = qs.count()
        print(f"COUNT BEFORE RETURN: {self.total}")

        # TODO: Add a check for when limit is 0 and total > raw_total.  Should raise an exception because the developer
        # added a many-related column and did not make it 1:1 with the base model
        print(f"{datetime.now()}: get_manipulated_queryset END")

        # NOTE: Pagination is controlled by the superclass and the override of the get_paginate_by method
        return qs

    def get_paginate_by(self, queryset):
        """An override of the superclass method to allow the user to change the rows per page."""

        limit = self.request.GET.get("limit", "")
        if limit == "":
            cookie_limit = self.get_cookie("limit")
            # Never set limit to 0 from a cookie, because it the page times out, the users will never be able to load it
            # deleting their browser cookie.
            if cookie_limit != "" and int(cookie_limit) != 0:
                limit = int(cookie_limit)
            else:
                limit = self.paginate_by
        else:
            limit = int(limit)

        # Setting the limit to 0 means "all", but returning 0 here would mean we wouldn't get a page object sent to the
        # template, so we set it to the number of results.  The template will turn that back into 0 so that we're not
        # adding an odd value to the rows per page select list and instead selecting "all".
        if limit == 0 or limit > queryset.count():
            limit = queryset.count()

        return limit

    def get_context_data(self, **kwargs):
        """An override of the superclass method to provide context variables to the page.  All of the values are
        specific to pagination and BST operations."""
        print(f"{datetime.now()}: get_context_data START")

        # context = super().get_context_data(**kwargs)
        context = super().get_context_data()

        print(f"{datetime.now()}: get_context_data SETTING BASIC CONTEXT DATA")

        # 1. Set context variables for initial defaults based on user-selections saved in cookies

        # Set search/sort context variables
        context["search_term"] = self.get_cookie("search") if len(self.cookie_resets) == 0 else ""
        context["order_by"] = self.get_cookie("order-by")
        context["order_dir"] = self.get_cookie("order-dir")

        # Set limit context variable
        # limit can be 0 to mean unlimited/all, but the paginator's page is set to the number of results because if it's
        # set to 0, the page object and paginator object are not included in the context,
        context["limit"] = self.get_cookie("limit", self.paginate_by)

        # 2. Set default interface context variables

        context["limit_default"] = self.paginate_by
        context["total"] = self.total
        context["raw_total"] = self.raw_total
        context["cookie_prefix"] = self.cookie_prefix
        context["table_id"] = self.view_name
        context["table_name"] = self.verbose_model_name_plural
        context["warnings"] = self.warnings
        context["model"] = self.model

        # 3. Set the BST column attribute context values to use in the th tag attributes

        context["not_exported"] = []
        context["filter_select_lists"] = {}

        print(f"{datetime.now()}: get_context_data SETTING COLUMNS, EXPORT, & SELECTLISTS")

        context["columns"] = []
        column: BSTColumn
        for column in self.columns:
            # Put the column object in the context.  It will render the th tag.  Update the filter and visibility first.
            column.visible = self.get_column_cookie(column, "visible", column.visible)
            column.filter = self.get_column_cookie(column, "filter", column.filter)
            # TODO: Remove context[column.name] and use context["columns"][column.name]
            context[column.name] = column
            context["columns"].append(column)

            # Tell the listview BST javascript which columns are not included in export
            if not column.exported:
                context["not_exported"].append(column.name)

            # Give the listview BST javascript a dict of only the populated select list options (for convenience)
            if column.select_options is not None:
                context["filter_select_lists"][column.name] = column.select_options

        # 4. Handle pagination rendering and the initialization of the table pagination code

        print(f"{datetime.now()}: get_context_data SETTING PAGE & EXPORT DATA")

        # This context variable determines whether the BST code on the pagination template will render
        context["is_bst_paginated"] = True
        if self.total == 0:
            # Django does not supply a page_obj when there are no results, but the pagination.html template is where the
            # table controlling code (integrated with pagination) is loaded, so we need a page_obj context variable with
            # this minimal information necessary to operate the table, so that a user can clear their search term that
            # resulted in no matches.
            context["page_obj"] = {
                "number": 1,
                "has_other_pages": False,
                "paginator": {"per_page": context["limit"]},
            }

        # 5. If the user has asked to export the data
        export_type = self.request.GET.get("export")
        if export_type:
            # print(f"EXPORTING {export_type}")
            context["export_data"], context["export_filename"] = self.get_export_data(export_type)
            context["export_type"] = export_type if export_type == "excel" else "text"

        print(f"{datetime.now()}: get_context_data GETTING OBJECT_LIST")

        # DEBUG: Testing this block to see if I can get this to work quickly.
        # Iterate over this page's worth of rows to pass along the related objects
        # page_qs = context["object_list"]
        # print(f"{datetime.now()}: get_context_data ITERATING RELATED_OBJECTS SQL: {page_qs.query}")
        # context["related_objects"] = {}
        # for rec in page_qs.all():
        #     print(f"{datetime.now()}: COMPILING REC {rec.pk}")
        #     context["related_objects"][rec.pk] = {}
        #     for column in self.columns:
        #         print(f"{datetime.now()}: COMPILING COL {column.name}")
        #         if column.many_related and column.converter is None:
        #             # many-related record values depend on annotations and those annotation names sometimes come in via
        #             # cookies (e.g. order-by).  So if the cookies are being cleared, we can assume that something has
        #             # gone wrong, so we will skip compiling the related record values in the hopes that the cookie reset
        #             # will have fixed the issue and the next request will be processed correctly.
        #             if not self.clear_cookies:
        #                 context["related_objects"][rec.pk][column.name] = self.get_many_related_rec_val(rec, column, related_limit=21)
        #                 if len(context["related_objects"][rec.pk][column.name]) == 21:
        #                     context["related_objects"][rec.pk][column.name][-1] = "..."
        #             else:
        #                 context["related_objects"][rec.pk][column.name] = []
        #             # print(
        #             #     f"SETTING REC related_objects.{rec.pk}.{column.name} to FWD?: {column.related_sort_fwd} "
        #             #     f"ORDERING OF: {context['related_objects'][rec.pk][column.name]}"
        #             # )
        print(f"{datetime.now()}: get_context_data END")

        return context

    def get_any_field_query(self, term: str) -> Tuple[Q, List[str]]:
        """Given a string search term, returns a Q expression that does a case-insensitive search of all fields from
        the table displayed in the template.  Note, annotation fields must be generated in order to apply the query.

        Args:
            term (str): search term applied to all columns of the view
        Exceptions:
            None
        Returns:
            q_exp (Q): A Q expression that can be used in a django ORM filter
            search_fields (List[str]): A list if database fields that are being queried
        """

        q_exp = Q()

        if term == "":
            return q_exp

        search_fields: List[str] = []
        column: BSTColumn
        for column in self.columns:
            if column.searchable:
                search_field = (
                    column.field
                    if column.field is not None and (
                        # There are multiple fields associated with the column
                        isinstance(column.field, list)
                        # or there is no converter
                        or column.converter is None
                    )
                    else column.name
                )
                if isinstance(search_field, list):
                    for many_related_search_field in column.field:
                        search_fields.append(many_related_search_field)
                        if(not is_number_field(get_field_from_model_path(self.model, many_related_search_field), default=False)):
                            q_exp |= Q(**{f"{many_related_search_field}__icontains": term})
                        else:
                            q_exp |= Q(**{many_related_search_field: term})
                elif column.field is not None:
                    search_fields.append(search_field)
                    if(not is_number_field(get_field_from_model_path(self.model, search_field), default=False)):
                        q_exp |= Q(**{f"{search_field}__icontains": term})
                    else:
                        q_exp |= Q(**{search_field: term})
                elif column.field is None:
                    raise ValueError(f"Column {column.name} must not be searchable if field is None.")

        return q_exp, search_fields

    def update_orderings(self, sort_by: str, sort_dir: str):
        """Takes sorting information (the column for the table's row sort and the direction) and updates the sort of the
        column groups in order to sort many-related, delimited values if one of the group is the sort column.

        Example:
            If the column group is for tracers on the sample page, including tracer name and concentration columns, and
            the concentration column is the sort column for the entire table, both the tracer name and concentrations on
            1 row are sorted as well based on the multiple tracer concentrations for each sample.
        Args:
            sort_by (str): The BSTColumn.name of the column sorting the table rows.
            sort_dir (str) {asc, desc}
        Exceptions:
            None
        Returns:
            None
        """
        sort_column: Optional[BSTColumn] = self.get_column(sort_by)
        if sort_column is None:
            warning = f"WARNING: Unable to find sort_by column '{sort_by}'.  Please report this warning."
            self.warnings.append(warning)
            print(warning)
            return
        model = (
            sort_column.related_model_path
            if sort_column.related_model_path is None or isinstance(sort_column.related_model_path, str)
            else sort_column.related_model_path[0]
        )
        print(f"ORDERING: SORT FLD: {sort_by} SORT DIR: {sort_dir} MODEL: {model} GROUPS: {self.groups_dict}")
        if model is not None and model in self.groups_dict.keys():
            column: BSTColumn
            for column in self.groups_dict[model]:
                column.related_sort_fld = sort_column.related_sort_fld_orig
                column.related_sort_fwd = not sort_dir.lower().startswith("d")
                print(f"SETTING GROUP ORDERING OF {column.related_model_path} {column.name} {column.related_sort_fld} FWD?: {column.related_sort_fwd}")
        elif sort_column.many_related:
                print(f"SETTING COLUMN ORDERING OF MODEL PATH {sort_column.related_model_path} COLUMN NAME {sort_column.name} SORT FIELD {sort_column.related_sort_fld} FWD?: {sort_column.related_sort_fwd}")
                # Only need to update the direction, because column.related_sort_fld should not change unless this
                # column is in a column group
                sort_column.related_sort_fwd = not sort_dir.lower().startswith("d")

    def get_column(self, column_name: Union[BSTColumn, str]):
        try:
            i = self.columns.index(column_name)
            return self.columns[i]
        except ValueError as ve:
            print(f"4 ValueError: {ve}")
            return None

    def get_export_data(self, format: str):
        """Turns the queryset into a base64 encoded string and a filename.

        Args:
            format (str) {excel, csv, tsv}
        Exceptions:
            NotImplementedError when the format is unrecognized.
        Returns:
            export_data (str): Base64 encoded string.
            (str): The filename
        """
        if format == "excel":
            ext = "xlsx"
            byte_stream = BytesIO()
            self.get_excel_streamer(byte_stream)
            export_data = base64.b64encode(byte_stream.read()).decode("utf-8")
        else:
            buffer = StringIO()
            if format == "csv":
                ext = "csv"
                # This fills the buffer
                self.get_text_streamer(buffer, delim=",")
                # This consumes the buffer
                export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
                # export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
            elif format == "txt":
                ext = "tsv"
                # This fills the buffer
                self.get_text_streamer(buffer, delim="\t")
                # This consumes the buffer
                export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
                # export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
            else:
                raise NotImplementedError(f"Export format {format} not supported.")

        return export_data, f"{self.model.__name__}.{self.filename_timestamp}.{ext}"

    def get_excel_streamer(self, byte_stream: BytesIO):
        """Returns an xlsxwriter for an excel file created from the self.dfs_dict.

        The created writer will output to the supplied stream_obj.

        Args:
            stream_obj (BytesIO)
        Exceptions:
            None
        Returns:
            xlsx_writer (xlsxwriter)
        """
        xlsx_writer = pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
            byte_stream, engine="xlsxwriter"
        )

        header_context = self.get_header_context()
        sheet = self.verbose_model_name_plural
        columns = self.row_headers()

        xlsx_writer.book.set_properties(
            {
                "title": sheet,
                "author": "Robert Leach",
                "company": "Princeton University",
                "comments": self.headtmplt.render(header_context),
            }
        )

        # Build the dict by iterating over the row lists
        qs_dict_by_index = dict((i, []) for i in range(len(columns)))
        for row in self.rows_iterator(headers=False):
            for i, val in enumerate(row):
                qs_dict_by_index[i].append(str(val))

        export_dict = {}
        # Now convert the indexes to the headers
        for i, col in enumerate(columns):
            export_dict[col] = qs_dict_by_index[i]

        # Create a dataframe and add it as an excel object to an xlsx_writer sheet
        pd.DataFrame.from_dict(export_dict).to_excel(
            excel_writer=xlsx_writer,
            sheet_name=sheet,
            columns=columns,
            index=False,
        )
        xlsx_writer.sheets[sheet].autofit()

        xlsx_writer.close()
        # Rewind the buffer so that when it is read(), you won't get an error about opening a zero-length file in Excel
        byte_stream.seek(0)

        return byte_stream

    def get_header_context(self):
        """Returns a context dict with metadata about the queryset used in the header template.

        Args:
            None
        Exceptions:
            None
        Returns:
            (dict)
        """
        search_term: Optional[str] = self.get_cookie("search")
        order_by: Optional[str] = self.get_cookie("order-by")
        order_dir: Optional[str] = self.get_cookie("order-dir", "asc")
        if order_by is not None and order_by != "":
            sort_str = f"{order_by}, {order_dir}"
        else:
            sort_str = "default"
        filters = []
        column: BSTColumn
        for column in self.columns:
            filter_value: str = self.get_column_cookie(column, "filter")
            if filter_value is not None and filter_value != "":
                filters.append({"column": column.header, "filter": filter_value})

        return {
            "table": self.verbose_model_name_plural,
            "date": self.fileheader_timestamp,
            "total": self.total,
            "search": search_term,
            "filters": filters,
            "sort": sort_str,
        }

    def get_text_streamer(self, buffer: StringIO, delim: str = "\t"):
        """Fills a supplied buffer with string-cast delimited values from the rows_iterator using a csv writer.

        Args:
            buffer (StringIO)
            delim (str) [\t]: Column delimiter
        Exceptions:
            None
        Returns:
            buffer (StringIO)
        """
        writer: "_csv._writer" = csv.writer(buffer, delimiter=delim)

        # Commented metadata header containing download date and info
        buffer.write(self.headtmplt.render(self.get_header_context()))

        for row in self.rows_iterator():
            writer.writerow([str(c) for c in row])

        return buffer

    def rows_iterator(self, headers=True):
        """Takes a queryset of records and returns a list of lists of column data.  Note that delimited many-related
        values are converted to strings, but everything else in the returned list of lists is the original type.

        Args:
            headers (bool): Whether to include the header row.
        Exceptions:
            None
        Returns:
            (List[list])
        """
        if headers:
            yield self.row_headers()
        rec: Model
        for rec in self.get_queryset():
            yield self.rec_to_row(rec)

    def row_headers(self):
        return [col.header for col in self.columns if col.exported]

    def rec_to_row(self, rec: Model) -> List[str]:
        """Takes a Model record and returns a list of values for a file.

        Args:
            rec (Model)
        Exceptions:
            None
        Returns:
            (List[str])
        """
        return [
            self.get_rec_val(rec, col)
            if not col.many_related
            else col.delim.join(
                [str(val) for val in self.get_many_related_rec_val(rec, col)]
            )
            for col in self.columns
            if col.exported
        ]

    def get_rec_val(self, rec: Model, col: BSTColumn, related_limit: Optional[int] = 10):
        """Given a model record, i.e. row-data, e.g. from a queryset, and a column, return the column value.

        NOTE: While this supports many-related columns, it is more efficient to call get_many_related_rec_val directly.

        Args:
            rec (Model)
            col (BSTColumn)
        Exceptions:
            ValueError when the BSTColumn is not labeled as many-related.
        Returns:
            (Any): Column value or values (if many-related).
        """
        # print(f"LOOKING UP {col.name} IN REC TYPE {type(rec).__name__}")
        # Getting an annotation is fast, and if it is None, we can skip potentially costly many_related lookups
        val, _, _ = self._get_rec_val_helper(rec, col.name.split("__"), related_limit=related_limit)
        if val == "":
            val = None

        # Replace a non-empty annotation with a delimited version if many_related
        if col.many_related and val is not None:
            # Do not call get_rec_val if col is many_related.  Call get_many_related_rec_val directly to be efficient
            print("WARNING: get_rec_val called in a many-related column")
            val = self.get_many_related_rec_val(rec, col, related_limit=related_limit)

        return val

    def get_many_related_rec_val(self, rec: Model, col: BSTColumn, related_limit: Optional[int] = 10):
        """Method to improve performance by grabbing the annotated value first and if it is not None, does an exhaustive
        search for all many-related values.  It also can perform a simulated version of Django's Coalesce functionality
        if there are multiple different models that link to the base model.

        Assumptions:
            1. If there are multiple models that link to the base model, either only 1 of them will be populated, or
                they will all result in the same outcome (i.e. the same set of related field values).
        Args:
            rec (Model)
            col (BSTColumn)
        Exceptions:
            ValueError when the BSTColumn is not labeled as many-related.
        Returns:
            (list): Sorted unique (to the many-related model) values.
        """
        if not col.many_related:
            raise ValueError(f"Column {col.name} is not many-related.")

        # Get the annotated version (because it's faster to skip the rigor below if it is None)
        val, _, _ = self._get_rec_val_helper(rec, col.name.split("__"))
        if val is None or isinstance(val, list) and len(val) == 0:
            return []
        # val is not None, which means there could be more than just 1 result...

        # If field is a list of fields, we treat it like Coalesce and take the first one that returns a value of any
        # kind.  This is for when multiple tables link to one table.  This is that one table trying to figure out which
        # one links to it.  If multiple different tables A, B, and C link to the same record in this table D, 1. their
        # field paths must all lead to the same table E, for which col.field is representing, and 2. this will
        # collect values only from table A via either a single connection in A, B, or C, and stop.  So if A results in 2
        # E records and B results in 3 E records, only the 2 E records retrieved via A are returned.
        if isinstance(col.field, list):
            for i in range(len(col.field)):
                val = self._get_many_related_rec_val_helper(
                    rec,
                    col.field[i],
                    col.related_sort_fld[i],
                    reverse=not col.related_sort_fwd,
                    related_limit=related_limit,
                )
                if val is not None and len(val) > 0:
                    # print(f"BREAKING ON {type(val).__name__} {val} UNIQ VALS1 {type(uniq_vals1).__name__}: {uniq_vals1} UNIQ VALS2 {type(uniq_vals2).__name__}: {uniq_vals2}")
                    break
        else:
            val = self._get_many_related_rec_val_helper(
                rec,
                col.field,
                col.related_sort_fld,
                reverse=not col.related_sort_fwd,
                related_limit=related_limit,
            )

        if val is  None:
            val = []

        return val

    def _get_many_related_rec_val_helper(
        self,
        rec: Model,
        field: str,
        sort_field: str,
        reverse: bool = False,
        related_limit: Optional[int] = 20,
    ):
        """Private helper method that calls the recursive _get_rec_val_helper and processes the result as a list.
        There are 2 reasons this method exists.  1 is the fact that column.field can be a list and this method
        simplifies the processing by taking only a string in order to simulate Django's Coalesce function.
        2. _get_rec_val_helper sometimes returns a single value even when the relation goes through a many-related
        model, and the caller expects all sunch values to be lists.

        Args:
            rec (Model): A Model object.
            field (str): A path from the rec object to a field/column value, delimited by dunderscores.
            sort_field (str): A path from the rec object to a sort field, delimited by dunderscores.
            reverse (bool) [False]: Whether the many-related values should be reverse sorted.
        Exceptions:
            ProgrammingError when an exception occurs during the sorting of the value received from _get_rec_val_helper.
        Returns:
            (list): A unique list of values from the many-related model at the end of the field path.
        """
        print(f"_get_many_related_rec_val_helper CALLED WITH FIELD '{field}' ON A {type(rec).__name__} REC {rec.pk} AND SORT FIELD '{sort_field}'")

        related_model_path = field_path_to_model_path(rec.__class__, field)
        related_model = model_path_to_related_model(rec.__class__, related_model_path)
        if field != related_model_path:
            distinct_fields = [field]
        else:
            # To use .distinct(), you need the ordering fields from the related model, otherwise you get an exception about the order_by and distinct fields being different
            distinct_fields = [f"{field}__{f}" for f in related_model._meta.ordering]

        # TODO: Incorporate reverse
        # qs = rec.__class__.objects.filter(pk=rec.pk).values(field, sort_field, *distinct_fields).order_by(sort_field, *distinct_fields).distinct(sort_field, *distinct_fields)
        # print(f"SQL: {qs.query}")
        # vals_list = list(qs.all())
        if is_string_field(rec.__class__, sort_field):
            if reverse:
                sort_field = Lower(sort_field).desc()
            else:
                sort_field = Lower(sort_field).asc()
        else:
            if reverse:
                sort_field = f"-{sort_field}"

        qs = rec.__class__.objects.filter(pk=rec.pk).order_by(sort_field, *distinct_fields).distinct(sort_field, *distinct_fields)
        print(f"SQL: {qs.query}")
        if field == related_model_path and qs.exists():
            print(f"field == related_model_path: {field} == {related_model_path} VALUES: {qs.values_list(field, flat=True)} related_model: {related_model}")
            vals_list = list([related_model.objects.get(pk=val) for val in qs.values_list(field, flat=True) if val is not None])
        else:
            vals_list = list(val for val in qs.values_list(field, flat=True) if val is not None)

        # vals_list = self._get_rec_val_helper(rec, field.split("__"), sort_field_path=sort_field.split("__"), related_limit=related_limit)
        if len(vals_list) < 2:
            return vals_list
        if related_limit is not None:
            vals_list = vals_list[0:related_limit]
        return vals_list
        # try:
        #     # Sorting with (t[1] is None, t[1]) is to sort None values to the end
        #     val = [
        #         # The first in the tuple is the column value
        #         val[0]
        #         for val in sorted(
        #             reduce(
        #                 lambda lst, val: lst + [val] if val not in lst else lst,
        #                 [tpl for tpl in vals_list if tpl is not None],
        #                 [],
        #             ),
        #             # The second value in the tuple is the value to sort by (a third pk exists for uniqueness)
        #             key=lambda t: (t[1] is not None, t[1]),
        #             reverse=reverse
        #         )
        #     ]
        #     # print(f"GOT2 '{val}' VALSLIST {vals_list}")
        # except Exception as e:
        #     val = [val for val in vals_list]
        #     raise ProgrammingError(
        #         f"Got exception: {type(e).__name__}: {e}\nIf this value from {vals_list} looks good: '{val}', "
        #         "consider accounting for this case somehow and removing this try/except block."
        #     ).with_traceback(e.__traceback__)

    def _get_many_related_rec_val_helperOLD(
        self,
        rec: Model,
        field: str,
        sort_field: str,
        reverse: bool = False,
        related_limit: Optional[int] = 20,
    ):
        """Private helper method that calls the recursive _get_rec_val_helper and processes the result as a list.
        There are 2 reasons this method exists.  1 is the fact that column.field can be a list and this method
        simplifies the processing by taking only a string in order to simulate Django's Coalesce function.
        2. _get_rec_val_helper sometimes returns a single value even when the relation goes through a many-related
        model, and the caller expects all sunch values to be lists.

        Args:
            rec (Model): A Model object.
            field (str): A path from the rec object to a field/column value, delimited by dunderscores.
            sort_field (str): A path from the rec object to a sort field, delimited by dunderscores.
            reverse (bool) [False]: Whether the many-related values should be reverse sorted.
        Exceptions:
            ProgrammingError when an exception occurs during the sorting of the value received from _get_rec_val_helper.
        Returns:
            (list): A unique list of values from the many-related model at the end of the field path.
        """
        # print(f"_get_many_related_rec_val_helper CALLED WITH FIELD '{field}' ON A {type(rec).__name__} REC AND SORT FIELD '{sort_field}'")
        vals_list = self._get_rec_val_helper(rec, field.split("__"), sort_field_path=sort_field.split("__"), related_limit=related_limit)
        if vals_list is None:
            val = []
            # print(f"GOT1 {vals_list}")
        elif isinstance(vals_list, list):
            if related_limit is not None:
                vals_list = vals_list[0:related_limit]
            try:
                # Sorting with (t[1] is None, t[1]) is to sort None values to the end
                val = [
                    # The first in the tuple is the column value
                    val[0]
                    for val in sorted(
                        reduce(
                            lambda lst, val: lst + [val] if val not in lst else lst,
                            [tpl for tpl in vals_list if tpl is not None],
                            [],
                        ),
                        # The second value in the tuple is the value to sort by (a third pk exists for uniqueness)
                        key=lambda t: (t[1] is not None, t[1]),
                        reverse=reverse
                    )
                ]
                # print(f"GOT2 '{val}' VALSLIST {vals_list}")
            except Exception as e:
                val = [val for val in vals_list]
                raise ProgrammingError(
                    f"Got exception: {type(e).__name__}: {e}\nIf this value from {vals_list} looks good: '{val}', "
                    "consider accounting for this case somehow and removing this try/except block."
                ).with_traceback(e.__traceback__)
        else:
            # Sometimes the related manager returns a single record, into which we want the first of the tuple
            val = [vals_list[0]]
            # print(f"GOT3 '{val}' {vals_list}")
        return val

    def _get_rec_val_helper(
        self,
        rec: Model,
        field_path: List[str],
        related_limit: Optional[int] = 20,
        sort_field_path: Optional[List[str]] = None,
        _sort_val: Optional[List[str]] = None,
    ):
        """Private recursive method that takes a record and a path and traverses the record along the path to return
        whatever ORM object's field value is at the end of the path.  If it traverses through a many-related model, it
        returns a list of such objects or None if empty.

        Assumptions:
            1. The related_sort_fld value will be a field under the related_model_path
        Args:
            rec (Model): A Model object.
            field_path (List[str]): A path from the rec object to a field/column value, that has been split by
                dunderscores.
            sort_field_path (Optional[List[str]]): A path from the rec object to a sort field, that has been split by
                dunderscores.  Only relevant if you know the field path to traverse through a many-related model.
            _sort_val (Optional[List[str]]): Do not supply.  This holds the sort value if the field path is longer than
                the sort field path.
        Exceptions:
            ValueError when the sort field returns more than 1 value.
        Returns:
            (Optional[Union[List[Any], Any]]): A list if passing through a populated many-related model or a field
                value.
        """
        print(f"_get_rec_val_helper CALLED WITH FIELD '{field_path}' ON A {type(rec).__name__} REC {rec.pk} AND SORT FIELD '{sort_field_path}'")
        if len(field_path) == 0 or rec is None:
            # print(f"field_path {field_path} cannot be an empty list and rec '{rec}' cannot be None.")
            return None, None, None
            # raise ValueError(f"field_path {field_path} cannot be an empty list and rec '{rec}' cannot be None.")
        elif type(rec).__name__ != "RelatedManager" and type(rec).__name__ != "ManyRelatedManager":
            val_or_rec = getattr(rec, field_path[0])
        else:
            # print(f"SETTING field_or_rec to a {type(rec).__name__} WHEN LOOKING FOR {attr_path[0]}")
            val_or_rec = rec

        if len(field_path) == 1:
            # TODO: Change pk and _sort_val to val_or_rec
            pk = 1
            _sort_val = 1
            # print(
            #     f"REC: {rec} GETTING: {field_path[0]} GOT: {val_or_rec} TYPE REC: {type(rec).__name__} TYPE GOTTEN: "
            #     f"{type(val_or_rec).__name__}"
            # )
            # DEBUG: Testing this block to see if I can get this to work quickly.
            # if type(val_or_rec).__name__ == "RelatedManager":
            if type(val_or_rec).__name__ == "RelatedManager" or type(val_or_rec).__name__ == "ManyRelatedManager":
                if val_or_rec is None or val_or_rec.count() == 0:
                    return []
                print(f"val_or_rec: {val_or_rec} HAS {val_or_rec.count()} RECORDS")
                for rec in val_or_rec.distinct()[0:related_limit]:
                    print(f"\t{rec.pk} {rec}")
                # Each rec gets its own sort value.  sort_field_path is assumed to be populated, bec. you cannot sort a
                # many-related series using a non-many-related value and the sort field must be under the many-related
                # record.  If the sort value is the record itself, then the sort value should be the string value of the
                # record, because that's how it will render.


                # TESTING THIS CODE... SEE COMMENTED PORTION BELOW
                lst = list(
                    (
                        rec,  # Model object is the value returned
                        # Each rec gets its own sort value.
                        self.lower(self._get_rec_val_helper(rec, sort_field_path[1:])[0]) if sort_field_path is not None and len(sort_field_path) > 1 else str(rec).lower(),
                        rec.pk,  # We don't need pk for uniqueness when including model objects, but callers expect it
                    )
                    # DOING .all() here, followed by a reduce below, is MUCH faster than calling .distinct()
                    # for rec in val_or_rec.distinct()
                    for rec in val_or_rec.all()
                )
                uniq_vals = [
                    rec_tpl
                    for rec_tpl in reduceuntil(
                        lambda ulst, val: ulst + [val] if val not in ulst else ulst,
                        lambda val: related_limit is not None and len(val) >= related_limit,
                        lst,
                        [],
                    )
                ]
                return uniq_vals


                # COMMENTING OUT TO TEST TRUNCATION OF THE REDUCE OUTPUT BY related_limit TO HOPEFULLY SPEED THIS UP
                # if related_limit is None:
                #     lst = list(
                #         (
                #             rec,  # Model object is the value returned
                #             # Each rec gets its own sort value.
                #             self.lower(self._get_rec_val_helper(rec, sort_field_path[1:])[0]) if len(sort_field_path) > 1 is not None else str(rec),
                #             rec.pk,  # We don't need pk for uniqueness when including model objects, but callers expect it
                #         )
                #         # DOING .all() here, followed by a reduce below, is MUCH faster than calling .distinct()
                #         for rec in val_or_rec.all()
                #     )
                #     uniq_vals = [
                #         rec_tpl
                #         for rec_tpl in reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, lst, [])
                #     ]
                #     return uniq_vals
                # else:
                #     print(f"val_or_rec: {val_or_rec} {val_or_rec.count()}")
                #     return list(
                #         (
                #             rec,  # Model object is the value returned
                #             # Each rec gets its own sort value.
                #             self.lower(self._get_rec_val_helper(rec, sort_field_path[1:])[0]) if len(sort_field_path) > 1 is not None else str(rec),
                #             rec.pk,  # We don't need pk for uniqueness when including model objects, but callers expect it
                #         )
                #         # DOING .distinct() here, because we want to reduce the total iterations and return a unique list
                #         for rec in val_or_rec.distinct()[0:related_limit]
                #     )



                #     return list((r, _sort_val, r.pk) for r in val_or_rec.distinct())
                # elif type(val_or_rec).__name__ == "ManyRelatedManager":
                #     return list((r, _sort_val, r.pk) for r in val_or_rec.distinct())
                #     # return list((r, _sort_val, r.pk) for r in val_or_rec.through.distinct())
            elif isinstance(val_or_rec, Model):
                # We add the primary key to the tuple so that the python reduce that happens upstream of this leaf in
                # the recursion ensures that we get a unique set of many-related records.  I had tried using
                # .distinct(), but the performance really suffered.  Handling it with reduce is MUCH MUCH faster.
                pk = val_or_rec.pk

            print(f"RETURNING ONE {type(val_or_rec).__name__}: {val_or_rec} WITH SORT VAL: {_sort_val}")
            return val_or_rec, _sort_val, pk

        # If we're at the end of the field path, we need to issue a separate recursive call to get the sort value
        next_sort_field_path = sort_field_path[1:] if sort_field_path is not None else None
        if sort_field_path is not None and (
            sort_field_path[0] != field_path[0]
            or (len(sort_field_path) == len(field_path) and len(field_path) == 1)
            # COMMENTED OUT BECAUSE IT WAS ONLY RELEVANT WHEN THIS BLOCK WAS ABOVE THE CODITIONAL BLOCK DIRECTLY ABOVE
            # ) and not (
            #     # If the record is many-related, we will let the many-related code below handle it
            #     type(val_or_rec).__name__ == "RelatedManager" or type(val_or_rec).__name__ == "ManyRelatedManager"
        ):
            print(f"GETTING SORT VAL {sort_field_path} FOR {field_path} FROM {rec}")
            sort_val, _, _ = self._get_rec_val_helper(rec, sort_field_path)
            if isinstance(sort_val, list):
                uniq_vals = reduce(lambda lst, val: lst + [val] if val not in lst else lst, sort_val, [])
                if len(uniq_vals) > 1:
                    # DEBUG: This might be a problem if the values are model objects (from a foreign key).  Their sort
                    # value SHOULD return multiple results.  This is handled below in the next RelatedManager
                    # conditional where it grabs each of multiple sort vals.  So this may need to change.  In fact, I'm
                    # surprised this ValueError hasn't already been encountered, which may be an issue in and of itself.
                    raise ValueError("Multiple values returned")
                elif len(uniq_vals) == 1:
                    self.lower(uniq_vals[0])
                else:
                    sort_val = None
            next_sort_field_path = None
            _sort_val = sort_val

        if type(val_or_rec).__name__ == "RelatedManager" or type(val_or_rec).__name__ == "ManyRelatedManager":
            if val_or_rec.count() > 0:
                possibly_nested_list = list(
                    self._get_rec_val_helper(
                        rel_rec,
                        field_path[1:],
                        related_limit=related_limit,
                        sort_field_path=next_sort_field_path,
                        _sort_val=_sort_val,
                    )
                    # for rel_rec in val_or_rec.distinct()
                    for rel_rec in val_or_rec.all()
                )
                if len(possibly_nested_list) == 0 or not isinstance(possibly_nested_list[0], list):
                    return possibly_nested_list
                lst = list(item for sublist in possibly_nested_list for item in sublist)



                # TESTING THIS CODE... SEE COMMENTED PORTION BELOW
                uniq_vals = [
                    rec_tpl
                    for rec_tpl in reduceuntil(
                        lambda ulst, val: ulst + [val] if val not in ulst else ulst,
                        lambda val: related_limit is not None and len(val) >= related_limit,
                        lst,
                        [],
                    )
                ]



                # COMMENTING OUT TO TEST TRUNCATION OF THE REDUCE OUTPUT BY related_limit TO HOPEFULLY SPEED THIS UP
                # uniq_vals = reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, lst, [])




                # print(f"RETURNING 1 {type(uniq_vals).__name__}.{type(uniq_vals[0]).__name__}: {uniq_vals}")
                return uniq_vals
            return []

        # TODO: I should probably use reduce here and remove the call to reduce from _get_many_related_rec_val_helper, because I think it will otherwise be called a few times in a row (from other places in this method and in _get_many_related_rec_val_helper)
        return self._get_rec_val_helper(
            val_or_rec,
            field_path[1:],
            related_limit=related_limit,
            sort_field_path=next_sort_field_path,
            _sort_val=_sort_val,
        )

    @classmethod
    def lower(cls, val):
        """Intended for use in list comprehensions to lower-case the sort value, IF IT IS A STRING.
        Otherwise it returns the unmodified value."""
        if isinstance(val, str):
            return val.lower()
        return val

    def get_cookie_name(self, name: str) -> str:
        """Retrieves a cookie name using a prepended view name.

        Args:
            name (str)
        Exceptions:
            None
        Returns:
            (str)
        """
        return f"{self.cookie_prefix}{name}"

    def get_cookie(self, name: str, default: str = "") -> str:
        """Retrieves a cookie using a prepended view name.

        Args:
            name (str)
            default (str) [""]
        Exceptions:
            None
        Returns:
            (str): The cookie value for the supplied name (with the view_name prepended) obtained from self.request or
                the default if the cookie was not found (or was an empty string).
        """
        # If a cookie reset occurred, it means one or more of the cookies is problematic, so just return the default.
        if hasattr(self, "request") and not self.clear_cookies:
            return get_cookie(self.request, self.get_cookie_name(name), default) or ""
        return default

    def get_column_cookie_name(self, column: Union[BSTColumn, str], name: str) -> str:
        """Retrieves a cookie name using a prepended view name.

        Args:
            column (Union[BSTColumn, str]): The name of the BST column or the column object
            name (str): The name of the cookie variable specific to the column
        Exceptions:
            None
        Returns:
            (str)
        """
        if isinstance(column, str):
            return f"{self.cookie_prefix}{name}-{column}"
        return f"{self.cookie_prefix}{name}-{column.name}"

    def get_column_cookie(self, column: BSTColumn, name: str, default: str = "") -> str:
        """Retrieves a cookie using a prepended view name.

        Args:
            column (str): The name of the BST column
            name (str): The name of the cookie variable specific to the column
            default (str) [""]
        Exceptions:
            None
        Returns:
            (str): The cookie value for the supplied name (with the view_name prepended) obtained from self.request or
                the default if the cookie was not found (or was an empty string).
        """
        if hasattr(self, "request"):
            return get_cookie(self.request, self.get_column_cookie_name(column, name), default) or ""
        return default
