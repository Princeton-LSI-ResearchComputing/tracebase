from __future__ import annotations
from collections import defaultdict
from datetime import datetime
from functools import reduce
from io import BytesIO, StringIO
from typing import Dict, List, Optional, Tuple, Union
import base64
import pandas as pd
import csv
import _csv

from django.core.exceptions import FieldError
from django.db import ProgrammingError
from django.db.models import F, Max, Min, Model, Q, QuerySet, Count, AutoField, CharField, Field
from django.db.models.functions import Coalesce, Lower
from django.template import loader
from django.utils.functional import classproperty
from django.views.generic import ListView

from DataRepo.models.utilities import get_field_class_from_model_path, is_number_field, is_string_field
from DataRepo.utils.exceptions import MutuallyExclusiveArgs
from DataRepo.utils.text_utils import camel_to_title, underscored_to_title
from DataRepo.views.utils import GracefulPaginator, get_cookie

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

    def set_default_columns(self, custom: Optional[Dict[str, Union[dict, BSTColumn]]] = None):
        related_columns: List[BSTColumn] = []
        many_related_models: dict = {}
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
        for fld in fields:
            # print(f"FIELD {fld.name} TYPE: {type(fld).__name__}")
            if self.exclude_fields is not None and fld.name in self.exclude_fields:
                continue
            case_insensitive = isinstance(fld, CharField)
            if fld.is_relation and (fld.one_to_many or fld.many_to_many):
                # DEBUG: Testing this block(/BSTColumn column addition) to see if I can get this to work quickly.
                # print(f"fld.one_to_many or fld.many_to_many: {fld.one_to_many} or {fld.many_to_many}")
                try:
                    related_name = fld.related_name
                except AttributeError:
                    related_name = fld.name
                first_sort_fld = f"{related_name}__pk"
                first_sortable = False

                kwargs_key = f"first_{related_name}" if f"first_{related_name}" in custom.keys() else related_name

                if (
                    kwargs_key in custom.keys()
                    and (isinstance(custom[kwargs_key], dict))
                    and "many_related_sort_fld" in custom[kwargs_key].keys()
                ):
                    first_sortable = True
                    order_fld = self.get_field_name(fld.related_model._meta.ordering[0])
                    # No way to automatically set case_insensitive, so the user must set sort_nocase explicitly
                elif len(fld.related_model._meta.ordering) == 1:
                    order_fld = self.get_field_name(fld.related_model._meta.ordering[0])
                    if isinstance(getattr(fld.related_model, order_fld), CharField):
                        case_insensitive = True
                    first_sort_fld = f"{related_name}__{order_fld}"
                    first_sortable = True
                elif len(fld.related_model._meta.ordering) > 1:
                    # Grab the first field that is unique, if it exists
                    f: Field
                    for f in fld.related_model._meta.get_fields():
                        if f.unique and f.name != "id":
                            order_fld = self.get_field_name(f.name)
                            if isinstance(getattr(fld.related_model, order_fld), CharField):
                                case_insensitive = True
                            first_sort_fld = f"{related_name}__{order_fld}"
                            first_sortable = True
                            print(f"MANYREL SORT FLD SET TO FIRST UNIQUE FIELD: {first_sort_fld}")
                            break
                    if not first_sortable:
                        print(
                            f"WARNING: A {fld.related_model.__name__}._meta.ordering with more than 1 ordering field "
                            "is not supported.  Supply BSTColumn objects to the constructor to enable sorting of the "
                            f"{self.model.__name__} BST ListView."
                        )
                else:
                    # TODO: Try to get the first field that is unique=True
                    pass
                if related_name not in self.exclude_fields and f"{related_name}_count" not in self.exclude_fields:
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
                            "sorter": BSTColumn.SORTER_CHOICES["NUMERIC"],
                            "converter": Count(fld.name, distinct=True),
                        }
                        if f"{related_name}_count" in custom.keys():
                            kwargs.update(custom[f"{related_name}_count"])

                        print(f"ADDING DEFAULT COLUMN {related_name}_count")
                        related_columns.append(BSTColumn(f"{fld.name}_count", **kwargs))

                if related_name not in self.exclude_fields and f"first_{related_name}" not in self.exclude_fields:
                    leftover_custom_columns.pop(f"first_{related_name}", None)
                    leftover_custom_columns.pop(related_name, None)
                    if (
                        f"first_{related_name}" in custom.keys()
                        and isinstance(custom[f"first_{related_name}"], BSTColumn)
                    ):
                        print(f"ADDING CUSTOM COLUMN first_{related_name}")
                        related_columns.append(custom[f"first_{related_name}"])
                    else:
                        kwargs = {
                            "field": related_name,
                            "header": underscored_to_title(related_name),
                            "many_related": True,
                            "many_related_model": related_name,
                            "many_related_sort_fld": first_sort_fld,
                            "sort_nocase": case_insensitive,
                            "sortable": first_sortable,
                            "sorter": BSTColumn.SORTER_CHOICES["HTML"],
                            # TODO: Figure out how to turn the __str__ function into a "search_field"
                            "searchable": False,
                        }
                        if f"first_{related_name}" in custom.keys():
                            kwargs.update(custom[f"first_{related_name}"])
                        elif related_name in custom.keys():
                            # Allow them to specify just the field in the key so that it maps correctly from what you
                            # get from Model._meta.get_fields()
                            kwargs.update(custom[related_name])

                        print(f"ADDING DEFAULT COLUMN first_{related_name} kwargs: {kwargs}")
                        related_columns.append(BSTColumn(f"first_{related_name}", **kwargs))

                # print(f"FIELD NAME: {fld.name}")
                # if fld.many_to_many and hasattr(fld, "through"):
                #     through_models.append(fld.through.__name__)
                many_related_models[fld.name] = fld.related_model.__name__

            elif fld.is_relation:
                print(f"RELATION: '{fld.name}' '{fld.verbose_name}' '{underscored_to_title(fld.name)}'")
                leftover_custom_columns.pop(fld.name, None)
                if (
                    fld.name in custom.keys()
                    and isinstance(custom[fld.name], BSTColumn)
                ):
                    print(f"ADDING CUSTOM COLUMN {fld.name}")
                    related_columns.append(custom[fld.name])
                else:
                    kwargs = {
                        "header": self.get_field_header(fld),
                        "sorter": BSTColumn.SORTER_CHOICES["ALPHANUMERIC"],
                        "sort_nocase": case_insensitive,
                    }
                    if fld.name in custom.keys():
                        print(f"ADDING EDITED DEFAULT COLUMN {fld.name}")
                        kwargs.update(custom[fld.name])
                    else:
                        print(f"ADDING DEFAULT COLUMN {fld.name}")
                    self.columns.append(BSTColumn(fld.name, **kwargs))

            elif not isinstance(fld, AutoField):
                print(f"NON-RELATION: {fld.name} CHOICES?: {fld.choices}")
                if (
                    fld.name in custom.keys()
                    and isinstance(custom[fld.name], BSTColumn)
                ):
                    print(f"ADDING CUSTOM COLUMN {fld.name}")
                    related_columns.append(custom[fld.name])
                else:
                    kwargs = {
                        "header": self.get_field_header(fld),
                        "sorter": BSTColumn.SORTER_CHOICES["ALPHANUMERIC"],
                        "sort_nocase": case_insensitive,
                    }
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
                print(f"ADDING LEFTOVER COLUMN {custom_key} as dict arguments {custom}")
                self.columns.append(BSTColumn(custom_key, **custom[custom_key]))

        if len(related_columns) > 0:
            for related_column in related_columns:
                self.columns.append(related_column)
                # if self.include_through_models or many_related_models[related_column.field] not in through_models:
                #     self.columns.append(related_column)

    @classmethod
    def get_field_header(cls, field: Field):
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
        qs = super().get_queryset()
        self.total = qs.count()
        self.raw_total = self.total
        return self.get_manipulated_queryset(qs)

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
        column: BSTColumn
        for column in self.columns:
            # Put all fields' model paths into model_paths, to be evaluated for entry into prefetches
            if isinstance(column.field, list):
                for fld in column.field:
                    mdl = column.field_to_related_model(fld)
                    if mdl is not None and mdl not in model_paths:
                        # print(f"ADDING MODEL {mdl} FROM COLUMN {column.name} AND FIELD {column.field}")
                        model_paths.append(mdl)
                    # else:
                    #     print(f"NOT ADDING MODEL {mdl} FROM COLUMN {column.name} AND FIELD {column.field}")
            elif column.field is not None:
                mdl = column.field_to_related_model(column.field)
                if mdl is not None and mdl not in model_paths:
                    # print(f"ADDING MODEL {mdl} FROM COLUMN {column.name} AND FIELD {column.field}")
                    model_paths.append(mdl)
                # else:
                #     print(f"NOT ADDING MODEL {mdl} FROM COLUMN {column.name} AND FIELD {column.field}")

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
                # print(f"FILTERING COLUMN '{column.name}' USING FIELD '{search_field}' AND TERM '{filter_value}'")
                if isinstance(search_field, list):
                    or_q_exp = Q()
                    for many_related_search_field in column.field:
                        search_fields.append(many_related_search_field)
                        if(not is_number_field(get_field_class_from_model_path(self.model, many_related_search_field))):
                            or_q_exp |= Q(**{f"{many_related_search_field}__icontains": filter_value})
                        else:
                            or_q_exp |= Q(**{many_related_search_field: filter_value})
                    q_exp &= or_q_exp
                elif column.field is not None:
                    search_fields.append(search_field)
                    if(not is_number_field(get_field_class_from_model_path(self.model, search_field))):
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

        prefetches = []
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
        # print(f"PREFETCHES: {prefetches} MODEL PATHS: {model_paths}")
        qs = qs.prefetch_related(*prefetches)

        # 3. Add annotations (which can be used in search & sort)

        annotations_before_filter = {}
        annotations_after_filter = {}
        for column in self.columns:
            try:
                # If a converter exists, the column is an annotation column, so annotate it
                if column.converter is not None:
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
                        raise ValueError(f"Column {column.name} must not have a converter if field is None.")
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
                    if isinstance(column.field, list):
                        # There are multiple fields that link to the reference model, so we use coalesce, assuming that
                        # the same reference model record is not linked to from multiple other model fields.
                        if order_by == "" or (
                            order_by == column.name
                            and not descending
                        ):
                            # Get the minimum value of the first populated many-related field
                            if column.many_related:
                                # Apply Min to prevent changing the number of resulting rows
                                annotations_after_filter[column.name] = Coalesce(*[Min(f) for f in column.field])
                            else:
                                annotations_after_filter[column.name] = Coalesce(*column.field)
                        else:
                            # Get the maximum value of the first populated many-related field
                            if column.many_related:
                                # Apply Max to prevent changing the number of resulting rows
                                annotations_after_filter[column.name] = Coalesce(*[Max(f) for f in column.field])
                            else:
                                annotations_after_filter[column.name] = Coalesce(*column.field)
                    elif column.many_related:
                        # The order_by_field could be a foreign key, which is sorted by the column object's
                        # many_related_sort_fld.  When sorting the rows, we son't want to sort by the value of the
                        # annotation (the foreign key number).  We need to give the user the annotation value they
                        # requested for column.name, AND sort by the value they explicitly said they want to sort on, so
                        # we need another (temporary) annotation for the sort value when sorting by this column.
                        if order_by_column == column:
                            order_by_field_name = f"{order_by_column.name}_sort"
                            order_by_field = F(order_by_field_name)
                        # This assumes column.field is not None
                        if order_by == "" or (
                            order_by == column.name
                            and not descending
                        ):
                            # Apply Min to prevent changing the number of resulting rows
                            # This is only ever be used when there is a single result. If creating your own template, be
                            # careful not to rely on the annotation for sorting rows if your many_related_sort_fld
                            # differs
                            annotations_before_filter[column.name] = Min(column.field)
                            if order_by_column == column:
                                # This one-off sorting annotation will only ever be used below if ordering by this
                                # column or group
                                annotations_before_filter[order_by_field_name] = Min(column.many_related_sort_fld_orig)
                        else:
                            # Apply Max to prevent changing the number of resulting rows
                            # This is only ever be used when there is a single result. If creating your own template, be
                            # careful not to rely on the annotation for sorting rows if your many_related_sort_fld
                            # differs
                            annotations_before_filter[column.name] = Max(column.field)
                            if order_by_column == column:
                                # This one-off sorting annotation will only ever be used below if ordering by this
                                # column or group
                                annotations_before_filter[order_by_field_name] = Max(column.many_related_sort_fld_orig)
                    elif column.field is not None:
                        # This is in case a user-supplied custom converter failed in the try block above and the field
                        # is not many_related and there are not multiple other model fields linking to the reference
                        # model
                        annotations_before_filter[column.name] = F(column.field)

        if len(annotations_before_filter.keys()) > 0:
            print(f"ANNOTATIONS BEFORE: {annotations_before_filter}")
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
                    f"Your search could not be executed.  {fld_msg}\n\nPlease report this error to the site "
                    "administrators."
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
            # print(f"ANNOTATIONS AFTER: {annotations_after_filter}")
            qs = qs.annotate(**annotations_after_filter)

        # 6. Apply the sort

        # Sort the results, if sort has a value
        if ordered:
            # print(f"COUNT BEFORE ORDERBY: {qs.count()} ORDER BY: {order_by}")

            if order_by_column.sort_nocase:
                order_by_field = Lower(order_by_field)

            if descending:
                # print(f"ORDERING ROWS BY {order_by_field}, desc, nulls last")
                qs = qs.order_by(order_by_field.desc(nulls_last=True))
            else:
                # print(f"ORDERING ROWS BY {order_by_field}, asc, nulls first")
                qs = qs.order_by(order_by_field.asc(nulls_first=True))

        # 7. Ensure distinct results (because annotations and/or sorting can cause the equivalent of a left join).

        # print(f"COUNT BEFORE DISTINCT: {qs.count()}")
        qs = qs.distinct()

        # 8. Update the count

        # print(f"COUNT BEFORE RETURN: {qs.count()}")
        # Set the total after the search
        self.total = qs.count()

        # TODO: Add a check for when limit is 0 and total > raw_total.  Should raise an exception because the developer
        # added a many-related column and did not make it 1:1 with the base model

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

        context = super().get_context_data(**kwargs)

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

        # DEBUG: Testing this block to see if I can get this to work quickly.
        # Iterate over this page's worth of rows to pass along the related objects
        qs = context["object_list"]
        context["related_objects"] = {}
        for rec in qs.all():
            context["related_objects"][rec.pk] = {}
            for column in self.columns:
                if column.many_related and column.converter is None:
                    context["related_objects"][rec.pk][column.name] = self.get_many_related_rec_val(rec, column)
                    # print(
                    #     f"SETTING REC related_objects.{rec.pk}.{column.name} to FWD?: {column.many_related_sort_fwd} "
                    #     f"ORDERING OF: {context['related_objects'][rec.pk][column.name]}"
                    # )

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
                        if(not is_number_field(get_field_class_from_model_path(self.model, many_related_search_field))):
                            q_exp |= Q(**{f"{many_related_search_field}__icontains": term})
                        else:
                            q_exp |= Q(**{many_related_search_field: term})
                elif column.field is not None:
                    search_fields.append(search_field)
                    if(not is_number_field(get_field_class_from_model_path(self.model, search_field))):
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
        model = sort_column.many_related_model
        print(f"ORDERING: SORT FLD: {sort_by} SORT DIR: {sort_dir} MODEL: {model} GROUPS: {self.groups_dict}")
        if model is not None and model in self.groups_dict.keys():
            column: BSTColumn
            for column in self.groups_dict[model]:
                column.many_related_sort_fld = sort_column.many_related_sort_fld_orig
                column.many_related_sort_fwd = not sort_dir.lower().startswith("d")
                print(f"SETTING GROUP ORDERING OF {column.many_related_model} {column.name} {column.many_related_sort_fld} FWD?: {column.many_related_sort_fwd}")
        elif sort_column.many_related:
                print(f"SETTING COLUMN ORDERING OF {sort_column.many_related_model} {sort_column.name} {sort_column.many_related_sort_fld} FWD?: {sort_column.many_related_sort_fwd}")
                # Only need to update the direction, because column.many_related_sort_fld should not change unless this
                # column is in a column group
                sort_column.many_related_sort_fwd = not sort_dir.lower().startswith("d")

    def get_column(self, column_name: Union[BSTColumn, str]):
        try:
            i = self.columns.index(column_name)
            return self.columns[i]
        except ValueError:
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

    def get_rec_val(self, rec: Model, col: BSTColumn):
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
        val, _, _ = self._get_rec_val_helper(rec, col.name.split("__"))
        if val == "":
            val = None

        # Replace a non-empty annotation with a delimited version if many_related
        if col.many_related and val is not None:
            # Do not call get_rec_val if col is many_related.  Call get_many_related_rec_val directly to be efficient
            print("WARNING: get_rec_val called in a many-related column")
            val = self.get_many_related_rec_val(rec, col)

        return val

    def get_many_related_rec_val(self, rec: Model, col: BSTColumn):
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
                # print(f"CHECKING {fld}")
                val = self._get_many_related_rec_val_helper(
                    rec,
                    col.field[i],
                    col.many_related_sort_fld[i],
                    reverse=not col.many_related_sort_fwd
                )
                if val is not None and len(val) > 0:
                    # print(f"BREAKING ON {type(val).__name__} {val} UNIQ VALS1 {type(uniq_vals1).__name__}: {uniq_vals1} UNIQ VALS2 {type(uniq_vals2).__name__}: {uniq_vals2}")
                    break
        else:
            val = self._get_many_related_rec_val_helper(
                rec,
                col.field,
                col.many_related_sort_fld,
                reverse=not col.many_related_sort_fwd
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
        vals_list = self._get_rec_val_helper(rec, field.split("__"), sort_field_path=sort_field.split("__"))
        if vals_list is None:
            val = []
            # print(f"GOT1 {vals_list}")
        elif isinstance(vals_list, list):
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
        sort_field_path: Optional[List[str]] = None,
        _sort_val: Optional[List[str]] = None,
    ):
        """Private recursive method that takes a record and a path and traverses the record along the path to return
        whatever ORM object's field value is at the end of the path.  If it traverses through a many-related model, it
        returns a list of such objects or None if empty.

        Assumptions:
            1. The many_related_sort_fld value will be a field under the many_related_model
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
            (Optional[Union[List[Any], Any]]): A list if passing through a populated mmany-related model or a field
                value.
        """
        if len(field_path) == 0 or rec is None:
            # print(f"field_path {field_path} cannot be an empty list and rec '{rec}' cannot be None.")
            return None, None
            # raise ValueError(f"field_path {field_path} cannot be an empty list and rec '{rec}' cannot be None.")
        elif type(rec).__name__ != "RelatedManager" and type(rec).__name__ != "ManyRelatedManager":
            val_or_rec = getattr(rec, field_path[0])
        else:
            # print(f"SETTING field_or_rec to a {type(rec).__name__} WHEN LOOKING FOR {attr_path[0]}")
            val_or_rec = rec

        # If we're at the end of the field path, we need to issue a separate recursive call to get the sort value
        next_sort_field_path = sort_field_path[1:] if sort_field_path is not None else None
        if sort_field_path is not None and (
            sort_field_path[0] != field_path[0]
            or (len(sort_field_path) == len(field_path) and len(field_path) == 1)
        ):
            # print(f"GETTING SORT VAL {sort_field_path} FOR {field_path} FROM {rec}")
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

        if len(field_path) == 1:
            pk = 1
            # print(
            #     f"REC: {rec} GETTING: {field_path[0]} GOT: {val_or_rec} TYPE REC: {type(rec).__name__} TYPE GOTTEN: "
            #     f"{type(val_or_rec).__name__}"
            # )
            # DEBUG: Testing this block to see if I can get this to work quickly.
            # if type(val_or_rec).__name__ == "RelatedManager":
            if type(val_or_rec).__name__ == "RelatedManager" or type(val_or_rec).__name__ == "ManyRelatedManager":
                if val_or_rec is None or val_or_rec.count() == 0:
                    return []
                lst = list(
                    (
                        rec,  # Model object is the value returned
                        # Each rec gets its own sort value
                        # sort_field_path is assumed to be populated, bec. you cannot sort a many-related series using a
                        # non-many-related value
                        self.lower(self._get_rec_val_helper(rec, sort_field_path[1:])[0]),
                        rec.pk,  # We don't need pk for uniqueness when including model objects, but callers expect it
                    )
                    # DOING .all() here, followed by a reduce below, is MUCH faster than calling .distinct()
                    for rec in val_or_rec.all()
                )
                uniq_vals = [
                    rec_tpl
                    for rec_tpl in reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, lst, [])
                ]
                return uniq_vals
                #     return list((r, _sort_val, r.pk) for r in val_or_rec.distinct())
                # elif type(val_or_rec).__name__ == "ManyRelatedManager":
                #     return list((r, _sort_val, r.pk) for r in val_or_rec.distinct())
                #     # return list((r, _sort_val, r.pk) for r in val_or_rec.through.distinct())
            elif isinstance(val_or_rec, Model):
                # We add the primary key to the tuple so that the python reduce that happens upstream of this leaf in
                # the recursion ensures that we get a unique set of many-related records.  I had tried using
                # .distinct(), but the performance really suffered.  Handling it with reduce is MUCH MUCH faster.
                pk = val_or_rec.pk

            # print(f"RETURNING ONE {type(val_or_rec).__name__}: {val_or_rec} WITH SORT VAL: {_sort_val}")
            return val_or_rec, _sort_val, pk

        if type(val_or_rec).__name__ == "RelatedManager":
            if val_or_rec.count() > 0:
                possibly_nested_list = list(
                    self._get_rec_val_helper(
                        rel_rec,
                        field_path[1:],
                        sort_field_path=next_sort_field_path,
                        _sort_val=_sort_val,
                    )
                    for rel_rec in val_or_rec.all()
                )
                if len(possibly_nested_list) == 0 or not isinstance(possibly_nested_list[0], list):
                    return possibly_nested_list
                lst = list(item for sublist in possibly_nested_list for item in sublist)
                uniq_vals = reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, lst, [])
                # print(f"RETURNING 1 {type(uniq_vals).__name__}.{type(uniq_vals[0]).__name__}: {uniq_vals}")
                return uniq_vals
            return None
        elif type(val_or_rec).__name__ == "ManyRelatedManager":
            if val_or_rec.count() > 0:
                possibly_nested_list = list(
                    self._get_rec_val_helper(
                        rel_rec,
                        field_path[1:],
                        sort_field_path=next_sort_field_path,
                        _sort_val=_sort_val,
                    )
                    for rel_rec in val_or_rec.all()
                )
                if len(possibly_nested_list) == 0 or not isinstance(possibly_nested_list[0], list):
                    return possibly_nested_list
                lst = list(item for sublist in possibly_nested_list for item in sublist)
                uniq_vals = reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, lst, [])
                # print(f"RETURNING 12 {type(uniq_vals).__name__}.{type(uniq_vals[0]).__name__}: {uniq_vals}")
                return uniq_vals
            return None

        # TODO: I should probably use reduce here and remove the call to reduce from _get_many_related_rec_val_helper, because I think it will otherwise be called a few times in a row (from other places in this method and in _get_many_related_rec_val_helper)
        return self._get_rec_val_helper(
            val_or_rec,
            field_path[1:],
            sort_field_path=next_sort_field_path,
            _sort_val=_sort_val,
        )

    @classmethod
    def lower(cls, val):
        """Intended for use in list comprehensions to lower-case the sort value, IF IT IS A STRING.  Otherwise it
        returns the unmodified value."""
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
        if hasattr(self, "request"):
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
