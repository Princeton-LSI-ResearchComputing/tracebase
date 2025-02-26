from __future__ import annotations
from datetime import datetime
from functools import reduce
from io import BytesIO, StringIO
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union, cast
import base64
import pandas as pd
import xlsxwriter
import csv
import _csv

from django.core.exceptions import FieldError
from django.core.paginator import PageNotAnInteger, EmptyPage, Paginator
from django.db import ProgrammingError
from django.db.models import F, Max, Min, Model, Q, QuerySet, Prefetch
from django.db.models.functions import Coalesce
from django.template import loader
from django.utils.functional import classproperty
from django.views.generic import ListView

from DataRepo.models.study import Study
from DataRepo.utils.text_utils import camel_to_title, underscored_to_title
from DataRepo.views.utils import get_cookie
from DataRepo.widgets import BSTHeader


class GracefulPaginator(Paginator):
    """This derived class of Paginator prevents page not found errors by defaulting to page 1 when the page is either
    out of range or not a number."""
    # See: https://forum.djangoproject.com/t/letting-listview-gracefully-handle-out-of-range-page-numbers/23037/4
    def page(self, num):
        try:
            num = self.validate_number(num)
        except PageNotAnInteger:
            num = 1
        except EmptyPage:
            num = self.num_pages
        return super().page(num)


class BootstrapTableColumn:
    """Class to represent the interface between a bootstrap column and a Model field.

    Usage: Use this class to populate the BootstrapTableListView.columns list, like this:

        self.columns = [
            BSTColumn("filename"),
            BSTColumn("imported_timestamp"),
            BSTColumn("data_format__name"),
            BSTColumn("data_type__name"),
        ]

    Use django "field paths" relative to the base model, or model annotation names for the name arguments to the
    constructor, as BootstrapTableListView uses these for server-side pagination, filtering, and sorting.

    Alter whatever settings you want in the constructor calls.  In the BootstrapTableListView's template, all you have
    to do to render the th tag for each column is just use the name:

        {{ filename }}
        {{ imported_timestamp }}
        {{ data_format__name }}
        {{ data_type__name }}

    It will render the column headers (by default) using a title version of the last 2 values in django's dunderscore-
    delimited field path.  For example, the header generated from the above objects would be:

        Filename
        Imported Timestamp
        Data Format Name
        Data Type Name

    It's also important to note that in order for BootstrapTableListView's search and sort to work as expected, each
    column should be converted to a simple string or number annotation that is compatible with django's annotate method.
    For example, as a DateTimeField, imported_timestamp, will sort correctly on the server side, but Bootstrap Table
    will sort the page's worth of results using alphanumeric sorting.  You can make the sorting behavior consistent by
    supplying a function using the converter argument, like this:

        BSTColumn(
            "imported_timestamp_str",
            field="imported_timestamp",
            converter=Func(
                F("imported_timestamp"),
                Value("YYYY-MM-DD HH:MI a.m."),
                output_field=CharField(),
                function="to_char",
            ),
        )

    Instance Attributes:
        name
        converter
        delim
        exported
        field
        filter
        filter_control
        header
        many_related
        searchable
        select_options
        sortable
        sorter
        strict_select
        visible
        widget
    """

    FILTER_CONTROL_CHOICES = {
        "INPUT": "input",  # default
        "SELECT": "select",
        "DATEPICKER": "datepicker",
        "DISABLED": "",
    }
    SORTER_CHOICES = {
        "ALPHANUMERIC": "alphanum",  # default
        "NUMERIC": "numericOnly",
        "HTML": "htmlSorter",  # See static/js/htmlSorter.js
    }
    NAME_IS_FIELD = "__same__"

    # TODO: Make this into an instance attribute
    DEF_DELIM = "; "  # For many-related fields

    def __init__(
        self,
        name: str,  # field path or annotation name (see converter)
        field: Optional[Union[str, List[str]]] = NAME_IS_FIELD,
        header: str = None,
        many_related: bool = False,
        sortable: bool = True,
        sorter: Optional[str] = None,
        visible: bool = True,
        exported: bool = True,
        searchable: Optional[bool] = None,
        filter_control: Optional[str] = None,
        select_options: Optional[Union[Dict[str, str], List[str]]] = None,

        # Advanced (some assign reasonable defaults)
        converter: Optional[Callable] = None,
        many_related_model: Optional[Union[str, List[str]]] = None,  # default = field's immediate parent
        many_related_sort_fld: Optional[Union[str, List[str]]] = None,  # default = {many_related_model}__pk
        many_related_sort_fwd: bool = True,
        many_related_delim: str = DEF_DELIM,
        strict_select: Optional[bool] = None,
    ):
        """Defines options used to populate the bootstrap table columns for a BootstrapListView and a single reference
        model.

        Args:
            name (str): The data-field attribute of a th tag.  Used for cookies too.  If filter_control is not None,
                this value should be a valid Django annotation name or database field filter path relative to the
                BootstrapListView.model that contains this instance.
            field (Optional[Union[str, List[str]]]) ["__same__"]: Name of the database field or fields corresponding to
                the column.  Supply this explicitly if the name argument is an annotation or set to None if no
                corresponding model field exists (or is desired for search, filter, or sort).  "__same__" means that
                name is a model field.  If a list, the related fields must all be CharFields.
            converter (Optional[Callable]): A method to convert a database field to a CharField.  This is necessary for
                searching and filtering because BST only does substring searches.  It also prevents sorting from
                increasing the number of resulting rows if there is a many-related field (i.e. when field is a list).
            many_related (bool) [False]: If this field is a reverse relation, e.g. the link resides in another model
                that links to "this" model or the link is a ManyToManyField, setting this value to True ensures that the
                number of rows in the table accurately reflects the number of records in the reference model when
                sorting is performed on this column.  It does this by sorting on the single annotated value instead of
                the related field.  Django has this left-join side effect when you sort on a many-related field.  By
                sorting on the annotated field, that row increase side-effect is prevented.  Setting this to True means
                that an annotation will be automatically created (unless a converter is supplied).  If there are
                multiple linked records, either a Min() or Max() will be applied if the user selects to sort on this
                column.  You can render whatever you link in the column, but to have each page sorted correctly by
                bootstrap, you should set the annotated value as a hidden element.  The overall DB sort in the query
                will be based on the annotated value.  Ideally, that value would be a joined string of all of the
                related values, but all thos functions are postgres-specific.
                Example for AnimalListView:
                    BootstrapTableColumn("study", field="studies__name", many_related=True)
                    If an animal belongs to multiple studies and the user selects to do an ascending sort on the study
                    column, the "study" field will be defined as and the order_by in the query will look like:
                        animal_queryset.annotate("study"=Min("studies__name")).order_by("study").distinct()
                    It's also notable that any filtering will still be on the DB field (and it will not affect the
                    number of resulting records), so a search for a study name "My Study":
                        animal_queryset.filter(studies__name__icontains="My Study")
                    Together, it looks like this:
                        animal_queryset
                            .filter(studies__name__icontains="My Study")
                            .annotate("study"=Min("studies__name"))
                            .order_by("study")
                            .distinct()
                Note, if many_related is true, name must differ from field.
            exported (bool) [True]: Adds to BST's exportOptions' ignoreColumn attribute if False.
            searchable (Optional[bool]) [True]: Whether or not a column is searchable.  Searchable being True is
                mutually exclusive with filter_control being None.  Automatically set to True if filter_control is not
                None.  It is set to False is filter_control is None.
            filter_control (Optional[str]) ["input"]: Set to "" to disable.  Must be in FILTER_CONTROL_CHOICES.
                This cannot be None if searchable is True.
            select_options (Optional[Union[Dict[str, str], List[str]]]): A dict or a list of select list options when
                filter_control is "select".  Supplying this argument will default filter_control to "select".
            strict_select (Optional[bool]): Ignored if select_options is not defined.  This apps an entire table
                attribute 'data-filter-strict-search' to the the filter_data context variable.
            sortable (bool) [True]
            sorter (Optional[str]) ["alphanum"]: Must be in SORTER_CHOICES.
            visible (bool) [True]: Controls whether a column is initially visible.
            header (Optional[str]) [auto]: The column header to display in the template.  Will be automatically
                generated using the title case conversion of the last (2, if present) dunderscore-delimited name values.
            many_related_sort_mld (Optional[Union[str, List[str]]]) [field]: All many_related columns will be delimited
                and sorted by their own value.  You can optionally sort based on any field under this parent model, and
                even via relations to that model, but there must be no many-to-many relations occurring in the field's
                path after that model.  Every field that should sort the same way must also have the same value for
                many_related_sort_mld.  E.g. if you have these 3 fields:
                    animal__infusate__tracer_links__tracer__name
                    animal__infusate__tracer_links__tracer__compound__id
                    animal__infusate__tracer_links__concentration
                and you want their delimited values to sort the same way, you can supply
                    many_related_sort_mld="animal__infusate__tracer_links"
                but note that both field paths MUST each start with that model, i.e. they must all start with:
                    "animal__infusate__tracer_links"
                The result will be if the user has sorted on the field/column for
                "animal__infusate__tracer_links__concentration", the other two columns will also be sorted by the values
                in that concentration field.
                This option is ignored if many_related is False.
            many_related_sort_fld (Optional[str]) [many_related_sort_mld + "__pk"]: The default sort field for many-
                related fields.
            many_related_sort_fwd (bool) [True]: Set to False to reverse sort by default.
        Exceptions:
            ValueError when:
            - Either many_related must be True or a converter must be supplied if the BST column name is not equal to
              the model field name.
            - The BST column name must differ from the model field name when either a converter is supplied or
              many_related is True.
            - filter_control value is not among FILTER_CONTROL_CHOICES.
            - sorter value is not among SORTER_CHOICES.
            - searchable and filter_control values are conflicting.
        Returns:
            instance (BootstrapTableColumn)
        """
        self.name = name

        self.field = (
            field
            if field is None or isinstance(field, list) or field != self.NAME_IS_FIELD
            else name
        )

        if self.field is not None:
            if isinstance(self.field, str) and name != self.field and converter is None and not many_related:
                raise ValueError(
                    f"Either many_related must be True or a converter must be supplied if the BST column name '{name}' "
                    f"is not equal to the model field name '{field}'."
                )
            elif (converter is not None or many_related) and isinstance(self.field, str) and name == field:
                raise ValueError(
                    f"The BST column name '{name}' must differ from the model field name '{field}' when either a "
                    "converter is supplied or many_related is True.\n"
                    "In the case of 'many_related', the name must differ in order to create an annotated field for "
                    "sorting, so as to prevent artificially increasing the number of rows in the resulting table due "
                    "to a left-join side-effect of sorting in the ORM."
                )
            elif (
                (converter is not None or many_related)
                and isinstance(self.field, str)
                and field == self.NAME_IS_FIELD
            ):
                raise ValueError(
                    f"The BST column field must be supplied and set to a different value from the name '{name}' when "
                    "either a converter is supplied or many_related is True.\n"
                    "In the case of 'many_related', the name must differ in order to create an annotated field for "
                    "sorting, so as to prevent artificially increasing the number of rows in the resulting table due "
                    "to a left-join side-effect of sorting in the ORM."
                )

        self.many_related = many_related
        self.many_related_model = many_related_model
        self.many_related_sort_fld = many_related_sort_fld
        self.many_related_sort_fwd = many_related_sort_fwd
        self.delim = many_related_delim
        if self.many_related_model is not None:
            if type(self.many_related_model) != type(self.field) or (
                isinstance(self.field, list) and len(self.many_related_model) != len(self.field)
            ):
                raise TypeError(
                    f"field and many_related_sort_mld must be the same type and size (if list type).  "
                    f"[{type(self.many_related_model).__name__} != {type(self.field).__name__}]"
                )
            if isinstance(self.field, list):
                for i, fld in enumerate(self.field):
                    sort_fld_mdl = self.many_related_model[i] + "__"
                    if not fld.startswith(sort_fld_mdl):
                        raise ValueError(
                            f"The field path of column '{self.name}': '{fld}' must start with many_related_sort_mld "
                            f"('{self.many_related_model[i]}') in order to link the sort of delimited values in "
                            "this column with those in other columns."
                        )
            else:
                sort_fld_mdl = self.many_related_model + "__"
                if not self.field.startswith(sort_fld_mdl):
                    raise ValueError(
                        f"The field path of column '{self.name}': '{self.field}' must start with many_related_sort_mld "
                        f"('{self.many_related_model}') in order to link the sort of delimited values in this "
                        "column with those in other columns."
                    )
        elif self.field is not None:
            # Default to sorting M:M field values by the primary key of the many_related_sort_mld model
            if isinstance(self.field, list):
                self.many_related_model = ["__".join(f.split("__")[0:-1]) for f in self.field]
            else:
                self.many_related_model = "__".join(self.field.split("__")[0:-1])
        if self.many_related and self.many_related_sort_fld is None:
            # Default to sorting M:M field values by the primary key of the many_related_sort_mld model
            if isinstance(self.many_related_model, list):
                self.many_related_sort_fld = [f"{f}__pk" for f in self.many_related_model]
            else:
                self.many_related_sort_fld = self.many_related_model + "__pk"
        elif self.many_related:
            if type(self.many_related_sort_fld) != type(self.field) or (
                isinstance(self.field, list) and len(self.many_related_sort_fld) != len(self.field)
            ):
                raise TypeError(
                    f"field and many_related_sort_fld must be the same type and size (if list type).  "
                    f"[{type(self.many_related_sort_fld).__name__} != {type(self.field).__name__}]"
                )
            if isinstance(self.field, list):
                for i, fld in enumerate(self.many_related_sort_fld):
                    sort_fld_mdl = self.many_related_model[i] + "__"
                    if not fld.startswith(sort_fld_mdl):
                        raise ValueError(
                            f"The default sort field path of column '{self.name}': '{fld}' must start with "
                            f"many_related_model ('{self.many_related_model[i]}') in order to link the sort of "
                            "delimited values in this column with those in other columns."
                        )
            else:
                sort_fld_mdl = self.many_related_model + "__"
                if not self.field.startswith(sort_fld_mdl):
                    raise ValueError(
                        f"The default sort field path of column '{self.name}': '{self.many_related_sort_fld}' must "
                        f"start with many_related_model ('{self.many_related_model}') in order to link the sort "
                        "of delimited values in this column with those in other columns."
                    )

        self.converter = converter
        self.is_annotation = converter is not None or isinstance(self.field, list) or many_related
        self.exported = exported

        if header is None:
            # Include the last foreign key name, if present
            last_2_names = "_".join(name.split("__")[-2:])
            self.header = underscored_to_title(last_2_names)
        else:
            self.header = header

        if select_options is None:
            default_filter_control = self.FILTER_CONTROL_CHOICES["INPUT"]
            self.select_options = None
        else:
            default_filter_control = self.FILTER_CONTROL_CHOICES["SELECT"]
            if isinstance(select_options, dict):
                self.select_options = select_options
            else:
                self.select_options = dict((opt, opt) for opt in select_options)
        self.strict_select = strict_select

        if filter_control is None:
            if searchable is None or searchable is True:
                # Full default settings
                self.filter_control = default_filter_control
                self.searchable = True
            else:
                self.searchable = False
                self.filter_control = self.FILTER_CONTROL_CHOICES["DISABLED"]
        elif filter_control in self.FILTER_CONTROL_CHOICES.values():
            self.filter_control = filter_control if filter_control is not None and filter_control != "" else ""
            tmp_searchable = filter_control is not None and filter_control != ""
            if searchable is None:
                self.searchable = tmp_searchable
            elif searchable != tmp_searchable:
                raise ValueError(
                    f"Conflict between searchable '{searchable}' and filter_control '{filter_control}'.  "
                    "searchable must be False if filter_control is not None."
                )
            else:
                self.searchable = searchable
            self.filter_control = filter_control if filter_control is not None and filter_control != "" else ""
        else:
            raise ValueError(
                f"Invalid filter_control value: '{filter_control}'.  "
                f"Valid choices are: {list(self.FILTER_CONTROL_CHOICES.values())}."
            )

        self.sortable = "true" if sortable else "false"

        self.sorter = sorter
        if sorter is None:
            self.sorter = self.SORTER_CHOICES["ALPHANUMERIC"]
        elif sorter in self.SORTER_CHOICES.values():
            self.sorter = sorter
        else:
            raise ValueError(
                f"Invalid sorter value: '{sorter}'.  "
                f"Valid choices are: {list(self.SORTER_CHOICES.values())}."
            )

        self.visible = "true" if visible else "false"
        self.filter = ""
        self.widget = BSTHeader()

    def __str__(self):
        return self.as_widget()

    def as_widget(self, attrs=None):
        return self.widget.render(
            self.name,
            self,
            attrs=attrs,
        )

    @classmethod
    def field_to_related_model(cls, field: str):
        """Turns a django field path into a related model path, e.g. mz_to_msrunsamples__sample__animal__studies__id ->
        mz_to_msrunsamples__sample__animal__studies"""
        mdl_path = field.split("__")
        if len(mdl_path) <= 1:
            return None
        return "__".join(mdl_path[0:-1])


class BootstrapTableListView(ListView):
    """Generic class-based view for a Model record list to make pages load faster, using server-side behavior for
    pagination."""

    # 0 = "ALL"
    PER_PAGE_CHOICES = [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0]

    paginator_class = GracefulPaginator
    paginate_by = 15

    export_header_template = "DataRepo/downloads/export_metadata_header.txt"
    HEADER_TIME_FORMAT = "%d/%m/%Y %H:%M:%S"
    FILENAME_TIME_FORMAT = "%d.%m.%Y.%H.%M.%S"

    # Must be set in derived class
    columns: List[BootstrapTableColumn] = []

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

    def __init__(self, *args, **kwargs):
        """An override of the superclass constructor intended to initialize custom instance attributes."""

        super().__init__(*args, **kwargs)

        # Check that the columns class attribute is valid
        if len(self.columns) == 0 or not isinstance(self.columns[0], BootstrapTableColumn):
            raise TypeError(
                "Invalid columns class attribute.  Must be a list of at least 1 BootstrapTableColumn "
                "object."
            )

        self.headtmplt = loader.get_template(
            self.export_header_template
        ) if self.export_header_template is not None else None

        now = datetime.now()
        self.date_str = now.strftime(self.HEADER_TIME_FORMAT)
        self.filename_date_str = now.strftime(self.FILENAME_TIME_FORMAT)

        self.total = 0
        self.raw_total = 0
        self.warnings = []
        self.cookie_resets = []

    def get_queryset(self):
        """An override of the superclass method intended to only set total and raw_total instance attributes."""

        qs = super().get_queryset()
        self.total = qs.count()
        self.raw_total = self.total
        return self.get_paginated_queryset(qs)

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

    def get_column_cookie_name(self, column: Union[BootstrapTableColumn, str], name: str) -> str:
        """Retrieves a cookie name using a prepended view name.

        Args:
            column (Union[BootstrapTableColumn, str]): The name of the BST column or the column object
            name (str): The name of the cookie variable specific to the column
        Exceptions:
            None
        Returns:
            (str)
        """
        if isinstance(column, str):
            return f"{self.cookie_prefix}{name}-{column}"
        return f"{self.cookie_prefix}{name}-{column.name}"

    def get_column_cookie(self, column: BootstrapTableColumn, name: str, default: str = "") -> str:
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

    def get_paginated_queryset(self, qs: QuerySet):
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
        order_dir: Optional[str] = self.get_cookie("order-dir", "asc")

        # We need the column names (from the BST data-field attributes) to use in Q expressions
        filter_columns = []
        search_fields = []
        model_paths = []
        column: BootstrapTableColumn
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
                        # Do not search an annotation that is a Coalesce, because it's REALLY slow
                        or isinstance(column.converter, Coalesce)
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
                        or_q_exp |= Q(**{f"{many_related_search_field}__icontains": filter_value})
                    q_exp &= or_q_exp
                elif column.field is not None:
                    search_fields.append(search_field)
                    q_exp &= Q(**{f"{search_field}__icontains": filter_value})
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
        qs = qs.prefetch_related(*prefetches)
        # print(f"PREFETCHES: {prefetches} MODEL PATHS: {model_paths}")

        # 3. Add annotations (which can be used in search & sort)

        annotations_before_filter = {}
        annotations_after_filter = {}
        for column in self.columns:
            try:
                # If a converter exists, the column is an annotation column, so annotate it
                if column.converter is not None:
                    if isinstance(column.converter, Coalesce):
                        if search_term != "" or column.name in filter_columns:
                            print(
                                f"WARNING: Excluding annotation {column.name} from search/filter because it has a "
                                "Coalesce converter, which is *really* inefficient/slow.  Searching the field instead."
                            )
                        annotations_after_filter[column.name] = column.converter
                    elif column.field is not None:
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
                            and not order_dir.lower().startswith("d")
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
                        # This assumes column.field is not None
                        if order_by == "" or (
                            order_by == column.name
                            and not order_dir.lower().startswith("d")
                        ):
                            # Apply Min to prevent changing the number of resulting rows
                            annotations_before_filter[column.name] = Min(column.field)
                        else:
                            # Apply Max to prevent changing the number of resulting rows
                            annotations_before_filter[column.name] = Max(column.field)
                    elif column.field is not None:
                        # This is in case a user-supplied custom converter failed in the try block above and the field
                        # is not many_related and there are not multiple other model fields linking to the reference
                        # model
                        annotations_before_filter[column.name] = F(column.field)

        if len(annotations_before_filter.keys()) > 0:
            qs = qs.annotate(**annotations_before_filter)

        # 4. Apply the search and filters

        if len(q_exp.children) > 0:
            try:
                qs = qs.filter(q_exp)
            except FieldError as fe:
                fld_str = "\n\t".join(search_fields)
                fld_msg = f"One or more of {len(search_fields)} fields is misconfigured.  Example:\n\t{fld_str}."
                warning = (
                    f"Your search could not be executed.  {fld_msg}\n\n\tPlease report this error to the site "
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
            qs = qs.annotate(**annotations_after_filter)

        # 6. Apply the sort

        # Sort the results, if sort has a value
        if order_by != "":
            # We don't want to string-sort fields like those of type datetime.  We want to date-sort them, so change the
            # order_by to the actual field.  But we also don't want to sort on many-related fields, because that would
            # increase the number of rows in the result artificially, so we only do this for individual fields.  Many-
            # related fields will sort by their annotated value.
            annotated_columns: List[BootstrapTableColumn] = list(
                c
                for c in cast(Iterable[BootstrapTableColumn], self.columns)
                if c.is_annotation and c.name == order_by and isinstance(c.field, str)
            )
            if len(annotated_columns) == 1:
                order_by = annotated_columns[0].field

            # Invert the order_by if descending
            if order_dir != "" and order_dir.lower().startswith("d"):
                # order_dir = "asc" or "desc"
                order_by = f"-{order_by}"

            qs = qs.order_by(order_by)

        # 7. Ensure distinct results (because annotations and/or sorting can cause the equivalent of a left join).

        qs = qs.distinct()

        # 8. Update the count

        # Set the total after the search
        self.total = qs.count()

        # TODO: Add a check for when limit is 0 and total > raw_total.  Should raise an exception because the developer added a many-related column and did not make it 1:1 with the base model

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
        """This sets up django-compatible pagination, search, and sort"""

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
        context["warnings"] = self.warnings

        # 3. Set the BST column attribute context values to use in the th tag attributes

        context["not_exported"] = []
        context["filter_select_lists"] = {}

        column: BootstrapTableColumn
        for column in self.columns:
            # Put the column object in the context.  It will render the th tag.  Update the filter and visibility first.
            column.visible = self.get_column_cookie(column, "visible", column.visible)
            column.filter = self.get_column_cookie(column, "filter", column.filter)
            context[column.name] = column

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
            print(f"EXPORTING {export_type}")
            context["export_data"], context["export_filename"] = self.get_export_data(export_type)
            context["export_type"] = export_type if export_type == "excel" else "text"

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
        column: BootstrapTableColumn
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
                        q_exp |= Q(**{f"{many_related_search_field}__icontains": term})
                elif column.field is not None:
                    search_fields.append(search_field)
                    q_exp |= Q(**{f"{search_field}__icontains": term})
                elif column.field is None:
                    raise ValueError(f"Column {column.name} must not be searchable if field is None.")

        return q_exp, search_fields

    def get_export_data(self, format: str):
        """This does all the processing of the submission after set_files() has been called."""

        if format == "excel":
            ext = "xlsx"
            byte_stream = BytesIO()
            self.get_excel_streamer(byte_stream)
            export_data = base64.b64encode(byte_stream.read()).decode("utf-8")
        else:
            buffer = StringIO()
            if format == "csv":
                ext = "csv"
                self.get_text_streamer(buffer, delim=",")
                export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
                # export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
            elif format == "txt":
                ext = "tsv"
                self.get_text_streamer(buffer, delim="\t")
                export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
                # export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
            else:
                raise NotImplementedError(f"Export format {format} not supported.")

        return export_data, f"{self.model.__name__}.{self.filename_date_str}.{ext}"

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

        sheet = self.verbose_model_name_plural
        columns = self.row_headers()

        print(f"SHEET {sheet}")
        # Build the dict by iterating over the row lists
        qs_dict_by_index = dict((i, []) for i in range(len(columns)))
        for r, row in enumerate(self.rows_iterator()):
            print(f"ROW {r}")
            for i, val in enumerate(row):
                print(f"COL {i}")
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

        xlsx_writer.close()
        # Rewind the buffer so that when it is read(), you won't get an error about opening a zero-length file in Excel
        byte_stream.seek(0)

        return byte_stream

    def get_text_streamer(self, buffer: StringIO, delim: str = "\t"):
        writer: "_csv._writer" = csv.writer(buffer, delimiter=delim)

        search_term: Optional[str] = self.get_cookie("search")
        order_by: Optional[str] = self.get_cookie("order-by")
        order_dir: Optional[str] = self.get_cookie("order-dir", "asc")
        if order_by is not None and order_by != "":
            sort_str = f"{order_by}, {order_dir}"
        else:
            sort_str = "default"
        filters = []
        column: BootstrapTableColumn
        for column in self.columns:
            filter_value: str = self.get_column_cookie(column, "filter")
            if filter_value is not None and filter_value != "":
                filters.append({"column": column.header, "filter": filter_value})

        # Commented metadata header containing download date and info
        self.headtmplt.render(
            {
                "name": self.verbose_model_name_plural,
                "date": self.date_str,
                "total": self.total,
                "search": search_term,
                "filters": filters,
                "sort": sort_str,
            }
        )

        for row in self.rows_iterator():
            writer.writerow([str(c) for c in row])

        return buffer

    def rows_iterator(self):
        """Takes a queryset of records and returns a list of lists of column data.  Note that delimited many-related
        values are converted to strings, but everything else in the returned list of lists is the original type.

        Args:
            None
        Exceptions:
            None
        Returns:
            (List[list])
        """
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

    def get_rec_val(self, rec: Model, col: BootstrapTableColumn):
        """Given a model record, i.e. row-data, e.g. from a queryset, and a column, return the column value."""
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

    def get_many_related_rec_val(self, rec: Model, col: BootstrapTableColumn):
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

    def _get_many_related_rec_val_helper(self, rec: Model, field: str, sort_field: str, reverse=False):
        # reverse = not col.many_related_sort_fwd
        # print(f"_get_many_related_rec_val_helper CALLED WITH FIELD '{field}' ON A {type(rec).__name__} REC AND SORT FIELD '{sort_field}'")
        vals_list = self._get_rec_val_helper(rec, field.split("__"), sort_field_path=sort_field.split("__"))
        if vals_list is None:
            val = []
            # print(f"GOT1 {vals_list}")
        elif isinstance(vals_list, list):
            try:
                # Sorting with (t[1] is None, t[1]) is to sort None values to the end
                # THIS UNIQUE STRATEGY IS PROBLEMATIC.  E.G. 2 TRACERS WITH THE SAME CONC WILL SHOW ONLY 1 CONC WHERE THERE ARE 2 DISTINCT RECORDS, SO THIS STRATEGY WAS REPLACED WITH THE USAGE OF DISTINCT IN _get_rec_val_helper FOR [Many]RelatedManager OBJECTS
                # uniq_vals = reduce(lambda lst, val: lst + [val] if val not in lst else lst, vals_list, [])
                # val = delim.join([str(val[0]) for val in sorted([tpl for tpl in uniq_vals if tpl is not None], key=lambda t: (t[1] is None, t[1]), reverse=reverse)])
                # print(f"GOT2 '{val}' VALSLIST {vals_list} UNIQUE '{uniq_vals}'")
                val = [val[0] for val in sorted([tpl for tpl in vals_list if tpl is not None], key=lambda t: (t[1] is None, t[1]), reverse=reverse)]
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

    def _get_rec_val_helper(self, rec: Model, field_path: List[str], sort_field_path: Optional[List[str]] = None, _sort_val=None):
        if len(field_path) == 0 or rec is None:
            # print(f"field_path {field_path} cannot be an empty list and rec '{rec}' cannot be None.")
            return None, None
            # raise ValueError(f"field_path {field_path} cannot be an empty list and rec '{rec}' cannot be None.")
        elif type(rec).__name__ != "RelatedManager" and type(rec).__name__ != "ManyRelatedManager":
            val_or_rec = getattr(rec, field_path[0])
        else:
            # print(f"SETTING field_or_rec to a {type(rec).__name__} WHEN LOOKING FOR {attr_path[0]}")
            val_or_rec = rec

        next_sort_field_path = sort_field_path[1:] if sort_field_path is not None else None
        if sort_field_path is not None and (sort_field_path[0] != field_path[0] or (len(sort_field_path) == len(field_path) and len(field_path) == 1)):
            # print(f"GETTING SORT VAL {sort_field_path} FOR {field_path} FROM {rec}")
            sort_val, _, _ = self._get_rec_val_helper(rec, sort_field_path)
            if isinstance(sort_val, list):
                uniq_vals = reduce(lambda lst, val: lst + [val] if val not in lst else lst, sort_val, [])
                if len(uniq_vals) > 1:
                    raise ValueError("Multiple values returned")
                elif len(uniq_vals) == 1:
                    sort_val = uniq_vals[0]
                else:
                    sort_val = None
            next_sort_field_path = None
            _sort_val = sort_val

        if len(field_path) == 1:
            pk = 1
            # print(f"REC: {rec} GETTING: {attr_path[0]} GOT: {field_or_rec} TYPE REC: {type(rec).__name__} TYPE GOTTEN: {type(field_or_rec).__name__}")
            if type(val_or_rec).__name__ == "RelatedManager":
                # THIS SHOULD NO LONGER BE A RESTRICTION?? GIVEN THE NEW STRATEGY OF SETTING A COMMON MODEL AND A DEFAULT SORT FIELD... Not sure. This doesn't return a list of tuples
                # if sort_field_path is not None and field_path != sort_field_path:
                #     raise NotImplementedError(
                #         f"Support for sorting record objects (e.g. {type(rec).__name__} records) by a different field "
                #         f"'{sort_field_path}' is not supported.  The field path must end in a non-key field."
                #     )
                # print(f"RETURNING ALL {field_or_rec.count()}")
                # return reduce(lambda lst, rec: lst + [rec] if rec not in lst else lst, field_or_rec.all(), [])
                return list((r, _sort_val, r.pk) for r in val_or_rec.distinct())
            elif type(val_or_rec).__name__ == "ManyRelatedManager":
                # THIS SHOULD NO LONGER BE A RESTRICTION?? GIVEN THE NEW STRATEGY OF SETTING A COMMON MODEL AND A DEFAULT SORT FIELD... Not sure. This doesn't return a list of tuples
                # if sort_field_path is not None and field_path != sort_field_path:
                #     raise NotImplementedError(
                #         f"Support for sorting record objects (e.g. {type(rec).__name__} records) by a different field "
                #         f"'{sort_field_path}' is not supported.  The field path must end in a non-key field."
                #     )
                # print(f"RETURNING ALL {field_or_rec.through.count()}")
                # return reduce(lambda lst, rec: lst + [rec] if rec not in lst else lst, field_or_rec.through.all(), [])
                return list((r, _sort_val, r.pk) for r in val_or_rec.through.distinct())
            elif isinstance(val_or_rec, Model):
                # We add the primary key to the tuple so that the python reduce that happens upstream of this leaf in the recursion ensures that we get a unique set of many-related records.  I had tried using .distinct(), but the performance really suffered.  Handling it with reduce is MUCH MUCH faster.
                pk = val_or_rec.pk

            # print(f"RETURNING ONE {type(val_or_rec).__name__}: {val_or_rec} WITH SORT VAL: {_sort_val}")
            return val_or_rec, _sort_val, pk

        if type(val_or_rec).__name__ == "RelatedManager":
            if val_or_rec.count() > 0:
                # return list(
                #     self._get_rec_val_helper(rel_rec, attr_path[1:])
                #     for rel_rec
                #     in reduce(
                #         lambda lst, rec: lst + [rec] if rec not in lst else lst,
                #         field_or_rec.all(),
                #         [],
                #     )
                # )
                possibly_nested_list = list(self._get_rec_val_helper(rel_rec, field_path[1:], sort_field_path=next_sort_field_path, _sort_val=_sort_val) for rel_rec in val_or_rec.all())
                if len(possibly_nested_list) == 0 or not isinstance(possibly_nested_list[0], list):
                    return possibly_nested_list
                # TODO: THIS IS A PROBLEM! IT WILL RETURN A SET OF UNIQUE TUPLES (THE COLUMN'S VALUE AND THE SORT VALUE) BUT THE COLUMN MAY BE LINKED TO ANOTHER COLUMN THAT IS THE ONLY THING THAT MAKES IT UNIQUE, SO THE NUMBER OF VALUES IN NEIGHBORING COLUMNS WILL NOT LINE UP!  WHAT I REALLY NEED IS .distinct() CALLED ON THE FOREIGN KEY OBJECTS!
                lst = list(item for sublist in possibly_nested_list for item in sublist)
                uniq_vals = reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, lst, [])
                # print(f"RETURNING 1 {type(uniq_vals).__name__}.{type(uniq_vals[0]).__name__}: {uniq_vals}")
                return uniq_vals
                # return list(item for sublist in possibly_nested_list for item in sublist)
            # TODO: Need to check if this return type is OK due to it not being a tuple or list. Should only happen when
            # expecting a list
            return None
        elif type(val_or_rec).__name__ == "ManyRelatedManager":
            if val_or_rec.count() > 0:
                # return list(
                #     self._get_rec_val_helper(rel_rec, attr_path[1:])
                #     for rel_rec
                #     in reduce(
                #         lambda lst, rec: lst + [rec] if rec not in lst else lst,
                #         field_or_rec.through.all(),
                #         [],
                #     )
                # )
                possibly_nested_list = list(self._get_rec_val_helper(rel_rec, field_path[1:], sort_field_path=next_sort_field_path, _sort_val=_sort_val) for rel_rec in val_or_rec.all())
                if len(possibly_nested_list) == 0 or not isinstance(possibly_nested_list[0], list):
                    return possibly_nested_list
                # TODO: THIS IS A PROBLEM! IT WILL RETURN A SET OF UNIQUE TUPLES (THE COLUMN'S VALUE AND THE SORT VALUE) BUT THE COLUMN MAY BE LINKED TO ANOTHER COLUMN THAT IS THE ONLY THING THAT MAKES IT UNIQUE, SO THE NUMBER OF VALUES IN NEIGHBORING COLUMNS WILL NOT LINE UP!  WHAT I REALLY NEED IS .distinct() CALLED ON THE FOREIGN KEY OBJECTS!
                lst = list(item for sublist in possibly_nested_list for item in sublist)
                uniq_vals = reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, lst, [])
                # print(f"RETURNING 12 {type(uniq_vals).__name__}.{type(uniq_vals[0]).__name__}: {uniq_vals}")
                return uniq_vals
                # return list(item for sublist in possibly_nested_list for item in sublist)
            # TODO: Need to check if this return type is OK due to it not being a tuple or list. Should only happen when
            # expecting a list
            return None
        return self._get_rec_val_helper(val_or_rec, field_path[1:], sort_field_path=next_sort_field_path, _sort_val=_sort_val)
