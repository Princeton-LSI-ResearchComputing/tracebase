from __future__ import annotations
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union, cast

from django.core.exceptions import FieldError
from django.core.paginator import PageNotAnInteger, EmptyPage, Paginator
from django.db.models import F, Max, Min, Q, QuerySet
from django.db.models.functions import Coalesce
from django.utils.functional import classproperty
from django.views.generic import ListView

from DataRepo.models import ArchiveFile
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
    SORTER_CHOICES = [
        "alphanum",  # default
        "numericOnly",
        "htmlSorter",  # See static/js/htmlSorter.js
    ]
    NAME_IS_FIELD = "__same__"

    def __init__(
        self,
        name: str,
        field: Optional[Union[str, List[str]]] = NAME_IS_FIELD,
        converter: Optional[Callable] = None,
        many_related: bool = False,
        exported: bool = True,
        searchable: Optional[bool] = None,  # Default set in-line
        filter_control: Optional[str] = None,  # Default set in-line
        select_options: Optional[Union[Dict[str, str], List[str]]] = None,
        strict_select: Optional[bool] = None,
        sortable: bool = True,
        sorter: Optional[str] = None,
        visible: bool = True,
        header: str = None,
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
                Example for AnimalList:
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
            elif (converter is not None or many_related) and isinstance(self.field, str) and name == self.field:
                raise ValueError(
                    f"The BST column name '{name}' must differ from the model field name '{field}' when either a "
                    "converter is supplied or many_related is True.\n"
                    "In the case of 'many_related', the name must differ in order to create an annotated field for "
                    "sorting, so as to prevent artificially increasing the number of rows in the resulting table due "
                    "to a left-join side-effect of sorting in the ORM."
                )

        self.converter = converter
        self.many_related = many_related
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
            sorter = self.SORTER_CHOICES[0]
        elif sorter in self.SORTER_CHOICES:
            self.sorter = sorter
        else:
            raise ValueError(f"Invalid sorter value: '{sorter}'.  Valid choices are: {self.SORTER_CHOICES}.")

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


class BootstrapTableListView(ListView):
    """Generic class-based view for a Model record list to make pages load faster, using server-side behavior for
    pagination."""

    # 0 = "ALL"
    PER_PAGE_CHOICES = [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0]

    paginator_class = GracefulPaginator
    paginate_by = 15

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
        return get_cookie(self.request, self.get_cookie_name(name), default) or ""

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
        return get_cookie(self.request, self.get_column_cookie_name(column, name), default) or ""

    def get_paginated_queryset(self, qs: QuerySet[ArchiveFile]):
        """The superclass handles actual pagination, but the number of pages can be affected by applied filters, and the
        Content of those pages can be affected by sorting.  And both can be affected by annotated fields.  This method
        applies those criteria based on cookies.

        Limitations:
            1. Searching imported_timestamp is not supported if the database is not postgres
            2. Sorting based on study will only be based on the first or last related study, not the combined string of
               all related study names.
        Args:
            qs (QuerySet[ArchiveFile])
        Exceptions:
            None
        Returns:
            qs (QuerySet[ArchiveFile])
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
        column: BootstrapTableColumn
        for column in self.columns:
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
                if isinstance(search_field, list):
                    or_q_exp = Q()
                    for many_related_search_field in column.field:
                        search_fields.append(many_related_search_field)
                        or_q_exp |= Q(**{f"{many_related_search_field}__icontains": filter_value})
                    q_exp &= or_q_exp
                else:
                    search_fields.append(search_field)
                    q_exp &= Q(**{f"{search_field}__icontains": filter_value})

        # Add a global search if one is defined
        if search_term != "":
            global_q_exp, all_search_fields = self.get_any_field_query(search_term)
            q_exp &= global_q_exp
            search_fields = all_search_fields

        # 2. Add annotations (which can be used in search & sort)

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
                    else:
                        annotations_before_filter[column.name] = column.converter
            except Exception as e:
                # The fallback is to have the template render the database values in the default manner.  Searching will
                # disabled.  Sorting will be a string sort (which is not ideal, e.g. if the value is a datetime).
                column.searchable = False
                print(
                    f"ERROR: {type(e).__name__}: {e}\nConverter for column '{column.name}' failed.  Falling back to "
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
                        if order_by == "" or (
                            order_by == column.name
                            and not order_dir.lower().startswith("d")
                        ):
                            # Apply Min to prevent changing the number of resulting rows
                            annotations_before_filter[column.name] = Min(column.field)
                        else:
                            # Apply Max to prevent changing the number of resulting rows
                            annotations_before_filter[column.name] = Max(column.field)
                    else:
                        # This is in case a user-supplied custom converter failed in the try block above and the field
                        # is not many_related and there are not multiple other model fields linking to the reference
                        # model
                        annotations_before_filter[column.name] = F(column.field)

        if len(annotations_before_filter.keys()) > 0:
            qs = qs.annotate(**annotations_before_filter)

        # 3. Apply the search and filters

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

        # 4. Apply coalesce annotations AFTER the filter, due to the inefficiency of WHERE clauses interacting with
        # COALESCE

        if len(annotations_after_filter.keys()) > 0:
            qs = qs.annotate(**annotations_after_filter)

        # 4. Apply the sort

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

        # 4. Ensure distinct results (because annotations and/or sorting can cause the equivalent of a left join).

        qs = qs.distinct()

        # 5. Update the count

        # Set the total after the search
        self.total = qs.count()

        # NOTE: Pagination is controlled by the superclass and the override of the get_paginate_by method
        return qs

    def get_paginate_by(self, queryset):
        """An override of the superclass method to allow the user to change the rows per page."""

        limit = self.request.GET.get("limit", "")
        if limit == "":
            cookie_limit = self.get_cookie("limit")
            if cookie_limit != "":
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
                else:
                    search_fields.append(search_field)
                    q_exp |= Q(**{f"{search_field}__icontains": term})

        return q_exp, search_fields
