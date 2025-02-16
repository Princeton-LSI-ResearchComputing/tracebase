from abc import ABC, abstractmethod
from typing import Callable, Dict, Iterable, List, Optional, Union, cast
from django.db.models import F, Max, Min, Q, QuerySet
from django.db.models.functions import Coalesce
from django.views.generic import DetailView, ListView

from DataRepo.models import ArchiveFile
from DataRepo.utils.text_utils import camel_to_title
from DataRepo.views.utils import get_cookie


class BootstrapTableColumn:
    """Class to represent the interface between a bootstrap column and a Model field.

    Attributes:
        name
        field
        converter
        exported
        filter_control
        sortable
        sorter
        visible
        searchable
    """

    FILTER_CONTROL_CHOICES = [
        "input",  # default
        "select",
        "datepicker",
        "",  # disabled
    ]
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
        filter_control: Optional[str] = FILTER_CONTROL_CHOICES[0],
        sortable: bool = True,
        sorter: Optional[str] = None,
        visible: bool = True,
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
            filter_control (Optional[str]) ["input"]: Set to None or "" to disable.  Must be in FILTER_CONTROL_CHOICES.
            sortable (bool) [True]
            sorter (Optional[str]) ["alphanum"]: Must be in SORTER_CHOICES.
            visible (bool) [True]: Controls whether a column is initially visible.
        Exceptions:
            ValueError when:
            - Either many_related must be True or a converter must be supplied if the BST column name is not equal to
              the model field name.
            - The BST column name must differ from the model field name when either a converter is supplied or
              many_related is True.
            - filter_control value is not among FILTER_CONTROL_CHOICES.
            - sorter value is not among SORTER_CHOICES.
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

        if filter_control is None or filter_control in self.FILTER_CONTROL_CHOICES:
            self.searchable = filter_control is not None and filter_control != ""
            self.filter_control = filter_control if filter_control is not None and filter_control != "" else ""
        else:
            raise ValueError(
                f"Invalid filter_control value: '{filter_control}'.  "
                f"Valid choices are: {self.FILTER_CONTROL_CHOICES}."
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

class BootstrapTableListView(ListView, ABC):
    """Generic class-based view for a Model record list to make pages load faster, using server-side behavior for
    pagination."""

    # 0 = "ALL"
    PER_PAGE_CHOICES = [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0]

    paginate_by = 15

    @property
    @abstractmethod
    def columns(self):
        """List[BootstrapTableColumn]"""
        pass

    @property
    def view_name(self):
        return f"{self.model.__name__}List"

    @property
    def cookie_prefix(self):
        return f"{self.view_name}-"

    @property
    def verbose_name(self):
        try:
            return f"{self.model._meta.__dict__['verbose_name_plural']} List"
        except:
            print(f"WARNING: Model {self.model.__name__} has no Meta.verbose_name_plural.")
            return f"{camel_to_title(self.model.__name__)}s List"

    def __init__(self, *args, **kwargs):
        """An override of the superclass constructor intended to initialize custom instance attributes."""

        super().__init__(*args, **kwargs)

        # Check that the columns class attribute is valid
        if not isinstance(self.columns, list) or len(self.columns) == 0 or len(
            [inv for inv in self.columns if not isinstance(inv, BootstrapTableColumn)]
        ):
            raise TypeError(
                "Invalid columns class attribute.  Must be a list of at least 1 BootstrapTableColumn "
                "object."
            )

        self.total = 0
        self.raw_total = 0

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

    def get_column_cookie_name(self, column: BootstrapTableColumn, name: str) -> str:
        """Retrieves a cookie name using a prepended view name.

        Args:
            column (str): The name of the BST column
            name (str): The name of the cookie variable specific to the column
        Exceptions:
            None
        Returns:
            (str)
        """
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
        column: BootstrapTableColumn
        for column in self.columns:
            filter_value: str = self.get_column_cookie(column, "filter")
            if column.searchable and filter_value != "":
                search_field = column.field if column.field is not None else column.name
                if isinstance(search_field, list):
                    or_q_exp = Q()
                    for many_related_field in column.field:
                        or_q_exp |= Q(**{f"{many_related_field}__icontains": filter_value})
                    q_exp &= or_q_exp
                else:
                    q_exp &= Q(**{f"{search_field}__icontains": filter_value})

        # Add a global search if one is defined
        if search_term != "":
            q_exp &= self.get_any_field_query(search_term)

        # 2. Add annotations (which can be used in search & sort)

        annotations = {}
        for column in self.columns:
            try:
                # If a converter exists, the column is an annotation column, so annotate it
                if column.converter is not None:
                    annotations[column.name] = column.converter
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
                if column.name not in annotations.keys() and column.is_annotation:
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
                                annotations[column.name] = Coalesce(*[Min(f) for f in column.field])
                            else:
                                annotations[column.name] = Coalesce(*column.field)
                        else:
                            # Get the maximum value of the first populated many-related field
                            if column.many_related:
                                # Apply Max to prevent changing the number of resulting rows
                                annotations[column.name] = Coalesce(*[Max(f) for f in column.field])
                            else:
                                annotations[column.name] = Coalesce(*column.field)
                    elif column.many_related:
                        if order_by == "" or (
                            order_by == column.name
                            and not order_dir.lower().startswith("d")
                        ):
                            # Apply Min to prevent changing the number of resulting rows
                            annotations[column.name] = Min(column.field)
                        else:
                            # Apply Max to prevent changing the number of resulting rows
                            annotations[column.name] = Max(column.field)
                    else:
                        # This is in case a user-supplied custom converter failed in the try block above and the field
                        # is not many_related and there are not multiple other model fields linking to the reference
                        # model
                        annotations[column.name] = F(column.field)

        if len(annotations.keys()) > 0:
            print("ANNOTATING START")
            qs = qs.annotate(**annotations)
            print(f"ANNOTATING DONE {annotations}")

        # 3. Apply the search and filters

        if len(q_exp.children) > 0:
            print("FILTERING START")
            qs = qs.filter(q_exp)
            print(f"FILTERING DONE {q_exp}")

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

            print(f"ORDER_BY {order_by}")

        # 4. Ensure distinct results (because annotations and/or sorting can cause the equivalent of a left join).

        print("DISTINCTING START")
        qs = qs.distinct()
        print("DISTINCTING DONE")

        # 5. Update the count

        # Set the total after the search
        self.total = qs.count()

        print("PAGINATED QS DONE")

        # NOTE: Pagination is controlled by the superclass and the override of the get_paginate_by method
        return qs

    def get_paginate_by(self, queryset):
        """An override of the superclass method to allow the user to change the rows per page."""

        print("PAGINATE_BY START")

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

        print("PAGINATE_BY DONE")

        return limit

    def get_context_data(self, **kwargs):
        """This sets up django-compatible pagination, search, and sort"""

        print("CONTEXT START")

        context = super().get_context_data(**kwargs)

        # 1. Set context variables for initial defaults based on user-selections saved in cookies

        # Set search/sort context variables
        context["search_term"] = self.get_cookie("search")
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

        # 3. Set the BST column attribute context values to use in the th tag attributes

        context["not_exported"] = []

        column: BootstrapTableColumn
        for column in self.columns:
            context[column.name] = {
                "name": column.name,
                "filter": self.get_column_cookie(column, "filter"),
                "filter_control": column.filter_control,
                "sortable": column.sortable,
                "sorter": column.sorter,
                "visible": self.get_column_cookie(column, "visible", column.visible),
                "searchable": column.searchable,
            }
            if not column.exported:
                context["not_exported"].append(column.name)

        print("CONTEXT DONE")

        return context

    def get_any_field_query(self, term: str):
        """Given a string search term, returns a Q expression that does a case-insensitive search of all fields from
        the table displayed in the template.  Note, annotation fields must be generated in order to apply the query.

        Args:
            term (str): search term applied to all columns of the view
        Exceptions:
            None
        Returns:
            q_exp (Q): A Q expression that can be used in a django ORM filter
        """

        q_exp = Q()

        if term == "":
            return q_exp

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
                    for many_related_field in column.field:
                        q_exp |= Q(**{f"{many_related_field}__icontains": term})
                else:
                    q_exp |= Q(**{f"{search_field}__icontains": term})

        print(f"Q EXP: {q_exp}")

        return q_exp
