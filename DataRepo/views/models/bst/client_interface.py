from typing import Dict, List, Optional, Union
from warnings import warn

from django.conf import settings
from django.db.models import QuerySet
from django.utils.functional import classproperty
from django.views.generic import ListView

from DataRepo.models.utilities import (
    model_title,
    model_title_plural,
    select_representative_field,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.utils import GracefulPaginator, get_cookie, get_cookie_dict


class BSTClientInterface(ListView):
    """This is a server-side interface to the Bootstrap Table javascript and cookies in the client's browser.  It
    encapsulates the javascript and cookie structure/functions and is tightly integrated with BSTListView (which is
    intended to inherit from this class).  This is not intended to be used on its own.

    Class Attributes:
        Templates:
            template_name (str) ["models/bst/list_view.html"]: The template used to render the Bootstrap Table.
            scripts (List[str]) ["DataRepo/static/js/bst/cookies.js", "DataRepo/static/js/bst/list_view.js"]
        Pagination:
            PER_PAGE_CHOICES (List[int]) [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0]: The rows per page select list
                will be populated by these increments (up to the number of rows among the results).  A value of 0 means
                "ALL" rows.
            paginator_class (Paginator) [GracefulPaginator]: The paginator class set for the ListView (super) class.
            paginate_by (int) [15]: The default number of rows per page.
        Context variable names:
            search_cookie_name (str) ["search"]
            filter_cookie_name (str) ["filter"]
            visible_cookie_name (str) ["visible"]
            sortcol_cookie_name (str) ["sortcol"]
            asc_cookie_name (str) ["asc"]
            limit_cookie_name (str) ["limit"]
            cookie_prefix_var_name (str) ["cookie_prefix"]
            cookie_resets_var_name (str) ["cookie_resets"]
            clear_cookies_var_name (str) ["clear_cookies"]
            model_var_name (str) ["model"]
            table_id_var_name (str) ["table_id"]
            title_var_name (str) ["table_name"]
            columns_var_name (str) ["columns"]
            scripts_var_name (str) ["scripts"]
            limit_default_var_name (str) ["limit_default"]
            warnings_var_name (str) ["warnings"]
            raw_total_var_name (str) ["raw_total"]
            total_var_name (str) ["total"]
    """

    template_name = "models/bst/list_view.html"

    scripts = [
        "DataRepo/static/js/bst/cookies.js",
        "DataRepo/static/js/bst/list_view.js",
    ]

    # Pagination
    paginator_class = GracefulPaginator
    PER_PAGE_CHOICES: List[int] = [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0]
    paginate_by = 15

    # Context variable names

    # Cookie names (also used in browser cookies)
    search_cookie_name = "search"
    filter_cookie_name = "filter"
    visible_cookie_name = "visible"
    sortcol_cookie_name = "sortcol"
    asc_cookie_name = "asc"
    limit_cookie_name = "limit"  # Also a URL param name

    # Cookie operations
    cookie_prefix_var_name = "cookie_prefix"
    cookie_resets_var_name = "cookie_resets"
    clear_cookies_var_name = "clear_cookies"

    # Table/column/page related
    model_var_name = "model"
    table_id_var_name = "table_id"
    title_var_name = "table_name"
    columns_var_name = "columns"

    # JavaScript
    scripts_var_name = "scripts"

    # Basics
    limit_default_var_name = "limit_default"
    warnings_var_name = "warnings"

    # QuerySet metadata
    raw_total_var_name = "raw_total"
    total_var_name = "total"

    def __init__(self, **kwargs):
        """An extension of the ListView constructor intended to initialize the javascript and cookie interface.  It
        facillitates communication between the browser and the view.

        Args:
            kwargs (dict): Keyword args passed to ListView's constructor.
        Exceptions:
            None
        Returns:
            None
        """
        # The Django core code needed this set.  Not used in this class.
        self.kwargs = kwargs

        super().__init__(**kwargs)

        # Allow derived classes to *add* scripts for import
        self.javascripts: List[str]
        if hasattr(self, "javascripts") and isinstance(self.javascripts, list):
            self.javascripts.insert(0, BSTClientInterface.scripts)
        else:
            self.javascripts = [*BSTClientInterface.scripts]

        # Cookie controls
        self.cookie_prefix = f"{self.__class__.__name__}-"
        self.cookie_resets = []
        self.clear_cookies = False

        self.warnings = []

        # Initialize the values obtained from cookies
        self.search_term: Optional[str] = self.get_cookie(self.search_cookie_name)
        self.filter_terms = self.get_column_cookie_dict(self.filter_cookie_name)
        self.visibles = self.get_boolean_column_cookie_dict(self.visible_cookie_name)
        self.sort_name: Optional[str] = self.get_cookie(self.sortcol_cookie_name)
        self.ordered = self.sort_name is not None
        self.asc: bool = self.get_boolean_cookie(self.asc_cookie_name, True)

        # This is an override of ListView.ordering, defined here to silence this warning from Django:
        #   Pagination may yield inconsistent results with an unordered object_list:
        #   <class 'DataRepo.tests.tracebase_test_case.BSTLVAnimalTestModel'> QuerySet.
        # It specifies the default *row* ordering of the model objects, which is already set in the model.
        self.ordering: Optional[list]
        has_ordering = (
            hasattr(self, "ordering")
            and self.ordering is not None
            and len(self.ordering) > 0
        )
        if self.model is not None and not has_ordering:
            # Bootstrap Table only supports a single ordering column.  The model can provide multiple, but there is no
            # way to apply that ordering by the user.  It is just the default initial ordering.
            ordering_field = select_representative_field(
                self.model, force=True, include_expression=True
            )
            self.ordering = [ordering_field]
        elif not has_ordering:
            self.ordering = ["id"]

        # Initialize values obtained from URL parameters (or cookies)
        limit_param = self.get_param(self.limit_cookie_name)
        if limit_param is None:
            cookie_limit = self.get_cookie(self.limit_cookie_name)
            # Never set limit to 0 from a cookie, because if the page times out, the users will never be able to load it
            # without deleting their browser cookie.
            if cookie_limit is not None and int(cookie_limit) != 0:
                self.limit = int(cookie_limit)
            else:
                self.limit = self.paginate_by
        else:
            self.limit = int(limit_param)

        # Used for the pagination control (be sure to update in get_queryset)
        self.total = 0
        self.raw_total = 0

    def get_paginate_by(self, qs: QuerySet):
        """An override of the superclass method to allow the user to change the rows per page.

        Assumptions:
            1. qs was obtained from get_queryset (or get_user_queryset).  This is for efficiency - to not issue a
               count() query.  In fact, I'm not sure why this method requires a queryset input.
            2. self.limit was already set based on both the URL param and cookie, but if it is 0 (meaning "all"), we are
               going to update it based on the queryset.
        Args:
            qs (QuerySet)
        Exceptions:
            None
        Returns:
            self.limit (int): The number of table rows per page.
        """

        # Setting the limit to 0 means "all", but returning 0 here would mean we wouldn't get a page object sent to the
        # template, so we set it to the number of results.  The template will turn that back into 0 so that we're not
        # adding an odd value to the rows per page select list and instead selecting "all".
        if (
            self.limit == 0
            or (self.total > 0 and self.limit > self.total)
            or (self.total == 0 and self.limit > qs.count())
        ):
            if self.total > 0:
                # This avoids the count query, if self.total is already set
                self.limit = self.total
            else:
                self.limit = qs.count()

        return self.limit

    def get_param(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieves a URL parameter.

        Args:
            name (str)
            default (str)
        Exceptions:
            None
        Returns:
            (Optional[str]): The param value for the supplied name obtained from self.request or the default if the
                parameter was not found (or was an empty string).
        """
        if hasattr(self, "request"):
            val = self.request.GET.get(name, default)
            return val if val is not None and val != "" else default
        return default

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

    def get_cookie(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieves a cookie using a prepended view name.

        Args:
            name (str)
            default (str)
        Exceptions:
            None
        Returns:
            (Optional[str]): The cookie value for the supplied name (with the view_name prepended) obtained from
                self.request or the default if the cookie was not found (or was an empty string).
        """
        # If a cookie reset occurred, it means one or more of the cookies is problematic, so just return the default.
        if hasattr(self, "request") and not self.clear_cookies:
            return get_cookie(self.request, self.get_cookie_name(name), default)
        return default

    def get_boolean_cookie(self, name: str, default: bool = False) -> bool:
        """Retrieves a cookie using a prepended view name.

        Assumptions:
            1. Any string starting with T or F (case insensitive) is True or False, respectively.
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
        boolstr = self.get_cookie(name)
        if boolstr == "" or boolstr is None:
            return default
        elif boolstr.lower().startswith("t"):
            return True
        elif boolstr.lower().startswith("f"):
            return False

        cookie_name = self.get_cookie_name(name)
        if cookie_name not in self.cookie_resets:
            self.cookie_resets.append(cookie_name)
            warning = f"Invalid '{name}' value encountered: '{boolstr}'.  Clearing cookie '{cookie_name}'."
            self.warnings.append(warning)
            if settings.DEBUG:
                warn(warning, DeveloperWarning)

        return default

    def get_column_cookie_name(
        self, column: Union[BSTBaseColumn, str], name: str
    ) -> str:
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

    def get_column_cookie_dict(self, name: str) -> dict:
        """Retrieves a dict of values where the column names are the keys and the values are the value of the cookie
        name for the current view.

        Example:
            get_column_cookie_dict("visible")
            # output example: {"column1": True, "column2": False}
        Args:
            name (str): The name of the cookie variable specific to the column
        Exceptions:
            ValueError when there is no request
        Returns:
            (dict): A dict of cookie values keyed on column names
        """
        if not hasattr(self, "request") or self.clear_cookies:
            # This is to avoid exceptions when testing in the shell.  That's when there's no request.
            return {}
        return get_cookie_dict(self.request, prefix=f"{self.cookie_prefix}{name}-")

    def get_boolean_column_cookie_dict(self, name: str) -> Dict[str, bool]:
        """Retrieves a dict of values where the column names are the keys and the values are the boolean version of the
        value of the cookie for the current view.

        Example:
            get_column_cookie_dict("visible")
            # output example: {"column1": True, "column2": False}
        Args:
            name (str): The name of the cookie variable specific to the column
        Exceptions:
            ValueError when there is no request
        Returns:
            (dict)
        """
        boolstrs_dict: Dict[str, str] = self.get_column_cookie_dict(name)
        bools_dict: Dict[str, bool] = {}
        for colname, boolstr in boolstrs_dict.items():
            if boolstr.lower().startswith("t"):
                bools_dict[colname] = True
            elif boolstr.lower().startswith("f"):
                bools_dict[colname] = False
            else:
                cookie_name = self.get_column_cookie_name(colname, name)
                if cookie_name not in self.cookie_resets:
                    self.cookie_resets.append(cookie_name)
                    # TODO: Change the column name to the header
                    warning = (
                        f"Invalid '{name}' cookie value encountered for column '{colname}': '{boolstr}'.  "
                        f"Clearing cookie '{cookie_name}'."
                    )
                    self.warnings.append(warning)
                    if settings.DEBUG:
                        warn(warning, DeveloperWarning)
        return bools_dict

    def get_column_cookie(
        self, column: BSTBaseColumn, name: str, default: Optional[str] = None
    ) -> Optional[str]:
        """Retrieves a cookie using a prepended view name.

        Args:
            column (str): The name of the BST column
            name (str): The name of the cookie variable specific to the column
            default (str) [""]
        Exceptions:
            None
        Returns:
            (Optional[str]): The cookie value for the supplied name (with the view_name prepended) obtained from
                self.request or the default if the cookie was not found (or was an empty string).
        """
        if hasattr(self, "request"):
            return get_cookie(
                self.request, self.get_column_cookie_name(column, name), default
            )
        return default

    def reset_column_cookie(self, column: Union[str, BSTBaseColumn], name: str):
        """Adds a cookie to the cookie_resets list.

        Args:
            column (Union[str, BSTBaseColumn]): A BST columns or column name.
            name (str): The name of the cookie variable specific to the column.
        Exceptions:
            TypeError when column is invalid.
        Returns:
            None
        """
        if column is None:
            raise TypeError(
                f"Invalid column: [{column}].  Must be a str or BSTBaseColumn."
            )
        cookie_name = self.get_column_cookie_name(str(column), name)
        if cookie_name not in self.cookie_resets:
            self.cookie_resets.append(cookie_name)

    def reset_column_cookies(self, columns: List[Union[str, BSTBaseColumn]], name: str):
        """Adds cookies to the cookie_resets list.

        Args:
            columns (List[Union[str, BSTBaseColumn]]): A list of BST columns or column names.
            name (str): The name of the cookie variable specific to the column.
        Exceptions:
            ValueError when columns is invalid.
        Returns:
            None
        """
        if columns is None or len(columns) == 0:
            raise ValueError(
                f"Invalid columns: [{columns}].  Must be a non-empty list of strs or BSTBaseColumns."
            )
        for col in columns:
            self.reset_column_cookie(col, name)

    def reset_cookie(self, name: str):
        """Adds a cookie to the cookie_resets list.

        Args:
            name (str): The name of the cookie variable.
        Exceptions:
            None
        Returns:
            None
        """
        cookie_name = self.get_cookie_name(name)
        if cookie_name not in self.cookie_resets:
            self.cookie_resets.append(cookie_name)

    def reset_filter_cookies(self):
        self.reset_column_cookies(
            list(self.filter_terms.keys()), self.filter_cookie_name
        )

    def reset_search_cookie(self):
        self.reset_cookie(self.search_cookie_name)

    @classproperty
    def model_title_plural(cls):  # pylint: disable=no-self-argument
        """Creates a title-case string from self.model, accounting for potentially set verbose settings.  Pays
        particular attention to pre-capitalized values in the model name, and ignores the potentially poorly automated
        title-casing in existing verbose values of the model so as to not lower-case acronyms in the model name, e.g.
        MSRunSample (which automatically gets converted to Msrun Sample instead of the preferred MS Run Sample).
        """
        return model_title_plural(cls.model)

    @classproperty
    def model_title(cls):  # pylint: disable=no-self-argument
        """Creates a title-case string from self.model, accounting for potentially set verbose settings.  Pays
        particular attention to pre-capitalized values in the model name, and ignores the potentially poorly automated
        title-casing in existing verbose values of the model so as to not lower-case acronyms in the model name, e.g.
        MSRunSample (which automatically gets converted to Msrun Sample instead of the preferred MS Run Sample).
        """
        return model_title(cls.model)

    def get_context_data(self, **kwargs):
        """An override of the superclass method to provide context variables to the page.  All of the values are
        specific to pagination and BST operations."""

        # context = super().get_context_data(**kwargs)
        context = super().get_context_data()

        context.update(
            {
                # The basic ListView attribute
                self.model_var_name: self.model,
                # Client interface specific
                self.cookie_prefix_var_name: self.cookie_prefix,
                self.cookie_resets_var_name: self.cookie_resets,
                self.clear_cookies_var_name: self.clear_cookies,
                # A unique set of javascripts needed for the BST interface
                self.scripts_var_name: self.javascripts,
                # General table details
                self.table_id_var_name: type(self).__name__,
                self.title_var_name: self.model_title_plural,
                self.warnings_var_name: self.warnings,
                self.limit_default_var_name: self.paginate_by,
                # Table content controls
                self.search_cookie_name: self.search_term,
                self.sortcol_cookie_name: self.sort_name,
                self.asc_cookie_name: self.asc,
                self.limit_cookie_name: self.limit,
                # Queryset metadata (initialized in derived class that handles queries, e.g. the BSTListView class)
                self.raw_total_var_name: self.raw_total,  # Total before filtering
                self.total_var_name: self.total,
            }
        )

        # This context variable determines whether the BST code on the pagination template will render
        if self.total == 0:
            # Django does not supply a page_obj when there are no results, but the list_view.html template is where the
            # table controlling code (integrated with pagination) is loaded, so we need a page_obj context variable with
            # this minimal information necessary to operate the table, so that a user can clear their search term that
            # resulted in no matches.
            context["page_obj"] = {
                "number": 1,
                "has_other_pages": False,
                "paginator": {"per_page": self.limit},
            }

        return context
