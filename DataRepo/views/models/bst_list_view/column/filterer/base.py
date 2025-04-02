from abc import ABC
from functools import reduce
from typing import Dict, List, Optional, Union
from warnings import warn

from django.conf import settings
from django.templatetags.static import static
from django.utils.functional import classproperty
from django.utils.safestring import mark_safe


class BSTBaseFilterer(ABC):
    """This class manages filtering of rows/objects based on a column in the Bootstrap Table of a ListView by providing
    a means of filtering rows based on the values in each column using a server-side or client-side method.  Server-side
    filtering uses Django field lookups in a query (e.g. 'field__icontains="term"').  Client-side filtering should only
    ever happen if the user has loaded the entire table, and is accomplished using Bootstrap Table javascript code.

    Bootstrap Table column (th) attributes controlled:
    - data-filter-control
    - data-filter-custom-search
    - Note that data-filter-strict-search is not used, as the behavior is controlled via javascript.
    See BSTListView for control of:
    - data-filter-data
    """

    INPUT_METHOD_TEXT = "input"
    INPUT_METHOD_SELECT = "select"
    INPUT_METHOD_DATEPICKER = "datepicker"
    INPUT_METHODS = [
        INPUT_METHOD_TEXT,
        INPUT_METHOD_SELECT,
        INPUT_METHOD_DATEPICKER,
    ]

    FILTERER_CONTAINS = "containsFilterer"
    FILTERER_STRICT = "strictFilterer"
    FILTERER_DJANGO = "djangoFilterer"
    FILTERERS = [
        FILTERER_CONTAINS,
        FILTERER_STRICT,
        FILTERER_DJANGO,
    ]

    LOOKUP_CONTAINS = "icontains"
    LOOKUP_STRICT = "iexact"

    JAVASCRIPT = "js/bst_list_view/filterer.js"

    # The default filterer - filtering handled server-side by Django.  Use client_filterer for client-side filtering.
    server_filterer = FILTERER_DJANGO
    is_annotation = False

    def __init__(
        self,
        name: str,
        input_method: Optional[str] = None,
        client_filterer: Optional[str] = None,
        lookup: Optional[str] = None,
        choices: Optional[Union[Dict[str, str], List[str]]] = None,
        client_mode: bool = False,
        initial: Optional[str] = None,
    ):
        """Constructor.

        Args:
            name (str): A model field path or annotation field name.
            input_method (Optional[str]) [auto]: The string to set the Bootstrap Table data-filter-control attribute.
                The default is "select" if choices is not None.  Otherwise it is "input".
                TODO: "datepicker" is not yet supported.
            client_filterer (Optional[str]) [auto]: The string to set the Bootstrap Table data-filter-custom-search
                attribute.  The default is "strictFilterer" if input_method is "select", otherwise "containsFilterer".
                NOTE: The client_filterer behavior should match the lookup behavior.
            lookup (Optional[str]): A Django Field Lookup (e.g. 'field__icontains="term"').
                NOTE: The lookup behavior should match the client_filterer behavior.
                See https://docs.djangoproject.com/en/5.1/topics/db/queries/#field-lookups.
            choices (Optional[Union[Dict[str, str], List[str]]]): Values to populate a select list, if input_method is
                "select".
                NOTE: Supplying this value will automatically set the input_method to "select".
                TODO: choices could be used for auto-complete in the text input method.
            client_mode (bool): Set to True if the page queryset is the same size as the total unfiltered queryset.
            initial (Optional[str]): Initial filter search term.
        Exceptions:
            ValueError when an argument is invalid.
        Returns:
            None
        """
        self.name = name
        self.input_method = input_method
        self.choices = choices
        self.client_filterer = client_filterer
        self.lookup = lookup
        self.client_mode = client_mode
        self.initial = initial

        if (
            self.input_method is not None
            and self.input_method not in self.INPUT_METHODS
        ):
            raise ValueError(
                f"input_method '{self.input_method}' must be one of {self.INPUT_METHODS}."
            )

        if choices is None and self.input_method == self.INPUT_METHOD_SELECT:
            raise ValueError(
                f"input_method '{self.input_method}' requires that choices be supplied."
            )
        elif choices is None or len(choices) == 0:
            self.choices = None
            if self.input_method == self.INPUT_METHOD_SELECT:
                raise ValueError(
                    f"input_method '{self.input_method}' requires that choices be populated."
                )
        else:
            # choices is populated, so...
            if self.input_method is None:
                self.input_method = input_method
            elif self.input_method != self.INPUT_METHOD_SELECT:
                raise ValueError(
                    f"input_method cannot be '{self.input_method}' if choices are supplied."
                )

            if not isinstance(choices, dict):  # list (by process of elimination)
                # NOTE: This filters for a unique case-insensitive sorted dict, and arbitrarily uses the first case
                # instance encountered.  I.e., it does not produce a lower-cased output dict - just a dict that is
                # unique when case is ignored for uniqueness.
                self.choices = dict(  # type: ignore
                    (opt, opt)
                    for opt in sorted(
                        reduce(
                            (
                                lambda lst, val: (  # type: ignore
                                    lst + [val]  # type: ignore
                                    if str(val).lower()
                                    not in [str(v).lower() for v in lst]  # type: ignore
                                    else lst
                                )
                            ),
                            choices,
                            [],
                        ),
                        key=str.casefold,
                    )
                )

        if client_filterer is None:
            if self.input_method == self.INPUT_METHOD_SELECT:
                self.client_filterer = self.FILTERER_STRICT
            else:
                self.client_filterer = self.FILTERER_CONTAINS
        elif (
            settings.DEBUG
            and client_filterer not in self.FILTERERS
            and not client_mode
            and lookup is None
        ):
            warn(
                f"Custom client_filterer '{client_filterer}' supplied in server mode without a custom lookup.  "
                "Server filtering may differ from client filtering."
            )

        if lookup is None:
            if self.client_filterer != self.FILTERER_STRICT:
                self.lookup = self.LOOKUP_CONTAINS
            else:
                self.lookup = self.LOOKUP_STRICT
        elif settings.DEBUG and (
            (
                self.client_filterer == self.FILTERER_STRICT
                and self.lookup != self.LOOKUP_STRICT
            )
            or (
                self.client_filterer == self.FILTERER_CONTAINS
                and self.lookup != self.LOOKUP_CONTAINS
            )
        ):
            warn(
                f"client_filterer '{self.client_filterer}' and lookup '{self.lookup}' may not behave the same.  "
                "Server filtering may differ from client filtering."
            )

    def __str__(self) -> str:
        return self.filterer

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.__dict__ == other.__dict__

    @property
    def filterer(self):
        return self.client_filterer if self.client_mode else self.server_filterer

    def set_client_mode(self, enabled: bool = True):
        self.client_mode = enabled

    def set_server_mode(self, enabled: bool = True):
        self.client_mode = not enabled

    @classproperty
    def javascript(cls) -> str:  # pylint: disable=no-self-argument
        """Returns an HTML script tag whose source points to cls.JAVASCRIPT."""
        return mark_safe(f"<script src='{static(cls.JAVASCRIPT)}'></script>")
