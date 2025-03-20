# BSTFilterer
#   Class Attributes
#     BST_FILTER_CHOICES
#     LV_FILTER_CHOICES
#   Instance Attributes
#     bst_filterer, default INPUT (SELECT requires select_list)
#     lv_filterer, default None
#     filter_control, default based on bst_filterer
#     select_list, default None
#     strict_select, default False
#   Methods
#     init_filters

from functools import reduce
from typing import Dict, List, Optional, Union

from django.db.models import Field
from django.templatetags.static import static
from django.utils.functional import classproperty
from django.utils.safestring import mark_safe

from DataRepo.models.utilities import is_number_field


class BSTFilterer:
    """This class manages filtering of rows/objects based on a column in the Bootstrap Table of a ListView by providing
    a means of filtering rows based in the values in each column using a server-side or client-side method.  Server-side
    filtering uses Django field lookups in a query (e.g. 'field__icontains="term"').  Client-side filtering should only
    ever happen if the user has loaded the entire table and is accomplished using Bootstrap Table javascript code.

    Bootstrap Table column (th) attributes controlled:
    - data-filter-control
    - data-filter-custom-search
    See BSTListView for control of:
    - data-filter-data

    If the Django Model Field is provided
    - The default server-side Django lookup for string fields will be icontains (i.e. case insensitive), otherwise
      (effectively) exact.
    - The default client-side Bootstrap Table filter-control will be "select" if the field has "choices".
      'data-filter-strict-search' will be true.
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

    JAVASCRIPT = "js/bst_list_view/filterer.js"

    # The default filterer - filtering handled server-side by Django.  Use client_filterer for client-side filtering.
    server_filterer = FILTERER_DJANGO

    def __init__(
        self,
        field: Optional[Field] = None,
        input_method: Optional[str] = None,
        client_filterer: Optional[str] = None,
        lookup: Optional[str] = None,
        choices: Optional[Union[Dict[str, str], List[str]]] = None,
        client_mode: bool = False,
    ):
        """Construct a BSTFilterer object.

        Args:
            field (Optional[Field]): A Model field, used for automatically selecting a client_filterer and input_method.
            input_method (Optional[str]) [auto]: The string to set the Bootstrap Table data-filter-control attribute.
                The default is "select" int he field has a populated choices attribute.  Otherwise it is "input".
                TODO: "datepicker" is not yet supported.
            client_filterer (Optional[str]) [auto]: The string to set the Bootstrap Table data-filter-custom-search
                attribute.  The default is strictFilterer if self.choices is None or field is numeric, otherwise
                containsFilterer is set.
                Note that the client_filterer behavior should match the lookup behavior.
            lookup (Optional[str]): A Django Field Lookup.  Note, this should match the behavior of client_filterer.
                Note that the lookup behavior should match the client_filterer behavior.
                See https://docs.djangoproject.com/en/5.1/topics/db/queries/#field-lookups.
            choices (Optional[Union[Dict[str, str], List[str]]]): Values to populate a select list, if input_method is
                "select".
                TODO: choices could be used for auto-complete in the text input method.
            client_mode (bool): Set to True if the initial table is not filtered and the page queryset is the same size
                as the total queryset.
        Exceptions:
            ValueError when an argument is invalid.
        Returns:
            None
        """
        if input_method is not None:
            self.input_method = input_method

            if input_method not in self.INPUT_METHODS:
                raise ValueError(
                    f"input_method '{input_method}' must be one of {self.INPUT_METHODS}."
                )

            if input_method == self.INPUT_METHOD_SELECT:
                self.client_filterer = self.FILTERER_STRICT
                self.choices = choices
                if choices is None:
                    raise ValueError(
                        f"input_method '{input_method}' requires that choices be supplied."
                    )
            else:
                self.client_filterer = self.FILTERER_CONTAINS
                self.choices = None

        elif choices is not None and len(choices) > 0:
            self.input_method = self.INPUT_METHOD_SELECT
            self.client_filterer = self.FILTERER_STRICT

            if isinstance(choices, dict):
                self.choices = choices
            else:  # list (by process of elimination)
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

        elif field is not None:
            if field.choices is not None and len(field.choices) > 0:
                self.client_filterer = self.FILTERER_STRICT
                self.input_method = self.INPUT_METHOD_SELECT
                self.choices = dict(*field.choices)
            else:
                self.client_filterer = (
                    self.FILTERER_CONTAINS
                    if not is_number_field(field)
                    else self.FILTERER_STRICT
                )
                self.input_method = self.INPUT_METHOD_TEXT
                self.choices = None

        else:
            self.client_filterer = self.FILTERER_CONTAINS
            self.input_method = self.INPUT_METHOD_TEXT
            self.choices = None

        # Let the caller override the default client filterer
        if client_filterer is not None:
            self.client_filterer = client_filterer

        self.lookup: Optional[str]
        if lookup is not None:
            self.lookup = lookup
        else:
            self.lookup = None

        self.client_mode = client_mode

    def __str__(self) -> str:
        return self.client_filterer if self.client_mode else self.server_filterer

    def set_client_mode(self, enabled: bool = True):
        self.client_mode = enabled

    def set_server_mode(self, enabled: bool = True):
        self.client_mode = not enabled

    @classproperty
    def javascript(cls) -> str:  # pylint: disable=no-self-argument
        """Returns an HTML script tag whose source points to cls.JAVASCRIPT."""
        return mark_safe(f"<script src='{static(cls.JAVASCRIPT)}'></script>")
