import warnings
from functools import reduce
from typing import Dict, List, Optional, Union

from django.conf import settings
from django.db.models import Model
from django.templatetags.static import static
from django.utils.functional import classproperty
from django.utils.safestring import mark_safe

from DataRepo.models.utilities import (
    field_path_to_field,
    is_many_related_to_root,
    is_number_field,
    is_string_field,
)


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

    If the field path and source model are provided
    - The default server-side Django lookup for string fields will be icontains (i.e. case insensitive), otherwise
      (effectively) exact.
    - The default client-side Bootstrap Table filter-control will be "select" if the field has "choices".
    - The client_filterer will be "strictFilterer" if the field has "choices" and is not many-related.
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
    LOOKUP_EXACT = "iexact"

    JAVASCRIPT = "js/bst_list_view/filterer.js"

    # The default filterer - filtering handled server-side by Django.  Use client_filterer for client-side filtering.
    server_filterer = FILTERER_DJANGO

    def __init__(
        self,
        field: Optional[str] = None,
        input_method: Optional[str] = None,
        client_filterer: Optional[str] = None,
        lookup: Optional[str] = None,
        choices: Optional[Union[Dict[str, str], List[str]]] = None,
        client_mode: bool = False,
        model: Optional[Model] = None,
        initial: Optional[str] = None,
    ):
        """Construct a BSTFilterer object.

        Args:
            field (Optional[str]): A model field path or annotation field, used (with model) to derive the Field in
                order to automatically select a client_filterer and input_method (if model is provided - if model is not
                provided, it is assumed to be an annotation).
            input_method (Optional[str]) [auto]: The string to set the Bootstrap Table data-filter-control attribute.
                The default is "select" int he field has a populated choices attribute.  Otherwise it is "input".
                TODO: "datepicker" is not yet supported.
            client_filterer (Optional[str]) [auto]: The string to set the Bootstrap Table data-filter-custom-search
                attribute.  The default is strictFilterer if self.choices is None or field is numeric, otherwise
                containsFilterer is set.
                NOTE: The client_filterer behavior should match the lookup behavior.
            lookup (Optional[str]): A Django Field Lookup.  Note, this should match the behavior of client_filterer.
                Note that the lookup behavior should match the client_filterer behavior.
                See https://docs.djangoproject.com/en/5.1/topics/db/queries/#field-lookups.
            choices (Optional[Union[Dict[str, str], List[str]]]): Values to populate a select list, if input_method is
                "select".
                TODO: choices could be used for auto-complete in the text input method.
            client_mode (bool): Set to True if the initial table is not filtered and the page queryset is the same size
                as the total queryset.
            model (Optional[Model]): The root model that the field starts from.  Ignored unless field is provided.
                Used to change the default client_filterer when the input_method ends up being "select" but the field is
                a many-related field.
                Explanation: Many-related fields are displayed as delimited values, but the client filterer javascript
                code is unaware of that, so the default of strict (full match) filtering will not match any delimited
                values.  Many to many relations can be changed without the source model, but one to many related fields
                are only one-to-many relative to the model you are coming from.  If you look at it from the perspective
                of the table's model, there will/can be multiple delimited values and so the filterer should be
                "contains".  But if the field is defined in the related model (i.e. it's a reverse relation), it will
                say it is one-to-many, but from the perspective of the table's model, it should be many-to-one.
                Supplying the source model allows us to determine the true case.
            initial (Optional[str]): Initial filter search term.
        Exceptions:
            ValueError when an argument is invalid.
        Returns:
            None
        """
        # We will assume a field is an annotation if the attribute is not found on the model.  We could accept
        # 'is_annotation' as an argument, but inferring it is sufficient and keepts the interface simple.
        self.is_annotation: bool = False

        self.field = field
        self.model = model
        if field is not None and model is not None:
            try:
                self.model_field = field_path_to_field(model, field)
                self.many_related = is_many_related_to_root(field, model)
            except AttributeError as ae:
                # Assume it is an annotation if the field is definitely not a field path (i.e. contains a dunderscore)
                if "__" not in field:
                    self.is_annotation = True
                    self.model_field = None
                    self.many_related = False
                else:
                    raise ae
        elif field is not None and "__" in field:
            # An annotation can theoretically be an attribute of an object further down the field path, but BSTListView
            # does not support that
            raise ValueError(
                "field and model must be supplied together when not an annotation."
            )
        else:
            # Assume it is an annotation
            self.is_annotation = True
            self.model_field = None
            self.many_related = False

        if input_method is not None:
            self.input_method = input_method

            if input_method not in self.INPUT_METHODS:
                raise ValueError(
                    f"input_method '{input_method}' must be one of {self.INPUT_METHODS}."
                )

            if input_method == self.INPUT_METHOD_SELECT:
                # NOTE: If a column is many-related, this should be explicitly set to FILTERER_CONTAINS
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

        elif self.model_field is not None:
            if (
                self.model_field.choices is not None
                and len(self.model_field.choices) > 0
            ):
                if self.many_related:
                    self.client_filterer = self.FILTERER_CONTAINS
                else:
                    self.client_filterer = self.FILTERER_STRICT
                self.input_method = self.INPUT_METHOD_SELECT
                self.choices = dict(self.model_field.choices)
            else:
                self.client_filterer = (
                    self.FILTERER_CONTAINS
                    if not is_number_field(self.model_field)
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
            if (
                settings.DEBUG
                and client_filterer not in self.FILTERERS
                and not client_mode
                and lookup is None
            ):
                warnings.warn(
                    f"Custom client_filterer '{client_filterer}' supplied in server mode without a custom lookup.  "
                    "Server filtering may differ from client filtering."
                )
            self.client_filterer = client_filterer

        self.lookup: Optional[str]
        if lookup is not None:
            self.lookup = lookup
        elif (
            self.model_field is not None
            and is_string_field(self.model_field)
            and self.choices is None
        ):
            self.lookup = self.LOOKUP_CONTAINS
        else:
            self.lookup = None

        self.client_mode = client_mode

        self.initial: Optional[str] = None
        if initial is not None:
            self.initial = initial

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
