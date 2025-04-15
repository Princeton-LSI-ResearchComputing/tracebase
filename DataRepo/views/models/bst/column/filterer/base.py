from __future__ import annotations

from abc import ABC, abstractmethod
from functools import reduce
from typing import Dict, List, NamedTuple, Optional, Union
from warnings import warn

from django.conf import settings
from django.templatetags.static import static
from django.utils.safestring import mark_safe

from DataRepo.utils.exceptions import DeveloperWarning


class InputMethods(NamedTuple):
    TEXT: str
    SELECT: str
    DATE: str  # NOTE: Currently unsupported


class ServerLookup(ABC):
    """This exists to hold a single lookup like "icontains", while also being able to distinguish 2 different classes
    when they contain the same lookup string, so that when client filterers differ but server filterers don't, we can
    tell which one was desired using the class.  This allows us to default select the corresponding client filterer
    based on the specific server filterer chosen by the user.

    Example:
        # _server_filterer -> "iexact"
        # client_filterer -> "strictMultipleFilterer"
        # StrictSingleLookup, StrictMulitipleLookup, and NoneLookup all define "iexact" as their lookup
        _server_filterer = StrictMulitipleLookup()
        if _server_filterer == StrictSingleLookup():
            client_filterer = "strictSingleFilterer"
        elif _server_filterer == StrictMulitipleLookup():
            client_filterer = "strictMultipleFilterer"  # <- THIS GETS SET
    """

    @property
    @abstractmethod
    def lookup(self):
        pass

    def __str__(self):
        return self.lookup

    def __eq__(self, other):
        if isinstance(other, ServerLookup):
            return self.__class__ is other.__class__ and self.lookup == other.lookup
        raise NotImplementedError(
            f"Comparison with type {type(other).__name__} not supporter."
        )


class ContainsLookup(ServerLookup):
    """A static Django lookup, e.g. 'icontains', used for case insensitive substring matches, which works whether or not
    the field is a text-like field (due to DB automatic casting)."""

    lookup = "icontains"


class StrictSingleLookup(ServerLookup):
    """A static Django lookup, e.g. 'icontains', used for case insensitive whole matches for fields, whether or not the
    field is a text-like field."""

    lookup = "iexact"


class StrictMulitipleLookup(ServerLookup):
    """A static Django lookup, e.g. 'icontains', used for case insensitive whole matches for delimited many-related
    fields, whether or not the field is a text-like field."""

    lookup = "iexact"


class NoneLookup(ServerLookup):
    """A static Django lookup, e.g. 'icontains', used for matches to field values for which case insensitivity does not
    apply."""

    lookup = "iexact"


class CustomLookup(ServerLookup):
    """A configurable Django lookup, e.g. 'icontains'."""

    lookup = "unknown"

    def __init__(self, lookup: str = lookup):
        self.lookup = lookup
        super().__init__()


class ClientFilterers(NamedTuple):
    """Instances of this class must define javascript function names.  Each function name MUST be different.  Checked
    via the BSTBaseFilterer's constructor by calling check_client_filterers()."""

    CONTAINS: str
    STRICT_SINGLE: str
    STRICT_MULTIPLE: str
    NONE: str
    UNKNOWN: str

    def get_key(self, val):
        for k in [k for k, v in self._asdict().items() if v == val]:
            return k
        raise ValueError(f"Value '{val}' not found.")


class ServerFilterers(NamedTuple):
    """Instances of this class must define Django lookups (e.g. "icontains").  Each type MUST be different."""

    CONTAINS: ContainsLookup
    STRICT_SINGLE: StrictSingleLookup
    STRICT_MULTIPLE: StrictMulitipleLookup
    NONE: NoneLookup
    UNKNOWN: CustomLookup

    def get_key(self, val):
        for k in [k for k, v in self._asdict().items() if v == val]:
            return k
        raise ValueError(f"{type(val).__name__} value '{val}' not found.")


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

    INPUT_METHODS = InputMethods(
        TEXT="input",
        SELECT="select",
        DATE="datepicker",
    )

    CLIENT_FILTERERS = ClientFilterers(
        CONTAINS="containsFilterer",
        STRICT_SINGLE="strictFilterer",
        # TODO: Put values inside spans in the template & create a javascript function to match 1 entire span's contents
        STRICT_MULTIPLE="strictMultipleFilterer",
        NONE="djangoFilterer",
        UNKNOWN="unknownFilterer",
    )

    SERVER_FILTERERS = ServerFilterers(
        CONTAINS=ContainsLookup(),
        STRICT_SINGLE=StrictSingleLookup(),
        STRICT_MULTIPLE=StrictMulitipleLookup(),  # Not terribly relevant, but present to synch with the client filterer
        NONE=NoneLookup(),  # Use this when icontains does not work with the field type
        UNKNOWN=CustomLookup(),
    )

    script_name = "js/bst_list_view/filterers.js"

    is_annotation = False

    def __init__(
        self,
        name: str,
        input_method: Optional[str] = None,
        choices: Optional[Union[Dict[str, str], List[str]]] = None,
        client_filterer: Optional[str] = None,
        _server_filterer: Optional[Union[ServerLookup, str]] = None,
        initial: Optional[str] = None,
        client_mode: bool = False,
    ):
        """Constructor.

        Args:
            name (str): A model field path or annotation field name, which is basically the same as BSTBaseColumn.name.
            input_method (Optional[str]) [auto]: The string to set the Bootstrap Table data-filter-control attribute.
                The default is "select" if choices is not None.  Otherwise it is "input".
                TODO: "datepicker" is not yet supported.
            client_filterer (Optional[str]) [auto]: The string to set the Bootstrap Table data-filter-custom-search
                attribute.  The default is "strictFilterer" if input_method is "select", otherwise "containsFilterer".
                NOTE: The client_filterer behavior should match the lookup behavior.
            _server_filterer (Optional[Union[ServerLookup, str]]) [auto]: A Django Field Lookup (e.g.
                'field__icontains="term"').  Default is based on client_filterer.
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
        self.check_client_filterers()

        self.name = name
        self.input_method = input_method
        self.client_filterer = client_filterer
        self.client_mode = client_mode
        self.initial = initial
        self.choices: Optional[Dict[str, str]]
        self._server_filterer: Optional[ServerLookup]

        self._server_filterer = self.process_server_filterer(_server_filterer)

        if choices is None or len(choices) == 0:
            self.choices = None
        elif not isinstance(choices, dict):  # list (by process of elimination)
            # choices is populated, but not a dict, so here, we convert it...

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
        else:
            self.choices = choices

        if self.input_method is None:
            if choices is None:
                self.input_method = self.INPUT_METHODS.TEXT
            else:
                self.input_method = self.INPUT_METHODS.SELECT
        elif self.input_method not in self.INPUT_METHODS._asdict().values():
            raise ValueError(
                f"input_method '{self.input_method}' must be one of {self.INPUT_METHODS._asdict().values()}."
            )
        elif self.input_method == self.INPUT_METHODS.SELECT and self.choices is None:
            raise ValueError(
                f"input_method '{self.input_method}' requires that choices be supplied."
            )
        elif (
            self.input_method != self.INPUT_METHODS.SELECT and self.choices is not None
        ):
            raise ValueError(
                f"input_method cannot be '{self.input_method}' if choices are supplied."
            )

        self.init_filterers()

    def __str__(self) -> str:
        return self.filterer

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.__dict__ == other.__dict__

    @property
    def filterer(self):
        return self.client_filterer if self.client_mode else self.CLIENT_FILTERERS.NONE

    @classmethod
    def process_server_filterer(
        cls, _server_filterer: Optional[Union[ServerLookup, str]]
    ) -> Optional[ServerLookup]:
        """Takes a server filterer specification as input and returns a ServerLookup or None."""
        if isinstance(_server_filterer, str):
            return CustomLookup(_server_filterer)
        elif isinstance(_server_filterer, ServerLookup):
            return _server_filterer
        elif _server_filterer is not None:
            raise TypeError(
                f"_server_filterer must be an instance of str or {ServerLookup.__name__}"
            )
        return None

    def check_client_filterers(self):
        """self.CLIENT_FILTERERS' values must be unique.  They are defined as a class attribute.  This checks to ensure
        they are indeed unique.

        NOTE: I initially tried both subclassing and multiple inheritance of a base Filterers class that was a
        NamedTuple, but apparently NamedTuple supports neither multiple inheritance nor subclassing, thus I do the check
        here, whenever a BSTBaseFilterer is instantiated.  (Called from __init__.)

        NOTE: This is not necessary for ServerFilterers, as their types are not the same (though that must be manually
        maintained).

        Args:
            None
        Exceptions:
            ValueError when the ClientFilterers contains duplicate values.
        Returns:
            None
        """
        dupes = []
        seen = {}
        for v in self.CLIENT_FILTERERS._asdict().values():
            if v in seen.keys():
                dupes.append(v)
                seen[v] = True
        if len(dupes) > 0:
            raise ValueError(f"No duplicate client filterers {dupes} allowed.")

    def init_filterers(self):
        """Initializes and validates self.client_filterer and self._server_filterer.

        Args:
            None
        Exceptions:
            ValueError when client and server filterers are incompatible
        Warns:
            When client and server filtering might behave differently.
            When client filtering has been disabled due to custom server filtering and no custom client filtering
                defined.
        Returns:
            None
        """
        if self.client_filterer is None and self._server_filterer is None:
            # Neither client or server filterer is defined
            # Base the default client and server filterers on the input method
            if self.input_method == self.INPUT_METHODS.SELECT:
                self.client_filterer = self.CLIENT_FILTERERS.STRICT_SINGLE
                self._server_filterer = self.SERVER_FILTERERS.STRICT_SINGLE
            else:
                self.client_filterer = self.CLIENT_FILTERERS.CONTAINS
                self._server_filterer = self.SERVER_FILTERERS.CONTAINS
        elif self.client_filterer is None or self._server_filterer is None:
            # One of the 2 (client/server) filterers is defined
            # Base the default client filterer on the server filterer or vice versa
            if self._server_filterer is None:
                if self.client_filterer == self.CLIENT_FILTERERS.CONTAINS:
                    self._server_filterer = self.SERVER_FILTERERS.CONTAINS
                elif self.client_filterer == self.CLIENT_FILTERERS.STRICT_SINGLE:
                    self._server_filterer = self.SERVER_FILTERERS.STRICT_SINGLE
                elif self.client_filterer == self.CLIENT_FILTERERS.STRICT_MULTIPLE:
                    self._server_filterer = self.SERVER_FILTERERS.STRICT_MULTIPLE
                else:
                    # Fallback to the input method
                    if self.input_method == self.INPUT_METHODS.SELECT:
                        self._server_filterer = self.SERVER_FILTERERS.STRICT_SINGLE
                    else:
                        self._server_filterer = self.SERVER_FILTERERS.CONTAINS
                    if settings.DEBUG:
                        warn(
                            "Cannot guarantee that the behavior of the default _server_filterer "
                            f"'{self._server_filterer}' (selected based on the input method '{self.input_method}') "
                            f"will match the behavior of the custom client_filterer '{self.client_filterer}'.  "
                            "Server filtering may differ from client filtering.  Supply a custom _server_filterer to "
                            "guarantee matching behavior.",
                            DeveloperWarning,
                        )
            else:
                if self._server_filterer == self.SERVER_FILTERERS.CONTAINS:
                    self.client_filterer = self.CLIENT_FILTERERS.CONTAINS
                elif self._server_filterer == self.SERVER_FILTERERS.STRICT_SINGLE:
                    self.client_filterer = self.CLIENT_FILTERERS.STRICT_SINGLE
                elif self._server_filterer == self.SERVER_FILTERERS.STRICT_MULTIPLE:
                    self.client_filterer = self.CLIENT_FILTERERS.STRICT_MULTIPLE
                else:
                    # Fallback to making the client filterer just parrot the server filterer
                    self.client_filterer = self.CLIENT_FILTERERS.NONE
                    if settings.DEBUG:
                        warn(
                            "Cannot select a matching default client_filterer corresponding to the _server_filterer "
                            f"'{self._server_filterer}', so disabling client filtering with '{self.client_filterer}' "
                            f"to match the behavior.  Supply a custom client_filterer to enable efficient filtering "
                            "when a user views 'all' rows.  Doing so reduces wait times in the 'all' rows use-case.",
                            DeveloperWarning,
                        )
        elif (
            self.client_filterer != self.CLIENT_FILTERERS.NONE
            and self.client_filterer != self.CLIENT_FILTERERS.UNKNOWN
            and not isinstance(self._server_filterer, CustomLookup)
            and (
                (self.client_filterer == self.CLIENT_FILTERERS.CONTAINS)
                != (self._server_filterer == self.SERVER_FILTERERS.CONTAINS)
                or (self.client_filterer == self.CLIENT_FILTERERS.STRICT_SINGLE)
                != (self._server_filterer == self.SERVER_FILTERERS.STRICT_SINGLE)
                or (self.client_filterer == self.CLIENT_FILTERERS.STRICT_MULTIPLE)
                != (self._server_filterer == self.SERVER_FILTERERS.STRICT_MULTIPLE)
            )
        ):
            # Both the client & server filterers are explicitly defined & do not correspond (ignoring UNKNOWN & NONE)
            raise ValueError(
                f"Mismatching client and server filterer types.  client_filterer '{self.client_filterer}' does not "
                f"correspond to the _server_filterer type '{type(self._server_filterer).__name__}'."
            )
        elif settings.DEBUG and (
            self.client_filterer == self.CLIENT_FILTERERS.UNKNOWN
            or self.client_filterer not in self.CLIENT_FILTERERS._asdict().values()
        ) != (isinstance(self._server_filterer, CustomLookup)):
            # Both the client & server filterers are explicitly defined & one is UNKNOWN(/custom)
            if self.client_filterer == self.CLIENT_FILTERERS.UNKNOWN:
                warn(
                    f"Client filtering disabled with '{self.client_filterer}'.  Supply a custom client_filterer that "
                    f"matches the behavior of _server_filterer '{self._server_filterer}' to enable efficient filtering "
                    "when a user views 'all' rows.  Doing so reduces wait times in the 'all' rows use-case.",
                    DeveloperWarning,
                )
            else:
                warn(
                    f"Cannot guarantee that the client_filterer '{self.client_filterer}' behavior will match the "
                    f"_server_filterer '{type(self._server_filterer).__name__}' behavior.  Server filtering may differ "
                    "from client filtering.",
                    DeveloperWarning,
                )

    def set_client_mode(self, enabled: bool = True):
        self.client_mode = enabled

    def set_server_mode(self, enabled: bool = True):
        self.client_mode = not enabled

    @property
    def script(self) -> str:
        """Returns an HTML script tag whose source points to self.script_name."""
        return mark_safe(f"<script src='{static(self.script_name)}'></script>")
