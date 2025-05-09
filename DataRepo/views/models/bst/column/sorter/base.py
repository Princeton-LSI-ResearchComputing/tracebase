from abc import ABC
from typing import NamedTuple, Optional, Type, Union
from warnings import warn

from django.conf import settings
from django.db import ProgrammingError
from django.db.models import F, Field
from django.db.models.expressions import Combinable, Expression
from django.db.models.functions import Lower
from django.templatetags.static import static
from django.utils.safestring import mark_safe

from DataRepo.models.utilities import (
    MultipleFields,
    NoFields,
    is_number_field,
    resolve_field_path,
)
from DataRepo.utils.exceptions import DeveloperWarning


class ClientSorters(NamedTuple):
    ALPHANUMERIC: str
    NUMERIC: str
    NONE: str
    UNKNOWN: str


class ServerSorters(NamedTuple):
    ALPHANUMERIC: Combinable
    NUMERIC: Combinable
    NONE: Combinable
    UNKNOWN: Combinable


class IdentityServerSorter(F):
    """No change to the sort behavior (e.g. for numeric values)"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class UnknownServerSorter(F):
    """The server sort could not be determined, e.g. we don't know the type of model field or annotation output_field"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BSTBaseSorter(ABC):
    """This class manages sorting of rows/objects based on a column in the Bootstrap Table of a ListView by providing a
    sorter for each column to both the Bootstrap Table javascript code and a transform to the field supplied to the
    Django ORM's order_by.
    """

    # Default client sorters
    CLIENT_SORTERS = ClientSorters(
        ALPHANUMERIC="alphanumericSorter",
        NUMERIC="numericSorter",
        NONE="djangoSorter",
        UNKNOWN="unknownSorter",
    )
    # Default server sorters
    SERVER_SORTERS = ServerSorters(
        ALPHANUMERIC=Lower,
        NUMERIC=IdentityServerSorter,  # No modification of how the database sorts numeric fields
        NONE=F,
        UNKNOWN=UnknownServerSorter,
    )

    script_name = "js/bst_list_view/sorter.js"

    is_annotation = False
    ascending: bool = True

    def __init__(
        self,
        expression: Union[Combinable, str],
        asc: Optional[bool] = None,
        name: Optional[str] = None,
        client_sorter: Optional[str] = None,
        client_mode: bool = False,
        _server_sorter: Optional[Type[Combinable]] = None,
    ):
        """Constructor.

        Args:
            expression (Union[Combinable, str]): A string or Combinable expression to use for django sorting of
                either an annotation of a model field path.  A Combinable can be things like a Transform, F, or
                Expression for example.
            asc (bool) [self.ascending]: Sort is ascending (or descending).  This is the default/initial sort.
            client_sorter (Optional[str]) [auto]: The string to set the Bootstrap Table data-sorter attribute.  The
                default is alphanumericSorter if field is not supplied, otherwise if the field type is a number field,
                numericSorter is set.
            client_mode (bool) [False]: Set to True if the initial table is not filtered and the page queryset is the
                same size as the total queryset.
            name (Optional[str]) [auto]: The name of the BSTColumn being sorted.  Will be inferred from expression.
                NOTE: Required if unable to unambiguously discern from expression.
            _server_sorter (Optional[Type[Combinable]]) [auto]: Explicitly set the server sorter to a Combinable
                (see super().SERVER_SORTERS for the defaults).  Set this to override (or apply on top of) the value
                derived from typing expression.  Instead of supplying this, just make sure that expression has
                an output_field set, but note that BSTSorter (and its derived classes) automatically sets
                _server_sorter.
        Exceptions:
            ValueError when an argument is invalid.
        Returns:
            None
        """
        self.expression: Combinable
        self.name = name
        self.asc = asc if asc is not None else self.ascending
        self.client_sorter = client_sorter
        self.client_mode = client_mode
        self._server_sorter: Type[Combinable]

        sort_field: Optional[Field] = None

        if isinstance(expression, Expression) and (
            # The default server sorter is unknown
            _server_sorter is None
            or _server_sorter not in self.SERVER_SORTERS
            or _server_sorter == self.SERVER_SORTERS.UNKNOWN
        ):
            try:
                # The sort expression has been explicitly specified, but we will make an attempt to apply the default
                # sort on top of what was supplied if it is an Expression (e.g. Lower).  That allows us to know the
                # field type, so that we can know whether applying case insensitivity is feasible.
                # NOTE: If you want to guarantee that default sorting is applied, a derived class must do it.
                if isinstance(expression.output_field, type):
                    sort_field = expression.output_field()
                else:
                    sort_field = expression.output_field
            except AttributeError as ae:
                if hasattr(expression, "output_field"):
                    msg = (
                        f"Invalid output_field in expression '{expression}'.  Try submitting the output_field as a "
                        f"Field instance.  [Original error: {ae}]"
                    )
                    raise AttributeError(msg).with_traceback(ae.__traceback__)

                # TODO: Implement a fallback to infer the field from the outer expression type, e.g. Upper -> CharField
                # The user must set output_field when defining the expression object, otherwise, you get:
                # AttributeError: 'F' object has no attribute '_output_field_or_none'.
                # To circumvent this, a derived class can supply _server_sorter.
                if settings.DEBUG and (
                    client_sorter is None or client_sorter in self.CLIENT_SORTERS
                ):
                    warn(
                        f"expression {expression} has no output_field set.  Unable to apply default server-"
                        "side sort behavior.  To avoid this, either set the output_field or supply a _server_sorter "
                        f"from SERVER_SORTERS or a custom client_sorter to the constructor.",
                        DeveloperWarning,
                    )

        if _server_sorter is None:
            if isinstance(sort_field, Field):
                if not is_number_field(sort_field):
                    self._server_sorter = self.SERVER_SORTERS.ALPHANUMERIC
                elif is_number_field(sort_field):
                    self._server_sorter = self.SERVER_SORTERS.NUMERIC
                else:
                    self._server_sorter = (
                        type(expression)
                        if isinstance(expression, Combinable)
                        else self.SERVER_SORTERS.UNKNOWN
                    )
            elif isinstance(expression, Expression) and hasattr(
                expression, "output_field"
            ):
                if not is_number_field(expression.output_field):
                    self._server_sorter = self.SERVER_SORTERS.ALPHANUMERIC
                elif is_number_field(expression.output_field):
                    self._server_sorter = self.SERVER_SORTERS.NUMERIC
                elif type(expression) in self.SERVER_SORTERS:
                    self._server_sorter = type(expression)
                else:
                    raise ProgrammingError("Unable to resolve field type.")
            elif (
                isinstance(expression, Expression)
                and type(expression) in self.SERVER_SORTERS
            ):
                self._server_sorter = type(expression)
            else:
                self._server_sorter = self.SERVER_SORTERS.UNKNOWN
        else:
            self._server_sorter = _server_sorter

        # Apply our sort criteria (potentially on top of the users' sort criteria)
        self.init_expression(expression)

        self.server_sort_type_known = (
            self._server_sorter in self.SERVER_SORTERS._asdict().values()
            and self._server_sorter != self.SERVER_SORTERS.UNKNOWN
        )

        if name is None:
            try:
                self.name = resolve_field_path(expression)
            except (NoFields, MultipleFields) as fe:
                raise ValueError(
                    f"name argument required.  (Unable to discern column name from expression '{expression}' "
                    f"due to '{type(fe).__name__}' error: {fe}.)"
                )

        # Set the default client_sorter to match the server sorter
        if client_sorter is None:
            # Base the default client_sorter on the server sorter
            if self._server_sorter == self.SERVER_SORTERS.NONE:
                self.client_sorter = self.CLIENT_SORTERS.NONE
            elif self._server_sorter == self.SERVER_SORTERS.NUMERIC:
                self.client_sorter = self.CLIENT_SORTERS.NUMERIC
            elif self._server_sorter == self.SERVER_SORTERS.ALPHANUMERIC:
                self.client_sorter = self.CLIENT_SORTERS.ALPHANUMERIC
            else:
                # We don't know the server sort, so default to None
                self.client_sorter = self.CLIENT_SORTERS.NONE
        self.client_sort_type_known = (
            self.client_sorter in self.CLIENT_SORTERS._asdict().values()
            and self.client_sorter != self.CLIENT_SORTERS.UNKNOWN
        )

        if (
            settings.DEBUG
            and self.server_sort_type_known != self.client_sort_type_known
            and self.client_sorter != self.CLIENT_SORTERS.UNKNOWN
            and self.client_sorter != self.CLIENT_SORTERS.NONE
        ):
            warn(
                f"Cannot guarantee that the server-side Django sort expression '{self.expression}' and "
                f"client_sorter '{self.client_sorter}' are equivalent.  Server sort may differ from client sort.  "
                "Be sure to explicitly set the field_expression and/or client_sorter to match.",
                DeveloperWarning,
            )

        if self.client_server_sort_mismatch():
            raise ValueError(
                f"Conflicting client '{self.client_sorter}' and server '{self._server_sorter}' sorters."
            )

    def __str__(self) -> str:
        return self.sorter

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.__dict__ == other.__dict__

    @property
    def sorter(self):
        return self.client_sorter if self.client_mode else self.CLIENT_SORTERS.NONE

    @property
    def order_by(self):
        """Returns an expression that can be supplied to a Django order_by() call."""
        if isinstance(self.expression, Expression):
            if self.asc:
                return self.expression.asc(nulls_first=True)
            return self.expression.desc(nulls_last=True)
        elif isinstance(self.expression, F):
            if self.asc:
                return self.expression.asc(nulls_first=True)
            return self.expression.desc(nulls_last=True)
        raise NotImplementedError(
            f"self.expression type '{type(self.expression).__name__}' not supported."
        )

    def set_client_mode(self, enabled: bool = True):
        self.client_mode = enabled

    def set_server_mode(self, enabled: bool = True):
        self.client_mode = not enabled

    def init_expression(self, expression: Union[Combinable, str]):
        """Initializes self.expression based on self._server_sorter, applying a default sort constraint potentially
        on top of customized sort criteria, e.g. applying case insensitivity.

        Args:
            expression (Union[Combinable, str]): A field name or F or other Combinable, like an Expression,
                Transform, field addition, concatenation, etc. to which default sort constraints will be applied, such
                as Lower() to impose case insensitivity.
        Exceptions:
            None
        Returns:
            None
        """
        # If we have a field type for sorting and the client sorter is either not specified, or it is a known client
        # sorter, apply default server sorting (either on top of or in the absense of an existing sort expression)
        if self._server_sorter == self.SERVER_SORTERS.NONE:
            if isinstance(expression, self.SERVER_SORTERS.NONE):
                self.expression = expression
            else:
                self.expression = self.SERVER_SORTERS.NONE(expression)
        elif self._server_sorter == self.SERVER_SORTERS.ALPHANUMERIC:
            if isinstance(expression, self.SERVER_SORTERS.ALPHANUMERIC):
                self.expression = expression
            elif self.SERVER_SORTERS.ALPHANUMERIC != IdentityServerSorter:
                self.expression = self.SERVER_SORTERS.ALPHANUMERIC(expression)
            elif isinstance(expression, Combinable):
                self.expression = expression
            else:
                self.expression = F(expression)
        elif self._server_sorter == self.SERVER_SORTERS.NUMERIC:
            if isinstance(expression, self.SERVER_SORTERS.NUMERIC):
                self.expression = expression
            elif self.SERVER_SORTERS.NUMERIC != IdentityServerSorter:
                self.expression = self.SERVER_SORTERS.NUMERIC(expression)
            elif isinstance(expression, Combinable):
                self.expression = expression
            else:
                self.expression = F(expression)
        elif self._server_sorter == self.SERVER_SORTERS.UNKNOWN:
            if isinstance(expression, Combinable):
                self.expression = expression
            else:
                self.expression = F(expression)
        elif isinstance(expression, str):
            self.expression = F(expression)
        elif isinstance(expression, Combinable):
            self.expression = expression
        else:
            raise TypeError(
                f"Invalid expression type: '{type(expression).__name__}'.  Expression: {expression}."
            )

    def client_server_sort_mismatch(self):
        """Returns whether the server and client sort methods definitely mismatch.  Does not report as a mismatch if
        we're not sure (because the user supplied their own and these Combinables can be nested, so they could match).
        Another way of looking at it is, we only want to report a mismatch if a default sort was applied to either the
        client or server sort."""
        if (
            not self.server_sort_type_known
            or not self.client_sort_type_known
            or self.client_sorter == self.CLIENT_SORTERS.NONE
        ):
            # We don't know if it's a mismatch.  Only returning certainty.
            # Or the client sorter just parrots the server sorter
            return False
        server_sort_keys = [
            k
            for k, v in self.SERVER_SORTERS._asdict().items()
            if v == self._server_sorter
        ]
        if len(server_sort_keys) > 0:
            server_sort_key = server_sort_keys[0]
        else:
            server_sort_key = "UNKNOWN"
        client_sort_keys = [
            k
            for k, v in self.CLIENT_SORTERS._asdict().items()
            if v == self.client_sorter
        ]
        if len(client_sort_keys) > 0:
            client_sort_key = client_sort_keys[0]
        else:
            client_sort_key = "UNKNOWN"
        return server_sort_key != client_sort_key

    @property
    def script(self) -> str:
        """Returns an HTML script tag whose source points to self.script_name.

        Example:
            # In the view's get_context_data
                context["sorter"] = BSTSorter(field=Model.name.field)  # name is a CharField
            # Template
                {{ sorter.script }}
            # Template result (assuming settings.STATIC_URL = "static/")
                <script src='static/js/bst_list_view/sorter.js'></script>
        """
        return mark_safe(f"<script src='{static(self.script_name)}'></script>")

    @classmethod
    def get_server_sorter_matching_expression(cls, expression: Combinable):
        """Takes an expression and tries to match it with a supporter server sorter.

        This is useful for (for example) annotation columns, because the expression is generated for the annotation in
        the select.  It would be a waste of cycles to also generate it in the ORDER BY, so it is better to set the ORDER
        BY value to the annotation name.  The problem is we need the expression in order to know what type of sort to
        do, so that's what this does.  Just call it with the converter before instantiating an object of this class.

        Args:
            expression (Combinable): E.g. the converter of an annotation.
        Exceptions:
            None
        Returns:
            _server_sorter (Type[Combinable])
        """
        _server_sorter = cls.SERVER_SORTERS.UNKNOWN
        if isinstance(expression, Expression):
            try:
                if isinstance(expression.output_field, type):
                    output_field = expression.output_field()
                else:
                    output_field = expression.output_field
            except AttributeError as ae:
                raise AttributeError(
                    f"Missing required output_field in expression '{expression}'.\nPlease supply the 'output_field' "
                    f"argument with a Field instance to the expression.  [Original error: {ae}]"
                )

            if not is_number_field(output_field):
                _server_sorter = cls.SERVER_SORTERS.ALPHANUMERIC
            elif is_number_field(output_field):
                _server_sorter = cls.SERVER_SORTERS.NUMERIC
        elif type(expression) in cls.SERVER_SORTERS:
            _server_sorter = type(expression)
        return _server_sorter
