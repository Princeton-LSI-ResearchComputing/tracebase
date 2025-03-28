from typing import Optional, Union
from warnings import warn

from django.conf import settings
from django.db.models import F, Field, Model
from django.db.models.expressions import Combinable, Expression
from django.db.models.functions import Lower
from django.templatetags.static import static
from django.utils.functional import classproperty
from django.utils.safestring import mark_safe

from DataRepo.models.utilities import (
    NoFields,
    field_path_to_field,
    is_number_field,
    resolve_field_path,
)


class BSTSorter:
    """This class manages sorting of rows/objects based on a column in the Bootstrap Table of a ListView by providing a
    sorter for each column to both the Bootstrap Table javascript code and a transform to the field supplied to the
    Django ORM's order_by.

    If the Django Model Field is provided
    - The default transform for string fields will be Lower (i.e. case insensitive), otherwise no transform.
    - The default sorter will be djangoSorter for all fields.

    Example:
        # BSTListView.__init__
            self.sorter = BSTSorter(field=Model.name.field)  # name is a CharField

        # BSTListView.get_context_data
            context["sorter"] = self.sorter

        # Template
            {{ sorter.javascript }}
            <th data-sorter="{% if total|lte:raw_total %}{{ sorter }}{% else %}{{ sorter.client_sorter }}{% endif %}">

        # Template result
            <script src='{settings.STATIC_URL}js/bst_list_view/sorter.js'></script>
            <th data-sorter="djangoSorter">

        # BSTListView.get_queryset
            return self.model.objects.order_by(self.sorter.transform("name"))
    """

    SORTER_BST_ALPHANUMERIC = "alphanumeric"
    SORTER_BST_NUMERIC = "numericOnly"
    SORTER_JS_ALPHANUMERIC = "alphanumericSorter"
    SORTER_JS_NUMERIC = "numericSorter"
    SORTER_JS_DJANGO = "djangoSorter"
    SORTERS = [
        SORTER_BST_ALPHANUMERIC,
        SORTER_BST_NUMERIC,
        SORTER_JS_ALPHANUMERIC,
        SORTER_JS_NUMERIC,
        SORTER_JS_DJANGO,
    ]

    JAVASCRIPT = "js/bst_list_view/sorter.js"

    # The default sorter - sorting handled server-side by Django.  Use client_sorter for client-side sorting.
    server_sorter = SORTER_JS_DJANGO

    def __init__(
        self,
        field_expression: Union[Combinable, Field, str],
        client_sorter: Optional[str] = None,
        client_mode: bool = False,
        model: Optional[Model] = None,
    ):
        """Construct a BSTSorter object.

        Assumptions:
            1. In the case of field_expression being a Field, the "field path" returned assumes that the context of the
                field path is the immediate model that the Field belongs to.
            2. The output type of the field_expression is the same as the field it contains.  E.g. if the
                field_expression is Lower("files__name"), then the output type of Lower is assumed to be the same type
                as the field type of "files__name", e.g. a CharField.
        Limitations:
            1. Only supports a single source field path.
        Args:
            field_expression (Union[Combinable, Field, str]): A Model Field, Combinable (e.g. Expression, Transform), or
                str (e.g. a field path or name [i.e. an annotation]) used for automatically selecting a client_sorter
                and transform.
            client_sorter (Optional[str]) [auto]: The string to set the Bootstrap Table data-sorter attribute.  The
                default is alphanumericSorter if field is not supplied, otherwise if the field type is a number field,
                numericSorter is set.
            client_mode (bool): Set to True if the initial table is not filtered and the page queryset is the same size
                as the total queryset.
            model (Optional[Model]): The root model of the field path, only used if field_expression is/contains a field
                path, to obtain the Field type and set the client_sorter based on it.
        Exceptions:
            ValueError when transform is invalid.
        Returns:
            None
        """
        # We will assume a field is an annotation if the attribute is not found on the model.  We could accept
        # 'is_annotation' as an argument, but inferring it is sufficient and keepts the interface simple.
        self.is_annotation: bool = False

        self.model = model
        self.field_path: Optional[str]

        try:
            # NOTE: field_path may end up being an annotation field.
            self.field_path = resolve_field_path(field_expression)
        except NoFields:
            # Assume the field is an annotation
            self.is_annotation = True
            self.field_path = None

        # Set model_field and sort_expression
        if isinstance(field_expression, Field):
            self.model_field = field_expression
            if not is_number_field(self.model_field):
                self.sort_expression = Lower(self.model_field.name)
            else:
                self.sort_expression = F(field_expression.name)
        else:
            if self.field_path is not None and self.model is not None:
                try:
                    self.model_field = field_path_to_field(self.model, self.field_path)
                except AttributeError as ae:
                    # Assume it is an annotation if the field is definitely not a field path (i.e. contains a
                    # dunderscore)
                    if "__" not in self.field_path:
                        self.is_annotation = True
                    else:
                        raise ae

                if (
                    self.model_field is not None
                    and not is_number_field(self.model_field)
                    and (
                        isinstance(field_expression, str)
                        or not isinstance(field_expression, Expression)
                    )
                ):
                    self.sort_expression = Lower(self.field_path)
                elif isinstance(field_expression, str):
                    self.sort_expression = F(field_expression)
                else:
                    if (
                        not isinstance(field_expression, Lower)
                        and client_sorter is None
                        and settings.DEBUG
                    ):
                        warn(
                            f"field_expression ({field_expression}) supplied without a corresponding client_sorter.  "
                            "Unable to select a client_sorter that matches the expression.  Server sort may "
                            "differ from client sort.  Selecting a default client_sorter based on the field type "
                            f"'{type(self.model_field).__name__}'."
                        )
                    self.sort_expression = field_expression
            else:
                self.model_field = None
                if settings.DEBUG:
                    warn(
                        f"field_expression ({field_expression}) supplied without a model.  Unable to determine field "
                        "type and apply default transform ('Lower') that matches the client_sorter.  Server sort may "
                        "differ from client sort.  Defaulting to expression as-is."
                    )
                if isinstance(field_expression, str):
                    self.sort_expression = F(field_expression)
                else:
                    self.sort_expression = field_expression

        if client_sorter is not None:
            self.client_sorter = client_sorter
        elif self.model_field is not None and is_number_field(self.model_field):
            self.client_sorter = self.SORTER_JS_NUMERIC
        else:
            # Rely on Django for the sorting.  Don't apply a javascript sort on top of it.
            self.client_sorter = self.SORTER_JS_ALPHANUMERIC

        self.client_mode = client_mode

    def __str__(self) -> str:
        return self.sorter

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.__dict__ == other.__dict__

    @property
    def sorter(self):
        return self.client_sorter if self.client_mode else self.server_sorter

    def set_client_mode(self, enabled: bool = True):
        self.client_mode = enabled

    def set_server_mode(self, enabled: bool = True):
        self.client_mode = not enabled

    @classproperty
    def javascript(cls) -> str:  # pylint: disable=no-self-argument
        """Returns an HTML script tag whose source points to cls.JAVASCRIPT.

        Example:
            # BSTListView.__init__
                self.sorter = BSTSorter(field=Model.name.field)  # name is a CharField
            # BSTListView.get_context_data
                context["sorter"] = self.sorter
            # Template
                {{ sorter.javascript }}
            # Template result (assuming settings.STATIC_URL = "static/")
                <script src='static/js/bst_list_view/sorter.js'></script> -->
        """
        return mark_safe(f"<script src='{static(cls.JAVASCRIPT)}'></script>")
