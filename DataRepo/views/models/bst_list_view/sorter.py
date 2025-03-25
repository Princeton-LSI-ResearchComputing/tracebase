from warnings import warn
from typing import Optional, Union

from django.conf import settings
from django.db.models import F, Field, Model
from django.db.models.expressions import Combinable, Expression
from django.db.models.functions import Lower
from django.templatetags.static import static
from django.utils.functional import classproperty
from django.utils.safestring import mark_safe

from DataRepo.models.utilities import field_path_to_field, is_number_field, is_string_field, resolve_field, resolve_field_path


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
        model: Optional[Model] = None
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
                str (e.g. a field path or name) used for automatically selecting a client_sorter and transform.
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
        self.model = model
        # Set field_path, field, and sort_expression
        if isinstance(field_expression, Field):
            self.field_path = resolve_field_path(field_expression)
            self.field = field_expression
            if not is_number_field(self.field):
                self.sort_expression = Lower(self.field.name)
            else:
                self.sort_expression = F(field_expression.name)
        else:
            self.field_path = resolve_field_path(field_expression)
            if self.model is not None:
                self.field = field_path_to_field(self.model, self.field_path)
                if (
                    (isinstance(field_expression, str) or not isinstance(field_expression, Expression))
                    and not is_number_field(self.field)
                ):
                    self.sort_expression = Lower(self.field_path)
                elif isinstance(field_expression, str):
                    self.sort_expression = F(field_expression)
                else:
                    self.sort_expression = field_expression
            else:
                self.field = None
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
        elif self.field is not None and is_number_field(self.field):
            self.client_sorter = self.SORTER_JS_NUMERIC
        else:
            # Rely on Django for the sorting.  Don't apply a javascript sort on top of it.
            self.client_sorter = self.SORTER_JS_ALPHANUMERIC

        self.client_mode = client_mode

    def __str__(self) -> str:
        return self.sorter

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
