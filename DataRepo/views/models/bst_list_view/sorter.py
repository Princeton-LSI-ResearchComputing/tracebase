import warnings
from typing import Optional, Union

from django.conf import settings
from django.db.models import Field
from django.db.models.expressions import Combinable
from django.db.models.functions import Lower
from django.templatetags.static import static
from django.utils.functional import classproperty
from django.utils.safestring import mark_safe

from DataRepo.models.utilities import is_number_field, is_string_field


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
        field: Optional[Field] = None,
        client_sorter: Optional[str] = None,
        transform=None,
        client_mode: bool = False,
    ):
        """Construct a BSTSorter object.

        Args:
            field (Optional[Field]): A Model field, used for automatically selecting a sorter and transform.
            client_sorter (Optional[str]) [auto]: The string to set the Bootstrap Table data-sorter attribute.  The
                default is alphanumericSorter if field is not supplied, otherwise if the field type is a number field,
                numericSorter is set.
            transform (Combinable) [BSTSorter.identity]: A method used for transforming the Django ORM order_by field,
                e.g. django.db.models.functions.Lower.  The default is an identity function (i.e. no transform).  Note,
                this should match the behavior of client_sorter.
            client_mode (bool): Set to True if the initial table is not filtered and the page queryset is the same size
                as the total queryset.
        Exceptions:
            ValueError when transform is invalid.
        Returns:
            None
        """
        if transform is not None:
            try:
                if not issubclass(transform, Combinable):
                    raise ValueError(
                        "transform must be a Combinable, e.g. type Transform."
                    )
            except TypeError:
                raise ValueError("transform must be a Combinable, e.g. type Transform.")
            self.transform = transform
        elif is_string_field(field):
            self.transform = Lower
        else:
            self.transform = BSTSorter.identity

        if client_sorter is not None:
            if settings.DEBUG and client_sorter not in self.SORTERS:
                warnings.warn(f"Custom client_sorter '{client_sorter}' supplied.")
            self.client_sorter = client_sorter
        elif is_number_field(field):
            self.client_sorter = self.SORTER_JS_NUMERIC
        else:
            # Rely on Django for the sorting.  Don't apply a javascript sort on top of it.
            self.client_sorter = self.SORTER_JS_ALPHANUMERIC

        self.client_mode = client_mode

    def __str__(self) -> str:
        return self.client_sorter if self.client_mode else self.server_sorter

    def set_client_mode(self, enabled: bool = True):
        self.client_mode = enabled

    def set_server_mode(self, enabled: bool = True):
        self.client_mode = not enabled

    @classmethod
    def identity(cls, expression: Union[str, Combinable]) -> Union[str, Combinable]:
        """identity() needs to work on any combinable or string it is given.  In other words, and value you can give to
        .order_by()"""
        return expression

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
