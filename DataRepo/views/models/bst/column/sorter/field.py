from typing import Optional, Type, Union, cast

from django.db.models import F, Field, Model
from django.db.models.expressions import Combinable

from DataRepo.models.utilities import (
    field_path_to_field,
    is_number_field,
    is_string_field,
    resolve_field_path,
)
from DataRepo.views.models.bst.column.sorter.base import BSTBaseSorter


class BSTSorter(BSTBaseSorter):
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
            {{ sorter.script }}
            <th data-sorter="{% if total|lte:raw_total %}{{ sorter }}{% else %}{{ sorter.client_sorter }}{% endif %}">

        # Template result
            <script src='{settings.STATIC_URL}js/bst/sorter.js'></script>
            <th data-sorter="djangoSorter">

        # BSTListView.get_queryset
            return self.model.objects.order_by(self.sorter.transform("name"))
    """

    is_annotation = False

    def __init__(
        self,
        field_expression: Union[Combinable, Field, str],
        model: Type[Model],
        *args,
        **kwargs,
    ):
        """Constructor.  Extends BSTBaseSorter.__init__.

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
            model (Type[Model]): The root model of the field path, only used if field_expression is/contains a field
                path, to obtain the Field type and set the client_sorter based on it.
        Exceptions:
            ValueError when transform is invalid.
        Returns:
            None
        """
        self.model = model
        self.field_path: str = cast(str, resolve_field_path(field_expression))
        self.field = None
        expression = kwargs.get("expression")
        _server_sorter: Optional[Type[Combinable]] = kwargs.get("_server_sorter")

        if field_expression is None:
            raise ValueError("field_expression must not be None.")
        elif model is None:
            raise ValueError("model must not be None.")

        if expression is not None:
            raise ValueError(
                "expression is not allowed as an argument in this class.  "
                "It is automatically discerned from field_expression."
            )

        # Set self.field
        if isinstance(field_expression, Field):
            self.field = field_expression
        else:
            try:
                self.field = field_path_to_field(self.model, self.field_path)
            except AttributeError as ae:
                if "__" not in self.field_path:
                    raise AttributeError(
                        f"{ae}  If field_path '{self.field_path}' is an annotation, use BSTAnnotFilterer"
                    ).with_traceback(ae.__traceback__)
                else:
                    raise ae

        # Set _server_sorter: The superclass cannot derive the field type from a str or F expression, and cannot handle
        # a Field object.  It can only determine field type from a 'Transform' (a subclass of Combinable) object's
        # output_field attribute, so we set the _server_sorter based on the Model Field.
        if (
            _server_sorter is None
            and self.field is not None
            and (
                # The field_expression is a raw field
                isinstance(field_expression, str)
                or isinstance(field_expression, F)
                or isinstance(field_expression, Field)
            )
        ):
            if is_number_field(self.field) and self.SERVER_SORTERS.NUMERIC is not None:
                _server_sorter = self.SERVER_SORTERS.NUMERIC
            elif (
                is_string_field(self.field)
                and self.SERVER_SORTERS.ALPHANUMERIC is not None
            ):
                _server_sorter = self.SERVER_SORTERS.ALPHANUMERIC
            else:
                _server_sorter = self.SERVER_SORTERS.UNKNOWN

        # Derive expression from field_expression
        if isinstance(field_expression, Field):
            # Set expression: The superclass does not take a Field, so convert it.
            expression = F(self.field.name)
        elif isinstance(field_expression, (Combinable, str)):
            expression = field_expression
        else:
            raise TypeError(
                f"field_expression must be a Union[Combinable, Field, str], not '{type(field_expression).__name__}'."
            )

        kwargs.update({"_server_sorter": _server_sorter})

        super().__init__(expression, *args, **kwargs)
