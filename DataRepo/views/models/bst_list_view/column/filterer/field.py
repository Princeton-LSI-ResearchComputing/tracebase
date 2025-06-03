from typing import Type

from django.db.models import Model

from DataRepo.models.utilities import (
    field_path_to_field,
    get_distinct_fields,
    is_many_related_to_root,
    is_number_field,
)
from DataRepo.views.models.bst_list_view.column.filterer.base import (
    BSTBaseFilterer,
)


class BSTFilterer(BSTBaseFilterer):
    """This class manages filtering of rows/objects based on a column in the Bootstrap Table for a model field of a
    ListView.

    - The default _server_filterer for string fields will be icontains (i.e. case insensitive), otherwise (effectively)
      exact.
    - The default client-side Bootstrap Table filter-control will be "select" if the field has "choices".
    - The client_filterer will be "strictFilterer" if the field has "choices" and is not many-related.
    """

    is_annotation = False

    def __init__(
        self,
        field_path: str,
        model: Type[Model],
        *args,
        distinct_choices: bool = False,
        **kwargs,
    ):
        """Constructor.

        Args:
            field (Optional[str]): A model field path or annotation field, used (with model) to derive the Field in
                order to automatically select a client_filterer and input_method (if model is provided - if model is not
                provided, it is assumed to be an annotation).
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
        Exceptions:
            ValueError when an argument is invalid.
        Returns:
            None
        """

        self.field_path = field_path
        self.model = model
        self.distinct_choices = distinct_choices

        name = field_path  # The field_path argument takes the place of the superclass's name argument
        choices = kwargs.get("choices")
        client_filterer = kwargs.get("client_filterer")
        _server_filterer = kwargs.get("_server_filterer")

        try:
            self.field = field_path_to_field(model, field_path)
            self.many_related = is_many_related_to_root(field_path, model)
        except AttributeError as ae:
            if "__" not in field_path:
                raise AttributeError(
                    f"{ae}\nIf field_path '{field_path}' is an annotation, use BSTAnnotFilterer"
                ).with_traceback(ae.__traceback__)
            else:
                raise ae

        if choices is not None and len(choices) > 0 and distinct_choices:
            raise ValueError(
                f"choices {choices} and distinct_choices '{distinct_choices}' are mutually exclusive."
            )
        elif (choices is None or len(choices) == 0) and distinct_choices:
            # If field_path is a foreign key, the way to construct the query is by getting all of the related model's
            # ordering fields, to avoid errors
            distinct_fields = get_distinct_fields(self.model, self.field_path)
            choices = {}
            for val in list(
                self.model.objects.order_by(*distinct_fields)
                .distinct(*distinct_fields)
                .values_list(self.field_path, flat=True)
            ):
                # The displayed and searchable values will be how the related model's objects render in string context
                choices[str(val)] = str(val)
        elif (
            (choices is None or len(choices) == 0)
            and hasattr(self.field, "choices")
            and self.field.choices is not None
            and len(self.field.choices) > 0
        ):
            choices = dict(self.field.choices)
        elif choices is not None and len(choices) == 0:
            choices = None

        if client_filterer is None:
            if _server_filterer is not None:
                _server_filterer = self.process_server_filterer(_server_filterer)
                try:
                    server_filterer_key = self.SERVER_FILTERERS.get_key(
                        _server_filterer
                    )
                    client_filterer = getattr(
                        self.CLIENT_FILTERERS, server_filterer_key
                    )
                except ValueError:
                    # We cannot match a custom server filterer, so just disable client filtereing to guarantee matching
                    # behavior, albeit inefficient when the user loads all rows.
                    client_filterer = self.CLIENT_FILTERERS.NONE
            else:
                # Base the default on the field type, the input method (via choices), and the field's relationship with
                # the root model
                client_filterer = self.get_default_client_filterer(choices)

        if _server_filterer is None:
            try:
                client_filterer_key = self.CLIENT_FILTERERS.get_key(client_filterer)
                _server_filterer = getattr(self.SERVER_FILTERERS, client_filterer_key)
            except ValueError:
                # Allow the base class to select a default, because if we explicitly select one, it will conflict and
                # raise a ValueError.  Allowing the base class to select one based on the input method and will simply
                # result in a warning about potentially different behavior.
                pass

        kwargs.update(
            {
                "choices": choices,
                "client_filterer": client_filterer,
                "_server_filterer": _server_filterer,
            }
        )

        super().__init__(name, *args, **kwargs)

    def get_default_client_filterer(self, choices):
        """Returns a default client_filterer.

        - The client filterer should require a full match if the field is numeric (because it match no sense to look for
          specific digits anywhere in a number).
        - The client filterer should not require a full match when the field is many-related with the root model,
          because the BST code delimits multiple values in a column.
        - The client filterer should require a full match if the input method will be a select list (except when the
          column is many-related).  (And the input_method will be "select" if choices are provided.)

        Args:
            choices (Optional[Any]): The choices that were supplied to the superclass constructor.
        Exceptions:
            None
        Returns:
            (str): A value from self.CLIENT_FILTERERS
        """
        select_list = (
            self.distinct_choices
            or choices is not None
            or (self.field.choices is not None and len(self.field.choices) > 0)
        )
        if select_list:
            return (
                self.CLIENT_FILTERERS.STRICT_MULTIPLE
                if self.many_related
                else self.CLIENT_FILTERERS.STRICT_SINGLE
            )

        return (
            self.CLIENT_FILTERERS.CONTAINS
            if not is_number_field(self.field)
            else self.CLIENT_FILTERERS.STRICT_SINGLE
        )
