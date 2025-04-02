from typing import Type

from django.db.models import Model

from DataRepo.models.utilities import (
    field_path_to_field,
    is_many_related_to_root,
    is_number_field,
)
from DataRepo.views.models.bst_list_view.column.filterer.base import (
    BSTBaseFilterer,
)


class BSTFilterer(BSTBaseFilterer):
    """This class manages filtering of rows/objects based on a column in the Bootstrap Table for a model field of a
    ListView.

    - The default server-side Django lookup for string fields will be icontains (i.e. case insensitive), otherwise
      (effectively) exact.
    - The default client-side Bootstrap Table filter-control will be "select" if the field has "choices".
    - The client_filterer will be "strictFilterer" if the field has "choices" and is not many-related.
    """

    is_annotation = False

    def __init__(
        self,
        field_path: str,
        model: Type[Model],
        *args,
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

        name = field_path
        choices = kwargs.get("choices")
        client_filterer = kwargs.get("client_filterer")
        lookup = kwargs.get("lookup")

        try:
            self.field = field_path_to_field(model, field_path)
            self.many_related = is_many_related_to_root(field_path, model)
        except AttributeError as ae:
            if "__" not in field_path:
                raise AttributeError(
                    f"{ae}  If field_path '{field_path}' is an annotation, use BSTAnnotFilterer"
                ).with_traceback(ae.__traceback__)
            else:
                raise ae

        if (
            (choices is None or len(choices) == 0)
            and self.field.choices is not None
            and len(self.field.choices) > 0
        ):
            choices = dict(self.field.choices)
        elif choices is not None and len(choices) == 0:
            choices = None

        if client_filterer is None:
            client_filterer = (
                self.FILTERER_CONTAINS if self._relaxed() else self.FILTERER_STRICT
            )

        if lookup is None:
            lookup = (
                self.LOOKUP_CONTAINS
                if not is_number_field(self.field)
                else self.LOOKUP_STRICT
            )

        kwargs.update(
            {
                "choices": choices,
                "client_filterer": client_filterer,
                "lookup": lookup,
            }
        )

        super().__init__(name, *args, **kwargs)

    def _relaxed(self):
        """Determines whether the default client_filterer should match a substring or not (i.e. should match full
        values).

        - The client filterer should require a full match if the field is numeric (because it match no sense to look for
          specific digits anywhere in a number).
        - The client filterer should not require a full match when the field is many-related with the root model,
          because the BST code delimits multiple values in a column.
        - The client filterer should require a full match if the input method will be a select list (except when the
          column is many-related).  (And the input_method will be "select" if choices are provided.)
        """
        return (
            not is_number_field(self.field)
            or self.many_related
            or self.field.choices is None
            or len(self.field.choices) == 0
        )
