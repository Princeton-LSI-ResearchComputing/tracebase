from typing import Optional, Type
from warnings import warn

from django.db.models import Model
from django.db.models.expressions import Combinable

from DataRepo.models.utilities import field_path_to_field, resolve_field_path
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.filterer.annotation import (
    BSTAnnotFilterer,
)
from DataRepo.views.models.bst.column.sorter.annotation import BSTAnnotSorter


class BSTAnnotColumn(BSTBaseColumn):
    """Class to extend BSTColumn to support annotations.

    BSTAnnotColumn sets 'name' to the name of the annotation field, which is used for sorting and filtering based on the
    column values.

    Note that in order for some BSTColumns' search and sort to work as expected, a BSTAnnotColumn should be used to
    convert them to a simple string or number annotation that is compatible with django's annotate method.

    For example, as a DateTimeField, imported_timestamp, will sort correctly on the server side, but Bootstrap Table
    will sort the page's worth of results using alphanumeric sorting.  Filtering will also be quirky, because of the
    differences between how the database compares a table field that is a datetime to a search string using LIKE versus
    Django's rendering of a datetime in a template.

    Specifically, postgres does not pad a date with zeroes, but django does (by default), so if you filter the column
    based on what you see (a date with a padded '0'), you will not get what you expect from the database.

    You can make the searching and sorting behavior consistent by supplying a function using the converter argument in
    the constructor, like this:

        BSTAnnotColumn(
            "imported_timestamp_str",
            converter=Func(
                F("imported_timestamp"),
                Value("YYYY-MM-DD HH:MI a.m."),
                output_field=CharField(),
                function="to_char",
            ),
        )

    See the BSTBaseColumn docstring for examples on how to customize filtering and sorting behavior.

    The BSTSorter and BSTFilterer provided by the base class will use the annotation field for their operations.
    """

    # Overrides BSTBaseColumn.is_annotation
    is_annotation = True

    def __init__(
        self,
        name: str,
        converter: Combinable,
        model: Optional[Type[Model]] = None,
        **kwargs,
    ):
        """Constructor.

        Args:
            name (str): The name of the annotation (and name of the column).
            converter (Combinable): A method to combine, transform, or create database field values, e.g. to a
                CharField.  Converting to a CharField for example, can be necessary for searching and filtering because
                BST only does substring searches.  Note that BSTManyRelatedColumn uses an annotation/converter behind
                the scenes for many-related model fields to prevent sorting from increasing the number of resulting
                rows.
                Recommendations: Refrain from using Coalesce, as its performance is slow in certain contexts.  Consider
                    using a converter that utilizes Case/When instead.
                Example Functions to use (non-comprehensive):
                    Regular fields:
                        Lower
                        Upper
                        etc.
                    Many-related fields:
                        Count
                        Min
                        Max
                        etc.
                    Combining multiple fields:
                        Concat
                        Case/When
                        Coalesce (see recommendations above)
                        etc.
                    Build your own:
                        Func
                        etc.
            model (Optional[Type[Model]]): If provided, an attempt will be made to resolve the field from the converter.
                If exactly 1 exists, it will be used to set (or prepend to) the tooltip.
        Exceptions:
            None
        Returns:
            None
        """
        self.converter = converter
        self.model = model

        # If we have a model, see if we can extract a field from the combinable in order to populate the tooltip with
        # the field's help_text
        if model is not None:
            try:
                field_path = resolve_field_path(converter)
            except ValueError as ve:
                field_path = None
                warn(
                    f"Unable to get help_text from field from model '{model.__name__}' in annotation '{name}' "
                    f"expression '{converter}'.  {ve}"
                )
            if field_path is not None:
                field = field_path_to_field(model, field_path)
                if field.help_text is not None:
                    new_tooltip = field.help_text
                    if "tooltip" in kwargs.keys() and kwargs["tooltip"] is not None:
                        new_tooltip += "\n\n" + kwargs["tooltip"]
                    kwargs.update({"tooltip": new_tooltip})

        super().__init__(name, **kwargs)

    def create_sorter(self, **kwargs) -> BSTAnnotSorter:
        if "_server_sorter" not in kwargs.keys() or kwargs["_server_sorter"] is None:
            kwargs["_server_sorter"] = (
                BSTAnnotSorter.get_server_sorter_matching_expression(self.converter)
            )
        if "name" not in kwargs.keys() or kwargs["name"] is None:
            kwargs["name"] = self.name
        return BSTAnnotSorter(self.converter, **kwargs)

    def create_filterer(
        self, field: Optional[str] = None, **kwargs
    ) -> BSTAnnotFilterer:
        field_path = field if field is not None else self.name
        return BSTAnnotFilterer(field_path, **kwargs)
