from __future__ import annotations

from typing import Optional, Type, Union, cast

from django.db.models import Field, Model
from django.db.models.expressions import Combinable

from DataRepo.models.utilities import field_path_to_model_path
from DataRepo.views.models.bst_list_view.column.related_field import (
    BSTRelatedColumn,
)
from DataRepo.views.models.bst_list_view.column.sorter.many_related_field import (
    BSTManyRelatedSorter,
)


class BSTManyRelatedColumn(BSTRelatedColumn):
    """Class to represent the interface between a bootstrap column and a many-related Model field.  Many-related fields
    are displayed as delimited values in a table cell.

    Usage:
        You can create a simple model field column using field paths like this:

            studycol = BSTManyRelatedColumn("studies", model=Animal)
            labelcol = BSTManyRelatedColumn("labels__element", model=Animal)

        Use django "field path lookups" relative to the base model.
        See https://docs.djangoproject.com/en/5.1/topics/db/queries/#lookups-that-span-relationships

        This creates attribute names that can be used to assign many-related data to root model objects in the queryset.
        For example:

            study_list = []
            for object in BSTListView.paginate_queryset():
                for study in object.studies.distinct():
                    study_list.append(study)
                setattr(object, studycol.list_attr_name, study_list)

        Then, in the template, you can iterate over those objects:

            {% for study in object|get_attr:studycol.list_attr_name %}
                <a href="{{ study|get_detail_url }}">{{ study }}</a>{% if not forloop.last %}{{ studycol.delim }}<br>
                {% endif %}
            {% endfor %}
    """

    is_many_related: bool = True
    delimiter: str = "; "
    limit: int = 3
    ascending: bool = True
    more_msg = "... (+{0} more)"
    list_attr_tail = "_mm_list"
    count_attr_tail = "_mm_count"

    def __init__(
        self,
        *args,
        list_attr_name: Optional[str] = None,
        count_attr_name: Optional[str] = None,
        delim: Optional[str] = delimiter,
        limit: int = limit,
        sort_expression: Optional[Union[Combinable, Field, str]] = None,
        asc: bool = ascending,
        **kwargs,
    ):
        """Defines options used to populate the Bootstrap Table columns for a BootstrapListView and a single reference
        model.

        Args:
            list_attr_name (Optional[str]) [auto]: The name of the attribute to create on each queryset object that will
                hold model objects or field values from the many-related model.
            count_attr_name (Optional[str]) [auto]: The name of the attribute to create on each queryset object that
                will hold a count of the many-related objects/field-values.
            delim (Optional[str]) [BSTManyRelatedColumn.delimiter]: The delimiter used to join values from the related
                model (NOTE: the template may add delimiting HTML).
            limit (int) [BSTManyRelatedColumn.limit]: A limit to the number of related model objects/field-values to
                include.  Set to 0 for unlimited.
            sort_expression (Optional[Union[Combinable, Field, str]]) [auto]: Initial 'field' used to sort the delimited
                values in the resulting Bootstrap Table.  The purpose of this argument is to provide a means of sorting
                multiple fields (all from the same many-related model) the same, so that they visually align.  The
                default value is the display_field (which is based on field_path).
            asc (bool) [BSTManyRelatedColumn.ascending]: Initial sort of the delimited values in each table cell.
                NOTE: This will be changed when the user sorts based on this column.  If the initial value here is
                False, the first time a user sorts the table based on this column, it will be changed to True to match
                the table row sort.
        Exceptions:
            None
        Returns:
            None
        """
        self.list_attr_name = list_attr_name
        self.count_attr_name = count_attr_name
        self.delim = delim
        self.limit = limit
        self.sort_expression = sort_expression
        self.asc = asc

        # Create attribute names to use to assign a list of related model objects and their count to the root model
        if self.list_attr_name is None or self.count_attr_name is None:
            # Get the required superclass constructor arguments we need for checks
            field_path: str = cast(str, args[0])
            model: Type[Model] = cast(Type[Model], args[1])

            self.related_model_path = field_path_to_model_path(model, field_path)
            if self.related_model_path == field_path:
                stub = field_path.split("__")[-1]
            else:
                stub = "_".join(field_path.split("__")[-2:])

            if self.list_attr_name is None:
                self.list_attr_name = stub + self.list_attr_tail

            if self.count_attr_name is None:
                self.count_attr_name = stub + self.count_attr_tail

        # NOTE: I don't think setting related_model_path to the last many-related model is necessary for
        # BSTManyRelatedColumn.  I only need to do that for BSTColumnGroup.
        # self.related_model_path = field_path_to_model_path(model, field_path, many_related=True)

        super().__init__(*args, **kwargs)

        if self.sort_expression is None:
            self.sort_expression = self.display_field_path

    def set_related_model_path(self, related_model_path: str):
        """This validates and sets self.related_model_path, overwriting any previous value.

        The intended purpose of this method is for use in BSTColumnGroup, in order to control the delimited value
        sorting such that fields under the same related model are sorted the same way.

        Args:
            related_model_path (str): A dunderscore-delimited field path ending in a many-related model foreign key
        Exceptions:
            ValueError when self.field_path is invalid
        Returns:
            None
        """
        self.related_model_path = related_model_path

        if (
            related_model_path is None
            or not isinstance(related_model_path, str)
            or related_model_path == ""
        ):
            raise ValueError(
                f"related_model_path '{related_model_path}' must be a non-empty string."
            )
        elif not self.field_path.startswith(related_model_path):
            raise ValueError(
                f"The field path '{self.field_path}' must start with the supplied related_model_path "
                f"'{related_model_path}'."
            )

    def create_sorter(
        self, field: Optional[Union[Combinable, Field, str]] = None, **kwargs
    ) -> BSTManyRelatedSorter:

        if field is not None:
            field_expression = field
        elif self.sort_expression is not None:
            field_expression = self.sort_expression
        elif self.display_field_path is not None:
            field_expression = self.display_field_path
        else:
            field_expression = self.name

        kwargs.update(
            {
                "name": kwargs.get("name", self.name),
                "asc": kwargs.get("asc", self.asc),
            }
        )

        return BSTManyRelatedSorter(field_expression, self.model, **kwargs)
