from __future__ import annotations

from typing import List, Optional, Type, Union, cast

from django.db import ProgrammingError
from django.db.models import Field, Model
from django.db.models.expressions import Combinable

from DataRepo.models.utilities import (
    field_path_to_model_path,
    is_many_related_to_parent,
    is_many_related_to_root,
)
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn
from DataRepo.views.models.bst.column.sorter.many_related_field import (
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

    # These ellipsis versions are used when the number of values displayed has been limited
    more_msg = "... (+{0} more)"
    more_unknown_msg = "..."
    # TODO: Add a way to link these 'more' strings to a model's list view (with filters/sorting, if one exists)

    # This template handles the list attribute added to the root model record
    value_template: str = "models/bst/value_list.html"

    # These are used in the construction of attributes off the root model record
    _list_attr_tail = "_mm_list"
    _count_attr_tail = "_mm_count"

    def __init__(
        self,
        *args,
        list_attr_name: Optional[str] = None,
        count_attr_name: Optional[str] = None,
        delim: Optional[str] = delimiter,
        limit: int = limit,
        sort_expression: Optional[Union[Combinable, Field, str]] = None,
        asc: Optional[bool] = None,
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
            asc (Optional[bool]) [BSTManyRelatedColumn.ascending]: Initial sort of the delimited values in each table
                cell.
                NOTE: This will be changed when the user sorts based on this column.  If the initial value here is
                False, the first time a user sorts the table based on this column, it will be changed to True to match
                the table row sort.
        Exceptions:
            None
        Returns:
            None
        """
        self.list_attr_name: str
        self.count_attr_name: str
        self.delim = delim
        self.limit = limit
        self.sort_expression = sort_expression
        self.asc = asc if asc is not None else self.ascending
        self._in_group = False  # Changed by BSTColumnGroup

        # True if the field is a foreign key to the many-related model.  This is different from self.is_fk, which can
        # concurrently be True or False, as it relates to the field at the very end of the path.
        self.is_many_fk: bool  # Set below

        # Get the required superclass constructor arguments we need for checks
        field_path: str = cast(str, args[0])
        model: Type[Model] = cast(Type[Model], args[1])

        if not is_many_related_to_root(field_path, model):
            raise ValueError(
                f"field_path '{field_path}' must be many-related to model '{model.__name__}'."
            )

        self.many_related_model_path = field_path_to_model_path(
            model, field_path, many_related=True
        )

        self.is_many_fk = self.many_related_model_path == field_path

        # Create attribute names to use to assign a list of related model objects and their count to the root model
        if list_attr_name is None or count_attr_name is None:

            if isinstance(list_attr_name, str):
                self.list_attr_name = list_attr_name
            else:
                self.list_attr_name = self.get_list_name(field_path, model)

            if isinstance(count_attr_name, str):
                self.count_attr_name = count_attr_name
            else:
                # We only want 1 count for the many-related model records
                self.count_attr_name = self.get_count_name(
                    self.many_related_model_path, model
                )

        super().__init__(*args, **kwargs)

        if self.sort_expression is None:
            self.sort_expression = self.display_field_path

        # Apply specific type to self.sorter (which was initialized via super().__init__)
        self.sorter = cast(BSTManyRelatedSorter, self.sorter)

    @classmethod
    def get_attr_stub(cls, path: str, model: Type[Model]) -> str:
        """Creates the unique portion of an attribute name to be applied to the model objects of the root model in a
        queryset.

        Args:
            path (str): A dunderscore-delimited path.
            model (Type[Model])
        Exceptions:
            None
        Returns:
            stub (str)
        """
        many_related_model_path = field_path_to_model_path(
            model, path, many_related=True
        )

        # Create attribute names for many-related values and a many-related count
        if many_related_model_path == path:
            stub = path.split("__")[-1]
        else:
            stub = "_".join(path.split("__")[-2:])

        return stub

    @classmethod
    def get_count_name(cls, many_related_model_path: str, model: Type[Model]) -> str:
        """Creates an attribute name to be applied to the model objects of the root model in a queryset, denoting the
        count of the unique values associated with the root model record.

        Args:
            many_related_model_path (str): A dunderscore-delimited path.
            model (Type[Model])
        Exceptions:
            None
        Returns:
            stub (str)
        """
        if not is_many_related_to_parent(many_related_model_path, model):
            raise ProgrammingError(
                "get_count_name must only be used for many_related_model_path, but the last field in the path "
                f"'{many_related_model_path}' is not many-related to its parent field."
            )
        return cls.get_attr_stub(many_related_model_path, model) + cls._count_attr_tail

    @classmethod
    def get_list_name(cls, field_path: str, model: Type[Model]) -> str:
        """Creates an attribute name to be applied to the model objects of the root model in a queryset, denoting a
        list of values associated with the root model record.

        Args:
            field_path (str): A dunderscore-delimited path.
            model (Type[Model])
        Exceptions:
            None
        Returns:
            stub (str)
        """
        return cls.get_attr_stub(field_path, model) + cls._list_attr_tail

    def set_list_attr(self, rec: Model, subrecs: List[Model]):
        """Adds a list of supplied many-related records as an attribute to the supplied root model record.  Truncates
        the list down to the size indicated self.limit and appends an ellipsis (if there are more records not shown).

        Args:
            rec (Model): A record from self.model.
            subrecs (List[Model]): A list of Model field values (or Model objects) from a model that is many-related
                with self.model.
        Exceptions:
            ProgrammingError when there is an attribute name collision
        Returns:
            None
        """
        if not isinstance(rec, self.model):
            raise ProgrammingError(
                f"rec must be of type {self.model.__name__}, not  {type(rec).__name__}"
            )
        if len(subrecs) >= (self.limit + 1):
            n = self.limit + 1
            limited_subrecs = subrecs[0:n]
            if hasattr(rec, self.count_attr_name):
                count = getattr(rec, self.count_attr_name)
                limited_subrecs[-1] = self.more_msg.format(count - self.limit)
            else:
                # The derived class must've eliminated the {colname}_mm_count column, so we cannot tell them how many
                # there are left to display.
                limited_subrecs[-1] = self.more_unknown_msg
        else:
            limited_subrecs = subrecs

        if hasattr(rec, self.list_attr_name):
            raise ProgrammingError(
                f"Attribute '{self.list_attr_name}' already exists on '{self.model.__name__}' object."
            )

        setattr(rec, self.list_attr_name, limited_subrecs)

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
