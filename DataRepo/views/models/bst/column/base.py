from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Union
from warnings import warn

from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.views.models.bst.column.filterer.base import BSTBaseFilterer
from DataRepo.views.models.bst.column.sorter.base import BSTBaseSorter


class BSTBaseColumn(ABC):
    """Abstract base class to represent bootstrap table column and a Model object.

    See the following derived classes for usage information:

        BSTColumn (for a regular Model Field)
        BSTRelatedColumn (adds the ability to link non-foreign key fields in related models to their detail pages)
        BSTManyRelatedColumn (for delimited-value columns)
        BSTAnnotColumn (for an annotation attached to the root Model object)
        BSTColumnGroup (used to control the sort of delimited values in multiple BSTManyRelatedColumns from the same
            related model)
    """

    is_annotation: bool = False
    # See: BSTAnnotColumn (For rendering an annotation)

    is_related: bool = False
    # See: BSTRelatedColumn (For rendering a related foreign key using the related object)

    is_many_related: bool = False
    # See: BSTManyRelatedColumn (For rendering a many-related foreign key using the related object)

    def __init__(
        self,
        name: str,
        header: Optional[str] = None,
        searchable: Optional[bool] = None,
        sortable: Optional[bool] = None,
        visible: bool = True,
        exported: bool = True,
        linked: bool = False,
        sorter: Optional[Union[str, BSTBaseSorter]] = None,
        filterer: Optional[Union[str, BSTBaseFilterer]] = None,
        th_template: str = "models/bst/th.html",
        td_template: str = "models/bst/bst_td.html",
        value_template: str = "models/bst/bst_value.html",
    ):
        """Defines options used to customize bootstrap table columns.

        Args:
            name (str): The column name used by Bootstrap Table to uniquely identify a column, particularly for sorting
                and filtering operations.
            header (Optional[str]) [auto]: The column header to display in the template.  Will be automatically
                generated using the title case conversion of the last (2, if present) dunderscore-delimited name values.

            searchable (Optional[bool]) [auto]: Whether or not a column is searchable.  This affects whether the column
                is searched as a part of the table's search box and whether the column filter input will be enabled.
                The default is based on whether the column is a foreign key or not, because Django turns keys into model
                objects that do not render the actual numeric key value, so what the user sees would not behave as
                expected when searched.
            sortable (Optional[bool]) [auto]: Whether or not a column's values can be used to sort the table rows.  The
                default is based on whether the column is a foreign key or not, because Django turns keys into model
                objects that do not render the actual numeric key value, so what the user sees would not behave as
                expected when sorted.
            visible (bool) [True]: Controls whether a column is initially visible.
            exported (bool) [True]: Adds to BST's exportOptions' ignoreColumn attribute if False.
            linked (bool) [False]: Whether or not the value in the column should link to a detail page for the model
                record the row represents.
                NOTE: The model must have a "get_absolute_url" method.  Checked in the template.

            sorter (Optional[Union[str, BSTBaseSorter]]) [auto]: If the value is a str, must be in
                BSTBaseSorter.CLIENT_SORTERS.  Default will be based on the name and the sorter (if it is a str).
            filterer (Optional[Union[str, BSTBaseFilterer]]) [auto]: If the value is a str, must be in
                BSTBaseFilterer.CLIENT_FILTERERS.  Default will be based on the name and the filterer (if it is a str).

            th_template (str) ["models/bst/th.html"]: Template path to an html file used to render the th
                element for the column header.  This must handle the initial sort field, search term, and filter term.
            td_template (str) ["models/bst/bst_td.html"]: Template path to an html file used to render the td
                element for a column cell.
            value_template (str) ["models/bst/bst_value.html"]: Template path to an html file used to render
                the value inside the td element for a column cell.
        Exceptions:
            ValueError when arguments are invalid
            TypeError when arguments are invalid
            AttributeError when 'name' not set by BSTAnnotColumn
        Returns:
            None
        """

        self.name = name
        self.header = header
        self.searchable = searchable
        self.sortable = sortable
        self.visible = visible
        self.exported = exported
        self.linked = linked
        self.th_template = th_template
        self.td_template = td_template
        self.value_template = value_template

        # Initialized below
        self.sorter: BSTBaseSorter
        self.filterer: BSTBaseFilterer

        # Modified by BSTColumnGroup
        self._in_group = False

        if not hasattr(self, "is_fk") or getattr(self, "is_fk", None) is None:
            self.is_fk = False

        if self.linked:
            if self.is_related:
                raise ValueError(
                    f"Column {self.name} cannot be linked to the root model when it is from a related model."
                )
            elif self.is_fk:
                raise ValueError(
                    f"Column {self.name} cannot be linked to the root model when it is a foreign key."
                )

        # Handle the defaults for searchable and sortable
        if not self.is_related:
            # The defaults are False if this is not a related column.  In fact, they cannot be True if the fiueld is a
            # foreign key, because Django turns foreign key fields into model objects that do not render the actual
            # numeric key value, so what the user sees would not behave as expected when searched or sorted.
            if self.is_fk:
                if self.searchable is True:
                    raise ValueError(
                        f"Column {self.name} cannot be searchable when it is a foreign key unless the column class is "
                        "either BSTRelatedColumn or BSTManyRelatedColumn."
                    )
                if self.sortable is True:
                    raise ValueError(
                        f"Column {self.name} cannot be sortable when it is a foreign key unless the column class is "
                        "either BSTRelatedColumn or BSTManyRelatedColumn."
                    )
                self.searchable = False
                self.sortable = False
            else:
                if self.searchable is None:
                    self.searchable = True
                if self.sortable is None:
                    self.sortable = True
        else:
            # The defaults are True if this is a related column
            if self.searchable is None:
                self.searchable = True
            if self.sortable is None:
                self.sortable = True

        if self.header is None:
            self.header = self.generate_header()

        # NOTE: self.name will be either a field_path or an annotation field.
        # NOTE: We set a sorter even if the field is not sortable.
        if sorter is None:
            self.sorter = self.create_sorter()
        elif isinstance(sorter, str):
            self.sorter = self.create_sorter(client_sorter=sorter)
        elif isinstance(sorter, BSTBaseSorter):
            self.sorter = sorter
        else:
            raise TypeError(
                f"sorter must be a str or a BSTBaseSorter, not a '{type(sorter).__name__}'."
            )

        # NOTE: self.name will be either a field_path or an annotation field.
        # NOTE: We set a filterer even if the field is not searchable.
        if filterer is None:
            self.filterer = self.create_filterer()
        elif isinstance(filterer, str):
            self.filterer = self.create_filterer(client_filterer=filterer)
        elif isinstance(filterer, BSTBaseFilterer):
            self.filterer = filterer
        else:
            raise TypeError(
                f"filterer must be a str or a BSTBaseFilterer, not a '{type(filterer).__name__}'."
            )

    def __str__(self):
        return self.name

    def __eq__(self, other):
        """This is a convenience override to be able to compare a column name with a column object to see if the object
        is for that column.  It also enables the `in` operator to work between strings and objects.

        Args:
            other (Optional[Union[str, BSTColumn]]): A value to equate with self
                NOTE: Cannot apply this type hint due to mypy superclass requirements that it be 'object'.
        Exceptions:
            NotImplementedError when the type of other is invalid
        Returns:
            (bool)
        """
        if isinstance(other, __class__):  # type: ignore
            if self._in_group != other._in_group:
                warn(
                    "Equating BSTBaseColumns where one is in a group and the other is not will always fail because "
                    "BSTColumnGroup modifies the sorter.",
                    DeveloperWarning,
                )
            return self.__class__ == other.__class__ and self.__dict__ == other.__dict__
        elif isinstance(other, str):
            return self.name == other
        elif other is None:
            return False
        else:
            raise NotImplementedError(
                f"Equivalence of {__class__.__name__} to {type(other).__name__} not implemented."  # type: ignore
            )

    def generate_header(self):
        """Generate a column header from the column name."""
        return underscored_to_title(self.name)

    @abstractmethod
    def create_sorter(self, field=None, **kwargs):
        """Derived classes must define this method to set self.sorter to a BSTBaseSorter"""
        pass

    @abstractmethod
    def create_filterer(self, field=None, **kwargs):
        """Derived classes must define this method to set self.filterer to a BSTBaseFilterer"""
        pass
