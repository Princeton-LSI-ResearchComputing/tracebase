from __future__ import annotations

from abc import ABC
from typing import Optional, Union

from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.views.models.bst_list_view.column.filterer.base import (
    BSTBaseFilterer,
)
from DataRepo.views.models.bst_list_view.column.sorter.base import (
    BSTBaseSorter,
)


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

    is_many_related: bool = False
    # See: BSTManyRelatedColumn (For rendering a many-related foreign key using the related object)

    def __init__(
        self,
        name: str,
        header: Optional[str] = None,
        searchable: bool = True,
        sortable: bool = True,
        visible: bool = True,
        exported: bool = True,
        linked: bool = False,
        sorter: Optional[Union[str, BSTBaseSorter]] = None,
        filterer: Optional[Union[str, BSTBaseFilterer]] = None,
        th_template: str = "models/bst_list_view/th.html",
        td_template: str = "models/bst_list_view/bst_td.html",
        value_template: str = "models/bst_list_view/bst_value.html",
    ):
        """Defines options used to customize bootstrap table columns.

        Args:
            name (str): The column name (used for identifying the correct sorter object).
            header (Optional[str]) [auto]: The column header to display in the template.  Will be automatically
                generated using the title case conversion of the last (2, if present) dunderscore-delimited name values.

            searchable (bool) [True]: Whether or not a column is searchable.  This affects whether the column is
                searched as a part of the table's search box and whether the column filter input will be enabled.
            sortable (bool) [True]: Whether or not a column's values can be used to sort the table rows.
            visible (bool) [True]: Controls whether a column is initially visible.
            exported (bool) [True]: Adds to BST's exportOptions' ignoreColumn attribute if False.
            linked (bool) [False]: Whether or not the value in the column should link to a detail page for the model
                record the row represents.
                NOTE: The model must have a "get_absolute_url" method.  Checked in the template.

            sorter (Optional[Union[str, BSTBaseSorter]]) [auto]: If the value is a str, must be in
                BSTBaseSorter.SORTERS.  Default will be based on the name and the sorter (if it is a str).
            filterer (Optional[Union[str, BSTbaseFilterer]]) [auto]: If the value is a str, must be in
                BSTbaseFilterer.FILTERERS.  Default will be based on the name and the filterer (if it is a str).

            th_template (str) ["models/bst_list_view/th.html"]: Template path to an html file used to render the th
                element for the column header.  This must handle the initial sort field, search term, and filter term.
            td_template (str) ["models/bst_list_view/bst_td.html"]: Template path to an html file used to render the td
                element for a column cell.
            value_template (str) ["models/bst_list_view/bst_value.html"]: Template path to an html file used to render
                the value inside the td element for a column cell.
        Exceptions:
            ValueError when arguments are invalid
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

        if getattr(self, "is_fk", None) is None:
            self.is_fk = False

        if self.linked:
            if self.is_many_related:
                raise ValueError(
                    f"Argument 'linked' must not be true when the column '{self.name}' is many-related."
                )
            elif self.is_fk:
                raise ValueError(
                    f"Argument 'linked' must not be true when the column '{self.name}' is a foreign key."
                )

        if self.header is None:
            self.header = self.generate_header()

        # NOTE: self.name will be either a field_path or an annotation field.
        if sorter is None:
            self.sorter = BSTBaseSorter(name=self.name, sort_expression=self.name)
        elif isinstance(sorter, str):
            self.sorter = BSTBaseSorter(
                name=self.name,
                sort_expression=self.name,
                client_sorter=sorter,
            )
        elif isinstance(sorter, BSTBaseSorter):
            self.sorter = sorter
        else:
            raise ValueError("sorter must be a str or a BSTBaseSorter.")

        # NOTE: self.name will be either a field_path or an annotation field.
        if filterer is None:
            self.filterer = BSTBaseFilterer(name=self.name)
        elif isinstance(filterer, str):
            self.filterer = BSTBaseFilterer(
                name=self.name,
                client_filterer=filterer,
            )
        elif isinstance(filterer, BSTBaseFilterer):
            self.filterer = filterer
        else:
            raise ValueError("filterer must be a str or a BSTBaseFilterer.")

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
            return self.__dict__ == other.__dict__
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
