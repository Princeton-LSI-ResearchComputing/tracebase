from __future__ import annotations

from typing import Optional, Type, Union

from django.db.models import Model

from DataRepo.models.utilities import (
    field_path_to_field,
    is_many_related_to_root,
    is_unique_field,
)
from DataRepo.utils.text_utils import camel_to_title, underscored_to_title
from DataRepo.views.models.bst_list_view.filterer import BSTFilterer
from DataRepo.views.models.bst_list_view.sorter import BSTSorter


class BSTColumn:
    """Class to represent the interface between a bootstrap column and a Model field.

    Usage: You can create a simple model field column using field paths like this:

        filecol = BSTColumn("filename", model=ArchiveFile)
        timecol = BSTColumn("imported_timestamp", model=ArchiveFile)
        frmtcol = BSTColumn("data_format__name", model=ArchiveFile)
        typecol = BSTColumn("data_type__name", model=ArchiveFile)

    Use django "field path lookups" relative to the base model.
    See https://docs.djangoproject.com/en/5.1/topics/db/queries/#lookups-that-span-relationships

    Alter whatever settings you want in the constructor calls.  In the BootstrapTableListView's template, all you have
    to do to render the th element for each column is just include the associated generic template:

        {% include filecol.th_template %}
        {% include timecol.th_template %}
        {% include frmtcol.th_template %}
        {% include typecol.th_template %}

    Note that the column headers (by default) will use a title version of the last 2 values in django's dunderscore-
    delimited field path.  For example, the header generated from the above objects would be:

        Filename
        Imported Timestamp
        Data Format Name
        Data Type Name

    You can supply custom templates for the th, td, and value (rendered inside the td element).  To include the td
    element:

        {% include filecol.td_template %}
        {% include timecol.td_template %}
        {% include frmtcol.td_template %}
        {% include typecol.td_template %}

    It's also important to note that in order for search and sort to work as expected, each column should be converted
    to a simple string or number annotation that is compatible with django's annotate method.  To supply the annotation,
    see BSTAnnotColumn.
    """

    is_annotation: bool = False
    # See: BSTAnnotColumn (For rendering an annotation)

    is_fk: bool = False
    # See: BSTRelatedColumn (For rendering a one-related foreign key using the related object)

    is_many_related: bool = False
    # See: BSTManyRelatedColumn (For rendering a many-related foreign key using the related object)

    def __init__(
        self,
        model: Type[Model],
        field_path: Optional[str],
        header: Optional[str] = None,
        searchable: bool = True,
        sortable: bool = True,
        visible: bool = True,
        exported: bool = True,
        link: bool = False,
        sorter: Optional[Union[str, BSTSorter]] = None,
        filterer: Optional[Union[str, BSTFilterer]] = None,
        th_template: str = "models/bst_list_view/th.html",
        td_template: str = "models/bst_list_view/bst_td.html",
        value_template: str = "models/bst_list_view/bst_value.html",
    ):
        """Defines options used to populate the bootstrap table columns for a BootstrapListView and a single reference
        model.

        Args:
            model (Type[Model]): Model class that the field_path starts from.
            field_path (Optional[str]): Name of the database field (including the field path) corresponding to the
                column.  A value must be supplied, but that value may be None (to support derived classes that do not
                use it).
                NOTE: Adding a many-related field will increase the number of resulting rows in the table.  See
                BSTManyRelatedColumn to prevent this (and display many-related records as delimited values).
            header (Optional[str]) [auto]: The column header to display in the template.  Will be automatically
                generated using the title case conversion of the last (2, if present) dunderscore-delimited name values.

            searchable (bool) [True]: Whether or not a column is searchable.  This affects whether the column is
                searched as a part of the table's search box and whether the column filter input will be enabled.
            sortable (bool) [True]: Whether or not a column's values can be used to sort the table rows.
            visible (bool) [True]: Controls whether a column is initially visible.
            exported (bool) [True]: Adds to BST's exportOptions' ignoreColumn attribute if False.
            link (bool) [False]: Whether or not to link the value in the column to a detail page for the model record
                the row represents.  The model must have a "get_absolute_url" method.

            sorter (Optional[Union[str, BSTSorter]]) [auto]: If the value is a str, must be in BSTSorter.SORTERS.
                Default will be based on the model field type.
            filterer (Optional[Union[str, BSTFilterer]]) [auto]: If the value is a str, must be in
                BSTFilterer.FILTERERS.  Default will be based on the model field type.

            th_template (str) ["models/bst_list_view/th.html"]: Template path to an html file used to render the th
                element for the column header.  This must handle the initial sort field, search term, and filter term.
            td_template (str) ["models/bst_list_view/bst_td.html"]: Template path to an html file used to render the td
                element for a column cell.
            value_template (str) ["models/bst_list_view/bst_value.html"]: Template path to an html file used to render
                the value inside the td element for a column cell.
        Exceptions:
            ValueError when arguments are invalid
        Returns:
            None
        """

        self.model = model
        self.field_path = field_path
        self.header = header
        self.searchable = searchable
        self.sortable = sortable
        self.visible = visible
        self.exported = exported
        self.link = link
        self.th_template = th_template
        self.td_template = td_template
        self.value_template = value_template
        # Initialized below
        self.sorter: BSTSorter
        self.filterer: BSTFilterer
        self.name: str  # Used in derived classes

        if self.field_path is None and not self.is_annotation:
            raise ValueError(
                "field_path is required for non-annotation fields.  Use BSTAnnotColumn for annotation fields."
            )

        if not hasattr(self, "name") or self.name is None:
            if self.is_annotation:
                raise ValueError("name not set by BSTAnnotColumn.")
            if self.field_path is not None:
                self.name = self.field_path
            else:
                raise ValueError("field_path is required (when not a BSTAnnotColumn).")

        if self.header is None:
            self.header = self.generate_header()

        if self.field_path is not None:
            is_many_related = is_many_related_to_root(self.field_path, self.model)
            if is_many_related and not self.is_many_related and not self.is_annotation:
                raise ValueError(
                    f"field_path '{field_path}' must not be many-related with model '{self.model.__name__}'.  Use "
                    "BSTAnnotColumn or BSTManyRelatedColumn instead."
                )

        if self.link:
            if self.field_path is None:
                raise ValueError("link must not be true when field_path is None.")
            elif is_many_related:
                raise ValueError(
                    f"link must not be true when field_path '{field_path}' is many-related with model "
                    f"'{self.model.__name__}'."
                )
            elif "__" in self.field_path:
                raise ValueError(
                    f"link must not be true when field_path '{field_path}' passes through a related model."
                )
            elif not hasattr(self.model, "get_absolute_url"):
                raise ValueError(
                    f"link must not be true when model '{self.model.__name__}' does not have a 'get_absolute_url' "
                    "method."
                )

        if sorter is None:
            self.sorter = BSTSorter(self.field_path, model=self.model)
        elif isinstance(sorter, str):
            self.sorter = BSTSorter(
                self.field_path, model=self.model, client_sorter=sorter
            )
        elif isinstance(sorter, BSTSorter):
            self.sorter = sorter
        else:
            raise ValueError("sorter must be a str or a BSTSorter.")

        if filterer is None:
            self.filterer = BSTFilterer(model=self.model, field_path=self.field_path)
        elif isinstance(filterer, str):
            self.filterer = BSTFilterer(
                model=self.model,
                field_path=self.field_path,
                client_filterer=filterer,
            )
        elif isinstance(filterer, BSTFilterer):
            self.filterer = filterer
        else:
            raise ValueError("filterer must be a str or a BSTFilterer.")

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
        """Generate a column header from the field_path, model name, or column name.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # If this column is an annotation, use self.name
        if self.is_annotation is True:
            return underscored_to_title(self.name)

        # Get the field
        field = field_path_to_field(self.model, self.field_path)

        # If the field has a verbose name different from name, use it
        if field.name != field.verbose_name:
            if any(c.isupper() for c in field.verbose_name):
                # If the field has a verbose name with caps, use it as-is
                return field.verbose_name
            else:
                # Otherwise convert it to a title
                return underscored_to_title(field.verbose_name)

        # Special case: If the name of the field is name, use the model name
        if self.field_path == "name" and is_unique_field(field):
            verbose_model_name_without_automods = self.model._meta.__dict__[
                "verbose_name"
            ].replace(" ", "")
            if (
                self.model.__name__.lower()
                != verbose_model_name_without_automods.lower()
            ):
                # Use the model's verbose name
                if any(c.isupper() for c in self.model._meta.__dict__["verbose_name"]):
                    # If the verbose name contains upper-case characters
                    return self.model._meta.__dict__["verbose_name"]
                else:
                    return underscored_to_title(
                        self.model._meta.__dict__["verbose_name"]
                    )
            else:
                # Use the model name
                return camel_to_title(self.model.__name__)

        # Grab as many of the last 2 items from the field_path as is present
        path_tail = self.field_path.split("__")[-2:]

        # If the length is 2, the last element is "name", and the field is unique, use the related model name
        if len(path_tail) == 2 and path_tail[1] == "name" and is_unique_field(field):
            return underscored_to_title(path_tail[0])

        # Default is to use the last 2 elements of the path
        return underscored_to_title("_".join(path_tail))
