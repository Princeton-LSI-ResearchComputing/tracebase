from __future__ import annotations

from typing import Optional, Type

from django.db.models import Model

from DataRepo.models.utilities import (
    field_path_to_field,
    is_many_related_to_root,
    is_unique_field,
)
from DataRepo.utils.text_utils import camel_to_title, underscored_to_title
from DataRepo.views.models.bst_list_view.column.base import BSTBaseColumn
from DataRepo.views.models.bst_list_view.column.filterer.field import (
    BSTFilterer,
)
from DataRepo.views.models.bst_list_view.column.sorter.field import BSTSorter


class BSTColumn(BSTBaseColumn):
    """Class to represent the interface between a bootstrap column and a Model field.

    Limitations:
        1. Annotations are not supported.  See BSTAnnotColumn.
        2. Related columns are only partially supported, as they will not be linked.  See BSTRelatedColumn.
        3. Many-related columns are not supported.  See BSTManyRelatedColumn.

    Usage:
        You can create a simple model field column using field paths like this:

            filecol = BSTColumn("filename", model=ArchiveFile)
            timecol = BSTColumn("imported_timestamp", model=ArchiveFile)
            frmtcol = BSTColumn("data_format__name", model=ArchiveFile)
            typecol = BSTColumn("data_type__name", model=ArchiveFile)

        Use django "field path lookups" relative to the base model.
        See https://docs.djangoproject.com/en/5.1/topics/db/queries/#lookups-that-span-relationships

        Alter whatever settings you want in the constructor calls.  In the BootstrapTableListView's template, all you
        have to do to render the th element for each column is just include the associated generic template:

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

        It's also important to note that in order for search and sort to work as expected, each column should be
        converted to a simple string or number annotation that is compatible with django's annotate method.  To supply
        the annotation, see BSTAnnotColumn.
    """

    def __init__(
        self,
        field_path: str,
        model: Type[Model],
        *args,
        **kwargs,
    ):
        """Defines options used to populate the bootstrap table columns for a BootstrapListView and a single reference
        model.

        Args:
            model (Optional[Type[Model]]): Model class that the field_path starts from.
            field_path (Optional[str]): Name of the database field (including the field path) corresponding to the
                column.  A value must be supplied, but that value may be None (to support derived classes that do not
                use it).
                NOTE: Adding a many-related field will increase the number of resulting rows in the table.  See
                BSTManyRelatedColumn to prevent this (and display many-related records as delimited values).
        Exceptions:
            ValueError when arguments are invalid
        Returns:
            None
        """

        self.field_path = field_path
        self.model = model

        # Get some superclass instance members we need for checks
        linked = kwargs.get("linked")
        sorter = kwargs.get("sorter")
        filterer = kwargs.get("filterer")

        # Set the name for the superclass based on field_path
        name = self.field_path

        if self.field_path is None:
            raise ValueError(
                "field_path is required for non-annotation fields.  Use BSTAnnotColumn for annotation fields."
            )
        elif model is None:
            raise ValueError(
                "model is required for non-annotation fields.  Use BSTAnnotColumn for annotation fields."
            )
        elif linked and not hasattr(model, "get_absolute_url"):
            # NOTE: An annotation can link as well, but no need to force supplying a model just to check for
            # get_absolute_url.  It will be checked in the template.
            raise ValueError(
                f"Argument 'linked' must not be true when model '{model.__name__}' does not have a "
                "'get_absolute_url' method."
            )
        elif (
            not hasattr(model, self.field_path.split("__")[0])
            and len(self.field_path.split("__")) == 1
        ):
            raise AttributeError(
                f"The field_path ('{self.field_path}') is not an attribute of model '{model.__name__}'.  "
                "If this is an annotation, use 'BSTAnnotColumn'."
            )
        elif not self.is_many_related and is_many_related_to_root(
            self.field_path, model
        ):
            raise ValueError(
                f"field_path '{field_path}' must not be many-related with model '{model.__name__}'.  Instead, use "
                "BSTAnnotColumn to create an annotation or BSTManyRelatedColumn to create a delimited-value column."
            )
        elif linked and "__" in self.field_path:
            raise ValueError(
                f"Argument 'linked' must not be true when 'field_path' '{field_path}' passes through a related "
                "model."
            )

        # Set a default sorter, if necessary
        if sorter is None:
            sorter = BSTSorter(self.field_path, model=model)
        elif isinstance(sorter, str):
            sorter = BSTSorter(self.field_path, model=model, client_sorter=sorter)
        elif type(sorter) is not BSTSorter:
            # Checks exact type bec. we don't want this to be a BSTRelatedSorter or BSTManyRelatedSorter
            raise TypeError(
                f"sorter must be a str or a BSTBaseSorter, not a '{type(sorter).__name__}'"
            )

        # Set a default filterer, if necessary
        if filterer is None:
            filterer = BSTFilterer(name, model)
        elif isinstance(filterer, str):
            filterer = BSTFilterer(name, model, client_filterer=filterer)
        elif type(filterer) is not BSTFilterer:
            # Checks exact type bec. we don't want this to be a BSTRelatedFilterer or BSTManyRelatedFilterer
            raise TypeError(
                f"filterer must be a str or a BSTBaseFilterer, not a '{type(filterer).__name__}'"
            )

        kwargs.update(
            {
                "sorter": sorter,
                "filterer": filterer,
            }
        )

        super().__init__(name, *args, **kwargs)

    def generate_header(self):
        """Generate a column header from the field_path, model name, or column name.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
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

    def create_sorter(self, sorter: Optional[str] = None) -> BSTSorter:
        if sorter is None:
            sorter_obj = BSTSorter(self.field_path, model=self.model)
        elif isinstance(sorter, str):
            sorter_obj = BSTSorter(
                self.field_path, model=self.model, client_sorter=sorter
            )
        else:
            # Checks exact type bec. we don't want this to be a BSTRelatedSorter or BSTManyRelatedSorter
            raise TypeError(f"sorter must be a str, not {type(sorter).__name__}")
        return sorter_obj

    def create_filterer(self, filterer: Optional[str] = None) -> BSTFilterer:
        if filterer is None:
            filterer_obj = BSTFilterer(self.name, model=self.model)
        elif isinstance(filterer, str):
            filterer_obj = BSTFilterer(
                self.name, model=self.model, client_filterer=filterer
            )
        else:
            # Checks exact type bec. we don't want this to be a BSTRelatedFilterer or BSTManyRelatedFilterer
            raise TypeError(f"filterer must be a str, not {type(filterer).__name__}")
        return filterer_obj
