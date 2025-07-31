from __future__ import annotations

from typing import Optional, Type, Union

from django.db.models import Field, Model
from django.db.models.expressions import Combinable

from DataRepo.models.utilities import (
    field_path_to_field,
    is_key_field,
    is_long_field,
    is_many_related_to_root,
    is_model_field,
    is_unique_field,
)
from DataRepo.utils.text_utils import camel_to_title, underscored_to_title
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.filterer.field import BSTFilterer
from DataRepo.views.models.bst.column.sorter.field import BSTSorter


class BSTColumn(BSTBaseColumn):
    """Class to represent the interface between a bootstrap column and a Model field.

    Limitations:
        1. Annotations are not supported.  See BSTAnnotColumn.
        2. Related columns are only partially supported, as they will not be linked.  See BSTRelatedColumn.
        3. Many-related columns are not supported.  See BSTManyRelatedColumn.
        4. Foreign keys cannot be searchable/filterable or sortable.  See BSTRelatedColumn.

    Usage:
        You can create a simple model field column using the field names like this:

            filecol = BSTColumn("filename", model=ArchiveFile)
            timecol = BSTColumn("imported_timestamp", model=ArchiveFile)
            frmtcol = BSTColumn("data_format", model=ArchiveFile)
            typecol = BSTColumn("data_type", model=ArchiveFile)

        Alter whatever settings you want in the constructor calls.  In the BootstrapTableListView's template, all you
        have to do to render the th element for each column is just include the associated generic template:

            {% include filecol.th_template %}
            {% include timecol.th_template %}
            {% include frmtcol.th_template %}
            {% include typecol.th_template %}

        Note that the column headers (by default) will be a title version of the field name.  For example, the headers
        generated from the above objects would be:

            Filename
            Imported Timestamp
            Data Format
            Data Type

        You can supply custom templates for the th, td, and value (rendered inside the td element).  To include the td
        element:

            {% include filecol.td_template %}
            {% include timecol.td_template %}
            {% include frmtcol.td_template %}
            {% include typecol.td_template %}

        It's also important to note that in order for search and sort to work as expected, each column whose string-
        context rendered value (as appears in the template) does not exactly match the database's text version of the
        value, should be converted to a simple string or number annotation that is the same as seen in the rendered
        template and in the database.  See BSTAnnotColumn.

    See the BSTBaseColumn docstring for examples on how to customize filtering and sorting behavior.
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
        tooltip = kwargs.get("tooltip")
        wrapped = kwargs.get("wrapped")

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
        elif linked and not self.has_detail():
            # NOTE: An annotation can link as well, but no need to force supplying a model just to check for
            # get_absolute_url.  It will be checked in the template.
            raise ValueError(
                f"Argument 'linked' must not be true when model '{model.__name__}' does not have a "
                "'get_absolute_url' method."
            )
        elif (
            not is_model_field(model, self.field_path.split("__")[0])
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
                "BSTManyRelatedColumn to create a delimited-value column."
            )
        elif linked and "__" in self.field_path:
            raise ValueError(
                f"Argument 'linked' must not be true when 'field_path' '{field_path}' passes through a related "
                "model."
            )

        self.field = field_path_to_field(self.model, self.field_path)
        if (
            (not hasattr(self, "is_fk") or getattr(self, "is_fk", None) is None)
            and is_key_field(self.field)
            # FK fields must be defined in a BSTRelatedColumn, not BSTColumn
            and type(self) is __class__  # type: ignore[name-defined]
        ):
            raise TypeError(
                f"'{name}' is a foreign key.  Foreign keys must be added as either a 'BSTRelatedColumn' or "
                f"'BSTManyRelatedColumn', not '{type(self).__name__}', so that a representative field from the related "
                "model can be selected for display."
            )

        # Reverse relations do not have a help_text attribute
        # TODO: Put this hasattr logic in the related_field file
        remote_field = field_path_to_field(self.model, self.field_path, real=False)
        if (
            hasattr(remote_field, "help_text")
            and remote_field.help_text is not None
            and remote_field.help_text != ""
        ):
            new_tooltip = remote_field.help_text
            if tooltip is not None:
                new_tooltip += "\n\n" + tooltip
            kwargs.update({"tooltip": new_tooltip})

        if wrapped is None and is_long_field(self.field):
            kwargs.update({"wrapped": True})

        super().__init__(name, *args, **kwargs)

    def generate_header(self, real: bool = False, **_):
        """Generate a column header from the field_path, field.name, or the field's verbose_name.
        Overrides super().generate_header.

        Assumptions:
            1. self.field_path contains no dunderscores.  I.e. this is a field directly on self.model.
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # If the field has a verbose name different from name (because it's automatically filled in with name), use it
        if self.field.name != self.field.verbose_name:
            if any(c.isupper() for c in self.field.verbose_name):
                # If the field has a verbose name with caps, use it as-is
                return self.field.verbose_name
            else:
                # Otherwise convert it to a title
                return underscored_to_title(self.field.verbose_name)

        # Special case: If the name of the field is name, use the model name
        if real is False and self.field_path == "name" and is_unique_field(self.field):
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

        # Default is to use the field name, as saved in the field_path.  Assumed to not be dunderscored.
        return underscored_to_title(self.field_path)

    def create_sorter(
        self, field: Optional[Union[Combinable, Field, str]] = None, **kwargs
    ) -> BSTSorter:
        field_expression = field if field is not None else self.field_path
        kwargs.update({"name": kwargs.get("name", self.name)})
        return BSTSorter(field_expression, self.model, **kwargs)

    def create_filterer(self, field: Optional[str] = None, **kwargs) -> BSTFilterer:
        field_path = field if field is not None else self.name
        return BSTFilterer(field_path, self.model, **kwargs)

    def has_detail(self):
        """Override of superclass class method."""
        return hasattr(self.model, "get_absolute_url")
