from __future__ import annotations

from typing import Optional, Type, Union, cast
from warnings import warn

from django.conf import settings
from django.db.models import Field, Model
from django.db.models.expressions import Combinable

from DataRepo.models.utilities import (
    field_path_to_field,
    field_path_to_model_path,
    is_key_field,
    is_many_related,
    is_reverse_related_field,
    is_unique_field,
    model_path_to_model,
    select_representative_field,
)
from DataRepo.utils.exceptions import DeveloperWarning, trace
from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.views.models.bst.column.field import BSTColumn
from DataRepo.views.models.bst.column.filterer.field import BSTFilterer
from DataRepo.views.models.bst.column.sorter.field import BSTSorter


class BSTRelatedColumn(BSTColumn):
    """Class to represent the interface between a bootstrap column and a one-related Model field.

    Limitations:
        1. Many-related columns are not supported.  See BSTManyRelatedColumn.
        2. Only 1 display_field is allowed.  Use BSTAnnotColumn to use a field combination.

    Usage:
        You can create a simple model field column using field paths like this:

            frmtcol = BSTRelatedColumn("data_format", model=ArchiveFile)
            typecol = BSTRelatedColumn("data_type__name", model=ArchiveFile)

        Use django "field path lookups" relative to the base model.
        See https://docs.djangoproject.com/en/5.1/topics/db/queries/#lookups-that-span-relationships

        Note that the column headers (by default) will use a title version of the last 2 values in django's dunderscore-
        delimited field path.  For example, the header generated from the above objects would be:

            Data Format
            Data Type Name

    See the BSTBaseColumn docstring for examples on how to customize filtering and sorting behavior.
    """

    is_related: bool = True

    def __init__(
        self,
        *args,
        display_field_path: Optional[str] = None,
        **kwargs,
    ):
        """Defines options used to populate the bootstrap table columns for a BootstrapListView and a single reference
        model.

        Args:
            display_field (Optional[str]): The field path of the field that is displayed in the template.  This is the
                field that is used for searching and sorting.  This cannot be an annotation and only applies to fields
                that are foreign keys.  If a value is supplied for a non-foreign key field (that differs from field_path
                - see the superclass), a ValueError exception will be raised.
        Exceptions:
            ValueError when arguments are invalid
        Returns:
            None
        """
        self.display_field_path = display_field_path
        tooltip = kwargs.get("tooltip")

        # Get some superclass instance members we need for checks
        field_path: str = cast(str, args[0])
        model: Type[Model] = cast(Type[Model], args[1])

        # We need to initialize/check the display_field_path before we call the superclass constructor, because the
        # display field is used in the sorter/filterer initialized by the superclass constructor.  And for that, we need
        # to set self.is_fk (which the superclass constructor does, if it's not already set).
        self.is_fk = is_key_field(field_path, model)

        if (
            not self.is_fk
            and self.display_field_path is not None
            and self.display_field_path != field_path
        ):
            raise ValueError(
                f"display_field_path '{display_field_path}' is only allowed to differ from field_path '{field_path}' "
                "when the field is a foreign key."
            )

        # Used to supply to prefetch
        try:
            self.related_model_path = field_path_to_model_path(model, field_path)
        except ValueError as ve:
            raise ValueError(
                "field_path must contain a foreign key to be used in a BSTRelatedColumn object."
            ).with_traceback(ve.__traceback__)

        self.related_model = model_path_to_model(model, self.related_model_path)

        if self.display_field_path is None:
            self.display_field_path = self.get_default_display_field(field_path, model)
            if self.is_fk and (
                self.display_field_path is None or self.display_field_path == field_path
            ):
                # If a default could not be automatically selected, we must disallow search/sort
                disallowed = []

                searchable = kwargs.get("searchable")
                if searchable is True:
                    disallowed.append("searchable")

                filterable = kwargs.get("filterable")
                if filterable is True:
                    disallowed.append("filterable")

                sortable = kwargs.get("sortable")
                if sortable is True:
                    disallowed.append("sortable")

                if len(disallowed) > 0:
                    # NOTE: Consult the docstring Args for an explanation.
                    raise ValueError(
                        f"{disallowed} cannot be True because field_path is a foreign key and a default "
                        "display_field_path could not be determined.  Supply display_field_path to allow search/sort."
                    )

                tooltip = "" if tooltip is None else tooltip + "\n\n"
                tooltip += (
                    "Search and sort is disabled for this field because the displayed values do not exist in the "
                    "database as a single field"
                )

                # Fall back to the actual foreign key as the display field.  This will end up rendering related objects
                # in string context, which is what is not searchable/filterable/sortable.
                self.display_field_path = field_path

                kwargs.update(
                    {
                        "searchable": False,
                        "filterable": False,
                        "sortable": False,
                        "tooltip": tooltip,
                    }
                )
        elif not self.display_field_path.startswith(field_path):
            # Check the display field
            raise ValueError(
                f"display_field_path '{display_field_path}' must start with the field_path '{field_path}'."
            )

        self.display_field = field_path_to_field(model, self.display_field_path)

        super().__init__(*args, **kwargs)

        # Reverse relations do not have a help_text attribute
        remote_field = field_path_to_field(self.model, self.field_path, real=False)
        if not hasattr(remote_field, "help_text"):
            is_mr = is_many_related(remote_field, source_model=self.model)
            if is_reverse_related_field(remote_field) and not is_mr:
                new_tooltip = (
                    f"This is a reverse relationship to a single record of the '{self.related_model.__name__}' "
                    "model."
                )
                if self.tooltip is not None:
                    new_tooltip = f"{new_tooltip}\n\n{self.tooltip}"
                self.tooltip = new_tooltip
            elif not is_mr:
                warn(
                    f"{trace()}\nNo help_text found for field {self.model.__name__}.{remote_field.name}",
                    DeveloperWarning,
                )

        if self.is_fk:
            # To use .distinct(), you need the ordering fields from the related model, otherwise you get an exception
            # about the order_by and distinct fields being different
            # TODO: REFACTOR: This needs to be recursive.  Create/call a method in DataRepo.models.utilities and move
            # this code/call into the sorter class
            self.distinct_fields = [
                f"{self.field_path}__{f}" for f in self.related_model._meta.ordering
            ]
        else:
            self.distinct_fields = [self.field_path]

    def get_default_display_field(self, field_path: str, model: Type[Model]):
        """Select the best display field.

        A display field is chosen based on the following criteria, in order of precedence:

        1. The related model's _meta.ordering field is chosen if there is only 1
        2. The first non-ID field that is unique is chosen, if such a field exists
        3. If the model has only 1 non-ID field, it is chosen.
        4. Otherwise, None is returned and a warning is issued.

        Args:
            field_path (str)
            model (Type[Model])
        Exceptions:
            None
        Returns:
            full_rep_field (Optional[str]): The field path to the display field, or None if a non-ID field could not be
                selected.
        """
        if not self.is_fk:
            return field_path

        # We know that field_path is a model path because of the is_fk conditional above
        related_model = model_path_to_model(model, field_path)

        rep_field = select_representative_field(related_model)

        if rep_field is None:
            # The select_representative_field selected an ID field that we do not want.  We want to let the field render
            # using the related model's __str__ method and disable search/sort based on this return being None.
            if settings.DEBUG:
                warn(
                    "Unable to automatically select a searchable/sortable display_field_path for the foreign key "
                    f"field_path '{field_path}'.  The default is '{related_model.__name__}._meta.ordering[0]' when "
                    f"only 1 ordering field is defined, the first non-ID unique field in '{related_model.__name__}', "
                    "or the only field if there is only 1 non-ID field, but none of those default cases existed.",
                    DeveloperWarning,
                )

            return None

        full_rep_field = f"{field_path}__{rep_field}"

        return full_rep_field

    def generate_header(self, **_):
        """Generate a column header from the field_path.  Overrides super().generate_header.  This only uses the field's
        name or verbose_name if the field is not a reverse relation.  The field names of reverse relations refer to the
        model that linked to it, which is "backwards".  For example, such a column header for a reverse relation off the
        root model would end up with a header that is the same as the root model.  This also does not use the model name
        of related model unless the leaf of the field path is "name".  The leaf of the field path provides context that
        the field name or the related model name does not provide.  E.g. Compound linked to from Tracer or PeakGroup
        could be "measured_compound" or "tracer_compound".

        Args:
            None
        Exceptions:
            None
        Returns:
            (str): A header string, with pretty capitalization and underscores removed.
        """
        # Grab as many of the last 2 items from the field_path as is present
        path = self.field_path.split("__")

        # If the length is greater than 1, the last element is "name", and the field is unique, use the name of the
        # foreign key to this model's field only.
        if len(path) > 1 and path[-1] == "name" and is_unique_field(self.field):
            return underscored_to_title(path[-2])

        # If the field belongs to the model of the last foreign key in the field_path (because the verbose name of a
        # reverse relation refers to the reverse model) and has a verbose name different from name (because it's
        # automatically filled in with name), use it
        remote_field = field_path_to_field(self.model, self.field_path, real=False)
        if (
            not is_reverse_related_field(remote_field)
            and remote_field.name != remote_field.verbose_name
        ):
            if any(c.isupper() for c in remote_field.verbose_name):
                # If the field has a verbose name with caps, use it as-is
                return remote_field.verbose_name
            else:
                return underscored_to_title(remote_field.verbose_name)

        # Otherwise, use the last 2 elements of the path
        return underscored_to_title(path[-1])

    def create_sorter(
        self, field: Optional[Union[Combinable, Field, str]] = None, **kwargs
    ) -> BSTSorter:
        if field is not None:
            field_expression = field
        elif self.display_field_path is not None:
            field_expression = self.display_field_path
        else:
            field_expression = self.name

        kwargs.update({"name": kwargs.get("name", self.name)})

        return super().create_sorter(field=field_expression, **kwargs)

    def create_filterer(self, field: Optional[str] = None, **kwargs) -> BSTFilterer:
        if field is not None:
            field_path = field
        elif self.display_field_path is not None:
            field_path = self.display_field_path
        else:
            field_path = self.name
        return super().create_filterer(field=field_path, **kwargs)
