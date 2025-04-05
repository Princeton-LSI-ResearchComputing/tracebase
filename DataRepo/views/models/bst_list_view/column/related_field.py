from __future__ import annotations

from typing import List, Optional, Type, cast
from warnings import warn

from django.conf import settings
from django.db.models import Field, Model

from DataRepo.models.utilities import (
    is_key_field,
    model_path_to_model,
    resolve_field,
    resolve_field_path,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst_list_view.column.field import BSTColumn
from DataRepo.views.models.bst_list_view.column.filterer.field import (
    BSTFilterer,
)
from DataRepo.views.models.bst_list_view.column.sorter.field import BSTSorter


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

        # Get some superclass instance members we need for checks
        field_path: str = cast(str, args[0])
        model: Type[Model] = cast(Type[Model], kwargs.get("model"))

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

                sortable = kwargs.get("sortable")
                if sortable is True:
                    disallowed.append("sortable")

                if len(disallowed) > 0:
                    # NOTE: Consult the docstring Args for an explanation.
                    raise ValueError(
                        f"{disallowed} cannot be True because field_path is a foreign key and a default "
                        "display_field_path could not be determined.  Supply display_field_path to allow search/sort."
                    )

                kwargs.update(
                    {
                        "searchable": False,
                        "sortable": False,
                    }
                )
        elif not self.display_field_path.startswith(field_path):
            # Check the display field
            raise ValueError(
                f"display_field_path '{display_field_path}' must start with the field_path '{field_path}'."
            )

        super().__init__(*args, **kwargs)

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
            None
        """
        if not self.is_fk:
            return field_path

        related_model = model_path_to_model(model, field_path)

        if len(related_model._meta.ordering) == 1:
            # If there's only 1 ordering field, use it
            return (
                f"{field_path}__{resolve_field_path(related_model._meta.ordering[0])}"
            )
        else:
            # Grab the first non-ID field from the related model that is unique, if one exists
            f: Field
            non_relations: List[Field] = []
            for f in related_model._meta.get_fields():
                related_field = resolve_field(f)
                if not related_field.is_relation and related_field.name != "id":
                    if related_field.unique:
                        return f"{field_path}__{related_field.name}"
                    else:
                        non_relations.append(related_field)
            if len(non_relations) == 1:
                return f"{field_path}__{non_relations[0].name}"
        if settings.DEBUG:
            warn(
                "Unable to automatically select an appropriate display_field_path for the foreign key field_path "
                f"'{field_path}'.  The default is '{related_model.__name__}._meta.ordering[0]' when only 1 ordering "
                f"field is defined, the first non-ID unique field in '{related_model.__name__}', or the only field if "
                "there is only 1 non-ID field, but none of those default cases existed.  This column cannot be "
                "searchable or sortable unless a display_field_path is supplied.",
                DeveloperWarning,
            )

    def create_sorter(self, sorter: Optional[str] = None) -> BSTSorter:
        name: str
        if isinstance(self.display_field_path, str):
            name = self.display_field_path
        else:
            name = self.name
        if sorter is None:
            sorter_obj = BSTSorter(name, model=self.model)
        elif isinstance(sorter, str):
            sorter_obj = BSTSorter(name, model=self.model, client_sorter=sorter)
        else:
            # Checks exact type bec. we don't want this to be a BSTRelatedSorter or BSTManyRelatedSorter
            raise TypeError(f"sorter must be a str, not {type(sorter).__name__}")
        return sorter_obj

    def create_filterer(self, filterer: Optional[str] = None) -> BSTFilterer:
        name: str
        if isinstance(self.display_field_path, str):
            name = self.display_field_path
        else:
            name = self.name
        if filterer is None:
            filterer_obj = BSTFilterer(name, model=self.model)
        elif isinstance(filterer, str):
            filterer_obj = BSTFilterer(name, model=self.model, client_filterer=filterer)
        else:
            # Checks exact type bec. we don't want this to be a BSTRelatedFilterer or BSTManyRelatedFilterer
            raise TypeError(f"filterer must be a str, not {type(filterer).__name__}")
        return filterer_obj
