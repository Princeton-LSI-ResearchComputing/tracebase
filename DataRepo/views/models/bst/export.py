from __future__ import annotations

from typing import Dict, List, Type

from django.db.models import Model
from django.urls import reverse

from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn
from DataRepo.views.models.bst.exporters.exporters import BSTExportView
from DataRepo.views.models.bst.query import BSTListView, QueryMode


class BSTExportedListView(BSTListView):
    """BSTExportedListView handles the export interface used by all BSTListViews.  The specifics of the export
    functionality are left to a derived class named BSTExportView.

    BSTExportView.gather_exporters is used to go through its derived classes and find the available export formats (its
    derived classes).

    Class Attributes:
        export_script_names (List[str]): A list of javascripts required for the client (added to the parent class' list)
        export_enabled_var_name (str): The template variable indicating whether export is enabled.
        export_types_var_name (str): The template variable indicating the available export types.
        export_view_class (Type[BSTExportView]): A helper class from which to retrieve available formats and their URLs.
    Instance Attributes:
        BSTExportedListView (this class):
            export_enabled (bool)
            exporters (Dict[str, Type[BSTExportView]])
        BSTListView (parent class):
            javascripts (List[str])
    """

    export_script_names = ["js/bst/exporter.js"]

    export_enabled_var_name = "export_enabled"
    export_types_var_name = "export_types"
    export_view_class = BSTExportView

    def __init__(self, export_enabled=True, **kwargs):
        super().__init__(**kwargs)

        self.export_enabled = export_enabled
        self.exporters: Dict[str, Type[BSTExportView]] = (
            self.export_view_class.gather_exporters()
        )

        if self.export_script_names:
            for script in self.export_script_names:
                self.javascripts.insert(0, script)

    def get_context_data(self, **kwargs):
        """Retrieve context data for export functionality.
        See design in: https://princeton-university.atlassian.net/wiki/x/GQAgH
        """
        context = super().get_context_data()

        # List of dicts containing the export type name and its URL
        export_types = list(
            {"name": name, "url": reverse(cls.__name__)}
            for name, cls in self.exporters.items()
        )

        context.update(
            {
                self.export_enabled_var_name: self.export_enabled,
                self.export_types_var_name: export_types,
            }
        )

        return context

    def get_column_val(self, rec: Model, col: BSTBaseColumn):
        """Given a model record, i.e. row-data, e.g. from a queryset, and a column, return the column value.

        NOTE: While this supports many-related columns, it is more efficient to call get_many_related_rec_val directly.

        Args:
            rec (Model)
            col (BSTBaseColumn)
        Exceptions:
            ValueError when the BSTColumn is not labeled as many-related.
        Returns:
            (str): Column value or values (if many-related).
        """
        # Determine the method that will be used to retrieve the column value
        if self.query_mode == QueryMode.SUBQUERY:
            method = self.get_many_related_column_val_by_subquery
        elif self.query_mode == QueryMode.ITERATE:
            method = self.get_column_val_by_iteration
        else:
            raise NotImplementedError(f"QueryMode {self.query_mode} not implemented.")

        # Many-related columns are handled a bit differently.  Their values must be joined using the column's delimiter.
        if isinstance(col, BSTManyRelatedColumn):
            return col.delim.join(
                [
                    # If the value is a foreign key/model object, get the display field defined in the column object.
                    (
                        getattr(val, col.display_field_name)
                        if (
                            isinstance(val, Model)
                            and col.display_field_name
                            and col.display_field_path != col.display_field_name
                        )
                        # Else just return the stringified value
                        else str(val)
                    )
                    for val in method(rec, col)
                ]
            )
        else:
            # NOTE: Regardless of QueryMode, we call get_column_val_by_iteration, because this is not a
            # BSTManyRelatedColumn object, and QueryMode.SUBQUERY is only for BSTManyRelatedColumns
            val = self.get_column_val_by_iteration(rec, col)
            if (
                isinstance(val, Model)
                and isinstance(col, BSTRelatedColumn)
                and col.display_field_name
                and col.display_field_path != col.display_field_name
            ):
                # If the value is a foreign key/model object (managed by a BSTRelatedColumn), get the display field
                # defined in the column object.
                return getattr(val, col.display_field_name)
            elif (
                isinstance(val, Model) and isinstance(col, BSTAnnotColumn) and col.is_fk
            ):
                # If the value is a foreign key/model object (managed by a BSTAnnotColumn), convert the ID the
                # annotation generates to a model object.  We will stringify the object so that it makes some sense
                # to the user, since the integer value of the foreign key is meaningless to the user.  NOTE: There
                # does not yet exist a display field name for annotations that return foreign keys.
                # TODO: Add a display_field_name to BSTAnnotColumn for annotations that return foreign keys
                # TODO: Handle the case where an annotation returns a many-related list of values (not yet
                # supported: so add a raise/exception until it **is** supported)
                return str(col.get_model_object(val))
            else:
                return str(val)

    def row_headers(self):
        return [col.header for col in self.columns.values() if col.exported]

    def rows_iterator(self, headers=True):
        """Takes a queryset of records and returns a list of lists of column data.  Note that delimited many-related
        values are converted to strings, but everything else in the returned list of lists is the original type.

        Args:
            headers (bool): Whether to include the header row.
        Exceptions:
            None
        Returns:
            (List[list])
        """
        if headers:
            yield self.row_headers()
        rec: Model
        for rec in self.get_queryset():
            yield self.rec_to_row(rec)

    def rec_to_row(self, rec: Model) -> List[str]:
        """Takes a Model record and returns a list of values for a file.

        Args:
            rec (Model)
        Exceptions:
            None
        Returns:
            (List[str])
        """
        return [
            self.get_column_val(rec, col)
            for col in self.columns.values()
            if col.exported
        ]
