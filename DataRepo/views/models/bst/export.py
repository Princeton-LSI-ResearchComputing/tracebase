from __future__ import annotations

from typing import Dict, Type

from django.urls import reverse

from DataRepo.views.models.bst.exporters.exporters import BSTExportView
from DataRepo.views.models.bst.query import BSTListView


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
