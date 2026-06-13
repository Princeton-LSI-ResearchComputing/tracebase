from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional
from warnings import warn

from django.urls import reverse

from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.query import BSTListView

if TYPE_CHECKING:
    from .exporters.exporters import BSTExportView


class BSTExportedListView(BSTListView):
    """BSTExportedListView handles the export interface used by all BSTListViews.  The specifics of the export
    functionality is left to a derived class named BSTExportView.

    In order to allow extension of this class to add new features (instead of just adding more export formats), an
    optional get_exporters method must be implemented in the derived class if it is adding export formats.
    gather_exporters goes through the derived classes and finds the export formats and ignores derived classes that do
    not provide export formats.

    Class Attributes:
        export_script_names (List[str]): A list of javascripts required for the client (added to the parent class' list)
        export_enabled_var_name (str): The template variable indicating whether export is enabled.
        export_types_var_name (str): The template variable indicating the available export types.
    Instance Attributes:
        BSTExportedListView (this class):
            export_enabled (bool)
            exporters (Dict[str, BSTExportView])
        BSTListView (parent class):
            javascripts (List[str])
    """

    export_script_names = ["js/bst/exporter.js"]

    export_enabled_var_name = "export_enabled"
    export_types_var_name = "export_types"

    def __init__(self, export_enabled=True, **kwargs):
        super().__init__(**kwargs)

        self.export_enabled = export_enabled
        self.exporters = self.gather_exporters()

        if self.export_script_names:
            for script in self.export_script_names:
                self.javascripts.insert(0, script)

    @classmethod
    def get_exporter_classes(cls):
        """Generator to plumb the hierarchy and yield concrete/leaf exporter classes that are derived from this class
        and from BSTExportView.

        Uses BSTExportView.is_exporter to determine derivation.

        Args:
            None
        Exceptions:
            None
        Returns:
            (List[Type[BSTExportView]]): A list of derived classes that are also derived from BSTExportView.
        """

        def concrete_exporters(c):
            subclasses = c.__subclasses__()

            if not subclasses:
                if getattr(c, "is_exporter", False):
                    yield c
                return

            for subcls in subclasses:
                yield from concrete_exporters(subcls)

        return list(concrete_exporters(cls))

    @classmethod
    def gather_exporters(cls) -> Dict[str, BSTExportView]:
        """This goes through the exporter subclasses and collects their export types, checking for duplicate export
        names.

        Args:
            None
        Exceptions:
            KeyError - When a duplicate export name is encountered.
            NoExporters - When no subclass has defined get_exporters to return exporters.
        Returns:
            export_types (Dict[str, BSTExportView])
        """
        export_types: Dict[str, BSTExportView] = {}

        # This intentionally uses __class__ rather than type(self).  We always want subclasses of BSTExportedListView
        # itself, not subclasses of the runtime type and we want to be robust to change if the class name is ever
        # edited.
        for subcls in __class__.get_exporter_classes():  # type: ignore[name-defined]
            tmp_export_types = subcls.get_exporters()

            if not tmp_export_types:
                warn(
                    f"{subcls.__name__}.get_exporters() did not return any export types.",
                    category=DeveloperWarning,
                )
                continue

            # Check for duplicate export types
            for export_name in tmp_export_types.keys():
                if export_name in export_types.keys():
                    raise KeyError(
                        f"Derived class '{subcls.__name__}' defines a duplicate export type ('{export_name}') already "
                        f"defined in exporter '{export_types[export_name].__name__}'."
                    )
            export_types.update(tmp_export_types)

        if len(export_types.keys()) == 0:
            raise NoExporters(
                "Must implement get_exporters in at least 1 derived BSTExportView class.  Current derived "
                "BSTExportView classes: "
                f"{[subcls.__name__ for subcls in __class__.get_exporter_classes()]}"  # type: ignore[name-defined]
            )

        return export_types

    @classmethod
    def get_exporters(self) -> Optional[Dict[str, BSTExportView]]:
        """Optional derived class method.  Must be overridden in BSTExportView.
        See gather_exporters for how these exporters are collected."""
        pass

    def get_context_data(self, **kwargs):
        """Retrieve context data for export functionality.
        See design in: https://princeton-university.atlassian.net/wiki/x/GQAgH
        """
        context = super().get_context_data()

        # List of dicts containing the export type name and its URL
        export_types = list(
            {"name": exp.name, "url": reverse(type(exp).__name__)}
            for exp in self.exporters
        )

        context.update(
            {
                self.export_enabled_var_name: self.export_enabled,
                self.export_types_var_name: export_types,
            }
        )

        return context


class NoExporters(Exception):
    pass
