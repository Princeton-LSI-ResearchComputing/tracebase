from __future__ import annotations

from abc import ABC
from datetime import datetime
from inspect import isabstract
from typing import TYPE_CHECKING, ClassVar, Dict, Optional, Type
from warnings import warn

from django.db.models import Model
from django.template import loader
from django.template.backends.django import Template
from django.urls import NoReverseMatch, Resolver404, resolve, reverse
from django.views import View

from DataRepo.utils.exceptions import DeveloperWarning

if TYPE_CHECKING:
    from DataRepo.views.models.bst.export import BSTExportedListView


class BSTExportView(View, ABC):
    """An abstract view class that each export format class will be derived from.

    It encapsulates the common components of every export format:

    - A template for the download metadata
    - A time format for the filename and the time displayed in the download metadata
    - abstract requirements
      - context type
      - format name
      - An IO class
      - file extension
      - A get method (needed by the View superclass)
    - Its constructor will
      - set the download time
      - check the derived class's class attributes
      - create the filename
    - A method (gather_exporters) to obtain all the derived format classes keyed by name (called by BSTExportedListView)

    Class Attributes:
        Abstract:
            content_type (str): A content type for the downloaded file, E.g. 'text/csv'.
            name (str): A format name, e.g. 'CSV', unique to that derived class, E.g. 'TSV'.
            extension (str): A file extension for the downloaded file, E.g. 'tsv'.
            view_name (str): The name of the view, used to resolve the URL and set in urls.py as the 'name' argument.
        Regular:
            download_header_template_name (str): The template file path relative to the templates folder.
            download_header_template (_BaseTemplate): The template object from Django.
            timestamp_var_name (str): The template variable name for the timestamp value.
            header_time_format (str): The format string for datetime.strftime to use for dates in the header in the
                exported file.
            filename_time_format (str): The format string for datetime.strftime to use for dates in the exported
                filename.
    Instance Attributes:
        fileheader_timestamp (str): The filename formatted timestamp.
        export_file (str): The filename of the exported file.
        model (Type[Model]): A Django Model class (or subclass).
    """

    # Commented header template containing metadata
    download_header_template_name: str = "models/bst/download_metadata_header.txt"
    download_header_template: ClassVar[Optional[Template]] = None

    timestamp_var_name: str = "timestamp"
    header_time_format: str = "%Y-%m-%d %H:%M:%S"
    filename_time_format: str = "%Y.%m.%d.%H.%M.%S"

    # Abstract class attributes
    content_type: ClassVar[str]  # E.g. 'text/csv'
    name: ClassVar[str]  # E.g. 'TSV'
    extension: ClassVar[str]  # E.g. 'tsv'
    view_name: ClassVar[str]  # E.g. 'tsv_list_export'

    def __init__(self, **kwargs):
        self._validate_class()
        View.__init__(self, **kwargs)

        # These are the instance attributes
        self.model: Model
        self.fileheader_timestamp: str
        self.export_file: str

    @classmethod
    def _validate_class(cls):
        """Enforce class attributes that are not natively supprted by python or ABC (yet).

        NOTE: __init_sublass__ has limitations when child classes are a mix of abstract and concrete, as is the case in
        this hierarchy.

        Args:
            None
        Exceptions:
            TyperError - When a concrete class attribute does not exist or is the wrong type.
        Returns:
            None
        """

        cls.download_header_template = (
            loader.get_template(cls.download_header_template_name)
            if cls.download_header_template_name is not None
            else None
        )

        if not hasattr(cls, "content_type"):
            raise TypeError(
                f"{cls.__name__} must define class attribute 'content_type'."
            )
        if not isinstance(cls.content_type, str):
            raise TypeError(
                f"Class attribute 'content_type' must be a '{str.__name__}', "
                f"not '{type(cls.content_type).__name__}'."
            )

        if not hasattr(cls, "name"):
            raise TypeError(f"{cls.__name__} must define class attribute 'name'.")
        if not isinstance(cls.name, str):
            raise TypeError(
                f"Class attribute 'name' must be a '{str.__name__}', not '{type(cls.name).__name__}'."
            )

        if not hasattr(cls, "extension"):
            raise TypeError(f"{cls.__name__} must define class attribute 'extension'.")
        if not isinstance(cls.extension, str):
            raise TypeError(
                f"Class attribute 'extension' must be a '{str.__name__}', "
                f"not '{type(cls.extension).__name__}'."
            )

        if not hasattr(cls, "view_name"):
            raise TypeError(f"{cls.__name__} must define class attribute 'view_name'.")
        if not isinstance(cls.view_name, str):
            raise TypeError(
                f"Class attribute 'view_name' must be a '{str.__name__}', "
                f"not '{type(cls.view_name).__name__}'."
            )

    @classmethod
    def get_exporter_classes(cls):
        """Generator to plumb the hierarchy and yield leaf exporter classes that are derived from this class.

        Args:
            None
        Exceptions:
            None
        Returns:
            (List[Type[BSTExportView]]): A list of derived classes that are also derived from BSTExportView.
        """

        def leaf_exporters(c):
            subclasses = c.__subclasses__()

            if not subclasses and not isabstract(c):
                yield c
            elif not subclasses:
                return

            for subcls in subclasses:
                yield from leaf_exporters(subcls)

        return list(leaf_exporters(cls))

    @classmethod
    def gather_exporters(cls) -> Dict[str, Type[BSTExportView]]:
        """This goes through the exporter subclasses and collects their export types, checking for duplicate export
        names.

        Args:
            None
        Exceptions:
            KeyError - When a duplicate export name is encountered.
            NoExporters - When there are no concrete derived classes.
        Returns:
            export_types (Dict[str, Type[BSTExportView]])
        """
        export_types: Dict[str, Type[BSTExportView]] = {}

        subcls: Type[BSTExportView]
        # This intentionally uses __class__ rather than type(self).  We always want subclasses of BSTExportedListView
        # itself, not subclasses of the runtime type and we want to be robust to change if the class name is ever
        # edited.
        for subcls in __class__.get_exporter_classes():  # type: ignore[name-defined]
            export_name = subcls.name

            if export_name not in export_types:
                export_types[export_name] = subcls
            elif issubclass(subcls, export_types[export_name]):
                warn(
                    (
                        f"Derived class {subcls.__name__} overriding format type {export_name} of the parent "
                        f"class {export_types[export_name].__name__}."
                    ),
                    category=DeveloperWarning,
                )
            elif issubclass(export_types[export_name], subcls):
                warn(
                    (
                        f"Derived class {export_types[export_name].__name__} overriding format type {export_name} "
                        f"of the parent class {subcls.__name__}."
                    ),
                    category=DeveloperWarning,
                )
                continue
            else:
                raise KeyError(
                    f"Derived class '{subcls.__name__}' defines a duplicate export type ('{export_name}') "
                    f"already defined in exporter '{export_types[export_name].__name__}'."
                )

        if len(export_types.keys()) == 0:
            raise NoExporters(
                "Must implement at least 1 derived non-abstract BSTExportView class.  Current derived "
                "BSTExportView classes: "
                f"{[subcls.__name__ for subcls in __class__.get_exporter_classes()]}"  # type: ignore[name-defined]
            )

        return export_types

    def get_header_context(self, source_view: BSTExportedListView):
        """Context for rendering the download metadata in a commented file header using template:
        DataRepo/templates/models/bst/download_metadata_header.txt

        Args:
            source_view (BSTExportedListView): The concrete view where the Bootstrap Table export menu was clicked.
        Exceptions:
            None
        Returns:
            (Dict[str, str])
        """
        # Needed for the file header
        export_filters = dict(
            (column.name, column.filterer.initial)
            for column in source_view.columns.values()
            if column.filterable and column.filterer.initial
        )
        try:
            context = {
                source_view.title_var_name: (
                    source_view.model_title_plural
                    if source_view.title is None
                    else source_view.title
                ),
                self.timestamp_var_name: self.fileheader_timestamp,
                source_view.total_var_name: source_view.total,
                source_view.search_cookie_name: source_view.search_term,
                source_view.columns_var_name: source_view.columns,
                source_view.sortcol_cookie_name: source_view.sort_col,
                source_view.asc_cookie_name: source_view.asc,
                "export_filters": export_filters,
            }
        except AttributeError as ae:
            raise AttributeError(
                f"{ae}.  Be sure to call init_export before calling get_header_context."
            ).with_traceback(ae.__traceback__)
        return context

    def init_export(self, source_view: BSTExportedListView):
        """Initialize export state derived from the source view.

        This prepares the exporter for file generation by storing the source model, generating timestamps for the file
        header and export filename, cand onstructing the export filename.

        Args:
            source_view (BSTExportedListView): The view supplying the data to be exported.
        Exceptions:
            None
        Returns:
            None
        """
        self.model = source_view.model

        now = datetime.now()
        self.fileheader_timestamp = now.strftime(self.header_time_format)

        filename_timestamp = now.strftime(self.filename_time_format)

        self.export_file = (
            f"{self.model.__name__}.{filename_timestamp}.{self.extension}"
        )

    def get_source_view(self, request):
        source_name = request.GET["source"]

        try:
            # 1. Reverse the view name to get a URL path
            url_path = reverse(source_name)

            # 2. Resolve that URL path to inspect the view
            resolved_match = resolve(url_path)

            # 3. Access the underlying class
            source_view_class = resolved_match.func.view_class

            # 4. Create and set up the source view object
            source_view: BSTExportedListView = source_view_class()
            source_view.request = request
            source_view.init_interface()

        except (NoReverseMatch, Resolver404, AttributeError) as oe:
            raise ValueError(
                f"Invalid or unsupported view class: {source_name}"
            ).with_traceback(oe.__traceback__)

        return source_view


class NoExporters(Exception):
    pass
