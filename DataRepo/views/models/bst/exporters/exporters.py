from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from inspect import isabstract
from io import IOBase
from typing import ClassVar, Dict, Final, Optional, Type, cast
from warnings import warn

from django.db.models import Model
from django.template import loader
from django.template.backends.django import Template

from DataRepo.utils.exceptions import DeveloperWarning

_MISSING: Final = object()


class BSTExportView(ABC):
    """An abstract view class that each export format class will be derived from.

    It encapsulates the common components of every export format:

    - A template for the download metadata
    - A time format for the filename and the time displayed in the download metadata
    - abstract requirements
      - context type
      - format name
      - An IO class
      - file extension
      - A method to buffer the file in memory to prepare for download
    - Its constructor will
      - set the download time
      - check the derived class's class attributes
      - create the filename
    - A method (gather_exporters) to obtain all the derived format classes keyed by name (called by BSTExportedListView)

    Class Attributes:
        Abstract:
            content_type (str): A content type for the downloaded file, E.g. 'text/csv'.
            name (str): A format name, e.g. 'CSV', unique to that derived class, E.g. 'TSV'.
            buffer (Type[IOBase]): An IO buffer class for the downloaded file, E.g. BytesIO.
            extension (str): A file extension for the downloaded file, E.g. 'tsv'.
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

    # Abstract class attributes.  (_MISSING is a sentinel, so that these attributes exist in the base class at run time)
    content_type: ClassVar[str] = cast(str, _MISSING)  # E.g. 'text/csv'
    name: ClassVar[str] = cast(str, _MISSING)  # E.g. 'TSV'
    buffer: ClassVar[Type[IOBase]] = cast(Type[IOBase], _MISSING)  # E.g. ByteIO
    extension: ClassVar[str] = cast(str, _MISSING)  # E.g. 'tsv'

    def __init__(self, model: Type[Model], **kwargs):
        now = datetime.now()
        self.fileheader_timestamp = now.strftime(self.header_time_format)

        filename_timestamp = now.strftime(self.filename_time_format)

        # TODO: The model argument can probably be removed and the file name generation done in the get method, when the
        # cookies and URL indicating the source view comes in, either of which will contain a model.
        self.model = model

        self.export_file = (
            f"{self.model.__name__}.{filename_timestamp}.{self.extension}"
        )

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        cls.download_header_template = (
            loader.get_template(cls.download_header_template_name)
            if cls.download_header_template_name is not None
            else None
        )

        if cls.content_type is _MISSING:
            raise TypeError(
                f"{cls.__name__} must define class attribute 'content_type'."
            )
        if not isinstance(cls.content_type, str):
            raise TypeError(
                f"Class attribute 'content_type' must be a '{str.__name__}', "
                f"not '{type(cls.content_type).__name__}'."
            )

        if cls.name is _MISSING:
            raise TypeError(f"{cls.__name__} must define class attribute 'name'.")
        if not isinstance(cls.name, str):
            raise TypeError(
                f"Class attribute 'name' must be a '{str.__name__}', not '{type(cls.name).__name__}'."
            )

        if cls.buffer is _MISSING:
            raise TypeError(f"{cls.__name__} must define class attribute 'buffer'.")
        if not isinstance(cls.buffer, type):
            raise TypeError(
                f"Class attribute 'buffer' must be a subclass of '{IOBase.__name__}', "
                f"not an instance of '{type(cls.buffer).__name__}' (set to {str(cls.buffer)})."
            )
        if not issubclass(cls.buffer, IOBase):
            raise TypeError(
                f"Class attribute 'buffer' must be a subclass of '{IOBase.__name__}', "
                f"not '{cls.buffer.__name__}'"
            )

        if cls.extension is _MISSING:
            raise TypeError(f"{cls.__name__} must define class attribute 'extension'.")
        if not isinstance(cls.extension, str):
            raise TypeError(
                f"Class attribute 'extension' must be a '{str.__name__}', "
                f"not '{type(cls.extension).__name__}'."
            )

    @abstractmethod
    def buffer_file(self, header_content: str):
        pass

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


class NoExporters(Exception):
    pass
