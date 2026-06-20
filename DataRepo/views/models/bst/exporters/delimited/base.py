import csv
from abc import ABC
from typing import ClassVar

import _csv
from django.http import StreamingHttpResponse
from django.template.backends.django import Template

from DataRepo.views.models.bst.export import BSTExportedListView
from DataRepo.views.models.bst.exporters.base import BSTExportView


class Echo:
    def write(self, value):
        return value


class TextBSTExportView(BSTExportView, ABC):
    content_type = "application/text"

    # Abstract class attributes
    delim: ClassVar[str]  # E.g. ','

    @classmethod
    def _validate_class(cls):
        super()._validate_class()

        if not hasattr(cls, "delim"):
            raise TypeError(f"{cls.__name__} must define class attribute 'delim'.")
        if not isinstance(cls.delim, str):
            raise TypeError(
                f"Class attribute 'delim' must be a '{str.__name__}', "
                f"not '{type(cls.delim).__name__}'."
            )

    def get(self, request, **kwargs) -> StreamingHttpResponse:
        """Generate and return an exported file response.

        This resolves the source view from the request, initializes export state, generates the export contents using
        the derived exporter implementation, and returns the resulting file as an HTTP download response.

        If a download metadata header template is configured, the rendered metadata is supplied to the exporter as a
        header content string.

        Args:
            request (HTTPRequest): The HTTP request containing the source view name.
            kwargs (Dict[str, Any]): Additional Django view arguments.
        Exceptions:
            None
        Returns:
            (HttpResponse): A download response containing the generated export file.
        """
        source_view: BSTExportedListView = self.get_source_view(request)
        # Getting the queryset initializes the stats for the metadata header
        source_view.get_queryset()

        self.init_export(source_view)

        if isinstance(self.download_header_template, Template):
            header_content = self.download_header_template.render(
                self.get_header_context(source_view)
            )
        else:
            header_content = ""

        writer: "_csv._writer" = csv.writer(
            Echo(), delimiter=self.delim, lineterminator="\n"
        )

        return StreamingHttpResponse(
            self.stream_file(source_view, writer, header_content),
            content_type=self.content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{self.export_file}"'
            },
        )

    def stream_file(
        self,
        source_view: BSTExportedListView,
        writer: "_csv._writer",
        header_content: str,
    ):
        yield header_content

        for row in source_view.rows_iterator():
            yield writer.writerow(row)
