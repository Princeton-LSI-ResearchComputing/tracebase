from io import BytesIO
from typing import Dict, List

import pandas as pd
from django.http import HttpResponse
from django.template.backends.django import Template

from DataRepo.views.models.bst.export import BSTExportedListView
from DataRepo.views.models.bst.exporters.base import BSTExportView


class ExcelBSTExportView(BSTExportView):
    name = "Excel"
    extension = "xlsx"
    content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    view_name = "excel_list_export"

    def get(self, request, **kwargs) -> HttpResponse:
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

        # We must cast because StringIO and BytesIO don't have the same interface, so mypy would complain.
        buffer = BytesIO()

        # Create the file in memory
        # Note, a derived class can decide not to have a header
        if isinstance(self.download_header_template, Template):
            self.buffer_file(
                source_view,
                self.download_header_template.render(
                    self.get_header_context(source_view)
                ),
                buffer,
            )
        else:
            self.buffer_file(source_view, "", buffer)

        response = HttpResponse(
            buffer.getvalue(),
            headers={
                "Content-Type": self.content_type,
                "Content-Disposition": f'attachment; filename="{self.export_file}"',
            },
        )

        return response

    def buffer_file(
        self, source_view: BSTExportedListView, header_content: str, buffer: BytesIO
    ):
        # pylint false positive abstract-class-instantiated triggered because pandas.ExcelWriter acts as an abstract
        # base class behind the scenes, but dynamically returns a concrete subclass
        xlsx_writer = pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
            buffer, engine="xlsxwriter"
        )

        sheet = source_view.model_title_plural
        columns = source_view.row_headers()

        xlsx_writer.book.set_properties(
            {
                "title": sheet,
                "author": "Robert Leach",
                "company": "Princeton University",
                "comments": header_content,
            }
        )

        # Build the dict by iterating over the row lists
        qs_dict_by_index: Dict[int, List[str]] = dict(
            (i, []) for i in range(len(columns))
        )
        for row in source_view.rows_iterator(headers=False):
            for i, val in enumerate(row):
                qs_dict_by_index[i].append(str(val))

        export_dict = {}
        # Now convert the indexes to the headers
        for i, col in enumerate(columns):
            export_dict[col] = qs_dict_by_index[i]

        # Create a dataframe and add it as an excel object to an xlsx_writer sheet
        pd.DataFrame.from_dict(export_dict).to_excel(
            excel_writer=xlsx_writer,
            sheet_name=sheet,
            columns=columns,
            index=False,
        )
        xlsx_writer.sheets[sheet].autofit()
        xlsx_writer.save()
