import csv
from unittest.mock import MagicMock

import _csv

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst.exporters.delimited.base import Echo
from DataRepo.views.models.bst.exporters.delimited.csv import CSVBSTExportView


class CSVBSTExportViewTests(TracebaseTestCase):
    def test_stream_file_writes_header_and_rows(self):
        exporter = CSVBSTExportView()

        source_view = MagicMock()
        source_view.rows_iterator.return_value = [
            ["a", "b"],
            ["c", "d"],
        ]

        writer: "_csv._writer" = csv.writer(
            Echo(), delimiter=exporter.delim, lineterminator="\n"
        )

        self.assertEqual(
            "# HEADER\na,b\nc,d\n",
            "".join(
                [
                    line
                    for line in exporter.stream_file(source_view, writer, "# HEADER\n")
                ]
            ),
        )

    def test_stream_file_stringifies_values(self):
        exporter = CSVBSTExportView()

        source_view = MagicMock()
        source_view.rows_iterator.return_value = [[1, True, None]]

        writer: "_csv._writer" = csv.writer(
            Echo(), delimiter=exporter.delim, lineterminator="\n"
        )

        self.assertEqual(
            "1,True,\n",
            "".join([line for line in exporter.stream_file(source_view, writer, "")]),
        )
