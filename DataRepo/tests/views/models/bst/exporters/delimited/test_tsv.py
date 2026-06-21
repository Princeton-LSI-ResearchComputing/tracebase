import csv
from unittest.mock import MagicMock

import _csv

from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.views.models.bst.exporters.delimited.base import Echo
from DataRepo.views.models.bst.exporters.delimited.tsv import TSVBSTExportView


class TSVBSTExportViewTests(TracebaseTestCase):
    def test_stream_file_uses_tab_delimiter(self):
        exporter = TSVBSTExportView()

        source_view = MagicMock()
        source_view.rows_iterator.return_value = [["a", "b"]]

        writer: "_csv._writer" = csv.writer(
            Echo(), delimiter=exporter.delim, lineterminator="\n"
        )

        self.assertEqual(
            "".join([line for line in exporter.stream_file(source_view, writer, "")]),
            "a\tb\n",
        )
