from DataRepo.views.models.bst.exporters.delimited.base import (
    TextBSTExportView,
)


class CSVBSTExportView(TextBSTExportView):
    name = "CSV"
    extension = "csv"
    delim = ","
    view_name = "csv_list_export"
