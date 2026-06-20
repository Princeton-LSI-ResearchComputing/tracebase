from DataRepo.views.models.bst.exporters.delimited.base import (
    TextBSTExportView,
)


class TSVBSTExportView(TextBSTExportView):
    name = "TSV"
    extension = "tsv"
    delim = "\t"
    view_name = "tsv_list_export"
