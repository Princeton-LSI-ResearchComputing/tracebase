from DataRepo.views.models.bst.export import BSTExportedListView


class BSTExportView(BSTExportedListView):
    is_exporter = True
