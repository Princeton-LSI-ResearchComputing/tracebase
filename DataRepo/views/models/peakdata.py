from DataRepo.models import PeakData
from DataRepo.views.models.bst.query import BSTDetailView
from DataRepo.views.models.bst.export import BSTExportedListView


class PeakDataListView(BSTExportedListView):
    model = PeakData
    paginate_by = 200
    column_settings = {
        "labels": {
            "tooltip": "Labels detected, expressed as mass number, element, and isotope count."
        },
    }


class PeakDataDetailView(BSTDetailView):
    model = PeakData
