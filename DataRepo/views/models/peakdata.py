from DataRepo.models import PeakData
from DataRepo.views.models.bst.query import BSTDetailView, BSTListView


class PeakDataListView(BSTListView):
    model = PeakData
    paginate_by = 200
    column_settings = {
        "labels": {
            "tooltip": "Labels detected, expressed as mass number, element, and isotope count."
        },
    }


class PeakDataDetailView(BSTDetailView):
    model = PeakData
