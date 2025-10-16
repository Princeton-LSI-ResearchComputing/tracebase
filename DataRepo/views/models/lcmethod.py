from django.db.models import F, Value
from django.db.models.functions import Extract
from django.views.generic import DetailView

from DataRepo.models import DURATION_SECONDS_ATTRIBUTE, LCMethod
from DataRepo.views.models.bst.export import BSTExportedListView


class LCMethodListView(BSTExportedListView):
    model = LCMethod
    exclude = ["id", "msrunsequence", "run_length"]
    column_ordering = [
        "name",
        "type",
        "description",
        "run_length_mins",
        "msrunsequence_mm_count",
    ]
    column_settings = {
        "type": {"filterer": {"distinct_choices": True}},
        "run_length_mins": {"header": "Run Length (m)", "tooltip": "Units:  minutes"},
        "msrunsequence_mm_count": {
            "header": "MSRun Sequence Count",
            "tooltip": "Number of runs of the MS instrument.",
        },
    }
    annotations = {
        "run_length_mins": Extract(
            F("run_length"),
            DURATION_SECONDS_ATTRIBUTE,
        )
        / Value(60)
    }


class LCMethodDetailView(DetailView):
    model = LCMethod
    context_object_name = "lcmethod"
    template_name = "models/lcmethod/lcmethod_detail.html"
