from DataRepo.models import Tracer
from DataRepo.views.models.bst.query import BSTDetailView
from DataRepo.views.models.bst.export import BSTExportedListView


class TracerDetailView(BSTDetailView):
    model = Tracer
    exclude = ["id", "fcircs", "infusate_links"]
    column_settings = {
        "infusates": {
            "td_template": "models/tracer/infusates_td.html",
            "value_template": "models/tracer/infusates_value_detail.html",
            "delim": ",",
            "limit": 25,
        },
        "label_combo": {"filterer": {"distinct_choices": True}},
    }


class TracerListView(BSTExportedListView):
    model = Tracer
    exclude = ["id", "fcircs", "infusate_links"]
    column_settings = {
        "infusates": {
            "td_template": "models/tracer/infusates_td.html",
            "value_template": "models/tracer/infusates_value_list.html",
            "delim": ",",
            "visible": False,
        },
        "label_combo": {"filterer": {"distinct_choices": True}},
    }
