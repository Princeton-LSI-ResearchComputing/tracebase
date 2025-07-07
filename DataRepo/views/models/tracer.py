from DataRepo.models import Tracer
from DataRepo.views.models.bst.query import BSTDetailView, BSTListView


class TracerDetailView(BSTDetailView):
    model = Tracer
    exclude = ["id", "fcircs", "infusate_links"]
    column_settings = {
        "infusates": {
            "td_template": "models/tracer/infusates_td.html",
            "value_template": "models/tracer/infusates_value_detail.html",
            "delim": "\n",
            "limit": 25,
        },
        "label_combo": {"filterer": {"distinct_choices": True}},
    }


class TracerListView(BSTListView):
    model = Tracer
    exclude = ["id", "fcircs", "infusate_links"]
    column_settings = {
        "infusates": {
            "td_template": "models/tracer/infusates_td.html",
            "value_template": "models/tracer/infusates_value_list.html",
            "delim": "\n",
            "visible": False,
        },
        "label_combo": {"filterer": {"distinct_choices": True}},
    }
