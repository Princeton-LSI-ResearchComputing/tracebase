from django.views.generic import DetailView

from DataRepo.models import MSRunSample
from DataRepo.views.models.bst.query import BSTListView


class MSRunSampleDetailView(DetailView):
    model = MSRunSample
    template_name = "models/msrunsample/msrunsample_detail.html"
    context_object_name = "msrun_sample"


class MSRunSampleListView(BSTListView):
    model = MSRunSample
    exclude = ["id", "peak_groups"]
    column_ordering = [
        "details",
        "sample",
        "msrun_sequence",
        "polarity",
        "mz_min",
        "mz_max",
        "ms_raw_file",
        "ms_data_file",
    ]
    column_settings = {
        "ms_raw_file": {"display_field_path": "ms_raw_file__filename"},
        "ms_data_file": {"display_field_path": "ms_data_file__filename"},
    }
    paginate_by = 20
