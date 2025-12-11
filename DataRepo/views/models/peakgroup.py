from django.views.generic import DetailView

from DataRepo.models import PeakGroup
from DataRepo.views.models.bst.query import BSTListView


class PeakGroupListView(BSTListView):
    model = PeakGroup
    exclude = ["id", "peak_data", "msrun_sample"]
    column_settings = {
        "name": {"header": "Peak Group"},
        "msrun_sample__sample": {},
        "msrun_sample_sample_msrun_samples_mm_count": {
            "header": "MS Run Samples Count",
            "visible": False,
        },
        "compounds": {"visible": False, "header": "Compound"},
        "compounds_mm_count": {"visible": False},
        "labels_mm_count": {"visible": False},
        "peak_annotation_file": {
            "display_field_path": "peak_annotation_file__filename"
        },
        "msrun_sample__sample__msrun_samples": {"header": "MS Run Sample"},
    }


class PeakGroupDetailView(DetailView):
    """Generic class-based detail view for a peak group"""

    model = PeakGroup
    template_name = "models/peakgroup/peakgroup_detail.html"
