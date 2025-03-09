from DataRepo.models import Tracer
from DataRepo.views.models.base import BSTListView


class TracerListView(BSTListView):
    """Generic class-based view for a list of tracers"""
    model = Tracer
    paginate_by = 100
    exclude_fields = ["id", "fcircs", "infusate_links"]

    def __init__(self):
        custom_columns = {
            "labels": {
                "related_sort_fld": "labels__name",
            },
        }
        super().__init__(custom=custom_columns)
