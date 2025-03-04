from DataRepo.models import Tracer
from DataRepo.views.models.base import BSTListView


class TracerListView(BSTListView):
    """Generic class-based view for a list of tracers"""
    model = Tracer
    paginate_by = 100
    exclude_fields = ["id", "fcircs", "infusate_links"]
