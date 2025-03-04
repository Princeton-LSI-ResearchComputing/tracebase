from django.views.generic import DetailView

from DataRepo.models import Tracer
from DataRepo.views.models.base import BSTListView


class TracerDetailView(DetailView):
    """Generic class-based detail view for an infusate"""
    model = Tracer
    # template_name = "DataRepo/tracer_detail.html"


class TracerListView(BSTListView):
    """Generic class-based view for a list of tracers"""
    model = Tracer
    paginate_by = 100
    exclude_fields = ["fcircs"]
