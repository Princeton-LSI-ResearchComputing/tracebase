from django.views.generic import DetailView

from DataRepo.models import Sample
from DataRepo.views.models.bst.list_view import BSTListView


class SampleListView(BSTListView):
    model = Sample


class SampleDetailView(DetailView):
    """Generic class-based detail view for a sample"""

    model = Sample
    template_name = "models/sample/sample_detail.html"
