from DataRepo.models import Tissue
from django.views.generic import DetailView, ListView


class TissueListView(ListView):
    """Generic class-based view for a list of tissues"""

    model = Tissue
    context_object_name = "tissue_list"
    template_name = "DataRepo/tissue_list.html"
    ordering = ["name"]


class TissueDetailView(DetailView):
    """Generic class-based detail view for a tissue"""

    model = Tissue
    template_name = "DataRepo/tissue_detail.html"
