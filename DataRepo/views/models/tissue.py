from django.views.generic import DetailView

from DataRepo.models import Tissue
from DataRepo.views.models.bst.export import BSTExportedListView


class TissueListView(BSTExportedListView):
    model = Tissue
    exclude = ["id", "samples"]
    column_ordering = ["name", "description", "samples_mm_count"]
    paginate_by = 0
    collapsed_default = False


class TissueDetailView(DetailView):
    """Generic class-based detail view for a tissue"""

    model = Tissue
    template_name = "models/tissue/tissue_detail.html"
