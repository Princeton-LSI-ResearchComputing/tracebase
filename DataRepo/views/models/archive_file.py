from django.db.models.functions import Coalesce
from django.views.generic import DetailView, ListView

from DataRepo.models import ArchiveFile


class ArchiveFileListView(ListView):
    """Generic class-based view for a list of ArchiveFiles"""

    model = ArchiveFile
    context_object_name = "archive_file_list"
    template_name = "DataRepo/archive_file_list.html"
    ordering = ["id"]

    def get_queryset(self):
        qs = super().get_queryset()
        qs = qs.annotate(
            studies=Coalesce(
                "peak_groups__msrun_sample__sample__animal__studies",
                "mz_to_msrunsamples__sample__animal__studies",
                "raw_to_msrunsamples__sample__animal__studies",
            ),
        )
        return qs


class ArchiveFileDetailView(DetailView):
    """Generic class-based detail view for an ArchiveFile"""

    model = ArchiveFile
    template_name = "DataRepo/archive_file_detail.html"
