from django.views.generic import DetailView, ListView

from DataRepo.models import ArchiveFile


class ArchiveFileListView(ListView):
    """Generic class-based view for a list of ArchiveFiles"""

    model = ArchiveFile
    context_object_name = "archive_file_list"
    template_name = "DataRepo/archive_file_list.html"
    ordering = ["id"]


class ArchiveFileDetailView(DetailView):
    """Generic class-based detail view for an ArchiveFile"""

    model = ArchiveFile
    template_name = "DataRepo/archive_file_detail.html"
