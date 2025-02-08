from django.db.models.functions import Coalesce
from django.views.generic import DetailView, ListView

from DataRepo.models import ArchiveFile


class ArchiveFileListView(ListView):
    """Generic class-based view for a list of ArchiveFiles"""

    model = ArchiveFile
    context_object_name = "archive_file_list"
    template_name = "DataRepo/archive_file_list.html"
    paginate_by = 20

    # def get_queryset(self):
    #     fields = list(ArchiveFile._meta.__dict__["ordering"]).extend(["id", "studies"])
    #     return (
    #         super()
    #         .get_queryset()
    #         .annotate(
    #             studies=Coalesce(
    #                 "peak_groups__msrun_sample__sample__animal__studies",
    #                 "mz_to_msrunsamples__sample__animal__studies",
    #                 "raw_to_msrunsamples__sample__animal__studies",
    #             ),
    #         )
    #     ).order_by(*fields).distinct(*fields)


class ArchiveFileDetailView(DetailView):
    """Generic class-based detail view for an ArchiveFile"""

    model = ArchiveFile
    template_name = "DataRepo/archive_file_detail.html"
