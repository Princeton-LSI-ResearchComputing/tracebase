from django.db.models import Q
from django.views.generic import DetailView, ListView

from DataRepo.models import ArchiveFile, MSRunSample, PeakGroup


class ArchiveFileListView(ListView):
    """Generic class-based view for a list of ArchiveFiles"""

    model = ArchiveFile
    context_object_name = "archive_file_list"
    template_name = "DataRepo/archive_file_list.html"
    ordering = ["id"]

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        studies_by_afid = self.get_studies_by_archive_file_id()
        context["studies"] = studies_by_afid
        return context

    def get_studies_by_archive_file_id(self):
        studies_by_afid = {}

        # Query PeakGroup for its peak annotation files
        pafl_stdy = (
            "peak_annotation_file",
            "msrun_sample__sample__animal__studies__name",
        )
        for pg in PeakGroup.objects.order_by(*pafl_stdy).distinct(*pafl_stdy):
            study_qs = pg.msrun_sample.sample.animal.studies.all()
            studies_by_afid[pg.peak_annotation_file.id] = study_qs

        # Query MSRunSample for its mass spec files
        msrs_stdy = ("ms_data_file", "sample__animal__studies__name")
        for msrs in (
            MSRunSample.objects.filter(
                Q(ms_data_file__isnull=False) | Q(ms_raw_file__isnull=False)
            )
            .order_by(*msrs_stdy)
            .distinct(*msrs_stdy)
        ):
            study_qs = msrs.sample.animal.studies.all()
            if msrs.ms_data_file:
                studies_by_afid[msrs.ms_data_file.id] = study_qs
            if msrs.ms_raw_file:
                studies_by_afid[msrs.ms_raw_file.id] = study_qs

        return studies_by_afid


class ArchiveFileDetailView(DetailView):
    """Generic class-based detail view for an ArchiveFile"""

    model = ArchiveFile
    template_name = "DataRepo/archive_file_detail.html"
