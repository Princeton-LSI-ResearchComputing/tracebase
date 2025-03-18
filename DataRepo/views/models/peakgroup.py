from django.shortcuts import get_object_or_404
from django.views.generic import DetailView, ListView

from DataRepo.models import MSRunSample, PeakGroup


class PeakGroupListView(ListView):
    """
    Generic class-based view for a list of peak groups
    "model = PeakGroup" is shorthand for queryset = PeakGroup.objects.all()
    use queryset syntax for PeakGroup list with or without filtering
    """

    queryset = PeakGroup.objects.all()
    context_object_name = "peakgroup_list"
    template_name = "models/peakgroup/peakgroup_list.html"
    ordering = ["msrun_sample_id", "peak_annotation_file_id", "name"]
    paginate_by = 50

    # filter the peakgroup_list by msrun_sample_id
    def get_queryset(self):
        queryset = super().get_queryset()
        # get query string from request
        msrun_sample_pk = self.request.GET.get("msrun_sample_id", None)
        if msrun_sample_pk is not None:
            self.msrun = get_object_or_404(MSRunSample, id=msrun_sample_pk)
            queryset = PeakGroup.objects.filter(msrun_sample_id=msrun_sample_pk)
        return queryset


class PeakGroupDetailView(DetailView):
    """Generic class-based detail view for a peak group"""

    model = PeakGroup
    template_name = "models/peakgroup/peakgroup_detail.html"
