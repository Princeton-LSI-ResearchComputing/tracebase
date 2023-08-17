from django.shortcuts import get_object_or_404
from django.views.generic import DetailView, ListView

from DataRepo.models import MSRun, PeakGroup


class PeakGroupListView(ListView):
    """
    Generic class-based view for a list of peak groups
    "model = PeakGroup" is shorthand for queryset = PeakGroup.objects.all()
    use queryset syntax for PeakGroup list with or without filtering
    """

    queryset = PeakGroup.objects.all()
    context_object_name = "peakgroup_list"
    template_name = "DataRepo/peakgroup_list.html"
    ordering = ["msrun_id", "archive_file_id", "name"]
    paginate_by = 50

    # filter the peakgroup_list by msrun_id
    def get_queryset(self):
        queryset = super().get_queryset()
        # get query string from request
        msrun_pk = self.request.GET.get("msrun_id", None)
        if msrun_pk is not None:
            self.msrun = get_object_or_404(MSRun, id=msrun_pk)
            queryset = PeakGroup.objects.filter(msrun_id=msrun_pk)
        return queryset


class PeakGroupDetailView(DetailView):
    """Generic class-based detail view for a peak group"""

    model = PeakGroup
    template_name = "DataRepo/peakgroup_detail.html"
