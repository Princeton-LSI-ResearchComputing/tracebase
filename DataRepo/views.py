# from django.http import Http404
# from django.http import render
from django.shortcuts import get_object_or_404
# from django.views import generic
from django.views.generic import DetailView, ListView, TemplateView

from DataRepo.models import (
    Animal,
    Compound,
    MSRun,
    PeakData,
    PeakGroup,
    Protocol,
    Sample,
    Study,
)

# def home(request):
#    cpds = Compound.objects.all()
#    return render(request, "home.html", {"cpds": cpds})


class HomeView(TemplateView):
    template_name = "home.html"


class CompoundListView(ListView):
    """Generic class-based view for a list of compounds."""

    model = Compound
    template_name = "DataRepo/compound_list.html"
    paginate_by = 20


class CompoundDetailView(DetailView):
    """Generic class-based detail view for a compound."""

    model = Compound
    template_name = "DataRepo/compound_detail.html"


class StudyListView(ListView):
    """Generic class-based view for a list of studies."""

    model = Study
    paginate_by = 20


class StudyDetailView(DetailView):
    """Generic class-based detail view for a study."""

    model = Study


class ProtocolListView(ListView):
    """Generic class-based view for a list of protocols."""

    model = Protocol
    paginate_by = 20


class ProtocolDetailView(DetailView):
    """Generic class-based detail view for a protocol."""

    model = Protocol


class AnimalListView(ListView):
    """Generic class-based view for a list of animals."""

    model = Animal
    template_name = "DataRepo/animal_list.html"
    paginate_by = 20


class AnimalDetailView(DetailView):
    """Generic class-based detail view for an animal."""

    model = Animal
    template_name = "DataRepo/animal_detail.html"


class SampleListView(ListView):
    """Generic class-based view for a list of samples with or without filter
    directly use queryset to replace model name
    use context_object_name and template name for easy readability
    """

    # model = Sample
    paginate_by = 20

    context_object_name = "sample_list"
    template_name = "DataRepo/sample_list.html"
    queryset = Sample.objects.all()

    # filter the sample list by animal_id
    def get_queryset(self):
        queryset = super().get_queryset()
        # get query string from url
        animal_pk = self.request.GET.get("animal_id", None)
        if animal_pk is not None:
            self.animal = get_object_or_404(Animal, id=animal_pk)
            queryset = Sample.objects.filter(animal_id=animal_pk)
        return queryset


class SampleDetailView(DetailView):
    """Generic class-based detail view for a sample."""

    model = Sample
    template_name = "DataRepo/sample_detail.html"


class MSRunListView(ListView):
    """Generic class-based view for a list of MSRuns."""

    model = MSRun
    paginate_by = 20


class MSRunDetailView(DetailView):
    """Generic class-based detail view for a MSRuns."""

    model = MSRun


class PeakGroupListView(ListView):
    """
    Generic class-based view for a list of PeakGroups with or without filter
    directly use queryset to replace model name
    use context_object_name and template name for easy readability
    """

    # model = PeakGroup
    paginate_by = 20

    context_object_name = "peakgroup_list"
    template_name = "DataRepo/peakgroup_list.html"
    queryset = PeakGroup.objects.all()

    # filter the peakgroup_list by ms_run_id
    def get_queryset(self):
        queryset = super().get_queryset()
        msrun_pk = self.request.GET.get("ms_run_id", None)
        # print(msrun_pk)
        if msrun_pk is not None:
            self.msrun = get_object_or_404(MSRun, id=msrun_pk)
            queryset = PeakGroup.objects.filter(ms_run_id=msrun_pk)
        return queryset


class PeakGroupDetailView(DetailView):
    """Generic class-based detail view for a PeakGroup."""

    model = PeakGroup


class PeakDataListView(ListView):
    """
    Generic class-based view for a list of Peakdata with or without filter
    directly use queryset to replace model name
    use context_object_name and template name for easy readability
    """

    # model = PeakData
    paginate_by = 20

    context_object_name = "peakdata_list"
    template_name = "DataRepo/peakdata_list.html"
    queryset = PeakData.objects.all()

    # filter the peakgdata_list by peak_group_id
    def get_queryset(self):
        queryset = super().get_queryset()
        peakgroup_pk = self.request.GET.get("peak_group_id", None)
        print(peakgroup_pk)
        if peakgroup_pk is not None:
            self.peakgroup = get_object_or_404(PeakGroup, id=peakgroup_pk)
            queryset = PeakData.objects.filter(peak_group_id=peakgroup_pk)
        return queryset


# class PeakDataDetailView(DetailView):
