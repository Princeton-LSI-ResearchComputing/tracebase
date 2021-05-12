from django.forms import modelformset_factory
from django.shortcuts import get_object_or_404
from django.http import Http404
from django.shortcuts import render
from django.views import generic
from django.views.generic import (
    CreateView,
    DetailView,
    FormView,
    ListView,
    TemplateView,
)

from .forms import CompoundForm
from .models import *

# def home(request):
#    cpds = Compound.objects.all()
#    return render(request, "home.html", {"cpds": cpds})


class HomeView(TemplateView):
    template_name = "home.html"


class CompoundListView(generic.ListView):
    """Generic class-based view for a list of compounds."""

    model = Compound
    paginate_by = 20


class CompoundDetailView(generic.DetailView):
    """Generic class-based detail view for a compound."""

    model = Compound


def compound_view(request):
    context = {}

    # creating a formset
    CompoundFormSet = modelformset_factory(CompoundForm)
    formset = CompoundFormSet()
    context["formset"] = formset
    return render(request, "compound_form_view.html", context)


class StudyListView(generic.ListView):
    """Generic class-based view for a list of studies."""

    model = Study
    paginate_by = 20


class StudyDetailView(generic.DetailView):
    """Generic class-based detail view for a study."""

    model = Study


class ProtocolListView(generic.ListView):
    """Generic class-based view for a list of protocols."""

    model = Protocol
    paginate_by = 20


class ProtocolDetailView(generic.DetailView):
    """Generic class-based detail view for a protocol."""

    model = Protocol


class AnimalListView(generic.ListView):
    """Generic class-based view for a list of animals."""

    model = Animal
    paginate_by = 20


class AnimalDetailView(generic.DetailView):
    """Generic class-based detail view for an animal."""

    model = Animal


class SampleListView(generic.ListView):
    """Generic class-based view for a list of samples."""

    model = Sample
    paginate_by = 50


class SampleDetailView(generic.DetailView):
    """Generic class-based detail view for a sample."""

    model = Sample
    paginate_by = 50

class AnimalaSampleListView(ListView):
    """thinking about this view ..."""
    template_name = 'DataRepo/samples_by_animal.html'

    def get_queryset(self):
        self.animal = get_object_or_404(Animal, name=self.kwargs['animal'])
        return Sample.objects.filter(animal=self.animal) 


class MSRunListView(generic.ListView):
    """Generic class-based view for a list of MSRuns."""

    model = MSRun
    paginate_by = 50


class MSRunDetailView(generic.DetailView):
    """Generic class-based detail view for a MSRuns."""

    model = MSRun
    paginate_by = 50

class PeakGroupListView(generic.ListView):
    """Generic class-based view for a list of PeakGroup."""

    model = PeakGroup
    paginate_by = 50


class PeakGroupDetailView(generic.DetailView):
    """Generic class-based detail view for a PeakGroup."""

    model = PeakGroup
    paginate_by = 50