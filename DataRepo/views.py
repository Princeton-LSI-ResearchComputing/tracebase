from django.http import Http404
from django.shortcuts import render

from .models import Compound
from django.views import generic
from django.views.generic import (TemplateView, ListView, CreateView, DetailView, FormView)
from .forms import CompoundForm
from django.forms import modelformset_factory

#def home(request):
#    cpds = Compound.objects.all()
#    return render(request, "home.html", {"cpds": cpds})

class HomeView(TemplateView):
    template_name = 'home.html'

class CompoundListView(generic.ListView):
    """Generic class-based view for a list of compounds."""
    model = Compound
    paginate_by = 20

class CompoundDetailView(generic.DetailView):
    """Generic class-based detail view for a compound."""
    model = Compound

def compound_view (request):
    context ={}
  
    # creating a formset
    CompoundFormSet = modelformset_factory(CompoundForm)
    formset = CompoundFormSet()
    context['formset']= formset
    return render(request, "compound_form_view.html", context)