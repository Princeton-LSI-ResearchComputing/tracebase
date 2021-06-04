# from django.http import Http404
from django.shortcuts import render
from django.views.generic import DetailView, ListView

from .models import Compound, Study


def home(request):
    return render(request, "home.html")


# For better readability:
# list template name for each ListView or DetailView
# list context_object_name for each ListView
# Generic class-based view for a list of compounds
class CompoundListView(ListView):
    model = Compound
    context_object_name = "compound_list"
    template_name = "DataRepo/compound_list.html"
    paginate_by = 20


# Generic class-based detail view for a compound
class CompoundDetailView(DetailView):
    model = Compound
    template_name = "DataRepo/compound_detail.html"


# Generic class-based view for a list of studies
class StudyListView(ListView):
    model = Study
    context_object_name = "study_list"
    template_name = "DataRepo/study_list.html"
    paginate_by = 20


# Generic class-based detail view for a study
class StudyDetailView(DetailView):
    model = Study
    template_name = "DataRepo/study_detail.html"
