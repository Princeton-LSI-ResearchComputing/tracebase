from django.http import Http404
from django.shortcuts import render
from django.views.generic import DetailView, ListView, TemplateView
from django.apps import AppConfig
from django.apps import apps

from .models import Compound, Study


def home(request):
    return render(request, "home.html")


def compound_list(request):
    cpds = Compound.objects.all()
    return render(request, "compound_list.html", {"cpds": cpds})


def compound_detail(request, cpd_id):
    try:
        cpd = Compound.objects.get(id=cpd_id)
    except Compound.DoesNotExist:
        raise Http404("compound not found")
    return render(request, "compound_detail.html", {"cpd": cpd})


class StudyListView(ListView):
    """Generic class-based view for a list of studies."""
    model = Study
    paginate_by = 20


class StudyDetailView(DetailView):
    """Generic class-based detail view for a study."""
    model = Study


def search_basic(request, mdl, fld, cmp, val, fmt):
    """Generic basic search interface"""
    model = apps.get_app_config('DataRepo').get_model(mdl) #works
    field = fld
    comparator = cmp
    value = val
    format_name = fmt

    qry = {}
    qry["mdl"] = model.__name__
    qry["fld"] = field
    qry["cmp"] = comparator
    qry["val"] = value
    qry["fmt"] = format_name

    # https://stackoverflow.com/questions/4720079/django-query-filter-with-variable-column
    fld_cmp = field + '__' + comparator
    qs = [model.objects.get(**{ fld_cmp: value })]

    format_template = ""
    if fmt == "peakgroups":
        format_template = "peakgroups_results.html"
    else:
        raise Http404("Results format [" + fmt + "] page not found")
    
    return render(request, format_template, {"qry": qry, "qs": qs})
