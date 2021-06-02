from django.apps import apps
from django.http import Http404
from django.shortcuts import render
from django.views.generic import DetailView, ListView

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
    model = apps.get_app_config("DataRepo").get_model(mdl)  # works

    qry = {}
    qry["mdl"] = mdl
    qry["fld"] = fld
    qry["cmp"] = cmp
    qry["val"] = val
    qry["fmt"] = fmt

    # https://stackoverflow.com/questions/4720079/django-query-filter-with-variable-column
    fld_cmp = fld + "__" + cmp

    format_template = ""
    if fmt == "peakgroups":
        format_template = "peakgroups_results.html"

        # https://docs.djangoproject.com/en/3.2/topics/db/queries/#following-relationships-backward
        studies = model.objects.filter(**{fld_cmp: val}).prefetch_related("animals","animals__samples__tissue","animals__samples__msruns","animals__samples__msruns__peak_groups","animals__samples__msruns__peak_groups__peak_data")

        res = render(request, format_template, {"qry": qry, "studies": studies})
    else:
        raise Http404("Results format [" + fmt + "] page not found")

    return res
