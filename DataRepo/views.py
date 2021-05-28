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
        # study = model.objects.get(**{fld_cmp: val})
        # animals = study.animals.all()

        # This works (don't know why the second line is necessary, but without it, there's an
        # error, whether I use 'animals' in the template or not (and get then from study))
        # https://docs.djangoproject.com/en/3.2/topics/db/queries/#following-relationships-backward
        study = model.objects.get(**{fld_cmp: val})
        animals = study.animals.select_related('tracer_compound').all()

    else:
        raise Http404("Results format [" + fmt + "] page not found")

    return render(
        request, format_template, {"qry": qry, "study": study, "animals": animals}
    )
