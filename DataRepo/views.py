from django.apps import apps
from django.http import Http404
from django.shortcuts import render
from django.views.generic import DetailView, ListView
from django.db.models import Prefetch
from django.db.models import prefetch_related_objects
from django.db.models import FilteredRelation, Q

from .models import Compound, Study, Tissue, MSRun, PeakGroup, PeakData, Sample, Animal


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

    format_template = ""
    if fmt == "peakgroups":
        format_template = "peakgroups_results.html"

        if mdl == "Study":
            # https://stackoverflow.com/questions/4720079/django-query-filter-with-variable-column
            fld_cmp = fld + "__" + cmp

            # https://docs.djangoproject.com/en/3.2/topics/db/queries/#following-relationships-backward
            studies = Study.objects.filter(**{fld_cmp: val})
            animals = Animal.objects.filter(studies__in=studies)
            samples = Sample.objects.filter(animal__in=animals)
        elif mdl == "Animal":
            fld_cmp = fld + "__" + cmp
            study_field_cmp = "animals__" + fld_cmp

            # https://stackoverflow.com/questions/29358729/prefetch-object-with-multiple-levels-of-reverse-lookups/29381638
            animals = Animal.objects.filter(**{fld_cmp: val})
            studies = Study.objects.filter(**{study_field_cmp: val})
            samples = Sample.objects.filter(animal__in=animals)
        elif mdl == "Sample":
            fld_cmp = fld + "__" + cmp
            animal_field_cmp = "samples__" + fld_cmp
            study_field_cmp = "animals__" + animal_field_cmp

            samples = Sample.objects.filter(**{fld_cmp: val})
            animals = Animal.objects.filter(**{animal_field_cmp: val})
            studies = Study.objects.filter(animals__in=animals)
            #studies = Study.objects.all().prefetch_related(
            #    Prefetch("animals__samples",queryset=Sample.objects.filter(**{fld_cmp: val}),to_attr="sample_list"),
            #    "animals__samples__tissue",
            #    "animals__samples__msruns",
            #    "animals__samples__msruns__peak_groups__peak_data")
        elif mdl == "Tissue":
            fld_cmp = "tissues__" + fld + "__" + cmp

            studies = Study.objects.all().prefetch_related(
                Prefetch("animals__samples__tissue",queryset=Tissue.objects.filter(**{fld_cmp: val}),to_attr="tissue_list"),
                "animals__samples__msruns",
                "animals__samples__msruns__peak_groups__peak_data")
        elif mdl == "MSRun":
            fld_cmp = "msruns__" + fld + "__" + cmp

            studies = Study.objects.all().prefetch_related(
                "animals__samples__tissue",
                Prefetch("animals__samples__msruns",queryset=MSRun.objects.filter(**{fld_cmp: val}),to_attr="msrun_list"),
                "animals__samples__msruns__peak_groups__peak_data")
        elif mdl == "PeakGroup":
            fld_cmp = "peak_groups__" + fld + "__" + cmp

            studies = Study.objects.all().prefetch_related(
                "animals__samples__tissue",
                "animals__samples__msruns",
                Prefetch("animals__samples__msruns__peak_groups",queryset=PeakGroup.objects.filter(**{fld_cmp: val}),to_attr="peakgroup_list"),
                "animals__samples__msruns__peak_groups__peak_data")
        elif mdl == "PeakData":
            fld_cmp = "peak_data__" + fld + "__" + cmp

            studies = Study.objects.all().prefetch_related(
                "animals__samples__tissue",
                "animals__samples__msruns",
                Prefetch("animals__samples__msruns__peak_groups__peak_data",queryset=PeakData.objects.filter(**{fld_cmp: val}),to_attr="peakgroup_list"))

        res = render(request, format_template, {"qry": qry, "studies": studies, "animals":animals, "samples": samples})
    else:
        raise Http404("Results format [" + fmt + "] page not found")

    return res
