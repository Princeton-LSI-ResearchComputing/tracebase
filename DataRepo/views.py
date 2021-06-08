from django.core.exceptions import FieldError
from django.http import Http404
from django.shortcuts import render
from django.views.generic import DetailView, ListView

from .models import Compound, PeakData, Study


def home(request):
    return render(request, "home.html")


class CompoundListView(ListView):
    """Generic class-based view for a list of compounds"""

    model = Compound
    context_object_name = "compound_list"
    template_name = "DataRepo/compound_list.html"
    paginate_by = 20


class CompoundDetailView(DetailView):
    """Generic class-based detail view for a compound"""

    model = Compound


class StudyListView(ListView):
    """Generic class-based view for a list of studies."""

    model = Study
    paginate_by = 20


class StudyDetailView(DetailView):
    """Generic class-based detail view for a study."""

    model = Study


def search_basic(request, mdl, fld, cmp, val, fmt):
    """Generic function-based view for a basic search."""

    qry = {}
    qry["mdl"] = mdl
    qry["fld"] = fld
    qry["cmp"] = cmp
    qry["val"] = val
    qry["fmt"] = fmt

    format_template = ""
    if fmt == "peakgroups":
        format_template = "peakgroups_results.html"
        fld_cmp = ""

        if mdl == "Study":
            fld_cmp = "peak_group__ms_run__sample__animal__studies__"
        elif mdl == "Animal":
            fld_cmp = "peak_group__ms_run__sample__animal__"
        elif mdl == "Sample":
            fld_cmp = "peak_group__ms_run__sample__"
        elif mdl == "Tissue":
            fld_cmp = "peak_group__ms_run__sample__tissue__"
        elif mdl == "MSRun":
            fld_cmp = "peak_group__ms_run__"
        elif mdl == "PeakGroup":
            fld_cmp = "peak_group__"
        elif mdl != "PeakData":
            raise Http404(
                "Table [" + mdl + "] is not searchable in the [" + fmt + "] "
                "results format."
            )

        fld_cmp += fld + "__" + cmp

        try:
            peakdata = PeakData.objects.filter(**{fld_cmp: val})
        except FieldError as fe:
            raise Http404(
                "Table ["
                + mdl
                + "] either does not contain a field named ["
                + fld
                + "] or that field is not searchable.  Note, none of "
                "the cached property fields are searchable.  The error was: ["
                + str(fe)
                + "]."
            )

        res = render(request, format_template, {"qry": qry, "pds": peakdata})
    else:
        raise Http404("Results format [" + fmt + "] page not found")

    return res
