from django.core.exceptions import FieldError
from django.http import Http404
from django.shortcuts import get_object_or_404, render
from django.views.generic import DetailView, ListView

from DataRepo.models import (
    Animal,
    Compound,
    MSRun,
    PeakData,
    PeakGroup,
    PeakGroupSet,
    Protocol,
    Sample,
    Study,
)


def home(request):
    return render(request, "home.html")


class CompoundListView(ListView):
    """Generic class-based view for a list of compounds"""

    model = Compound
    context_object_name = "compound_list"
    template_name = "DataRepo/compound_list.html"
    ordering = ["name"]
    paginate_by = 20


class CompoundDetailView(DetailView):
    """Generic class-based detail view for a compound"""

    model = Compound
    template_name = "DataRepo/compound_detail.html"


class StudyListView(ListView):
    """Generic class-based view for a list of studies."""

    model = Study
    context_object_name = "study_list"
    template_name = "DataRepo/study_list.html"
    ordering = ["name"]
    paginate_by = 20


class StudyDetailView(DetailView):
    """Generic class-based detail view for a study."""

    model = Study
    template_name = "DataRepo/study_detail.html"


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
            fld_cmp = "peak_group__msrun__sample__animal__studies__"
        elif mdl == "Animal":
            fld_cmp = "peak_group__msrun__sample__animal__"
        elif mdl == "Sample":
            fld_cmp = "peak_group__msrun__sample__"
        elif mdl == "Tissue":
            fld_cmp = "peak_group__msrun__sample__tissue__"
        elif mdl == "MSRun":
            fld_cmp = "peak_group__msrun__"
        elif mdl == "PeakGroup":
            fld_cmp = "peak_group__"
        elif mdl != "PeakData":
            raise Http404(
                "Table [" + mdl + "] is not searchable in the [" + fmt + "] "
                "results format."
            )

        fld_cmp += fld + "__" + cmp

        try:
            peakdata = PeakData.objects.filter(**{fld_cmp: val}).prefetch_related(
                "peak_group__msrun__sample__animal__studies"
            )
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


class ProtocolListView(ListView):
    """Generic class-based view for aa list of protocols"""

    model = Protocol
    context_object_name = "protocol_list"
    template_name = "DataRepo/protocol_list.html"
    ordering = ["name"]
    paginate_by = 20


class ProtocolDetailView(DetailView):
    """Generic class-based detail view for a protocol"""

    model = Protocol
    template_name = "DataRepo/protocol_detail.html"


class AnimalListView(ListView):
    """Generic class-based view for aa list of animals"""

    model = Animal
    context_object_name = "animal_list"
    template_name = "DataRepo/animal_list.html"
    ordering = ["name"]
    paginate_by = 20


class AnimalDetailView(DetailView):
    """Generic class-based detail view for an animal"""

    model = Animal
    template_name = "DataRepo/animal_detail.html"


class SampleListView(ListView):
    """
    Generic class-based view for a list of samples
    "model = Sample" is shorthand for queryset = Sample.objects.all()
    use queryset syntax for sample list with or without filtering
    """

    # return all samples without query filter
    queryset = Sample.objects.all()
    context_object_name = "sample_list"
    template_name = "DataRepo/sample_list.html"
    ordering = ["animal_id", "name"]
    paginate_by = 20

    # filter sample list by animal_id
    def get_queryset(self):
        queryset = super().get_queryset()
        # get query string from request
        animal_pk = self.request.GET.get("animal_id", None)
        if animal_pk is not None:
            self.animal = get_object_or_404(Animal, id=animal_pk)
            queryset = Sample.objects.filter(animal_id=animal_pk)
        return queryset


class SampleDetailView(DetailView):
    """Generic class-based detail view for a sample"""

    model = Sample
    template_name = "DataRepo/sample_detail.html"


class MSRunListView(ListView):
    """Generic class-based view for a list of MS runs"""

    model = MSRun
    context_object_name = "msrun_list"
    template_name = "DataRepo/msrun_list.html"
    ordering = ["id"]
    paginate_by = 20


class MSRunDetailView(DetailView):
    """Generic class-based detail view for a MS run"""

    model = MSRun
    template_name = "DataRepo/msrun_detail.html"


class PeakGroupSetListView(ListView):
    """Generic class-based view for a list of PeakGroup sets"""

    model = PeakGroupSet
    context_object_name = "peakgroupset_list"
    template_name = "DataRepo/peakgroupset_list.html"
    ordering = ["id"]
    paginate_by = 20


class PeakGroupSetDetailView(DetailView):
    """Generic class-based detail view for a PeakGroup set"""

    model = PeakGroupSet
    template_name = "DataRepo/peakgroupset_detail.html"


class PeakGroupListView(ListView):
    """
    Generic class-based view for a list of peak groups
    "model = PeakGroup" is shorthand for queryset = PeakGroup.objects.all()
    use queryset syntax for PeakGroup list with or without filtering
    """

    queryset = PeakGroup.objects.all()
    context_object_name = "peakgroup_list"
    template_name = "DataRepo/peakgroup_list.html"
    ordering = ["msrun_id", "peak_group_set_id", "name"]
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


class PeakDataListView(ListView):
    """
    Generic class-based view for a list of peak data
    "model = PeakData" is shorthand for queryset = PeakData.objects.all()
    use queryset syntax for PeakData list with or without filtering
    """

    queryset = PeakData.objects.all()
    context_object_name = "peakdata_list"
    template_name = "DataRepo/peakdata_list.html"
    ordering = ["peak_group_id", "id"]
    paginate_by = 200

    # filter peakgdata_list by peak_group_id
    def get_queryset(self):
        queryset = super().get_queryset()
        # get query string from request
        peakgroup_pk = self.request.GET.get("peak_group_id", None)
        if peakgroup_pk is not None:
            self.peakgroup = get_object_or_404(PeakGroup, id=peakgroup_pk)
            queryset = PeakData.objects.filter(peak_group_id=peakgroup_pk)
        return queryset


def selected_data(request):
    context = {}
    template_name = "DataRepo/selected_data_list.html"

    study_pk = 1
    study_name = Study.objects.get(id=study_pk).name
    sub_qs = Sample.objects.select_related("animal").filter(
        animal__id__in=Animal.objects.values_list("id").filter(studies__id=study_pk)
    )
    sample_set = sub_qs.values_list("name")
    # print(sample_set[0])
    # sample_count = sub_qs.count()
    qs = (
        PeakData.objects.select_related("peak_group")
        .filter(peak_group__msrun__sample__name__in=sample_set)
        .all()
    )

    print(qs.count())

    context = {"study": study_name, "selected_data_list": qs}
    return render(request, template_name, context)


def selected_t1(request):
    context = {}
    template_name = "DataRepo/selected_data_t1.html"

    study_pk = 1
    study_name = Study.objects.get(id=study_pk).name
    sub_qs = Sample.objects.select_related("animal").filter(
        animal__id__in=Animal.objects.values_list("id").filter(studies__id=study_pk)
    )
    sample_set = sub_qs.values_list("name")
    # print(sample_set[0])
    # sample_count = sub_qs.count()
    qs = (
        PeakData.objects.select_related("peak_group")
        .filter(peak_group__msrun__sample__name__in=sample_set)
        .all()
    )

    print(qs.count())

    context = {"study": study_name, "selected_data_t1": qs}
    return render(request, template_name, context)


def selected_t2(request):
    context = {}
    template_name = "DataRepo/selected_data_t2.html"

    study_pk = 1
    study_name = Study.objects.get(id=study_pk).name
    sub_qs = Sample.objects.select_related("animal").filter(
        animal__id__in=Animal.objects.values_list("id").filter(studies__id=study_pk)
    )
    sample_set = sub_qs.values_list("name")
    # print(sample_set[0])
    # sample_count = sub_qs.count()
    qs = (
        PeakData.objects.select_related("peak_group")
        .filter(peak_group__msrun__sample__name__in=sample_set)
        .all()
    )

    fields = [
        "peak_group",
        "peak_group__name",
        "labeled_element",
        "labeled_count",
        "peak_group__msrun__sample__animal__name",
        "peak_group__msrun__sample__tissue__name",
        "peak_group__msrun__sample__name",
        "peak_group__msrun__sample__animal__feeding_status",
        "peak_group__msrun__sample__animal__tracer_infusion_rate",
        "peak_group__msrun__sample__animal__tracer_infusion_concentration",
        "peak_group__msrun__sample__animal__tracer_compound__name",
    ]

    # qs = qs.values_list(*fields)
    qs = qs.values(*fields)
    print(qs.count())

    context = {"study": study_name, "selected_data_t2": qs}
    return render(request, template_name, context)
