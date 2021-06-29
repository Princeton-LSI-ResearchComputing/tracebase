from django.core.exceptions import FieldError
from django.http import Http404
from django.shortcuts import get_object_or_404, render
from django.views.generic import DetailView, ListView
from django.views.generic.edit import FormView
from django.db.models import Q
from django.forms import formset_factory
import json

from DataRepo.forms import AdvSearchPeakGroupsForm

from DataRepo.models import (
    Animal,
    Compound,
    MSRun,
    PeakData,
    PeakGroup,
    PeakGroupSet,
    Protocol,
    Sample,
    Study
)


debug = False


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


class AdvSearchPeakGroupsView(FormView):
    form_class = formset_factory(AdvSearchPeakGroupsForm)
    template_name = 'DataRepo/search_peakgroups.html'
    success_url = ''

    # Override get_context_data to retrieve mode from the query string
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Optional url parameter should now be in self, so add it to the context
        mode = self.request.GET.get('mode', 'search')
        if mode != 'browse' and mode != 'search':
            mode = 'search'
            print("Invalid mode: ",mode)
        context['mode'] = mode
        return context

    def form_invalid(self, form):
        print("The form was invalid")
        print(form)
        qry = formsetToHash(form,AdvSearchPeakGroupsForm.base_fields.keys())
        res = {}
        return self.render_to_response(self.get_context_data(res=res, form=form, qry=qry, debug=debug))

    def form_valid(self, form):
        print("The form was valid")
        print(form.cleaned_data)
        qry = formsetToHash(form,AdvSearchPeakGroupsForm.base_fields.keys())
        print(json.dumps(qry, indent=4))
        res = {}
        if len(qry) == 1:
            q_exp = constructAdvancedQuery(qry[0])
            print("Query expression: ",q_exp)
            res = PeakData.objects.filter(q_exp).prefetch_related(
                "peak_group__ms_run__sample__animal__studies"
            )
        elif len(qry) == 0:
            print("Empty query")
            if mode == "browse":
                res = PeakData.objects.all().prefetch_related(
                    "peak_group__ms_run__sample__animal__studies"
                )

                # The form factory works by cloning, thus for new formsets to be
                # created when no forms were submitted, we need to produce a new
                # form from the factory
                form = formset_factory(AdvSearchPeakGroupsForm)
        else:
            print("Invalid query root")
        return self.render_to_response(self.get_context_data(res=res, form=form, qry=qry, debug=debug))


def constructAdvancedQuery(qry):
    curqry = qry
    if qry['type'] == "query":
        cmp = qry['ncmp'].replace("not_", "", 1)
        criteria = {'{0}__{1}'.format(qry['fld'], cmp): qry['val']}
        if cmp == qry['ncmp']:
            return Q(**criteria)
        else:
            return ~Q(**criteria)
    elif qry['type'] == "group":
        q = Q()
        gotone = False
        for elem in qry['queryGroup']:
            gotone = True
            if qry['val'] == "all":
                nq = constructAdvancedQuery(elem)
                if nq is None:
                    return None
                else:
                    q &= nq
            elif qry['val'] == "any":
                nq = constructAdvancedQuery(elem)
                if nq is None:
                    return None
                else:
                    q |= nq
            else:
                return None
        if not gotone or q is None:
            return None
        else:
            return q
    return None

def formsetToHash(rawformset, form_fields):
    qry = []

    # We take a raw form instead of cleaned_data so that form_invalid will repopulate the bad form as-is
    isRaw = False
    try:
        formset = rawformset.cleaned_data
    except:
        isRaw = True
        formset = rawformset

    for rawform in formset:

        if isRaw:
            form = rawform.saved_data
        else:
            form = rawform

        curqry = qry
        path = form["pos"].split(".")
        for spot in path:
            pos_gtype = spot.split("-")
            if len(pos_gtype) == 2:
                pos = pos_gtype[0]
                gtype = pos_gtype[1]
            else:
                pos = spot
                gtype = None
            pos = int(pos)
            while len(curqry) <= pos:
                curqry.append({})
            if gtype is not None:
                if not curqry[pos]:
                    # This is a group
                    curqry[pos]["pos"] = ""
                    curqry[pos]["type"] = "group"
                    curqry[pos]["val"] = gtype
                    curqry[pos]["queryGroup"] = []
                curqry = curqry[pos]["queryGroup"]
                print("Setting pointer to",pos,"queryGroup")
            else:
                # This is a query
                print("Setting pos",pos,"type to query")

                # Keep track of keys encountered
                keys_seen = {}
                for key in form_fields:
                    keys_seen[key] = 0
                cmpnts = []

                curqry[pos]["type"] = "query"
                for key in form.keys():
                    cmpnts = key.split("-")
                    keyname = cmpnts[-1]
                    keys_seen[key] = 1
                    if keyname == "pos":
                        curqry[pos][key] = ""
                    elif key not in curqry[pos]:
                        print("Setting pos",pos,key,"to",form[key])
                        curqry[pos][key] = form[key]
                    else:
                        print("ERROR: NOT setting pos",pos,key,"to",form[key])

                # Now initialize anything missing a value to an empty string
                # This is to be able to correctly reconstruct the user's query upon form_invalid
                for key in form_fields:
                    if keys_seen[key] == 0:
                        curqry[pos][key] = ""

                print()
    return qry


# used by templatetags/advsrch_tags.py to pre-populate search results in browse mode
def getAllPeakGroupsFmtData():
    return PeakData.objects.all().prefetch_related(
        "peak_group__ms_run__sample__animal__studies"
    )


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
