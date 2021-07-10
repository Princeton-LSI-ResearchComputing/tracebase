from django.conf import settings
from django.core.exceptions import FieldError
from django.db.models import Q
from django.forms import formset_factory
from django.http import Http404
from django.shortcuts import get_object_or_404, render
from django.views.generic import DetailView, ListView

from DataRepo.forms import AdvSearchPeakDataForm, AdvSearchPeakGroupsForm
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
from DataRepo.multiforms import MultiFormsView


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


# Based on:
#   https://stackoverflow.com/questions/15497693/django-can-class-based-views-accept-two-forms-at-a-time
class AdvancedSearchView(MultiFormsView):
    template_name = "DataRepo/search_advanced.html"
    form_classes = {
        "pgtemplate": formset_factory(AdvSearchPeakGroupsForm),
        "pdtemplate": formset_factory(AdvSearchPeakDataForm),
    }
    success_url = ""
    mixedform_selected_formtype = "fmt"
    mixedform_prefix_field = "pos"

    # Override get_context_data to retrieve mode from the query string
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Optional url parameter should now be in self, so add it to the context
        mode = self.request.GET.get("mode", "search")
        format = self.request.GET.get("format", "pgtemplate")
        if mode != "browse" and mode != "search":
            mode = "search"
            print("Invalid mode: ", mode)
        context["mode"] = mode
        context["format"] = format
        return context

    def form_invalid(self, formset):
        qry = formsetsToDict(
            formset,
            {
                "pgtemplate": AdvSearchPeakGroupsForm.base_fields.keys(),
                "pdtemplate": AdvSearchPeakDataForm.base_fields.keys(),
            },
        )
        res = {}
        return self.render_to_response(
            self.get_context_data(
                res=res, forms=self.form_classes, qry=qry, debug=settings.DEBUG
            )
        )

    def form_valid(self, formset):
        qry = formsetsToDict(
            formset,
            {
                "pgtemplate": AdvSearchPeakGroupsForm.base_fields.keys(),
                "pdtemplate": AdvSearchPeakDataForm.base_fields.keys(),
            },
        )
        res = {}
        if len(qry.keys()) == 2:
            q_exp = constructAdvancedQuery(qry)
            if qry["selectedtemplate"] == "pgtemplate":
                res = PeakData.objects.filter(q_exp).prefetch_related(
                    "peak_group__msrun__sample__animal__studies"
                )
            elif qry["selectedtemplate"] == "pdtemplate":
                res = PeakData.objects.filter(q_exp).prefetch_related(
                    "peak_group__msrun__sample__animal"
                )
        elif len(qry.keys()) == 0:
            # There was no search, so pre-populate with the default format if in browse mode
            # Optional url parameter should now be in self, so add it to the context
            mode = self.request.GET.get("mode", "search")
            format = self.request.GET.get("format", "pgtemplate")
            if mode == "browse":
                if format == "pdtemplate":
                    res = PeakData.objects.all().prefetch_related(
                        "peak_group__msrun__sample__animal"
                    )
                else:
                    res = PeakData.objects.all().prefetch_related(
                        "peak_group__msrun__sample__animal__studies"
                    )
        else:
            # Log a warning
            print("WARNING: Invalid query root:", qry)
        return self.render_to_response(
            self.get_context_data(
                res=res, forms=self.form_classes, qry=qry, debug=settings.DEBUG
            )
        )


def constructAdvancedQuery(qryRoot):
    return constructAdvancedQueryHelper(
        qryRoot["searches"][qryRoot["selectedtemplate"]]["tree"]
    )


def constructAdvancedQueryHelper(qry):
    if qry["type"] == "query":
        cmp = qry["ncmp"].replace("not_", "", 1)
        negate = cmp != qry["ncmp"]

        # Special case for isnull (ignores qry['val'])
        if cmp == "isnull":
            if negate:
                negate = False
                qry["val"] = False
            else:
                qry["val"] = True

        criteria = {"{0}__{1}".format(qry["fld"], cmp): qry["val"]}
        if negate is False:
            return Q(**criteria)
        else:
            return ~Q(**criteria)

    elif qry["type"] == "group":
        q = Q()
        gotone = False
        for elem in qry["queryGroup"]:
            gotone = True
            if qry["val"] == "all":
                nq = constructAdvancedQueryHelper(elem)
                if nq is None:
                    return None
                else:
                    q &= nq
            elif qry["val"] == "any":
                nq = constructAdvancedQueryHelper(elem)
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


def formsetsToDict(rawformset, form_fields_dict):
    # All forms of each type are all submitted together in a single submission and are duplicated in the rawformset
    # dict.  We only need 1 copy to get all the data, so we will arbitrarily us the first one

    # Figure out which form class processed the forms (inferred by the presence of 'saved_data' - this is also the
    # selected format)
    processed_formkey = None
    for key in rawformset.keys():
        if "saved_data" in rawformset[key][0].__dict__:
            processed_formkey = key
            break

    # If we were unable to locate the selected output format (i.e. the copy of the formsets that were processed)
    if processed_formkey is None:
        raise Http404("Unable to find selected output format.")

    return formsetToDict(rawformset[processed_formkey], form_fields_dict)


def formsetToDict(rawformset, form_fields_dict):
    search = {"selectedtemplate": "", "searches": {}}

    # We take a raw form instead of cleaned_data so that form_invalid will repopulate the bad form as-is
    isRaw = False
    try:
        formset = rawformset.cleaned_data
    except AttributeError:
        isRaw = True
        formset = rawformset

    for rawform in formset:

        if isRaw:
            form = rawform.saved_data
        else:
            form = rawform

        path = form["pos"].split(".")
        [format, formatName, selected] = rootToFormatInfo(path.pop(0))
        rootinfo = path.pop(0)

        if format not in search["searches"]:
            search["searches"][format] = {}
            search["searches"][format]["tree"] = {}
            search["searches"][format]["name"] = formatName

            # Initialize the root of the tree
            [pos, gtype] = pathStepToPosGroupType(rootinfo)
            aroot = search["searches"][format]["tree"]
            aroot["pos"] = ""
            aroot["type"] = "group"
            aroot["val"] = gtype
            aroot["queryGroup"] = []
            curqry = aroot["queryGroup"]
        else:
            # The root already exists, so go directly to its child list
            curqry = search["searches"][format]["tree"]["queryGroup"]

        if selected is True:
            search["selectedtemplate"] = format

        for spot in path:
            [pos, gtype] = pathStepToPosGroupType(spot)
            while len(curqry) <= pos:
                curqry.append({})
            if gtype is not None:
                # This is a group
                # If the inner node was not already set
                if not curqry[pos]:
                    curqry[pos]["pos"] = ""
                    curqry[pos]["type"] = "group"
                    curqry[pos]["val"] = gtype
                    curqry[pos]["queryGroup"] = []
                # Move on to the next node in the path
                curqry = curqry[pos]["queryGroup"]
            else:
                # This is a query

                # Keep track of keys encountered
                keys_seen = {}
                for key in form_fields_dict[format]:
                    keys_seen[key] = 0
                cmpnts = []

                curqry[pos]["type"] = "query"

                # Set the form values in the query based on the form elements
                for key in form.keys():
                    # Remove "form-#-" from the form element ID
                    cmpnts = key.split("-")
                    keyname = cmpnts[-1]
                    keys_seen[key] = 1
                    if keyname == "pos":
                        curqry[pos][key] = ""
                    elif key not in curqry[pos]:
                        curqry[pos][key] = form[key]
                    else:
                        # Log a warning
                        print(
                            "WARNING: Unrecognized form element not set at pos",
                            pos,
                            ":",
                            key,
                            "to",
                            form[key],
                        )

                # Now initialize anything missing a value to an empty string
                # This is used to correctly reconstruct the user's query upon form_invalid
                for key in form_fields_dict[format]:
                    if keys_seen[key] == 0:
                        curqry[pos][key] = ""
    return search


def pathStepToPosGroupType(spot):
    pos_gtype = spot.split("-")
    if len(pos_gtype) == 2:
        pos = pos_gtype[0]
        gtype = pos_gtype[1]
    else:
        pos = spot
        gtype = None
    pos = int(pos)
    return [pos, gtype]


def rootToFormatInfo(rootInfo):
    val_name_sel = rootInfo.split("-")
    sel = False
    name = ""
    if len(val_name_sel) == 3:
        val = val_name_sel[0]
        name = val_name_sel[1]
        if val_name_sel[2] == "selected":
            sel = True
    elif len(val_name_sel) == 2:
        val = val_name_sel[0]
        name = val_name_sel[1]
    else:
        print("WARNING: Unable to parse format name from submitted form data.")
        val = val_name_sel
        name = val_name_sel
    return [val, name, sel]


# used by templatetags/advsrch_tags.py to pre-populate search results in browse mode
def getAllPeakGroupsFmtData():
    return PeakData.objects.all().prefetch_related(
        "peak_group__msrun__sample__animal__studies"
    )


class ProtocolListView(ListView):
    """Generic class-based view for a list of protocols"""

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
    """Generic class-based view for a list of animals"""

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
