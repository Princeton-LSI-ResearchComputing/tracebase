from django.conf import settings
from django.core.exceptions import FieldError
from django.db.models import Q
from django.forms import formset_factory
from django.http import Http404
from django.shortcuts import get_object_or_404, render
from django.views.generic import DetailView, ListView
from django.views.generic.edit import FormView
import json
from datetime import datetime

from DataRepo.forms import AdvSearchPeakDataForm, AdvSearchPeakGroupsForm, AdvSearchDownloadForm
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

    model_data = {
        "forms": {
            "pgtemplate": formset_factory(AdvSearchPeakGroupsForm),
            "pdtemplate": formset_factory(AdvSearchPeakDataForm),
        },
        "names": {
            "pgtemplate": "PeakGroups",
            "pdtemplate": "PeakData",
        },
        "prefetches": {
            "pgtemplate": "msrun__sample__animal__studies",
            "pdtemplate": "peak_group__msrun__sample__animal",
        },
        "searchables": {
            "pgtemplate": {
                "PeakGroups": {
                    "name": "name",
                    #"enrichment_fraction", #Cannot search cached property
                    #"total_abundance", #Cannot search cached property
                    #"normalized_labeling", #Cannot search cached property
                },
                "Sample": {
                    "id": "msrun__sample__id", # Used in link
                    "name": "msrun__sample__name",
                },
                "Tissue": {
                    "name": "msrun__sample__tissue__name",
                },
                "Animal": {
                    "tracer_labeled_atom": "msrun__sample__animal__tracer_labeled_atom",
                    "id": "msrun__sample__animal__id", # Used in link
                    "name": "msrun__sample__animal__name",
                    "feeding_status": "msrun__sample__animal__feeding_status",
                    "tracer_infusion_rate": "msrun__sample__animal__tracer_infusion_rate",
                    "tracer_infusion_concentration": "msrun__sample__animal__tracer_infusion_concentration",
                },
                "Compound": {
                    "name": "msrun__sample__animal__tracer_compound__name",
                },
                "Study": {
                    "id": "msrun__sample__animal__studies__id", # Used in link
                    "name": "msrun__sample__animal__studies__name",
                },
            },
            "pdtemplate": {
                "PeakData": {
                    "labeled_element": "labeled_element",
                    "corrected_abundance": "corrected_abundance",
                    "fraction": "fraction",
                },
                "PeakGroup": {
                    "id": "peak_group__id", # Used in link
                    "name": "peak_group__name",
                },
                "Sample": {
                    "id": "peak_group__msrun__sample__id", # Used in link
                    "name": "peak_group__msrun__sample__name",
                },
                "Tissue": {
                    "name": "peak_group__msrun__sample__tissue__name",
                },
                "Animal": {
                    "id": "peak_group__msrun__sample__animal__id", # Used in link
                    "name": "peak_group__msrun__sample__animal__name",
                },
                "Compund": {
                    "name": "peak_group__msrun__sample__animal__tracer_compound__name",
                },
            },
        },
    }


    format_template = "DataRepo/search_advanced.html"
    qry = createNewBasicQuery(model_data, mdl, fld, cmp, val, fmt)
    q_exp = constructAdvancedQuery(qry)
    res = performQuery(qry, q_exp, model_data["prefetches"])
    return render(request, format_template, {"forms": model_data["forms"], "qry": qry, "res": res, "debug": settings.DEBUG})


# Based on:
#   https://stackoverflow.com/questions/15497693/django-can-class-based-views-accept-two-forms-at-a-time
class AdvancedSearchView(MultiFormsView):
    # MultiFormView class vars
    template_name = "DataRepo/search_advanced.html"
    form_classes = {
        "pgtemplate": formset_factory(AdvSearchPeakGroupsForm),
        "pdtemplate": formset_factory(AdvSearchPeakDataForm),
    }
    success_url = ""
    mixedform_selected_formtype = "fmt"
    mixedform_prefix_field = "pos"
    # Local class vars
    form_class_info = {
        "default_mode": "search",
        "modes": ["search", "browse"],
        "default_class": "pgtemplate",
        "fields": {
            "pgtemplate": AdvSearchPeakGroupsForm.base_fields.keys(),
            "pdtemplate": AdvSearchPeakDataForm.base_fields.keys(),
        },
        "prefetches": {
            "pgtemplate": "msrun__sample__animal__studies",
            "pdtemplate": "peak_group__msrun__sample__animal",
        },
        "displayfields": {
            "pgtemplate": [
                "name",
                "enrichment_fraction",
                "total_abundance",
                "normalized_labeling",
                "msrun__sample__id", # Used in link
                "msrun__sample__name",
                "msrun__sample__tissue__name",
                "msrun__sample__animal__tracer_labeled_atom",
                "msrun__sample__animal__id", # Used in link
                "msrun__sample__animal__name",
                "msrun__sample__animal__feeding_status",
                "msrun__sample__animal__tracer_infusion_rate",
                "msrun__sample__animal__tracer_infusion_concentration",
                "msrun__sample__animal__tracer_compound__name",
                "msrun__sample__animal__studies__id", # Used in link
                "msrun__sample__animal__studies__name",
            ],
            "pdtemplate": [
                "peak_group__id",
                "peak_group__name",
                "peak_group__msrun__sample__id", # Used in link
                "peak_group__msrun__sample__name",
                "peak_group__msrun__sample__tissue__name",
                "peak_group__msrun__sample__animal__id", # Used in link
                "peak_group__msrun__sample__animal__name",
                "peak_group__msrun__sample__animal__tracer_compound__name",
                "labeled_element",
                "corrected_abundance",
                "fraction",
            ],
        },
        "names": {
            "pgtemplate": "PeakGroups",
            "pdtemplate": "PeakData",
        },
    }
    download_form = AdvSearchDownloadForm()

    # Override get_context_data to retrieve mode from the query string
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Optional url parameter should now be in self, so add it to the context
        mode = self.request.GET.get("mode", self.form_class_info["default_mode"])
        format = self.request.GET.get("format", self.form_class_info["default_class"])
        if mode not in self.form_class_info["modes"]:
            mode = self.form_class_info["default_mode"]
            print("Invalid mode: ", mode)
        context["mode"] = mode
        context["format"] = format
        context["default_format"] = self.form_class_info["default_class"]
        addInitialContext(context, self.form_class_info)
        print("Context Data: ",context)
        return context

    def form_invalid(self, formset):
        qry = formsetsToDict(formset, self.form_class_info["fields"])
        res = {}
        return self.render_to_response(
            self.get_context_data(
                res=res, forms=self.form_classes, qry=qry, debug=settings.DEBUG
            )
        )

    def form_valid(self, formset):
        qry = formsetsToDict(formset, self.form_class_info["fields"])
        res = {}
        download_form = {}
        if isQryObjValid(qry, self.form_classes.keys()):
            download_form = AdvSearchDownloadForm(initial={'qryjson': json.dumps(qry)})
            q_exp = constructAdvancedQuery(qry)
            performQuery(qry, q_exp, self.form_class_info["prefetches"])
        else:
            # Log a warning
            print("WARNING: Invalid query root:", qry)
        return self.render_to_response(
            self.get_context_data(
                res=res, forms=self.form_classes, qry=qry, download_form=download_form, debug=settings.DEBUG
            )
        )


def addInitialContext(context, form_class_info):
    mode = form_class_info["default_mode"]
    if "mode" in context and context["mode"] == "browse":
        mode = "browse"
    context["mode"] = mode
    if "qry" not in context or mode == "browse" and len(context["qry"]["searches"][context["qry"]["selectedtemplate"]]["tree"]["queryGroup"]) == 0:
        print("Establishing browse data")
        if "qry" not in context:
            print("Creating query")
            qry = createNewQuery(form_class_info, context)
        else:
            qry = context["qry"]
        if mode == "browse":
            context["download_form"] = AdvSearchDownloadForm(initial={'qryjson': json.dumps(qry)})
            res = getAllBrowseData(qry["selectedtemplate"], form_class_info["prefetches"])
            context["res"] = res
    elif "qry" in context and len(context["qry"]["searches"][context["qry"]["selectedtemplate"]]["tree"]["queryGroup"]) > 0 and ("res" not in context or len(context["res"]) == 0):
        print("Running query")
        context["download_form"] = AdvSearchDownloadForm(initial={'qryjson': json.dumps(context["qry"])})
        q_exp = constructAdvancedQuery(context["qry"])
        res = performQuery(context["qry"], q_exp, form_class_info["prefetches"])
        context["res"] = res


def createNewQuery(form_class_info, context):
    qry = {}
    qry["searches"] = {}
    if "format" in context:
        qry["selectedtemplate"] = context["format"]
    else:
        qry["selectedtemplate"] = form_class_info["default_class"]
    for format, formatName in form_class_info["names"].items():
        qry["searches"][format] = {}
        qry["searches"][format]["name"] = formatName
        qry["searches"][format]["tree"] = {}
        qry["searches"][format]["tree"]["pos"] = ""
        qry["searches"][format]["tree"]["type"] = "group"
        qry["searches"][format]["tree"]["val"] = "all"
        qry["searches"][format]["tree"]["queryGroup"] = []
    return qry


def createNewBasicQuery(model_data, mdl, fld, cmp, val, fmt):
    qry = {}

    foundit = False
    if fmt in model_data["names"]:
        qry["selectedtemplate"] = fmt # Note, the fmt is different than when search_basic used to use
        foundit = True
    else:
        for fmtid, fmtnm in model_data["names"].items():
            print("Checking " + fmtnm.lower() + " against " + fmt.lower())
            if fmtnm.lower() == fmt.lower():
                print("Equal")
                fmt = fmtid
                foundit = True

    if foundit is False:
        raise Http404("Invalid format [" + fmt + "].  Must be one of: [" + ",".join(model_data["names"].keys()) + "," + ",".join(model_data["names"].values()) + "]")

    qry["selectedtemplate"] = fmt

    qry["searches"] = {}
    for format in model_data["searchables"]:
        qry["searches"][format] = {}
        qry["searches"][format]["name"] = model_data["names"][format]
        qry["searches"][format]["tree"] = {}
        qry["searches"][format]["tree"]["pos"] = ""
        qry["searches"][format]["tree"]["type"] = "group"
        qry["searches"][format]["tree"]["val"] = "all"
        qry["searches"][format]["tree"]["queryGroup"] = []

    if mdl not in model_data["searchables"][fmt]:
        raise Http404("Invalid model [" + mdl + "].  Must be one of [" + ",".join(model_data["searchables"][fmt].keys()) + "].")
    if fld not in model_data["searchables"][fmt][mdl]:
        raise Http404("Invalid field [" + fld + "].  Must be one of [" + ",".join(model_data["searchables"][fmt][mdl].keys()) + "].")
    
    qry["searches"][fmt]["tree"]["queryGroup"].append({})
    qry["searches"][fmt]["tree"]["queryGroup"][0]["type"] = "query"
    qry["searches"][fmt]["tree"]["queryGroup"][0]["pos"] = ""
    qry["searches"][fmt]["tree"]["queryGroup"][0]["fld"] = model_data["searchables"][fmt][mdl][fld]
    qry["searches"][fmt]["tree"]["queryGroup"][0]["ncmp"] = cmp
    qry["searches"][fmt]["tree"]["queryGroup"][0]["val"] = val

    return qry


def performQuery(qry, q_exp, prefetches):
    res = {}
    if qry["selectedtemplate"] == "pgtemplate":
        res = PeakGroup.objects.filter(q_exp).prefetch_related(
            prefetches[qry["selectedtemplate"]]
        )
    elif qry["selectedtemplate"] == "pdtemplate":
        res = PeakData.objects.filter(q_exp).prefetch_related(
            prefetches[qry["selectedtemplate"]]
        )
    else:
        # Log a warning
        print("WARNING: Invalid selected format:", qry["selectedtemplate"])
    return res



# Based on: https://stackoverflow.com/questions/29672477/django-export-current-queryset-to-csv-by-button-click-in-browser
class AdvancedSearchTSVView(FormView):
    form_class = AdvSearchDownloadForm
    template_name = 'DataRepo/search_advanced.tsv'
    content_type = 'application/text'
    success_url = ""

    prefetches = {
        "pgtemplate": "msrun__sample__animal__studies",
        "pdtemplate": "peak_group__msrun__sample__animal",
    }

    def form_invalid(self, form):
        sform = form.saved_data
        qry = json.loads(sform["qryjson"])
        res = {}
        return self.render_to_response(
            self.get_context_data(
                res=res, qry=qry, debug=settings.DEBUG
            )
        )

    def form_valid(self, form):
        cform = form.cleaned_data
        print("Cleaned data: ",cform)
        print("Should be qry json: It is of type ",type(cform["qryjson"]),":",cform["qryjson"])
        qry = json.loads(cform["qryjson"])
        if not isQryObjValid(qry, self.prefetches.keys()):
            raise Http404("Invalid json")
        print("Form is valid and contained: ", qry)

        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        filename = qry["searches"][qry["selectedtemplate"]]["name"] + '_' + now.strftime("%d.%m.%Y.%H.%M.%S") + ".tsv"

        if isValidQryObjPopulated(qry):
            q_exp = constructAdvancedQuery(qry)
            res = performQuery(qry, q_exp, self.prefetches)
        else:
            fmt = qry["selectedtemplate"]
            res = getAllBrowseData(fmt, self.prefetches)
        print("Sending " + str(len(res)) + " records")
        response = self.render_to_response(self.get_context_data(res=res, qry=qry, dt=dt_string, debug=settings.DEBUG))
        response["Content-Disposition"] = "attachment; filename={}".format(filename)
        return response


def getJoinedFields(display_field_objects):
    field_object_list = []
    for model_field_dict in display_field_objects:
        for dispfield in model_field_dict["model"]._meta.get_fields():
            if dispfield.name in model_field_dict["fields"]:
                field_object_list.append(dispfield.name)
    return field_object_list


def isQryObjValid(qry, form_class_list):
    """
    Determines if a qry object was properly constructed/populated (only at the root).
    """
    if (
        type(qry) is dict
        and "selectedtemplate" in qry
        and "searches" in qry
        and len(form_class_list) == len(qry["searches"].keys())
    ):
        for key in form_class_list:
            if (
                key not in qry["searches"]
                or type(qry["searches"][key]) is not dict
                or "tree" not in qry["searches"][key]
                or "name" not in qry["searches"][key]
            ):
                return False
        return True
    else:
        return False


def isValidQryObjPopulated(qry):
    selfmt = qry["selectedtemplate"]
    return len(qry["searches"][selfmt]["tree"]["queryGroup"]) > 0


def constructAdvancedQuery(qryRoot):
    """
    Turns a qry object into a complex Q object by calling its helper and supplying the selected format's tree.
    """
    return constructAdvancedQueryHelper(
        qryRoot["searches"][qryRoot["selectedtemplate"]]["tree"]
    )


def constructAdvancedQueryHelper(qry):
    """
    Recursively build a complex Q object based on a hierarchical tree defining the search terms.
    """
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
    """
    Takes a series of forms and a list of form fields and uses the pos field to construct a hierarchical qry tree.
    """
    # All forms of each type are all submitted together in a single submission and are duplicated in the rawformset
    # dict.  We only need 1 copy to get all the data, so we will arbitrarily use the first one

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

        # If this format has not yet been initialized
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
    """
    Takes a substring from a pos field defining a single tree node and returns its position and group type (if it's an
    inner node).  E.g. "0-all"
    """
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
    """
    Takes the first substring from a pos field defining the root node and returns the format code, format name, and
    whether it is the selected format.
    """
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


def getAllBrowseData(format, prefetches):
    if format == "pgtemplate":
        return PeakGroup.objects.all().prefetch_related(
            prefetches[format]
        )
    elif format == "pdtemplate":
        return PeakData.objects.all().prefetch_related(
            prefetches[format]
        )
    else:
        # Log a warning
        print("WARNING: Unknown format: " + format)
        return {}


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
