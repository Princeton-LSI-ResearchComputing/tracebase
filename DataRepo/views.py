import json
from datetime import datetime

from django.conf import settings
from django.db.models import Q
from django.http import Http404
from django.shortcuts import get_object_or_404, render
from django.views.generic import DetailView, ListView
from django.views.generic.edit import FormView

from DataRepo.compositeviews import BaseAdvancedSearchView
from DataRepo.forms import AdvSearchDownloadForm, AdvSearchForm
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

    # Base Advanced Search View Metadata
    basv_metadata = BaseAdvancedSearchView()

    # Base Advanced Search Form
    basf = AdvSearchForm()

    format_template = "DataRepo/search/query.html"
    fmtkey = basv_metadata.formatNameOrKeyToKey(fmt)
    if fmtkey is None:
        names = basv_metadata.getFormatNames()
        raise Http404(
            "Invalid format ["
            + fmt
            + "].  Must be one of: ["
            + ",".join(names.keys())
            + ","
            + ",".join(names.values())
            + "]"
        )

    qry = createNewBasicQuery(basv_metadata, mdl, fld, cmp, val, fmtkey)
    download_form = AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})
    q_exp = constructAdvancedQuery(qry)
    res = performQuery(q_exp, fmtkey, basv_metadata)
    root_group = createNewAdvancedQuery(basv_metadata, {})

    return render(
        request,
        format_template,
        {
            "forms": basf.form_classes,
            "qry": qry,
            "res": res,
            "download_form": download_form,
            "debug": settings.DEBUG,
            "root_group": root_group,
        },
    )


# Based on:
#   https://stackoverflow.com/questions/15497693/django-can-class-based-views-accept-two-forms-at-a-time
class AdvancedSearchView(MultiFormsView):
    """
    This is the view for the advanced search page.
    """

    # Base Advanced Search View
    basv_metadata = BaseAdvancedSearchView()

    # Base Advanced Search Form
    basf = AdvSearchForm()

    # Advanced search download form
    download_form = AdvSearchDownloadForm()

    # MultiFormView class vars
    template_name = "DataRepo/search/query.html"
    form_classes = basf.form_classes
    success_url = ""
    mixedform_selected_formtype = basf.format_select_list_name
    mixedform_prefix_field = basf.hierarchy_path_field_name

    # Override get_context_data to retrieve mode from the query string
    def get_context_data(self, **kwargs):
        """
        Retrieves page context data.
        """

        context = super().get_context_data(**kwargs)

        # Optional url parameter should now be in self, so add it to the context
        mode = self.request.GET.get("mode", self.basv_metadata.default_mode)
        format = self.request.GET.get("format", self.basv_metadata.default_format)
        if mode not in self.basv_metadata.modes:
            mode = self.basv_metadata.default_mode
            # Log a warning
            print("WARNING: Invalid mode: ", mode)

        context["mode"] = mode
        context["format"] = format
        context["default_format"] = self.basv_metadata.default_format
        self.addInitialContext(context)

        return context

    def form_invalid(self, formset):
        """
        Upon invalid advanced search form submission, rescues the query to add back to the context.
        """

        qry = formsetsToDict(formset, self.form_classes)

        root_group = createNewAdvancedQuery(self.basv_metadata, {})

        return self.render_to_response(
            self.get_context_data(
                res={},
                forms=self.form_classes,
                qry=qry,
                debug=settings.DEBUG,
                root_group=root_group,
                default_format=self.basv_metadata.default_format,
            )
        )

    def form_valid(self, formset):
        """
        Upon valid advanced search form submission, adds results (& query) to the context of the search page.
        """

        qry = formsetsToDict(formset, self.form_classes)
        res = {}
        download_form = {}

        if isQryObjValid(qry, self.form_classes.keys()):
            download_form = AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})
            q_exp = constructAdvancedQuery(qry)
            performQuery(q_exp, qry["selectedtemplate"], self.basv_metadata)
        else:
            # Log a warning
            print("WARNING: Invalid query root:", qry)

        root_group = createNewAdvancedQuery(self.basv_metadata, {})

        return self.render_to_response(
            self.get_context_data(
                res=res,
                forms=self.form_classes,
                qry=qry,
                download_form=download_form,
                debug=settings.DEBUG,
                root_group=root_group,
                default_format=self.basv_metadata.default_format,
                order=self.basv_metadata.getFieldOrder(qry["selectedtemplate"]),
                modeldata=self.basv_metadata.getModelData(qry["selectedtemplate"]),
            )
        )

    def addInitialContext(self, context):
        """
        Prepares context data for the initial page load.
        """

        mode = self.basv_metadata.default_mode
        if "mode" in context and context["mode"] == "browse":
            mode = "browse"
        context["mode"] = mode

        context["root_group"] = createNewAdvancedQuery(self.basv_metadata, {})

        if "qry" not in context or (
            mode == "browse" and not isValidQryObjPopulated(context["qry"])
        ):
            if "qry" not in context:
                qry = createNewAdvancedQuery(self.basv_metadata, context)
            else:
                qry = context["qry"]

            if mode == "browse":
                context["download_form"] = AdvSearchDownloadForm(
                    initial={"qryjson": json.dumps(qry)}
                )
                context["res"] = getAllBrowseData(
                    qry["selectedtemplate"], self.basv_metadata
                )

        elif (
            "qry" in context
            and isValidQryObjPopulated(context["qry"])
            and ("res" not in context or len(context["res"]) == 0)
        ):
            qry = context["qry"]
            context["download_form"] = AdvSearchDownloadForm(
                initial={"qryjson": json.dumps(qry)}
            )
            q_exp = constructAdvancedQuery(qry)
            context["res"] = performQuery(
                q_exp, qry["selectedtemplate"], self.basv_metadata
            )


# Basis: https://stackoverflow.com/questions/29672477/django-export-current-queryset-to-csv-by-button-click-in-browser
class AdvancedSearchTSVView(FormView):
    """
    This is the download view for the advanced search page.
    """

    form_class = AdvSearchDownloadForm
    template_name = "DataRepo/search/downloads/download.tsv"
    content_type = "application/text"
    success_url = ""
    basv_metadata = BaseAdvancedSearchView()

    def form_invalid(self, form):
        saved_form = form.saved_data
        qry = {}
        if "qryjson" in saved_form:
            # Discovered this can cause a KeyError during testing, so...
            qry = json.loads(saved_form["qryjson"])
        else:
            print("ERROR: qryjson hidden input not in saved form.")
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        res = {}
        return self.render_to_response(
            self.get_context_data(res=res, qry=qry, dt=dt_string, debug=settings.DEBUG)
        )

    def form_valid(self, form):
        cform = form.cleaned_data
        try:
            qry = json.loads(cform["qryjson"])
            # Apparently this causes a TypeError exception in test_views. Could not figure out why, so...
        except TypeError:
            qry = cform["qryjson"]
        if not isQryObjValid(qry, self.basv_metadata.getFormatNames().keys()):
            raise Http404("Invalid json")

        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        filename = (
            qry["searches"][qry["selectedtemplate"]]["name"]
            + "_"
            + now.strftime("%d.%m.%Y.%H.%M.%S")
            + ".tsv"
        )

        if isValidQryObjPopulated(qry):
            q_exp = constructAdvancedQuery(qry)
            res = performQuery(q_exp, qry["selectedtemplate"], self.basv_metadata)
        else:
            res = getAllBrowseData(qry["selectedtemplate"], self.basv_metadata)

        response = self.render_to_response(
            self.get_context_data(res=res, qry=qry, dt=dt_string, debug=settings.DEBUG)
        )
        response["Content-Disposition"] = "attachment; filename={}".format(filename)

        return response


def getAllBrowseData(format, basv):
    """
    Grabs all data without a filtering match for browsing.
    """

    if format in basv.getFormatNames().keys():
        model = basv.modeldata[format].rootmodel.__class__
        res = model.objects.all()
    else:
        # Log a warning
        print("WARNING: Unknown format: " + format)
        return {}

    prefetches = basv.getPrefetches(format)
    if prefetches is not None:
        res2 = res.prefetch_related(*prefetches)
    else:
        res2 = res

    return res2


def createNewAdvancedQuery(basv_metadata, context):
    """
    Constructs an empty qry for the advanced search interface.
    """

    if "format" in context:
        selfmt = context["format"]
    else:
        selfmt = basv_metadata.default_format

    fmt_name_dict = basv_metadata.getFormatNames()

    qry = createNewQueryRoot(fmt_name_dict, selfmt)

    return qry


def createNewQueryRoot(fmt_name_dict, selfmt):
    qry = {}
    qry["selectedtemplate"] = selfmt
    qry["searches"] = {}
    for format, formatName in fmt_name_dict.items():
        qry["searches"][format] = {}
        qry["searches"][format]["name"] = formatName
        qry["searches"][format]["tree"] = {}
        qry["searches"][format]["tree"]["pos"] = ""
        qry["searches"][format]["tree"]["type"] = "group"
        qry["searches"][format]["tree"]["val"] = "all"
        qry["searches"][format]["tree"]["queryGroup"] = []
    return qry


def createNewBasicQuery(basv_metadata, mdl, fld, cmp, val, fmt):
    """
    Constructs a new qry object for an advanced search from basic search input.
    """

    qry = createNewQueryRoot(basv_metadata.getFormatNames(), fmt)

    models = basv_metadata.getModels(fmt)

    if mdl not in models:
        raise Http404(
            "Invalid model [" + mdl + "].  Must be one of [" + ",".join(models) + "]."
        )

    sfields = basv_metadata.getSearchFields(fmt, mdl)

    if fld not in sfields:
        raise Http404(
            "Field ["
            + fld
            + "] is not searchable.  Must be one of ["
            + ",".join(sfields.keys())
            + "]."
        )

    qry["searches"][fmt]["tree"]["queryGroup"].append({})
    qry["searches"][fmt]["tree"]["queryGroup"][0]["type"] = "query"
    qry["searches"][fmt]["tree"]["queryGroup"][0]["pos"] = ""
    qry["searches"][fmt]["tree"]["queryGroup"][0]["fld"] = sfields[fld]
    qry["searches"][fmt]["tree"]["queryGroup"][0]["ncmp"] = cmp
    qry["searches"][fmt]["tree"]["queryGroup"][0]["val"] = val

    dfld, dval = searchFieldToDisplayField(basv_metadata, mdl, fld, val, fmt, qry)
    # Set the field path for the display field
    qry["searches"][fmt]["tree"]["queryGroup"][0]["fld"] = sfields[dfld]
    qry["searches"][fmt]["tree"]["queryGroup"][0]["val"] = dval

    return qry


def searchFieldToDisplayField(basv_metadata, mdl, fld, val, fmt, qry):
    """
    Takes a field from a basic search and converts it to a non-hidden field for an advanced search select list.
    """

    dfld = fld
    dval = val
    dfields = basv_metadata.getDisplayFields(fmt, mdl)
    if dfields[fld] != fld:
        # If fld is not a displayed field, perform a query to convert the undisplayed field query to a displayed query
        q_exp = constructAdvancedQuery(qry)
        recs = performQuery(q_exp, fmt, basv_metadata)
        if len(recs) == 0:
            raise Http404("Records not found for field [" + mdl + "." + fld + "].")
        # Set the field path for the display field
        dfld = dfields[fld]
        dval = getJoinedRecFieldValue(recs, basv_metadata, fmt, mdl, dfields[fld])

    return dfld, dval


def getJoinedRecFieldValue(recs, basv_metadata, fmt, mdl, fld):
    """
    Takes a queryset object and a model.field and returns its value.
    """

    if len(recs) == 0:
        raise Http404("Records not found.")

    kpl = basv_metadata.getKeyPathList(fmt, mdl)
    kpl.append(fld)
    ptr = recs[0]
    for key in kpl:
        # If this is a many-to-many
        if ptr.__class__.__name__ == "ManyRelatedManager":
            tmprecs = ptr.all()
            if len(tmprecs) != 1:
                # Log an error
                print(
                    "ERROR: Handoff to "
                    + mdl
                    + "."
                    + fld
                    + " failed.  Check the AdvSearch class handoffs."
                )
                raise Http404(
                    "ERROR: Unable to find a single value for ["
                    + mdl
                    + "."
                    + fld
                    + "]."
                )
            ptr = getattr(tmprecs[0], key)
        else:
            ptr = getattr(ptr, key)

    return ptr


def performQuery(q_exp, fmt, basv):
    """
    Executes an advanced search query.
    """

    res = {}
    if fmt in basv.getFormatNames().keys():
        model = basv.modeldata[fmt].rootmodel.__class__
        res = model.objects.filter(q_exp)
    else:
        # Log a warning
        print("WARNING: Invalid selected format:", fmt)

    prefetches = basv.getPrefetches(fmt)
    if prefetches is not None:
        res2 = res.prefetch_related(*prefetches)
    else:
        res2 = res

    return res2


def isQryObjValid(qry, form_class_list):
    """
    Determines if an advanced search qry object was properly constructed/populated (only at the root).
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
    """
    Checks whether a query object is fully populated with at least 1 search term.
    """

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


def formsetsToDict(rawformset, form_classes):
    """
    Takes a series of forms and a list of form fields and uses the pos field to construct a hierarchical qry tree.
    """

    # All forms of each type are all submitted together in a single submission and are duplicated in the rawformset
    # dict.  We only need 1 copy to get all the data, so we will arbitrarily use the first one

    # Figure out which form class processed the forms (inferred by the presence of 'saved_data' - this is also the
    # selected format)
    processed_formkey = None
    for key in rawformset.keys():
        # We need to identify the form class that processed the form to infer the selected output format.  We do that
        # by checking the dictionary of each form class's first form for evidence that it processed the forms, i.e. the
        # presence of the "saved_data" class data member which is created upon processing.
        if "saved_data" in rawformset[key][0].__dict__:
            processed_formkey = key
            break

    # If we were unable to locate the selected output format (i.e. the copy of the formsets that were processed)
    if processed_formkey is None:
        raise Http404("Unable to find selected output format.")

    return formsetToDict(rawformset[processed_formkey], form_classes)


def formsetToDict(rawformset, form_classes):
    """
    Helper for formsetsToDict that handles only the forms belonging to the selected output format.
    """

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
                for key in form_classes[format].form.base_fields.keys():
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
                for key in form_classes[format].form.base_fields.keys():
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
