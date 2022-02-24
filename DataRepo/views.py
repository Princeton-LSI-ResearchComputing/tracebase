import json
import traceback
from datetime import datetime
from typing import List

from celery.result import AsyncResult
from django.conf import settings
from django.core.management import call_command
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.generic import DetailView, ListView
from django.views.generic.edit import FormView

import DataRepo.tasks as tasks
from DataRepo.advanced_search_utils import (
    constructAdvancedQuery,
    createNewBasicQuery,
    formsetsToDict,
    getAllBrowseData,
    isQryObjValid,
    isValidQryObjPopulated,
    performQuery,
)
from DataRepo.compositeviews import BaseAdvancedSearchView
from DataRepo.forms import (
    AdvSearchDownloadForm,
    AdvSearchForm,
    AdvSearchPageForm,
    DataSubmissionValidationForm,
)
from DataRepo.models import (
    Animal,
    Compound,
    CompoundSynonym,
    MSRun,
    PeakData,
    PeakGroup,
    PeakGroupSet,
    Protocol,
    Sample,
    Study,
    Tissue,
    get_all_models,
)
from DataRepo.multiforms import MultiFormsView
from DataRepo.pager import Pager
from DataRepo.utils import MissingSamplesError
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df
from DataRepo.utils import ResearcherError, leaderboard_data


def home(request):
    """
    Home page contains 8 cards for browsing data
    keep card attributes in two lists for displaying cards in two rows
    """
    card_attrs_list1 = []
    card_attrs_list2 = []

    # first list
    card_attrs_list1.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Study.objects.all().count()) + " Studies",
            "card_foot_url": reverse("study_list"),
        }
    )

    card_attrs_list1.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Animal.objects.all().count()) + " Animals",
            "card_foot_url": reverse("animal_list"),
        }
    )

    card_attrs_list1.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Tissue.objects.all().count()) + " Tissues",
            "card_foot_url": reverse("tissue_list"),
        }
    )

    card_attrs_list1.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Sample.objects.all().count()) + " Samples",
            "card_foot_url": reverse("sample_list"),
        }
    )

    # second list
    card_attrs_list2.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(PeakGroupSet.objects.all().count())
            + " AccuCor Files",
            "card_foot_url": reverse("peakgroupset_list"),
        }
    )

    comp_count = Compound.objects.all().count()
    tracer_count = (
        Animal.objects.exclude(tracer_compound_id__isnull=True)
        .order_by("tracer_compound_id")
        .values_list("tracer_compound_id")
        .distinct("tracer_compound_id")
        .count()
    )

    card_attrs_list2.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(comp_count)
            + " Compounds ("
            + str(tracer_count)
            + " tracers)",
            "card_foot_url": reverse("compound_list"),
        }
    )

    card_attrs_list2.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Protocol.objects.all().count()) + " Protocols",
            "card_foot_url": reverse("protocol_list"),
        }
    )

    card_attrs_list2.append(
        {
            "card_bg_color": "bg-card-2",
            "card_body_title": "Advanced Search",
            "card_foot_url": reverse("search_advanced"),
        }
    )

    card_row_list = [card_attrs_list1, card_attrs_list2]

    context = {}
    context["card_rows"] = card_row_list
    context["leaderboards"] = leaderboard_data()

    return render(request, "home.html", context)


def upload(request):
    context = {
        "data_submission_email": settings.DATA_SUBMISSION_EMAIL,
        "data_submission_url": settings.DATA_SUBMISSION_URL,
    }
    return render(request, "upload.html", context)


def validation_disabled(request):
    return render(request, "validation_disabled.html")


class CompoundListView(ListView):
    """Generic class-based view for a list of compounds"""

    model = Compound
    context_object_name = "compound_list"
    template_name = "DataRepo/compound_list.html"
    ordering = ["name"]

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(CompoundListView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        comp_tracer_list_df = qs2df.get_compound_synonym_list_df()
        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(comp_tracer_list_df)
        context["df"] = data
        return context


class CompoundDetailView(DetailView):
    """Generic class-based detail view for a compound"""

    model = Compound
    template_name = "DataRepo/compound_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(CompoundDetailView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        anim_list_stats_df = qs2df.get_animal_list_stats_df()

        pk = self.kwargs.get("pk")
        per_tracer_anim_list_stats_df = anim_list_stats_df[
            anim_list_stats_df["tracer_compound_id"] == pk
        ]
        # convert DataFrame to a list of dictionary
        tracer_data = qs2df.df_to_list_of_dict(per_tracer_anim_list_stats_df)
        context["tracer_df"] = tracer_data
        context["measured"] = (
            PeakGroup.objects.filter(compounds__id__exact=pk).count() > 0
        )
        return context


class StudyListView(ListView):
    """Generic class-based view for a list of studies."""

    model = Study
    context_object_name = "study_list"
    template_name = "DataRepo/study_list.html"
    ordering = ["name"]

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(StudyListView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        stud_list_stats_df = qs2df.get_study_list_stats_df()
        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(stud_list_stats_df)
        context["df"] = data
        return context


def study_summary(request):
    """
    function-based view for studies based summary data, including selected
    data fileds for animal, tissue, sample, and MSRun
    get DataFrame for summary data, then convert to JSON format
    """

    all_stud_msrun_df = qs2df.get_study_msrun_all_df()

    # convert DataFrame to a list of dictionary
    data = qs2df.df_to_list_of_dict(all_stud_msrun_df)
    context = {"df": data}
    return render(request, "DataRepo/study_summary.html", context)


class StudyDetailView(DetailView):
    """Generic class-based detail view for a study."""

    model = Study
    template_name = "DataRepo/study_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(StudyDetailView, self).get_context_data(**kwargs)

        pk = self.kwargs.get("pk")
        per_stud_msrun_df = qs2df().get_per_study_msrun_df(pk)
        per_stud_stat_df = qs2df().get_per_study_stat_df(pk)

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(per_stud_msrun_df)
        stats_data = qs2df.df_to_list_of_dict(per_stud_stat_df)

        context["df"] = data
        context["stats_df"] = stats_data
        return context


def search_basic(request, mdl, fld, cmp, val, fmt):
    """Generic function-based view for a basic search."""

    # Base Advanced Search View Metadata
    basv_metadata = BaseAdvancedSearchView()

    # Base Advanced Search Form
    basf = AdvSearchForm()

    pager = Pager(
        action="/DataRepo/search_advanced/",
        form_id_field="adv_search_page_form",
        rows_per_page_choices=AdvSearchPageForm.ROWS_PER_PAGE_CHOICES,
        page_form_class=AdvSearchPageForm,
        other_fields=["qryjson"],
        page_field="page",
        rows_per_page_field="rows",
        order_by_field="order_by",
        order_dir_field="order_direction",
        form_name="paging",
    )

    format_template = "DataRepo/search/query.html"
    fmtkey = basv_metadata.formatNameOrKeyToKey(fmt)
    if fmtkey is None:
        names = basv_metadata.getFormatNames()
        raise Http404(
            f"Invalid format [{fmt}].  Must be one of: [{','.join(names.keys())},{','.join(names.values())}]"
        )

    qry = createNewBasicQuery(basv_metadata, mdl, fld, cmp, val, fmtkey)
    download_form = AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})

    rows_per_page = int(
        get_cookie(
            request,
            pager.rows_per_page_field,
            pager.default_rows,
        )
    )

    res, tot = performQuery(
        qry,
        qry["selectedtemplate"],
        basv_metadata,
        limit=rows_per_page,
        offset=0,
    )

    pager.update(
        other_field_dict={"qryjson": json.dumps(qry)},
        tot=tot,
    )

    root_group = basv_metadata.getRootGroup()

    return render(
        request,
        format_template,
        {
            "forms": basf.form_classes,
            "qry": qry,
            "res": res,
            "tot": tot,
            "pager": pager,
            "download_form": download_form,
            "debug": settings.DEBUG,
            "root_group": root_group,
            "mode": "search",
            "default_format": basv_metadata.default_format,
            "ncmp_choices": basv_metadata.getComparisonChoices(),
            "fld_types": basv_metadata.getFieldTypes(),
            "fld_choices": basv_metadata.getSearchFieldChoicesDict(),
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

    #
    # The following forms each submit to this view
    #

    # Base Advanced Search Form
    basf = AdvSearchForm()

    pager = Pager(
        action="/DataRepo/search_advanced/",
        form_id_field="adv_search_page_form",
        rows_per_page_choices=AdvSearchPageForm.ROWS_PER_PAGE_CHOICES,
        page_form_class=AdvSearchPageForm,
        other_fields=["qryjson"],
        page_field="page",
        rows_per_page_field="rows",
        order_by_field="order_by",
        order_dir_field="order_direction",
        form_name="paging",
    )

    #
    # This form submits to the AdvSearchDownloadView
    #

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

    def post(self, request, *args, **kwargs):
        # TODO: THIS NEEDS TO BE REFACTORED.  SHOULD RELY ON THE POST METHOD IN MULTIFORMS.PY.  SEE THE TODO IN
        # multiforms.py
        if request.method == "POST" and self.pager.form_id_field in request.POST:
            return self._process_individual_form(
                self.pager.form_name,
                {self.pager.form_name: self.pager.page_form_class},
            )

        # This calls the post method in multiforms.py
        return super().post(request, *args, **kwargs)

    def form_invalid(self, formset):
        """
        Upon invalid advanced search form submission, rescues the query to add back to the context.
        """

        qry = formsetsToDict(formset, self.form_classes)

        root_group = self.basv_metadata.getRootGroup()

        return self.render_to_response(
            self.get_context_data(
                res={},
                forms=self.form_classes,
                qry=qry,
                debug=settings.DEBUG,
                root_group=root_group,
                default_format=self.basv_metadata.default_format,
                ncmp_choices=self.basv_metadata.getComparisonChoices(),
                fld_types=self.basv_metadata.getFieldTypes(),
                fld_choices=self.basv_metadata.getSearchFieldChoicesDict(),
                error="All fields are required",  # Unless hacked, this is the only thing that can go wrong
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
            rows_per_page = int(
                self.get_template_cookie(
                    qry["selectedtemplate"],
                    self.pager.rows_per_page_field,
                    self.pager.default_rows,
                )
            )
            res, tot = performQuery(
                qry,
                qry["selectedtemplate"],
                self.basv_metadata,
                limit=rows_per_page,
                offset=0,
                order_by=None,
                order_direction=None,
            )
            self.pager.update(
                other_field_dict={"qryjson": json.dumps(qry)},
                tot=tot,
                page=1,
                rows=rows_per_page,
            )
        else:
            # Log a warning
            print("WARNING: Invalid query root:", qry)

        root_group = self.basv_metadata.getRootGroup()

        return self.render_to_response(
            self.get_context_data(
                res=res,
                forms=self.form_classes,
                qry=qry,
                download_form=download_form,
                debug=settings.DEBUG,
                root_group=root_group,
                pager=self.pager,
                default_format=self.basv_metadata.default_format,
                ncmp_choices=self.basv_metadata.getComparisonChoices(),
                fld_types=self.basv_metadata.getFieldTypes(),
                fld_choices=self.basv_metadata.getSearchFieldChoicesDict(),
            )
        )

    def get_template_cookie(self, template_name, cookie_name, cookie_default):
        full_cookie_name = ".".join([template_name, cookie_name])
        result = get_cookie(self.request, full_cookie_name, cookie_default)
        return result

    # Invalid form whose multiforms given name is "paging" will call this from the post override in multiforms.py
    # TODO: See Issue #370's comment on this method.
    def paging_form_invalid(self, formset):
        """
        Upon invalid advanced search form submission, rescues the query to add back to the context.
        """
        print(f"Invalid paging form: {formset}")

        qry = {}

        root_group = self.basv_metadata.getRootGroup()

        return self.render_to_response(
            self.get_context_data(
                res={},
                forms=self.form_classes,
                qry=qry,
                download_form=AdvSearchDownloadForm(),
                debug=settings.DEBUG,
                root_group=root_group,
                default_format=self.basv_metadata.default_format,
                ncmp_choices=self.basv_metadata.getComparisonChoices(),
                fld_types=self.basv_metadata.getFieldTypes(),
                fld_choices=self.basv_metadata.getSearchFieldChoicesDict(),
                error="All fields are required",  # Unless hacked, this is the only thing that can go wrong
            )
        )

    # Valid form whose multiforms given name is "paging" will call this from the post override in multiforms.py
    # TODO: See Issue #370's comment on this method.
    def paging_form_valid(self, form):
        cform = form.cleaned_data

        # Ensure valid query
        try:
            qry = json.loads(cform["qryjson"])
            # Apparently this causes a TypeError exception in test_views. Could not figure out why, so...
        except TypeError:
            qry = cform["qryjson"]

        if not isQryObjValid(qry, self.basv_metadata.getFormatNames().keys()):
            print("ERROR: Invalid qry object: ", qry)
            raise Http404("Invalid json")

        try:
            page = int(cform["page"])
            rows = int(cform["rows"])

            # Order_by and order_direction are optional
            if "order_by" in cform:
                order_by = cform["order_by"]
            else:
                order_by = None
            if "order_direction" in cform:
                order_dir = cform["order_direction"]
            else:
                order_dir = None

            offset = (page - 1) * rows
        except Exception as e:
            # Assumes this is an initial query, not a page form submission
            print(f"WARNING: {e}")
            self.pager.update()
            page = self.pager.page
            rows = self.pager.rows
            order_by = self.pager.order_by
            order_dir = self.pager.order_dir
            offset = 0

        if isValidQryObjPopulated(qry):
            # For some reason, the download form generated in either case below always generates an error in the
            # browser that says "Failed to load resource: Frame load interrupted" when the download button is
            # clicked, but it still seems to work.  If however, the form creation in the first case is moved to the
            # bottom of the block, the downloaded file will only contain the header and will not be named properly...
            # Might be a (Safari) browser issue (according to stack).
            download_form = AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})
            res, tot = performQuery(
                qry,
                qry["selectedtemplate"],
                self.basv_metadata,
                limit=rows,
                offset=offset,
                order_by=order_by,
                order_direction=order_dir,
            )
        else:
            print(f"Getting browse data with offset: {offset} and limit {rows}")
            res, tot = getAllBrowseData(
                qry["selectedtemplate"],
                self.basv_metadata,
                limit=rows,
                offset=offset,
                order_by=order_by,
                order_direction=order_dir,
            )
            # Remake the qry so it will be valid for downloading all data (not entirely sure why this is necessary, but
            # the download form created on the subsequent line doesn't work without doing this.  I suspect that the qry
            # object isn't built correctly when the initial browse link is clicked)
            qry = self.basv_metadata.getRootGroup(qry["selectedtemplate"])
            download_form = AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})

        self.pager.update(
            other_field_dict={"qryjson": json.dumps(qry)},
            tot=tot,
            page=page,
            rows=rows,
            order_by=order_by,
            order_dir=order_dir,
        )

        root_group = self.basv_metadata.getRootGroup()

        response = self.render_to_response(
            self.get_context_data(
                res=res,
                forms=self.form_classes,
                qry=qry,
                download_form=download_form,
                debug=settings.DEBUG,
                root_group=root_group,
                pager=self.pager,
                default_format=self.basv_metadata.default_format,
                ncmp_choices=self.basv_metadata.getComparisonChoices(),
                fld_types=self.basv_metadata.getFieldTypes(),
                fld_choices=self.basv_metadata.getSearchFieldChoicesDict(),
            )
        )

        return response

    def addInitialContext(self, context):
        """
        Prepares context data for the initial page load.
        """

        mode = self.basv_metadata.default_mode
        if "mode" in context and context["mode"] == "browse":
            mode = "browse"
        context["mode"] = mode

        context["root_group"] = self.basv_metadata.getRootGroup()
        context["ncmp_choices"] = self.basv_metadata.getComparisonChoices()
        context["fld_types"] = self.basv_metadata.getFieldTypes()
        context["fld_choices"] = self.basv_metadata.getSearchFieldChoicesDict()

        # Initial search page with no results
        if "qry" not in context or (
            mode == "browse" and not isValidQryObjPopulated(context["qry"])
        ):
            if "qry" not in context:
                # Initialize the qry object
                if "format" in context:
                    qry = self.basv_metadata.getRootGroup(context["format"])
                else:
                    qry = self.basv_metadata.getRootGroup()
                # If we're in browse more, put the qry object in context (because that's where the format name is
                # extracted)
                if mode == "browse":
                    context["qry"] = qry
            else:
                qry = context["qry"]

            if mode == "browse":
                context["download_form"] = AdvSearchDownloadForm(
                    initial={"qryjson": json.dumps(qry)}
                )
                self.pager.update()
                offset = 0
                context["res"], context["tot"] = getAllBrowseData(
                    qry["selectedtemplate"],
                    self.basv_metadata,
                    limit=self.pager.rows,
                    offset=offset,
                    order_by=self.pager.order_by,
                    order_direction=self.pager.order_dir,
                )
                context["pager"] = self.pager.update(
                    other_field_dict={"qryjson": json.dumps(qry)}, tot=context["tot"]
                )
        elif (
            "qry" in context
            and isQryObjValid(
                context["qry"], self.basv_metadata.getFormatNames().keys()
            )
            and isValidQryObjPopulated(context["qry"])
            and ("res" not in context or len(context["res"]) == 0)
        ):
            qry = context["qry"]
            context["download_form"] = AdvSearchDownloadForm(
                initial={"qryjson": json.dumps(qry)}
            )
            context["res"], context["tot"] = performQuery(
                qry, qry["selectedtemplate"], self.basv_metadata
            )
            context["pager"] = self.pager.update(
                other_field_dict={"qryjson": json.dumps(qry)}, tot=context["tot"]
            )


def get_cookie(request, cookie_name, cookie_default):
    return request.COOKIES.get(cookie_name, cookie_default)


# Basis: https://stackoverflow.com/questions/29672477/django-export-current-queryset-to-csv-by-button-click-in-browser
class AdvancedSearchTSVView(FormView):
    """
    This is the download view for the advanced search page.  A successful form submission results in a download
    progress page.
    """

    form_class = AdvSearchDownloadForm
    template_name = "DataRepo/search/downloads/download_progress.html"
    header_template = "DataRepo/search/downloads/download_header.tsv"
    row_template = "DataRepo/search/downloads/download_row.tsv"
    success_url = ""
    basv_metadata = BaseAdvancedSearchView()

    def form_invalid(self, form):
        saved_form = form.saved_data
        qry = {}
        if "qryjson" in saved_form:
            # Discovered this can cause a KeyError during testing, so...
            qry = json.loads(saved_form["qryjson"])
            print("ERROR: Invalid form.")
        else:
            print("ERROR: qryjson hidden input not in saved form.")
        return HttpResponse(f"Form submission error given query object: [{qry}]")

    def form_valid(self, form):
        cform = form.cleaned_data
        try:
            qry = json.loads(cform["qryjson"])
            # Apparently this causes a TypeError exception in test_views. Could not figure out why, so...
        except TypeError:
            qry = cform["qryjson"]
        if not isQryObjValid(qry, self.basv_metadata.getFormatNames().keys()):
            print("ERROR: Invalid qry object: ", qry)
            raise Http404("Invalid json")

        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        filename = (
            qry["searches"][qry["selectedtemplate"]]["name"]
            + "_"
            + now.strftime("%d.%m.%Y.%H.%M.%S")
            + ".tsv"
        )

        # Initiate a background task to compile the download data
        try:
            bgtask = tasks.tsv_producer.delay(
                filename, self.header_template, self.row_template, qry, dt_string
            )
        except Exception as e:
            print(f"Error running task: {e}")
            return self.render_to_response(
                self.get_context_data(progress={"state": "FAILURE", "id": "unknown"})
            )

        print(f"Starting task: {bgtask}")

        # Return an http response for the download progress page, supplying it the task ID
        return self.render_to_response(
            self.get_context_data(progress=bgtask, debug=settings.DEBUG)
        )


def get_advsrch_download_progress(request, task_id):
    """
    This view is simply for communicating progress fram an asynchronous task (compiling data for download from the
    advanced search page's download button).
    """
    # Grab the task data using the supplied task ID
    result = AsyncResult(task_id)

    # Wait until the result is ready
    result.ready()

    # Report the task's state
    response_data = {
        "state": result.state,
        "details": result.info,
    }

    return JsonResponse(response_data)


def get_advsrch_download_output(request, task_id):
    """
    This view is for initiating a download once results are done being compiled by an asynchronous task.
    """
    # Grab the task data using the supplied task ID
    result = AsyncResult(task_id)

    # Wait until the result is ready
    result.ready()

    # Retrieve the actual output from the result of the asynch task
    data = result.get(timeout=600)
    filename = data["filename"]
    output = data["output"]

    # Create an http response containing the data
    response = HttpResponse(
        content="",
        content_type="application/text",
        status=200,
        reason=None,
        charset="utf-8",
    )
    response["Content-Disposition"] = f"attachment; filename={filename}"
    response.writelines(output)

    return response


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

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(AnimalListView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        anim_list_stats_df = qs2df.get_animal_list_stats_df()

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(anim_list_stats_df)
        context["df"] = data
        return context


class AnimalDetailView(DetailView):
    """Generic class-based detail view for an animal"""

    model = Animal
    template_name = "DataRepo/animal_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(AnimalDetailView, self).get_context_data(**kwargs)

        pk = self.kwargs.get("pk")
        per_anim_msrun_df = qs2df().get_per_animal_msrun_df(pk)

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(per_anim_msrun_df)
        context["df"] = data
        return context


class TissueListView(ListView):
    """Generic class-based view for a list of tissues"""

    model = Tissue
    context_object_name = "tissue_list"
    template_name = "DataRepo/tissue_list.html"
    ordering = ["name"]


class TissueDetailView(DetailView):
    """Generic class-based detail view for a tissue"""

    model = Tissue
    template_name = "DataRepo/tissue_detail.html"


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

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(SampleListView, self).get_context_data(**kwargs)
        #  add data from the DataFrame to the context
        all_anim_msrun_df = qs2df.get_animal_msrun_all_df()

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(all_anim_msrun_df)

        context["df"] = data
        return context


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


class DataValidationView(FormView):
    form_class = DataSubmissionValidationForm
    template_name = "DataRepo/validate_submission.html"
    success_url = ""
    accucor_files: List[str] = []
    animal_sample_file = None
    submission_url = settings.DATA_SUBMISSION_URL

    def dispatch(self, request, *args, **kwargs):
        # check if there is some video onsite
        if not settings.VALIDATION_ENABLED:
            return redirect("validatedown")
        else:
            return super(DataValidationView, self).dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        form_class = self.get_form_class()
        form = self.get_form(form_class)
        self.accucor_files = request.FILES.getlist("accucor_files")
        try:
            self.animal_sample_file = request.FILES["animal_sample_table"]
        except Exception:
            # Ignore missing accucor files
            print("ERROR: No accucor file")
        if form.is_valid():
            return self.form_valid(form)
        else:
            return self.form_invalid(form)

    def form_valid(self, form):
        """
        Upon valid file submission, adds validation messages to the context of the validation page.
        """

        errors = {}
        debug = "untouched"
        valid = True
        results = {}

        debug = f"asf: {self.animal_sample_file} num afs: {len(self.accucor_files)}"

        animal_sample_dict = {
            str(self.animal_sample_file): self.animal_sample_file.temporary_file_path(),
        }
        accucor_dict = dict(
            map(lambda x: (str(x), x.temporary_file_path()), self.accucor_files)
        )

        [results, valid, errors] = self.validate_load_files(
            animal_sample_dict, accucor_dict
        )

        return self.render_to_response(
            self.get_context_data(
                results=results,
                debug=debug,
                valid=valid,
                form=form,
                errors=errors,
                submission_url=self.submission_url,
            )
        )

    def validate_load_files(self, animal_sample_dict, accucor_dict):
        errors = {}
        valid = True
        results = {}
        animal_sample_name = list(animal_sample_dict.keys())[0]

        try:
            # Load the animal and sample table in debug mode to check the researcher and sample name uniqueness
            errors[animal_sample_name] = []
            results[animal_sample_name] = ""
            try:
                # debug=True is supposed to NOT commit the DB changes, but it IS creating the study, so even though I'm
                # using debug here, I am also setting the database to the validation database...
                call_command(
                    "load_animals_and_samples",
                    animal_and_sample_table_filename=animal_sample_dict[
                        animal_sample_name
                    ],
                    debug=True,
                    validate=True,
                )
                results[animal_sample_name] = "PASSED"
            except ResearcherError as re:
                valid = False
                errors[animal_sample_name].append(
                    "[The following error about a new researcher name should only be addressed if the name already "
                    "exists in the database as a variation.  If this is a truly new researcher name in the database, "
                    f"it may be ignored.]\n{animal_sample_name}: {str(re)}"
                )
                results[animal_sample_name] = "WARNING"
            except Exception as e:
                estr = str(e)
                # We are using the presence of the string "Debugging..." to infer that it got to the end of the load
                # without an exception.  If there is no "Debugging" message, then an exception did not occur anyway
                if settings.DEBUG:
                    traceback.print_exc()
                    print(estr)
                if "Debugging" not in estr:
                    valid = False
                    errors[animal_sample_name].append(f"{e.__class__.__name__}: {estr}")
                    results[animal_sample_name] = "FAILED"
                else:
                    results[animal_sample_name] = "PASSED"

            can_proceed = False
            if results[animal_sample_name] != "FAILED":
                # Load the animal and sample data into the validation database, so the data is available for the accucor
                # file validation
                try:
                    call_command(
                        "load_animals_and_samples",
                        animal_and_sample_table_filename=animal_sample_dict[
                            animal_sample_name
                        ],
                        skip_researcher_check=True,
                        validate=True,
                    )
                    can_proceed = True
                except Exception as e:
                    estr = str(e)
                    # We are using the presence of the string "Debugging..." to infer that it got to the end of the load
                    # without an exception.  If there is no "Debugging" message, then an exception did not occur anyway
                    if settings.DEBUG:
                        traceback.print_exc()
                        print(estr)
                    if "Debugging" not in estr:
                        valid = False
                        errors[animal_sample_name].append(
                            f"{animal_sample_name} {e.__class__.__name__}: {str(e)}"
                        )
                        results[animal_sample_name] = "FAILED"
                        can_proceed = False
                    else:
                        results[animal_sample_name] = "PASSED"
                        can_proceed = True

            # Load the accucor file into a temporary test database in debug mode
            for af, afp in accucor_dict.items():
                errors[af] = []
                if can_proceed is True:
                    try:
                        self.validate_accucor(afp, [])
                        results[af] = "PASSED"
                    except MissingSamplesError as mse:
                        blank_samples = []
                        real_samples = []

                        # Determine whether all the missing samples are blank samples
                        for sample in mse.sample_list:
                            if "blank" in sample:
                                blank_samples.append(sample)
                            else:
                                real_samples.append(sample)

                        # Rerun ignoring blanks if all were blank samples, so we can check everything else
                        if len(blank_samples) > 0 and len(blank_samples) == len(
                            mse.sample_list
                        ):
                            try:
                                self.validate_accucor(afp, blank_samples)
                                results[af] = "PASSED"
                            except Exception as e:
                                estr = str(e)
                                # We are using the presence of the string "Debugging..." to infer that it got to the
                                # end of the load without an exception.  If there is no "Debugging" message, then an
                                # exception did not occur anyway
                                if settings.DEBUG:
                                    traceback.print_exc()
                                    print(estr)
                                if "Debugging" not in estr:
                                    valid = False
                                    results[af] = "FAILED"
                                    errors[af].append(estr)
                                else:
                                    results[af] = "PASSED"
                        else:
                            valid = False
                            results[af] = "FAILED"
                            errors[af].append(
                                "Samples in the accucor file are missing in the animal and sample table: "
                                + f"[{', '.join(real_samples)}]"
                            )
                    except Exception as e:
                        estr = str(e)
                        # We are using the presence of the string "Debugging..." to infer that it got to the end of the
                        # load without an exception.  If there is no "Debugging" message, then an exception did not
                        # occur anyway
                        if settings.DEBUG:
                            traceback.print_exc()
                            print(estr)
                        if "Debugging" not in estr:
                            valid = False
                            results[af] = "FAILED"
                            errors[af].append(estr)
                        else:
                            results[af] = "PASSED"
                else:
                    # Cannot check because the samples did not load
                    results[af] = "UNCHECKED"
        finally:
            # Clear out the user's validated data so that they'll be able to try again
            self.clear_validation_database()

        return [
            results,
            valid,
            errors,
        ]

    def clear_validation_database(self):
        """
        Clear out every table aside from compounds and tissues, which are intended to persist in the validation
        database, as they are needed to create related links for data inserted by the load animals/samples scripts
        """
        skips = [Compound, CompoundSynonym, Tissue]

        for mdl in get_all_models():
            if mdl not in skips:
                mdl.objects.using(settings.VALIDATION_DB).all().delete()

    def validate_accucor(self, accucor_file, skip_samples):
        if len(skip_samples) > 0:
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file=accucor_file,
                date="2021-09-14",
                researcher="anonymous",
                debug=True,
                skip_samples=skip_samples,
                validate=True,
            )
        else:
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file=accucor_file,
                date="2021-09-13",
                researcher="anonymous",
                debug=True,
                validate=True,
            )
