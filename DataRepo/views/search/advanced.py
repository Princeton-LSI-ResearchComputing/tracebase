import json

from django.conf import settings
from django.http import Http404

from DataRepo.formats.dataformat_group_query import (
    formsetsToDict,
    isQryObjValid,
    isValidQryObjPopulated,
)
from DataRepo.formats.search_group import SearchGroup
from DataRepo.forms import (
    AdvSearchDownloadForm,
    AdvSearchForm,
    AdvSearchPageForm,
)
from DataRepo.multiforms import MultiFormsView
from DataRepo.pager import Pager
from DataRepo.views.utils import get_cookie


# Based on:
#   https://stackoverflow.com/questions/15497693/django-can-class-based-views-accept-two-forms-at-a-time
class AdvancedSearchView(MultiFormsView):
    """
    This is the view for the advanced search page.
    """

    # Base Advanced Search View
    basv_metadata = SearchGroup()

    #
    # The following forms each submit to this view
    #

    # Base Advanced Search Form
    basf = AdvSearchForm()

    pager = Pager(
        action="/DataRepo/search_advanced/",
        # form_id_field holds the field *name* used to identify the form type in multiforms.py (among other form
        # submissions on the same page), not a field ID assignment used in  javascript
        form_id_field="paging",  # Must match the "<>_form_valid" and "<>_form_invalid" methods.
        rows_per_page_choices=AdvSearchPageForm.ROWS_PER_PAGE_CHOICES,
        page_form_class=AdvSearchPageForm,
        other_field_ids={
            "qryjson": None,
            "show_stats": "show_stats_id",
            "stats": "stats_id",
        },
        page_field="page",
        rows_per_page_field="rows",
        order_by_field="order_by",
        order_dir_field="order_direction",
    )

    #
    # This form submits to the AdvSearchDownloadView
    #

    # Advanced search download form
    download_form = AdvSearchDownloadForm()

    # MultiFormView class vars
    template_name = "DataRepo/search/query.html"
    success_url = ""

    def __init__(self, *args, **kwargs):
        # Set up the multiple form types that submit to this view
        # Add the advanced search forms as a mixed forms type
        self.add_mixed_forms(
            # This is the name of the field in the form that identifies the form as belonging to the "mixed" forms type
            self.basf.format_select_list_name,
            self.basf.form_classes,
        )
        # Add the paging form as an individual form type
        self.add_individual_form(self.pager.form_id_field, self.pager.page_form_class)

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
                fld_units=self.basv_metadata.getFieldUnitsDict(),
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

        if isQryObjValid(qry, self.basf.form_classes.keys()):
            download_form = AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})
            rows_per_page = int(
                self.get_template_cookie(
                    qry["selectedtemplate"],
                    self.pager.rows_per_page_field,
                    self.pager.default_rows,
                )
            )
            res, tot, stats = self.basv_metadata.performQuery(
                qry,
                qry["selectedtemplate"],
                limit=rows_per_page,
                offset=0,
                order_by=None,
                order_direction=None,
            )
            self.pager.update(
                other_field_inits={
                    "qryjson": json.dumps(qry),
                    "show_stats": False,
                    "stats": json.dumps(stats),
                },
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
                stats=stats,
                forms=self.form_classes,
                qry=qry,
                download_form=download_form,
                root_group=root_group,
                debug=settings.DEBUG,
                pager=self.pager,
                default_format=self.basv_metadata.default_format,
                ncmp_choices=self.basv_metadata.getComparisonChoices(),
                fld_types=self.basv_metadata.getFieldTypes(),
                fld_choices=self.basv_metadata.getSearchFieldChoicesDict(),
                fld_units=self.basv_metadata.getFieldUnitsDict(),
            )
        )

    def get_template_cookie(self, template_name, cookie_name, cookie_default):
        full_cookie_name = ".".join([template_name, cookie_name])
        result = get_cookie(self.request, full_cookie_name, cookie_default)
        return result

    # Invalid form whose multiforms given name is "paging" will call this from the post override in multiforms.py
    def paging_form_invalid(self, formset):
        """
        Upon invalid advanced search form submission, rescues the query to add back to the context.
        """
        print(f"WARNING: Invalid paging form: {formset}")

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
                fld_choices=self.basv_metadata.getSearchFieldChoicesDict(),
                error="All fields are required",  # Unless hacked, this is the only thing that can go wrong
                fld_types=self.basv_metadata.getFieldTypes(),
                fld_units=self.basv_metadata.getFieldUnitsDict(),
            )
        )

    # Valid form whose multiforms given name is "paging" will call this from the post override in multiforms.py
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

            show_stats = False
            if "show_stats" in cform and cform["show_stats"]:
                show_stats = True

            # Retrieve stats if present - It's ok if there's none
            try:
                received_stats = json.loads(cform["stats"])
                # Apparently this causes a TypeError exception in test_views. Could not figure out why, so...
            except (TypeError, KeyError):
                try:
                    if "stats" in cform:
                        received_stats = cform["stats"]
                    else:
                        received_stats = None
                except Exception as e:
                    print(
                        f"WARNING: The paging form encountered an exception of type {e.__class__.__name__} during "
                        f"stats field processing: [{e}]."
                    )
                    received_stats = None

            # Update the value in the stats data structure based on the current form value
            if received_stats is not None:
                received_stats["show"] = show_stats

            offset = (page - 1) * rows
        except Exception as e:
            # Assumes this is an initial query, not a page form submission
            print(
                f"WARNING: The paging form encountered an exception of type {e.__class__.__name__} during processing: "
                f"[{e}]."
            )
            self.pager.update()
            page = self.pager.page
            rows = self.pager.rows
            order_by = self.pager.order_by
            order_dir = self.pager.order_dir
            show_stats = False
            offset = 0

        # We only need to take the time to generate stats is they are not present and they've been requested
        generate_stats = False
        if (received_stats is None or not received_stats["populated"]) and show_stats:
            generate_stats = True

        if isValidQryObjPopulated(qry):
            # For some reason, the download form generated in either case below always generates an error in the
            # browser that says "Failed to load resource: Frame load interrupted" when the download button is
            # clicked, but it still seems to work.  If however, the form creation in the first case is moved to the
            # bottom of the block, the downloaded file will only contain the header and will not be named properly...
            # Might be a (Safari) browser issue (according to stack).
            download_form = AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})
            res, tot, stats = self.basv_metadata.performQuery(
                qry,
                qry["selectedtemplate"],
                limit=rows,
                offset=offset,
                order_by=order_by,
                order_direction=order_dir,
                generate_stats=generate_stats,
            )
        else:
            res, tot, stats = self.basv_metadata.getAllBrowseData(
                qry["selectedtemplate"],
                limit=rows,
                offset=offset,
                order_by=order_by,
                order_direction=order_dir,
                generate_stats=generate_stats,
            )
            # Remake the qry so it will be valid for downloading all data (not entirely sure why this is necessary, but
            # the download form created on the subsequent line doesn't work without doing this.  I suspect that the qry
            # object isn't built correctly when the initial browse link is clicked)
            qry = self.basv_metadata.getRootGroup(qry["selectedtemplate"])
            download_form = AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})

        # If we received populated stats from the paging form (i.e. they were previously calculated)
        if not generate_stats:
            stats = received_stats

        self.pager.update(
            other_field_inits={
                "qryjson": json.dumps(qry),
                "show_stats": show_stats,
                "stats": json.dumps(stats),
            },
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
                stats=stats,
                forms=self.form_classes,
                qry=qry,
                download_form=download_form,
                debug=settings.DEBUG,
                root_group=root_group,
                pager=self.pager,
                default_format=self.basv_metadata.default_format,
                ncmp_choices=self.basv_metadata.getComparisonChoices(),
                fld_types=self.basv_metadata.getFieldTypes(),
                fld_units=self.basv_metadata.getFieldUnitsDict(),
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
        context["fld_units"] = self.basv_metadata.getFieldUnitsDict()

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
                (
                    context["res"],
                    context["tot"],
                    context["stats"],
                ) = self.basv_metadata.getAllBrowseData(
                    qry["selectedtemplate"],
                    limit=self.pager.rows,
                    offset=offset,
                    order_by=self.pager.order_by,
                    order_direction=self.pager.order_dir,
                )
                context["pager"] = self.pager.update(
                    other_field_inits={
                        "qryjson": json.dumps(qry),
                        "show_stats": False,
                        "stats": None,
                    },
                    tot=context["tot"],
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
            (
                context["res"],
                context["tot"],
                context["stats"],
            ) = self.basv_metadata.performQuery(qry, qry["selectedtemplate"])
            context["pager"] = self.pager.update(
                other_field_inits={
                    "qryjson": json.dumps(qry),
                    "show_stats": False,
                    "stats": None,
                },
                tot=context["tot"],
            )
