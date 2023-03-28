import json

from django.conf import settings
from django.http import Http404
from django.shortcuts import render

from DataRepo.formats.search_group import SearchGroup
from DataRepo.forms import (
    AdvSearchDownloadForm,
    AdvSearchForm,
    AdvSearchPageForm,
)
from DataRepo.pager import Pager
from DataRepo.views.utils import get_cookie


def search_basic(request, mdl, fld, cmp, val, fmt, units=None):
    """
    Generic function-based view for a basic search.
    """

    # Base Advanced Search View Metadata
    basv_metadata = SearchGroup()

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

    format_template = "DataRepo/search/query.html"
    fmtkey = basv_metadata.formatNameOrKeyToKey(fmt)
    if fmtkey is None:
        names = basv_metadata.getFormatNames()
        raise Http404(
            f"Invalid format [{fmt}].  Must be one of: [{','.join(names.keys())},{','.join(names.values())}]"
        )

    qry = basv_metadata.createNewBasicQuery(mdl, fld, cmp, val, units, fmtkey)
    download_form = AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})

    rows_per_page = int(
        get_cookie(
            request,
            pager.rows_per_page_field,
            pager.default_rows,
        )
    )

    res, tot, stats = basv_metadata.performQuery(
        qry,
        qry["selectedtemplate"],
        limit=rows_per_page,
        offset=0,
    )

    pager.update(
        other_field_inits={
            "qryjson": json.dumps(qry),
            "show_stats": False,
            "stats": None,
        },
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
            "stats": stats,
            "pager": pager,
            "download_form": download_form,
            "debug": settings.DEBUG,
            "root_group": root_group,
            "mode": "search",
            "default_format": basv_metadata.default_format,
            "ncmp_choices": basv_metadata.getComparisonChoices(),
            "fld_types": basv_metadata.getFieldTypes(),
            "fld_choices": basv_metadata.getSearchFieldChoicesDict(),
            "fld_units": basv_metadata.getFieldUnitsDict(),
        },
    )
