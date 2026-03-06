import json

from django.conf import settings
from django.core.exceptions import (
    FieldError,
    ObjectDoesNotExist,
    ValidationError,
)
from django.http import Http404
from django.shortcuts import render

from DataRepo.formats.search_group import SearchGroup
from DataRepo.forms import AdvSearchForm, AdvSearchPageForm
from DataRepo.pager import Pager
from DataRepo.views.search.advanced import AdvancedSearchView
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

    format_template = "search/query.html"
    fmtkey = basv_metadata.format_name_or_key_to_key(fmt)
    if fmtkey is None:
        names = basv_metadata.get_format_names()
        raise Http404(
            f"Invalid format [{fmt}].  Must be one of: [{','.join(names.keys())},{','.join(names.values())}]"
        )

    try:
        qry = basv_metadata.create_new_basic_query(mdl, fld, cmp, val, fmtkey, units)
    except (KeyError, ObjectDoesNotExist, ValidationError, FieldError) as e:
        raise Http404(e)
    download_forms = AdvancedSearchView().get_download_form_tuples(qry=qry)

    rows_per_page = int(
        get_cookie(
            request,
            pager.rows_per_page_field,
            pager.default_rows,
        )
    )

    res, tot, stats = basv_metadata.perform_query(
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

    root_group = basv_metadata.get_root_group()

    return render(
        request,
        format_template,
        {
            "forms": basf.form_classes,
            "qry": qry,
            "res": res,
            "stats": stats,
            "pager": pager,
            "download_forms": download_forms,
            "debug": settings.DEBUG,
            "root_group": root_group,
            "mode": "search",
            "default_format": basv_metadata.default_format,
            "ncmp_choices": basv_metadata.get_comparison_choices(),
            "fld_types": basv_metadata.get_field_types(),
            "fld_choices": basv_metadata.get_search_field_choices_dict(),
            "fld_units": basv_metadata.get_field_units_dict(),
        },
    )
