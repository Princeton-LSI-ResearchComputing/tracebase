from django.shortcuts import render


def view_search_results(request, format, queryset):
    """
    Call this to display a queryset in one of the output formats.  Note, this is a static convenience method.  It does
    not allow the user to refine the search.  To do so would require a qry object.
    """

    format_template = "search/query.html"

    return render(
        request,
        format_template,
        {
            "res": queryset,
            "format": format,  # pgtemplate = peakgroups, pdtemplate = peakdata, fctemplate = fcirc
            "mode": "view",
        },
    )
