from django.shortcuts import render

from DataRepo.models import PeakGroup


# THIS IS JUST AN EXAMPLE to show how to link to one of the advanced search results views
def example_barebones_advanced_search(request):
    """
    Demonstrates the flexibility added to the advanced search templates
    """

    format_template = "DataRepo/search/query.html"

    res = PeakGroup.objects.filter(name__contains="glut")

    return render(
        request,
        format_template,
        {
            "res": res,
            "format": "pgtemplate",  # pgtemplate = peakgroups, pdtemplate = peakdata, fctemplate = fcirc
            "mode": "view",  # This is a new mode that means "I'm only providing a queryset"
        },
    )
