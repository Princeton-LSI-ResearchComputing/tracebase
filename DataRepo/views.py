from django.http import Http404
from django.shortcuts import render

from .models import Compound


def home(request):
    cpds = Compound.objects.all()
    return render(
        request, "home.html", {"cpds": cpds, "HMDB_CPD_URL": Compound.HMDB_CPD_URL}
    )


def compound_detail(request, cpd_id):
    try:
        cpd = Compound.objects.get(id=cpd_id)
    except Compound.DoesNotExist:
        raise Http404("compound not found")
    return render(request, "compound_detail.html", {"cpd": cpd})
