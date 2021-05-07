from django.http import Http404
from django.shortcuts import render
from django.views.generic import ListView

from DataRepo.models import Compound


def home(request):
    return render(request, "home.html")


class compound_list(ListView):
    model = Compound
    template_name = 'listview.html'
    #paginate_by = 10
    allow_empty = True
    queryset = model.objects.order_by(model._meta.ordering[0])

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        model = self.model
        context['table'] = model.__name__
        context['fieldnames'] = [field.name for field in model._meta.fields]
        return context


def compound_detail(request, cpd_id):
    try:
        cpd = Compound.objects.get(id=cpd_id)
    except Compound.DoesNotExist:
        raise Http404("compound not found")
    return render(request, "compound_detail.html", {"cpd": cpd})
