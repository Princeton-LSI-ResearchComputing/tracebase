from django.http import Http404
from django.shortcuts import render

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
        def is_shown_field(field):
            shown = (field.get_internal_type() != 'AutoField' and
                not getattr(field, "is_relation"))
            return shown

        context = super().get_context_data(**kwargs)
        model = self.model
        context['table'] = model.__name__
        all_fields = model._meta.get_fields(include_parents=False, include_hidden=False)
        filt_fields = list(filter(lambda x:is_shown_field(x), all_fields))
        context['fieldnames'] = [field.name for field in filt_fields]
        return context



def compound_detail(request, cpd_id):
    try:
        cpd = Compound.objects.get(id=cpd_id)
    except Compound.DoesNotExist:
        raise Http404("compound not found")
    return render(request, "compound_detail.html", {"cpd": cpd})
