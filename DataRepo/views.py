from django.http import Http404
from django.shortcuts import render

from DataRepo.models import Compound, Study


def home(request):
    return render(request, "home.html")


class generic_list(ListView):
    """
    This class displays all list views of every model. It is an abstract class.
    """
    model = Study
    template_name = 'listview.html'
    #paginate_by = 10
    allow_empty = True
    if hasattr(model._meta, 'ordering'):
        if isinstance(model._meta.ordering, str):
            queryset = model.objects.order_by(model._meta.ordering)
        elif isinstance(model._meta.ordering, list) and len(model._meta.ordering) > 0:
            queryset = model.objects.order_by(model._meta.ordering[0])

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        model = self.model
        context['table'] = model.__name__
        all_fields = model._meta.get_fields()
        # Alternative, if necessary:
        # all_fields = model._meta.get_fields(include_parents=False, include_hidden=False)
        filt_fields = list(filter(lambda x:self.is_shown_field(x), all_fields))
        context['fieldnames'] = [field.name for field in filt_fields]
        return context

    def is_shown_field(self, field):
        shown = (field.get_internal_type() != 'AutoField' and
            not getattr(field, "is_relation"))
        return shown



    def is_shown_field(self, field):
        shown = (field.get_internal_type() != 'AutoField' and
            not getattr(field, "is_relation"))
        return shown




class compound_list(generic_list):
    generic_list.model = Compound


class study_list(generic_list):
    generic_list.model = Study





def compound_detail(request, cpd_id):
    try:
        cpd = Compound.objects.get(id=cpd_id)
    except Compound.DoesNotExist:
        raise Http404("compound not found")
    return render(request, "compound_detail.html", {"cpd": cpd})
