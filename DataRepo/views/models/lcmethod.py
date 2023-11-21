from django.views.generic import DetailView, ListView

from DataRepo.models import LCMethod


class LCMethodListView(ListView):
    model = LCMethod
    context_object_name = "lcmethods"
    template_name = "DataRepo/lcmethod_list.html"


class LCMethodDetailView(DetailView):
    model = LCMethod
    context_object_name = "lcmethod"
    template_name = "DataRepo/lcmethod_detail.html"
