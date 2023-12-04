from django.views.generic import DetailView, ListView

from DataRepo.models import MSRunSequence


class MSRunSequenceDetailView(DetailView):
    model = MSRunSequence
    template_name = "DataRepo/msrunsequene_detail.html"
    context_object_name = "sequences"


class MSRunSequenceListView(ListView):
    model = MSRunSequence
    template_name = "DataRepo/msrunsequene_list.html"
    context_object_name = "sequence"
    paginate_by = 20
