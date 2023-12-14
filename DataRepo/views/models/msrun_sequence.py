from django.views.generic import DetailView, ListView

from DataRepo.models import MSRunSequence


class MSRunSequenceDetailView(DetailView):
    model = MSRunSequence
    template_name = "DataRepo/msrunsequence_detail.html"
    context_object_name = "sequence"


class MSRunSequenceListView(ListView):
    model = MSRunSequence
    template_name = "DataRepo/msrunsequence_list.html"
    context_object_name = "sequences"
    paginate_by = 20
