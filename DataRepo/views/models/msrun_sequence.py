from django.views.generic import DetailView, ListView

from DataRepo.models import MSRunSequence


class MSRunSequenceDetailView(DetailView):
    model = MSRunSequence
    template_name = "DataRepo/msrunsequene_detail.html"


class MSRunSequenceListView(ListView):
    model = MSRunSequence
    template_name = "DataRepo/msrunsequene_list.html"
    paginate_by = 20
