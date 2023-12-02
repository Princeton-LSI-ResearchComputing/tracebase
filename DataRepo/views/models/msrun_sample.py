from django.views.generic import DetailView, ListView

from DataRepo.models import MSRunSample


class MSRunSampleDetailView(DetailView):
    model = MSRunSample
    template_name = "DataRepo/msrunsample_detail.html"


class MSRunSampleListView(ListView):
    model = MSRunSample
    template_name = "DataRepo/msrunsample_list.html"
    paginate_by = 20
