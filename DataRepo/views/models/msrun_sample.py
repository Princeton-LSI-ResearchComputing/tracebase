from django.views.generic import DetailView, ListView

from DataRepo.models import MSRunSample


class MSRunSampleDetailView(DetailView):
    model = MSRunSample
    template_name = "DataRepo/msrunsample_detail.html"
    context_object_name = "msrun_sample"


class MSRunSampleListView(ListView):
    model = MSRunSample
    template_name = "DataRepo/msrunsample_list.html"
    context_object_name = "msrun_samples"
    paginate_by = 20
