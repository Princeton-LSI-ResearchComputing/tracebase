from django.views.generic import DetailView, ListView

from DataRepo.models import Sample
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class SampleListView(ListView):
    """
    Generic class-based view for a list of samples
    "model = Sample" is shorthand for queryset = Sample.objects.all()
    use queryset syntax for sample list with or without filtering
    """

    # return all samples without query filter
    queryset = Sample.objects.all()
    context_object_name = "sample_list"
    template_name = "DataRepo/sample_list.html"
    ordering = ["animal_id", "name"]

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(SampleListView, self).get_context_data(**kwargs)
        #  add data from the DataFrame to the context
        all_anim_msrun_df = qs2df.get_animal_msrun_all_df()

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(all_anim_msrun_df)

        context["df"] = data
        return context


class SampleDetailView(DetailView):
    """Generic class-based detail view for a sample"""

    model = Sample
    template_name = "DataRepo/sample_detail.html"
