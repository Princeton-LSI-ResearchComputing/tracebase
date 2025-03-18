from django.views.generic import DetailView, ListView

from DataRepo.models import Infusate
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class InfusateDetailView(DetailView):
    """Generic class-based detail view for an infusate"""

    model = Infusate
    template_name = "models/infusate/infusate_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(InfusateDetailView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        infusate_all_df = qs2df.get_infusate_all_df()

        pk = int(self.kwargs.get("pk"))
        per_infusate_all_df = infusate_all_df[infusate_all_df["infusate_id"] == pk]
        # convert DataFrame to a list of dictionary
        tracer_data = qs2df.df_to_list_of_dict(per_infusate_all_df)
        context["tracer_df"] = tracer_data

        return context


class InfusateListView(ListView):
    """Generic class-based view for a list of infusates"""

    model = Infusate
    context_object_name = "infusate_list"
    template_name = "models/infusate/infusate_list.html"
    ordering = ["name"]

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(InfusateListView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        infusate_list_df = qs2df.get_infusate_list_df()

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(infusate_list_df)
        context["df"] = data
        return context
