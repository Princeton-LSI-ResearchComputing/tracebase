from django.views.generic import DetailView, ListView

from DataRepo.models import Compound, PeakGroup
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class CompoundListView(ListView):
    """Generic class-based view for a list of compounds"""

    model = Compound
    context_object_name = "compound_list"
    template_name = "DataRepo/compound_list.html"
    ordering = ["name"]

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(CompoundListView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        comp_tracer_list_df = qs2df.get_compound_synonym_list_df()
        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(comp_tracer_list_df)
        context["df"] = data
        return context


class CompoundDetailView(DetailView):
    """Generic class-based detail view for a compound"""

    model = Compound
    template_name = "DataRepo/compound_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(CompoundDetailView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        anim_list_stats_df = qs2df.get_animal_list_stats_df()

        pk = self.kwargs.get("pk")
        per_tracer_anim_list_stats_df = anim_list_stats_df[
            anim_list_stats_df["tracer_compound_id"] == pk
        ]
        # convert DataFrame to a list of dictionary
        tracer_data = qs2df.df_to_list_of_dict(per_tracer_anim_list_stats_df)
        context["tracer_df"] = tracer_data
        context["measured"] = (
            PeakGroup.objects.filter(compounds__id__exact=pk).count() > 0
        )
        return context
