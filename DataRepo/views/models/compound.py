from django.views.generic import DetailView, ListView

from DataRepo.models import Compound, PeakGroup
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class CompoundListView(ListView):
    """Generic class-based view for a list of compounds"""

    model = Compound
    context_object_name = "compound_list"
    template_name = "models/compound/compound_list.html"
    ordering = ["name"]

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(CompoundListView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        comp_list_stats_df = qs2df.get_compound_list_stats_df()
        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(comp_list_stats_df)
        context["df"] = data
        return context


class CompoundDetailView(DetailView):
    """Generic class-based detail view for a compound"""

    model = Compound
    template_name = "models/compound/compound_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(CompoundDetailView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        anim_list_stats_df = qs2df.get_animal_list_stats_df()

        pk = self.kwargs.get("pk")
        # get infusate(s) based on compound
        infusate_all_df = qs2df.get_infusate_all_df()
        inf_list = infusate_all_df[infusate_all_df["compound_id"] == pk]["infusate_id"]
        # animal list filtered by infusate(s)
        per_comp_anim_list_stats_df = anim_list_stats_df[
            anim_list_stats_df["infusate_id"].isin(inf_list)
        ]

        # convert DataFrame to a list of dictionary
        anim_per_comp_data = qs2df.df_to_list_of_dict(per_comp_anim_list_stats_df)
        context["anim_per_comp_df"] = anim_per_comp_data

        context["measured"] = (
            PeakGroup.objects.filter(compounds__id__exact=pk).count() > 0
        )
        return context
