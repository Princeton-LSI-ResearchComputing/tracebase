import pandas as pd
from django.views.generic import DetailView, ListView

from DataRepo.models import Protocol
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class AnimalTreatmentListView(ListView):
    """
    Generic class-based view for animal treatment protocols
    """

    model = Protocol
    context_object_name = "animal_treatment_list"
    template_name = "models/protocol/animal_treatments.html"

    def get_queryset(self):
        queryset = super().get_queryset()
        queryset = Protocol.objects.filter(category=Protocol.ANIMAL_TREATMENT).order_by(
            "name"
        )
        return queryset


class ProtocolDetailView(DetailView):
    """Generic class-based detail view for a protocol"""

    model = Protocol
    template_name = "models/protocol/protocol_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super().get_context_data(**kwargs)
        # filter data from DataFrames and then add to the context
        stud_list_stats_df = qs2df.get_study_list_stats_df()
        stud_msrun_df = qs2df.get_study_msrun_all_df()

        # filter study list by protocol category
        # default protocol display
        proto_display = "Protocol"
        pk = self.kwargs.get("pk")
        if self.object.category == Protocol.ANIMAL_TREATMENT:
            proto_display = "Animal Treatment"
            study_list = stud_msrun_df[stud_msrun_df["treatment_id"] == pk]["study_id"]
            per_proto_stud_list_stats_df = stud_list_stats_df[
                stud_list_stats_df["study_id"].isin(study_list)
            ]
        else:
            # currenly no plan for other protocol categories
            per_proto_stud_list_stats_df = pd.DataFrame()

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(per_proto_stud_list_stats_df)
        context["df"] = data
        context["proto_display"] = proto_display

        return context
