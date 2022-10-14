import pandas as pd
from django.db.models import Q
from django.views.generic import DetailView, ListView

from DataRepo.models import Protocol
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class ProtocolListView(ListView):
    """Generic class-based view for a list of protocols"""

    model = Protocol
    context_object_name = "protocol_list"
    template_name = "DataRepo/protocol_list.html"

    def get_queryset(self):
        queryset = super().get_queryset()
        queryset = {
            "animal_treatment": Protocol.objects.filter(
                category=Protocol.ANIMAL_TREATMENT
            ).order_by("name"),
            "msrun_protocol": Protocol.objects.filter(
                category=Protocol.MSRUN_PROTOCOL
            ).order_by("name"),
            "other_category": Protocol.objects.all()
            .exclude(
                Q(category=Protocol.ANIMAL_TREATMENT)
                | Q(category=Protocol.MSRUN_PROTOCOL)
            )
            .order_by("category", "name"),
        }
        return queryset


class ProtocolDetailView(DetailView):
    """Generic class-based detail view for a protocol"""

    model = Protocol
    template_name = "DataRepo/protocol_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super().get_context_data(**kwargs)
        # filter data from DataFrames and then add to the context
        stud_list_stats_df = qs2df.get_study_list_stats_df()
        stud_msrun_df = qs2df.get_study_msrun_all_df()
        # filter study list by protocol
        pk = self.kwargs.get("pk")
        if self.object.category == Protocol.ANIMAL_TREATMENT:
            study_list = stud_msrun_df[stud_msrun_df["treatment_id"] == pk]["study_id"]
            per_proto_stud_list_stats_df = stud_list_stats_df[
                stud_list_stats_df["study_id"].isin(study_list)
            ]
        elif self.object.category == Protocol.MSRUN_PROTOCOL:
            study_list = stud_msrun_df[stud_msrun_df["msrun_protocol_id"] == pk][
                "study_id"
            ]
            per_proto_stud_list_stats_df = stud_list_stats_df[
                stud_list_stats_df["study_id"].isin(study_list)
            ]
        else:
            # currenly no plan for other protocol categories
            per_proto_stud_list_stats_df = pd.DataFrame()

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(per_proto_stud_list_stats_df)
        context["df"] = data
        return context
