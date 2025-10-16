from django.views.generic import DetailView

from DataRepo.models import Infusate
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df
from DataRepo.views.models.bst.export import BSTExportedListView


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


class InfusateListView(BSTExportedListView):
    model = Infusate
    exclude = ["id", "animals", "tracers", "tracer_links"]
    column_ordering = [
        "name",
        "tracer_group_name",
        "label_combo",
        "tracer_links_mm_count",
        "tracer_links__tracer",
        "tracer_links__concentration",
        "animals_mm_count",
    ]
    column_settings = {
        "label_combo": {"filterer": {"distinct_choices": True}},
        "tracer_group_name": {"filterer": {"distinct_choices": True}},
        "tracer_links_mm_count": {"header": "Tracers Count"},
        "tracer_links__tracer": {"limit": 10},
        "tracer_links__concentration": {"limit": 10},
        "name": {
            "td_template": "models/infusate/infusate_td.html",
            "value_template": "models/infusate/infusate_value.html",
        },
    }
