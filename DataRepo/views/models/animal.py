from django.db.models import F, IntegerField, Value
from django.db.models.aggregates import Count
from django.db.models.functions import Extract
from django.views.generic import DetailView

from DataRepo.models import DURATION_SECONDS_ATTRIBUTE, Animal, Researcher
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df
from DataRepo.views.models.bst.list_view import BSTListView


class AnimalListView(BSTListView):
    model = Animal
    column_ordering = [
        "name",
        "studies",
        "genotype",
        "infusate",
        "infusate__tracer_links__tracer",
        "infusate__tracer_links__tracer__compound",
        "infusate__tracer_links__concentration",
        "labels_mm_count",
        "label_combo",
        "infusion_rate",
        "treatment",
        "body_weight",
        "age_weeks",  # age
        "sex",
        "diet",
        "feeding_status",
        "samples__researcher",
        "tissues_count",
        "samples_mm_count",
        "msrun_samples_count",
    ]
    exclude = [
        "id",
        "age",
        "samples",
        "labels",
        "last_serum_sample",
    ]
    column_settings = {
        "genotype": {
            "filterer": {"distinct_choices": True},
            "header": "Tracer Label Combo",
        },
        "label_combo": {"filterer": {"distinct_choices": True}},
        "feeding_status": {"filterer": {"distinct_choices": True}},
        "diet": {"visible": False, "filterer": {"distinct_choices": True}},
        "age_weeks": {
            "header": "Animal Age (w)",
            "visible": False,
            "tooltip": "Units: weeks.",
        },
        "sex": {"visible": False},
        "samples__researcher": {"filterer": {"choices": Researcher.get_researchers}},
        "tissues_count": {"header": "Tissues", "tooltip": "Total number of Tissues."},
        "samples_mm_count": {
            "header": "Samples",
            "tooltip": "Total number of Samples.",
        },
        "msrun_samples_count": {
            "header": "MSRuns",
            "tooltip": "Total number of MSRuns.",
        },
        "infusate": {
            "td_template": "models/animal/infusate_td.html",
            "value_template": "models/animal/infusate_value.html",
        },
        "labels_mm_count": {
            "header": "Tracer Label Count",
            "tooltip": "Total number of Tracer Labels.",
            "visible": False,
        },
        "tracer_links_mm_count": {
            "header": "Tracer Count",
            "tooltip": "Total number of Tracers.",
            "visible": False,
        },
        "infusate__tracer_links__tracer": {"header": "Tracer(s)", "visible": False},
        "infusate__tracer_links__tracer__compound": {
            "header": "Tracer Compound",
            "visible": False,
        },
        "studies_mm_count": {"tooltip": "Total number of Studies.", "visible": False},
    }
    annotations = {
        "tissues_count": Count(
            "samples__tissue", output_field=IntegerField(), distinct=True
        ),
        "msrun_samples_count": Count(
            "samples__msrun_samples", output_field=IntegerField(), distinct=True
        ),
        "age_weeks": Extract(
            F("age"),
            DURATION_SECONDS_ATTRIBUTE,
        )
        / Value(604800),
    }


class AnimalDetailView(DetailView):
    """Generic class-based detail view for an animal"""

    model = Animal
    template_name = "models/animal/animal_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(AnimalDetailView, self).get_context_data(**kwargs)

        pk = self.kwargs.get("pk")
        per_anim_msrun_df = qs2df().get_per_animal_msrun_df(pk)

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(per_anim_msrun_df)
        context["df"] = data
        return context
