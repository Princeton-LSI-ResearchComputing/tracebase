from django.db.models import F, Value
from django.db.models.functions import Extract
from django.views.generic import DetailView, ListView

from DataRepo.models import Animal
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df
from DataRepo.views.models.base import BSTListView, BSTColumn


class AnimalListView(BSTListView):
    """Generic class-based view for a list of tracers"""
    model = Animal
    paginate_by = 10
    include_through_models = True
    exclude_fields = ["id", "first_samples"]
    DURATION_SECONDS_ATTRIBUTE = "epoch"  # Postgres interval specific

    def __init__(self):
        custom_columns = {
            "age": BSTColumn(
                "age_weeks_str",
                field="age",
                converter=Extract(F("age"), self.DURATION_SECONDS_ATTRIBUTE) / Value(604800),
                header="Age (weeks)",
            ),
            "genotype": {
                "select_options": (
                    Animal.objects
                    .order_by("genotype")
                    .distinct("genotype")
                    .values_list("genotype", flat=True)
                ),
            },
            "feeding_status": {
                "select_options": (
                    Animal.objects
                    .order_by("feeding_status")
                    .distinct("feeding_status")
                    .values_list("feeding_status", flat=True)
                ),
            },
        }
        super().__init__(custom=custom_columns)


class AnimalListViewOLD(ListView):
    """Generic class-based view for a list of animals"""

    model = Animal
    context_object_name = "animal_list"
    template_name = "DataRepo/animal_list.html"
    ordering = ["name"]

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(AnimalListView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        anim_list_stats_df = qs2df.get_animal_list_stats_df()

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(anim_list_stats_df)
        context["df"] = data
        return context


class AnimalDetailView(DetailView):
    """Generic class-based detail view for an animal"""

    model = Animal
    template_name = "DataRepo/animal_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(AnimalDetailView, self).get_context_data(**kwargs)

        pk = self.kwargs.get("pk")
        per_anim_msrun_df = qs2df().get_per_animal_msrun_df(pk)

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(per_anim_msrun_df)
        context["df"] = data
        return context
