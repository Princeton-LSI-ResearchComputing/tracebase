from django.db.models import Count, F, Value, FloatField
from django.db.models.functions import Extract
from django.views.generic import DetailView, ListView

from DataRepo.models import Animal
from DataRepo.models.researcher import Researcher
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
                "age_weeks",
                field="age",
                converter=Extract(
                    F("age"),
                    self.DURATION_SECONDS_ATTRIBUTE) / Value(604800,
                    output_field=FloatField(),
                ),
                sorter=BSTColumn.SORTER_CHOICES.NUMERIC,
                header="Age (weeks)",
            ),
            "diet": {
                "select_options": (
                    Animal.objects
                    .filter(diet__isnull=False)
                    .order_by("diet")
                    .distinct("diet")
                    .values_list("diet", flat=True)
                ),
            },
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
            "labels": {
                "related_sort_fld": "labels__element",
            },
            "label_combo": {
                "select_options": (
                    Animal.objects
                    .filter(label_combo__isnull=False)
                    .order_by("label_combo")
                    .distinct("label_combo")
                    .values_list("label_combo", flat=True)
                ),
            },
            "samples__tissue": BSTColumn(
                "tissues_count",
                field="samples__tissue",
                header="Tissues Count",
                converter=Count("samples__tissue", distinct=True),
            ),
            "sample_owners": BSTColumn(
                "sample_owners",
                field="samples__researcher",
                many_related=True,
                header="Sample Owner(s)",
                select_options=Researcher.get_researchers(),
            ),
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
