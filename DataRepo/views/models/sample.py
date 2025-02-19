from django.db.models import CharField, Count, F, Func, Value
from django.db.models.functions import Coalesce, NullIf, Extract
from django.views.generic import DetailView, ListView

from DataRepo.models import Sample, Animal, ElementLabel, Researcher
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df
from DataRepo.views.models.base import BootstrapTableColumn, BootstrapTableListView


class BSTSampleListView(BootstrapTableListView):
    model = Sample
    context_object_name = "sample_list"
    template_name = "DataRepo/sample_list_new.html"
    paginate_by = 10

    DATE_FORMAT = "Mon. DD, YYYY"  # Postgres date format
    TEMPLATE_DATE_FORMAT = "%b. %d, %Y"  # datetime format - must match DATE_FORMAT
    DBSTRING_FUNCTION = "to_char"

    def __init__(self):
        """Only creating this method to keep database calls out of the class attribute area (for populating the
        select_options values).  Otherwise, columns can be defined as a class attribute."""

        researchers = Researcher.get_researchers()

        self.columns = [
            BootstrapTableColumn("name"),
            BootstrapTableColumn("animal__name"),
            BootstrapTableColumn("tissue__name"),
            BootstrapTableColumn("first_study", many_related=True, field="animal__studies__name"),
            BootstrapTableColumn("first_study_id", many_related=True, searchable=False, field="animal__studies"),
            BootstrapTableColumn(
                "animal__genotype",
                select_options=Animal.objects.order_by("genotype").distinct("genotype").values_list(
                    "genotype", flat=True
                ),
            ),
            BootstrapTableColumn("animal__infusate__name"),
            BootstrapTableColumn("first_tracer", many_related=True, field="animal__infusate__tracers__name"),
            BootstrapTableColumn(
                "first_tracer_compound_id", many_related=True, field="animal__infusate__tracers__compound",
            ),
            BootstrapTableColumn(
                "first_tracer_conc", many_related=True, field="animal__infusate__tracer_links__concentration"
            ),
            BootstrapTableColumn(
                "first_label",
                many_related=True,
                field="animal__labels__element",
                select_options=[e[0] for e in ElementLabel.LABELED_ELEMENT_CHOICES],
            ),
            BootstrapTableColumn("animal__infusion_rate"),
            BootstrapTableColumn("animal__treatment__name"),
            BootstrapTableColumn("animal__body_weight", visible=False),
            BootstrapTableColumn(
                "age_weeks_str",
                field="animal__age",
                converter=Extract(F("animal__age"), "day") / Value(7),
                visible=False,
            ),
            BootstrapTableColumn("animal__sex", visible=False, select_options=[s[0] for s in Animal.SEX_CHOICES]),
            BootstrapTableColumn("animal__diet", visible=False),
            BootstrapTableColumn(
                "animal__feeding_status",
                select_options=Animal.objects.order_by("feeding_status").distinct("feeding_status").values_list(
                    "feeding_status", flat=True
                ),
            ),
            BootstrapTableColumn("researcher", select_options=researchers),  # handler
            BootstrapTableColumn(
                "col_date_str",
                field="date",
                converter=Func(
                    F("date"),
                    Value(self.DATE_FORMAT),
                    output_field=CharField(),
                    function=self.DBSTRING_FUNCTION,
                ),
            ),
            BootstrapTableColumn("col_time_str", field="time_collected", converter=Extract(F("time_collected"), "minute")),
            BootstrapTableColumn(
                "sequence_count",
                many_related=True,
                searchable=False,
                converter=Coalesce(
                    NullIf(Count("msrun_samples__msrun_sequence", distinct=True), Value(0)),
                    Value(0),  # Default if no studies linked
                ),
                field="msrun_samples__msrun_sequence",
            ),
            BootstrapTableColumn(
                "first_ms_operator",
                many_related=True,
                field="msrun_samples__msrun_sequence__researcher",
                select_options=researchers,
            ),
            BootstrapTableColumn(
                "first_ms_date",
                many_related=True,
                converter=Func(
                    F("msrun_samples__msrun_sequence__date"),
                    Value(self.DATE_FORMAT),
                    output_field=CharField(),
                    function=self.DBSTRING_FUNCTION,
                ),
                field="msrun_samples__msrun_sequence__date",
            ),
            BootstrapTableColumn("first_ms_sample", many_related=True, field="msrun_samples", searchable=False, sortable=False),
        ]
        # Calling the super constructor AFTER defining self.columns, because that constructor validates it.
        super().__init__()

    def get_context_data(self, **kwargs):
        """Add the MSRunSequence date format string to the context"""
        context = super().get_context_data(**kwargs)
        context["date_format"] = self.TEMPLATE_DATE_FORMAT
        return context


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
        context = super().get_context_data(**kwargs)
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
