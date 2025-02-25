from django.db.models import Case, CharField, Count, F, Func, Value, When
from django.db.models.functions import Extract
from django.views.generic import DetailView

from DataRepo.models import Sample, Animal, ElementLabel, Researcher
from DataRepo.views.models.base import BootstrapTableColumn as BSTColumn
from DataRepo.views.models.base import BootstrapTableListView as BSTListView


class SampleDetailView(DetailView):
    model = Sample
    template_name = "DataRepo/sample_detail.html"


class SampleListView(BSTListView):
    model = Sample
    context_object_name = "sample_list"
    template_name = "DataRepo/sample_list.html"
    paginate_by = 10

    DATE_FORMAT = "YYYY-MM-DD"  # Postgres date format syntax
    TEMPLATE_DATE_FORMAT = "%Y-%b-%d"  # python datetime package format syntax - must match DATE_FORMAT
    DBSTRING_FUNCTION = "to_char"  # Postgres function
    DURATION_SECONDS_ATTRIBUTE = "epoch"  # Postgres interval specific

    def __init__(self, *args, **kwargs):
        """Only creating this method to keep database calls out of the class attribute area (for populating the
        select_options values).  Otherwise, BSTColumns can be defined as a class attribute."""

        researchers = Researcher.get_researchers()

        self.columns = [
            BSTColumn("name", header="Sample"),
            BSTColumn("animal__name", header="Animal"),
            BSTColumn("tissue__name", header="Tissue"),
            BSTColumn("first_study", many_related=True, field="animal__studies__name", header="Studies"),
            BSTColumn(
                "first_study_id",
                many_related=True,
                searchable=False,
                field="animal__studies__id",
                exported=False,
            ),
            BSTColumn(
                "animal__genotype",
                select_options=(
                    Animal.objects
                    .order_by("genotype")
                    .distinct("genotype")
                    .values_list("genotype", flat=True)
                ),
                header="Genotype",
            ),
            BSTColumn(
                "infusate_name",
                field="animal__infusate__name",
                many_related=True,
                header="Infusate",
                converter=Case(
                    When(
                        animal__infusate__tracer_group_name__isnull=False,
                        then="animal__infusate__tracer_group_name",
                    ),
                    When(
                        animal__infusate__tracer_group_name__isnull=True,
                        then="animal__infusate__name",
                    ),
                ),
            ),

            # Linked M:M related columns, all of which will be delimited and sorted by the
            # animal__infusate__tracers__name field
            BSTColumn(
                "first_tracer",
                many_related=True,
                field="animal__infusate__tracer_links__tracer__name",
                many_related_sort_mdl="animal__infusate__tracer_links",
                many_related_sort_def="animal__infusate__tracer_links__concentration",
                header="Tracer(s)",
            ),
            BSTColumn(
                "first_tracer_compound_id",
                many_related=True,
                many_related_sort_mdl="animal__infusate__tracer_links",
                many_related_sort_def="animal__infusate__tracer_links__concentration",
                field="animal__infusate__tracer_links__tracer__compound__id",
                exported=False,
            ),
            BSTColumn(
                "first_tracer_conc",
                many_related=True,
                many_related_sort_mdl="animal__infusate__tracer_links",
                many_related_sort_def="animal__infusate__tracer_links__concentration",
                field="animal__infusate__tracer_links__concentration",
                header="Tracer Concentration(s) (mM)",
            ),

            BSTColumn(
                "first_label",
                many_related=True,
                field="animal__labels__element",
                select_options=[e[0] for e in ElementLabel.LABELED_ELEMENT_CHOICES],
                header="Tracer Elements",
            ),
            BSTColumn("animal__infusion_rate", header="Infusion Rate (ul/min/g)"),
            BSTColumn("animal__treatment__name", header="Treatment"),
            BSTColumn("animal__body_weight", visible=False, header="Body Weight (g)"),
            BSTColumn(
                "age_weeks_str",
                field="animal__age",
                converter=Extract(F("animal__age"), self.DURATION_SECONDS_ATTRIBUTE) / Value(604800),
                visible=False,
                header="Age (weeks)",
            ),
            BSTColumn("animal__sex", visible=False, select_options=[s[0] for s in Animal.SEX_CHOICES], header="Sex"),
            BSTColumn("animal__diet", visible=False, header="Diet"),
            BSTColumn(
                "animal__feeding_status",
                select_options=(
                    Animal.objects
                    .order_by("feeding_status")
                    .distinct("feeding_status")
                    .values_list("feeding_status", flat=True)
                ),
                header="Feeding Status",
            ),
            BSTColumn("researcher", select_options=researchers, header="Sample Owner"),
            BSTColumn(
                "col_date_str",
                field="date",
                converter=Func(
                    F("date"),
                    Value(self.DATE_FORMAT),
                    output_field=CharField(),
                    function=self.DBSTRING_FUNCTION,
                ),
                header="Sample Date",
            ),
            BSTColumn(
                "col_time_str",
                field="time_collected",
                converter=Extract(F("time_collected"), self.DURATION_SECONDS_ATTRIBUTE) / Value(60),
                header="Time Collected (m)",
            ),
            BSTColumn(
                "sequence_count",
                many_related=True,
                searchable=False,
                converter=Count("msrun_samples__msrun_sequence", distinct=True),
                field="msrun_samples__msrun_sequence__id",
                exported=False,
            ),
            BSTColumn(
                "first_ms_operator",
                many_related=True,
                field="msrun_samples__msrun_sequence__researcher",
                select_options=researchers,
                header="MSRun Owner",
            ),
            BSTColumn(
                "first_ms_date",
                many_related=True,
                converter=Func(
                    F("msrun_samples__msrun_sequence__date"),
                    Value(self.DATE_FORMAT),
                    output_field=CharField(),
                    function=self.DBSTRING_FUNCTION,
                ),
                field="msrun_samples__msrun_sequence__date",
                header="MSRun Date",
            ),
            BSTColumn(
                "first_ms_sample",
                many_related=True,
                field="msrun_samples",
                searchable=False,
                sortable=False,
                filter_control="",
                header="MSRun Detail",
                exported=False,
            ),
        ]
        # Calling the super constructor AFTER defining self.columns, because that constructor validates it.
        super().__init__(*args, **kwargs)

    def get_context_data(self, **kwargs):
        """Add the MSRunSequence date format string to the context.  This is uniquely needed in the template due to the
        fact that MSRunSequence has a many-to-one relationship with a sample, thus to render them all in a BSTColumn in
        one row, we have to loop on actual database objects that do not have the annotated first_ms_date string"""
        context = super().get_context_data(**kwargs)
        context["date_format"] = self.TEMPLATE_DATE_FORMAT
        return context
