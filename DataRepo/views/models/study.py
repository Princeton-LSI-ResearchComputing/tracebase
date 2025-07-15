from django.db.models import IntegerField
from django.db.models.aggregates import Count
from django.shortcuts import render
from django.views.generic import DetailView

from DataRepo.models import Researcher, Study
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df
from DataRepo.views.models.bst.query import BSTListView


class StudyListView(BSTListView):
    model = Study
    below_template = "models/study/below_table.html"
    exclude = ["id", "animals"]
    column_ordering = [
        "name",
        "description",
        "animals__genotype",
        "total_infusates",
        "animals__infusate",
        "animals_infusate_tracer_links_mm_count",
        "animals__infusate__tracer_links__tracer",
        "animals__infusate__tracer_links__tracer__compound",
        "animals__label_combo",
        "animals__treatment",
        "animals__samples__researcher",
        "animals__samples__msrun_samples__msrun_sequence__researcher",
        "animals_mm_count",
        "total_tissues",
        "animals_samples_mm_count",
        "animals_samples_msrun_samples_mm_count",
    ]
    column_settings = {
        "animals__genotype": {"filterer": {"distinct_choices": True}, "unique": True},
        "total_infusates": {
            "tooltip": "Total number of infusates in a study.",
        },
        "animals__infusate": {
            "td_template": "models/study/infusates_td.html",
            "value_template": "models/study/infusates_value_list.html",
            "delim": ",",
            "unique": True,
        },
        "animals_infusate_tracer_links_mm_count": {
            "header": "Total Tracers",
            "tooltip": "Total number of tracers used in a study.",
        },
        "animals__infusate__tracer_links__tracer": {"unique": True},
        "animals__infusate__tracer_links__tracer__compound": {"unique": True},
        "animals__label_combo": {
            "filterer": {"distinct_choices": True},
            "unique": True,
            "limit": 10,
            "header": "Label Combos",
        },
        "animals__treatment": {"unique": True},
        "animals__samples__msrun_samples__msrun_sequence__researcher": {
            "filterer": {"choices": Researcher.get_researchers},
            "searchable": False,  # Disabled due to performance
            "filterable": True,
        },
        "animals__samples__researcher": {
            "filterer": {"choices": Researcher.get_researchers},
            "searchable": False,  # Disabled due to performance
            "filterable": True,
        },
        "total_tissues": {"tooltip": "Total number of tissue types in a study."},
        "animals_mm_count": {
            "header": "Total Animals",
            "tooltip": "Total number of animals in a study.",
        },
        "animals_samples_mm_count": {
            "header": "Total Samples",
            "tooltip": "Total number of samples in a study.",
        },
        "animals_samples_msrun_samples_mm_count": {
            "header": "Total MSRuns",
            "tooltip": (
                "The number of MS Run Sample records that peak groups link to (not representative of the number of MS "
                "Run Sequences or the number of samples analyzed in a sequence due to placeholder records)."
            ),
        },
    }
    annotations = {
        "total_infusates": Count(
            "animals__infusate", output_field=IntegerField(), distinct=True
        ),
        "total_tissues": Count(
            "animals__samples__tissue", output_field=IntegerField(), distinct=True
        ),
    }


class StudyDetailView(DetailView):
    """Generic class-based detail view for a study."""

    model = Study
    template_name = "models/study/study_detail.html"

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(StudyDetailView, self).get_context_data(**kwargs)

        pk = self.kwargs.get("pk")
        per_stud_msrun_df = qs2df().get_per_study_msrun_df(pk)
        per_stud_stat_df = qs2df().get_per_study_stat_df(pk)

        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(per_stud_msrun_df)
        stats_data = qs2df.df_to_list_of_dict(per_stud_stat_df)

        context["df"] = data
        context["stats_df"] = stats_data
        return context


def study_summary(request):
    """
    function-based view for studies based summary data, including selected
    data fileds for animal, tissue, sample, MSRunSample, and MSRunSequence
    get DataFrame for summary data, then convert to JSON format
    """

    all_stud_msrun_df = qs2df.get_study_msrun_all_df()

    # convert DataFrame to a list of dictionary
    data = qs2df.df_to_list_of_dict(all_stud_msrun_df)
    context = {"df": data}
    return render(request, "models/study/study_summary.html", context)
