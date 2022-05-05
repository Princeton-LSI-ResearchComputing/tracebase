from django.shortcuts import render
from django.views.generic import DetailView, ListView

from DataRepo.models import Study
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df


class StudyListView(ListView):
    """Generic class-based view for a list of studies."""

    model = Study
    context_object_name = "study_list"
    template_name = "DataRepo/study_list.html"
    ordering = ["name"]

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get the context
        context = super(StudyListView, self).get_context_data(**kwargs)
        # add data from the DataFrame to the context
        stud_list_stats_df = qs2df.get_study_list_stats_df()
        # convert DataFrame to a list of dictionary
        data = qs2df.df_to_list_of_dict(stud_list_stats_df)
        context["df"] = data
        return context


class StudyDetailView(DetailView):
    """Generic class-based detail view for a study."""

    model = Study
    template_name = "DataRepo/study_detail.html"

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
    data fileds for animal, tissue, sample, and MSRun
    get DataFrame for summary data, then convert to JSON format
    """

    all_stud_msrun_df = qs2df.get_study_msrun_all_df()

    # convert DataFrame to a list of dictionary
    data = qs2df.df_to_list_of_dict(all_stud_msrun_df)
    context = {"df": data}
    return render(request, "DataRepo/study_summary.html", context)
