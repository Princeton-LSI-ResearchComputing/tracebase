from django.views.generic import DetailView, ListView
from django.utils import dateparse
from django.http import JsonResponse

from DataRepo.models import Sample
from DataRepo.utils import QuerysetToPandasDataFrame as qs2df
import pandas as pd


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
        context = super(SampleListView, self).get_context_data(**kwargs)
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


def sample_json_data(request):
    """
    Get all sample data from the Pandas dataframe, and allow filtering based on
    Bootstrap-table query parameters for search and server-side pagination.

    Expected keys of query parameters of Bootstrap-Table: "offset", "limit", "search"
    Note that "limit" is the page size.

    It is easier to format time duration and date using Django's dateparse methods
    than JavaScript. Therefore, a few columns are added to the pandas dataframe to store formatted values
    before generating arrays of JSON data
    """

    # get Pandas dataframe for all samples
    all_anim_msrun_df = qs2df.get_animal_msrun_all_df()
    anim_sam_df = all_anim_msrun_df.copy()
    # add field to convert age to weeks
    anim_sam_df["age_in_weeks"] = anim_sam_df["age"].apply(
        lambda x: dateparse.parse_duration(str(x)).days // 7 if not pd.isna(x) else x
    )
    # add field to convert collection time to minutes
    anim_sam_df["sample_time_collected_m"] = anim_sam_df["sample_time_collected"].apply(
        lambda x: dateparse.parse_duration(str(x)).seconds // 60
        if not pd.isna(x)
        else x
    )
    # convert sample_date to string to keep yyyy-mm-dd format
    anim_sam_df["sample_date_formatted"] = anim_sam_df["sample_date"].apply(
        lambda x: str(x)
    )
    # convert msrun_date to string to keep yyyy-mm-dd format
    anim_sam_df["msrun_date_formatted"] = anim_sam_df["msrun_date"].apply(
        lambda x: str(x)
    )

    # get parameters from request.GET
    param_dict = dict(request.GET)
    # default values
    start = 1
    page_size = None
    search_text = None

    for k in param_dict:
        v = param_dict[k]
        if k == "offset" and v != []:
            start = start + int(v[0])
        if k == "limit" and v != []:
            page_size = int(v[0])
        if k == "search" and v != []:
            search_text = v[0]

    # get total rows before applying search string
    total_rows = anim_sam_df.shape[0]
    # filter the data based on search string first
    if search_text is not None:
        anim_sam_df1 = anim_sam_df[
            anim_sam_df.apply(
                lambda row: row.astype(str).str.contains(search_text).any(), axis=1
            )
        ]
    else:
        anim_sam_df1 = anim_sam_df.copy()

    # total count with search string
    total_rows_with_search = anim_sam_df1.shape[0]

    # slice dataframe based on start and end rows
    if page_size is not None:
        end = start + page_size
        anim_sam_df1 = anim_sam_df1[start:end]
    # convert data frame to dictionary
    anim_sam_data1 = qs2df.df_to_list_of_dict(anim_sam_df1)

    # final output based on format required by Bootstrap-Table data-url
    data_dict = {
        "totalnotfiltered": total_rows,
        "total": total_rows_with_search,
        "rows": anim_sam_data1,
    }
    return JsonResponse(data_dict, safe=False)
