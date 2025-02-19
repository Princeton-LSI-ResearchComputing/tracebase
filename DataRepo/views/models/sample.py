import os
import polars as pl

from django.urls import reverse
from django.http import JsonResponse

from django.views.generic import DetailView, ListView

from DataRepo.models import Sample
from DataRepo.utils import SummaryTableData as std

from TraceBase import settings


def sample_json_data(request):
    """
    This view is for formatting pagination data into json format based on the query
    parameters for server-side pagination defined by bootstrap-table plugin
    ref: https://bootstrap-table.com/docs/api/table-options/#queryparams

    use parquet file as data source for fast reading of sumarry data
    use polars dataframe for fast in-memory text search and data filtering
    """
    ARCHIVE_DIR = settings.MEDIA_ROOT
    summary_dir = os.path.join(ARCHIVE_DIR, "summary")
    sample_parquet_file = os.path.join(
        summary_dir, "sample_list_summary.all_columns.parquet"
    )
    if not os.path.exists(sample_parquet_file):
        # re-generate the parquet file from pandas dataframe
        sam_df = std.get_sample_summary_df()
        sam_df.to_parquet(sample_parquet_file)
    # read file to polars dataframe
    pl_sam_df = pl.read_parquet(sample_parquet_file)
    out_col_list = pl_sam_df.columns

    # default values for page data
    start = 0
    page_size = 10
    search_text = None
    # use polars dataframe default sorting setting
    is_descending = False
    # sort on sample as deafult
    sort_col = "sample"

    param_dict = dict(request.GET)

    # parse page parameters/values
    for k in param_dict:
        v = param_dict[k]
        if k == "offset" and v != []:
            start = start + int(v[0])
        if k == "limit" and v != []:
            page_size = int(v[0])
        if k == "search" and v != []:
            search_text = v[0]
        if k == "sort" and v != []:
            sort_col = v[0]
        if k == "order" and v != []:
            sort_order = v[0]
            if sort_order == "desc":
                is_descending = True

    # use polars "with_columns" expression to add columns for fast data search/filtering
    # note that the added columns were not in the output dataframe for search results
    if search_text:
        search_list = search_text.split()
        pl_sam_out_df = (
            pl_sam_df.with_columns(
                # concatenate all string columns
                pl.concat_str(pl.col(pl.String), ignore_nulls=True).alias(
                    "str_combined"
                ),
                # concatenate selected list columns
                pl.concat_list(
                    "tracers", "labeled_elements", "concentrations", "studies"
                ).alias("list_combined"),
            )
            .with_columns(
                # convert joined list values to string
                pl.col("list_combined")
                .list.join("|")
                .alias("list_combined_str"),
            )
            .with_columns(
                # concatenate selected column values for search purpose, remove "|" from string
                pl.concat_str(
                    pl.col("str_combined", "list_combined_str"), ignore_nulls=True
                )
                .str.replace_all(r"\|", "")
                .alias("row_str_combined"),
            )
            .filter(
                pl.col("row_str_combined").str.contains_any(
                    search_list, ascii_case_insensitive=True
                )
            )
            .select(out_col_list)
            .sort(sort_col, descending=is_descending)
        )
    else:
        pl_sam_out_df = pl_sam_df.clone().sort(
            sort_col, descending=is_descending, nulls_last=True
        )

    total_rows = pl_sam_out_df.shape[0]
    total_rows_with_search = total_rows
    # get output based on page parameters
    pl_sliced_df = pl_sam_out_df.slice(start, page_size)
    data_filtered_dict = pl_sliced_df.to_dicts()

    # output based on format required by boostrap-table data-url
    out_dict = {
        "totalnotfiltered": total_rows,
        "total": total_rows_with_search,
        "rows": data_filtered_dict,
    }
    return JsonResponse(out_dict, safe=False)


class SampleListView(ListView):
    """
    Generic class-based view for a list of samples
    """

    model = Sample
    template_name = "DataRepo/sample_list.html"

    def get_context_data(self, **kwargs):
        # customize the context
        context = super().get_context_data(**kwargs)
        # get mapping of column names to display names
        sample_summ_col_info = std.get_sample_summary_column_info()
        sam_col_display_mapping_dict = sample_summ_col_info.col_display_mapping_dict

        # get url prefix without pk argument for each of DetailViews
        url_prefix_dict = {}
        obj_list = [
            "animal",
            "compound",
            "infusate",
            "msrunsample",
            "protocol",
            "sample",
            "study",
            "tissue",
        ]
        for i in range(len(obj_list)):
            view_name = obj_list[i] + "_detail"
            k = obj_list[i] + "_detail_url_prefix"
            url_prefix_dict[k] = reverse(view_name, kwargs={"pk": 1})[:-2]

        context["col_mapping_dict"] = sam_col_display_mapping_dict
        context["url_prefix_dict"] = url_prefix_dict

        return context


class SampleDetailView(DetailView):
    """Generic class-based detail view for a sample"""

    model = Sample
    template_name = "DataRepo/sample_detail.html"
