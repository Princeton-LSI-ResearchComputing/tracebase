import os
from pathlib import Path

from django.core.management import BaseCommand

from DataRepo.utils import SummaryTableData as std
from TraceBase import settings

ARCHIVE_DIR = settings.MEDIA_ROOT


def get_or_create_summary_dir():
    summary_dir = os.path.join(ARCHIVE_DIR, "summary")
    try:
        Path(summary_dir).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(e)
    return Path(summary_dir)


def sample_summary_to_files():
    # directory for storing output files
    summary_dir = get_or_create_summary_dir()
    # output files
    file_prefix = "sample_list_summary"
    out_csv_file = os.path.join(summary_dir, file_prefix + ".csv")
    out_tsv_file = os.path.join(summary_dir, file_prefix + ".tsv")
    out_excel_file = os.path.join(summary_dir, file_prefix + ".xlsx")
    out_parquet_file = os.path.join(summary_dir, file_prefix + ".all_columns.parquet")

    # data frame for sample summary table
    sam_df = std.get_sample_summary_df()
    sam_download_df = std.get_sample_summary_download_df()

    print("sam_df total rows:", sam_df.shape[0])
    print("sam_download_df total rows:", sam_download_df.shape[0])
    # write output files
    sam_download_df.to_csv(out_csv_file, index=False)
    sam_download_df.to_csv(out_tsv_file, sep="\t", index=False)
    sam_download_df.to_excel(out_excel_file, index=False)
    sam_df.to_parquet(out_parquet_file)


class Command(BaseCommand):

    def handle(self, *args, **options):
        sample_summary_to_files()
