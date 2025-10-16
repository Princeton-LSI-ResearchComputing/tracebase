from django.db.models import (
    Case,
    CharField,
    F,
    Func,
    IntegerField,
    ManyToManyField,
    Value,
    When,
)
from django.db.models.aggregates import Count, Min
from django.views.generic import DetailView

from DataRepo.models import DATETIME_FORMAT, DBSTRING_FUNCTION, ArchiveFile
from DataRepo.views.models.bst.query import QueryMode
from DataRepo.views.models.bst.export import BSTExportedListView


class ArchiveFileListView(BSTExportedListView):
    model = ArchiveFile

    # The subquery strategy is multiple orders of magnitude faster for this model, given the multiple M:M relationships
    # between ArchiveFile and Study, and the numbers of those intermediate related records.
    query_mode = QueryMode.subquery

    column_ordering = [
        "filename",
        "checksum",
        "imported_timestamp_str",
        "data_type",
        "data_format",
        "studies_count",
        "studies",
        "peak_groups_link",
        "peak_data_link",
    ]
    exclude = [
        "id",
        "imported_timestamp",
        "file_location",
        "raw_to_msrunsamples",
        "mz_to_msrunsamples",
        "peak_groups",
    ]
    column_settings = {
        "filename": {"linked": True},
        "checksum": {"visible": False},
        "data_type": {"visible": False, "filterer": {"distinct_choices": True}},
        "data_format": {"filterer": {"distinct_choices": True}},
        "imported_timestamp_str": {"header": "Imported Timestamp"},
        "peak_groups_link": {
            "header": "Peak Groups",
            "searchable": False,
            "sortable": False,
            "exported": False,
            "value_template": "models/archive_file/peak_groups_link_value.html",
        },
        "peak_data_link": {
            "header": "Peak Data",
            "searchable": False,
            "sortable": False,
            "exported": False,
            "value_template": "models/archive_file/peak_data_link_value.html",
        },
    }
    annotations = {
        "imported_timestamp_str": Func(
            F("imported_timestamp"),
            Value(DATETIME_FORMAT),
            output_field=CharField(),
            function=DBSTRING_FUNCTION,
        ),
        "studies": Min(
            Case(
                When(
                    data_format__code="mzxml",
                    then="mz_to_msrunsamples__sample__animal__studies",
                ),
                When(
                    data_format__code="ms_raw",
                    then="raw_to_msrunsamples__sample__animal__studies",
                ),
                When(
                    data_type__code="ms_peak_annotation",
                    then="peak_groups__msrun_sample__sample__animal__studies",
                ),
                default=Value(None),
                output_field=ManyToManyField(to="DataRepo.Study"),
            ),
        ),
        "studies_count": Case(
            When(
                data_format__code="mzxml",
                then=Count(
                    "mz_to_msrunsamples__sample__animal__studies", distinct=True
                ),
            ),
            When(
                data_format__code="ms_raw",
                then=Count(
                    "raw_to_msrunsamples__sample__animal__studies", distinct=True
                ),
            ),
            When(
                data_type__code="ms_peak_annotation",
                then=Count(
                    "peak_groups__msrun_sample__sample__animal__studies", distinct=True
                ),
            ),
            default=Value(0),
            output_field=IntegerField(),
        ),
        "peak_groups_link": Value("Browse Peak Groups", output_field=CharField()),
        "peak_data_link": Value("Browse Peak Data", output_field=CharField()),
    }


class ArchiveFileDetailView(DetailView):
    """Generic class-based detail view for an ArchiveFile"""

    model = ArchiveFile
    template_name = "models/archive_file/archive_file_detail.html"
