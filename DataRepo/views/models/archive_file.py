from django.db.models import CharField, F, Func, Value
from django.views.generic import DetailView

from DataRepo.models import ArchiveFile
from DataRepo.views.models.base import BootstrapTableColumn, BootstrapTableListView


class ArchiveFileListView(BootstrapTableListView):
    """Generic class-based view for a list of ArchiveFiles"""

    model = ArchiveFile
    context_object_name = "archive_file_list"
    template_name = "DataRepo/archive_file_list.html"

    DATETIME_FORMAT = "Mon. DD, YYYY, HH:MI a.m."
    DBSTRING_FUNCTION = "to_char"

    columns = [
        # The first arg in each case is the template table's column name, which is expected to be the data-field value
        # in the bootstrap table's th tag.
        BootstrapTableColumn(
            "filename",
            sorter="htmlSorter",
        ),

        BootstrapTableColumn(
            # The column name in this case is an annotated field.  An annotated field is used in this case so that a BST
            # substring search can find matching values in the DB.
            "imported_timestamp_str",

            # We provide the actual model field name as a fallback in case the converter fails.  (Conversion functions
            # can be database-specific.  If the backend DB is different, the annotation will be created directly from
            # the field.  Searching will be disabled.)
            field="imported_timestamp",
            converter=Func(
                F("imported_timestamp"),
                Value(DATETIME_FORMAT),
                output_field=CharField(),
                function=DBSTRING_FUNCTION,
            ),
        ),

        # The name in the following 2 cases are a related field, but it's 1:1.  The field is automatically set to the
        # name's value.
        BootstrapTableColumn(
            "data_format__name",
            visible=False,  # Initial visibility
        ),
        BootstrapTableColumn("data_type__name"),

        BootstrapTableColumn(
            # The column name in this case is another annotated field, but this annotation is automatically created due
            # to the fact that the field is a list.  There are 3 links to ArchiveFile from different models.  For any
            # one of these 3 links, we know that only 1 will have a value because each links to a different ArchiveFile
            # type.  Coalesce is a DB function that takes the first populated value of the 3.
            "first_study",

            # Each of the fields in field below are many-to-many relations.  Setting many_related to true not only keeps
            # the number of rows consistent with the number of ArchiveFile records, but it also makes querying the
            # database much much faster
            many_related=True,

            # These will automatically get .annotate(first_study=Coalesce(Min(...), Min(...), Min(...)) applied
            field=[
                "peak_groups__msrun_sample__sample__animal__studies__name",
                "mz_to_msrunsamples__sample__animal__studies__name",
                "raw_to_msrunsamples__sample__animal__studies__name",
            ],
        ),

        BootstrapTableColumn(
            "peak_groups",
            sortable=False,
            filter_control=None,
        ),

        BootstrapTableColumn(
            "peak_data",
            sortable=False,
            filter_control=None,
        ),
    ]


class ArchiveFileDetailView(DetailView):
    """Generic class-based detail view for an ArchiveFile"""

    model = ArchiveFile
    template_name = "DataRepo/archive_file_detail.html"
