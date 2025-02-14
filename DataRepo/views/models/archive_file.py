from django.db.models import CharField, F, Func, Max, Min, Q, QuerySet, Value
from django.db.models.functions import Coalesce
from django.views.generic import DetailView, ListView

from DataRepo.models import ArchiveFile
from DataRepo.views.utils import get_cookie


class ArchiveFileListView(ListView):
    """Generic class-based view for a list of ArchiveFiles"""

    model = ArchiveFile
    context_object_name = "archive_file_list"
    template_name = "DataRepo/archive_file_list.html"
    paginate_by = 15

    bst_data_fields = [
        "filename",
        "imported_timestamp_str",
        "data_type__name",
        "data_format__name",
        "first_study",
        "peak_groups",
        "peak_data",
    ]
    DATETIME_FORMAT = "Mon. DD, YYYY, HH:MI a.m."
    DBSTRING_FUNCTION = "to_char"

    # TODO: Make a base class that supports ListView combined with bootstrap-table to make pages load faster, using
    # server-side behavior for pagination

    def __init__(self, *args, **kwargs):
        """An override of the superclass constructor intended to initialize custom instance attributes."""

        super().__init__(*args, **kwargs)
        self.timestamp_searchable = True
        self.total = 0
        self.raw_total = 0

    def get_queryset(self):
        """An override of the superclass method intended to only set total and raw_total instance attributes."""

        qs = super().get_queryset()
        self.total = qs.count()
        self.raw_total = self.total
        return self.get_paginated_queryset(qs)

    def get_paginated_queryset(self, qs: QuerySet[ArchiveFile]):
        """The superclass handles actual pagination, but the number of pages can be affected by applied filters, and the
        Content of those pages can be affected by sorting.  And both can be affected by annotated fields.  This method
        applies those criteria based on cookies.

        Limitations:
            1. Searching imported_timestamp is not supported if the database is not postgres
            2. Sorting based on study will only be based on the first or last related study, not the combined string of
               all related study names.
        Args:
            qs (QuerySet[ArchiveFile])
        Exceptions:
            None
        Returns:
            qs (QuerySet[ArchiveFile])
        """

        # 1. Retrieve search and sort settings (needed for annotations too, due to different annotations based on sort)

        # Search and filter criteria will be stored in a Q expression
        q_exp = Q()

        # Check the cookies for search/sort/filter settings
        search_term = get_cookie(self.request, "archive-file-search")
        order_by = get_cookie(self.request, "archive-file-order-by")
        order_dir = get_cookie(self.request, "archive-file-order-dir")
        # We need the column names (from the BST data-field attributes) to use in Q expressions
        filter_names = self.bst_data_fields.copy()
        for filter_name in filter_names:
            filter_value = get_cookie(
                self.request, f"archive-file-filter-{filter_name}"
            )
            if filter_value is not None and filter_value != "":
                # "first_study" is a convenient misnomer.  It can be first or last depending on sort direction
                if filter_name == "first_study":
                    # Studies is a special case.  We have to search 3 relations
                    or_q_exp = Q(
                        **{
                            "peak_groups__msrun_sample__sample__animal__studies__name__icontains": filter_value
                        }
                    )
                    or_q_exp |= Q(
                        **{
                            "mz_to_msrunsamples__sample__animal__studies__name__icontains": filter_value
                        }
                    )
                    or_q_exp |= Q(
                        **{
                            "raw_to_msrunsamples__sample__animal__studies__name__icontains": filter_value
                        }
                    )
                    q_exp &= or_q_exp
                else:
                    q_exp &= Q(**{f"{filter_name}__icontains": filter_value})
        # Add a global search if one is defined
        if search_term is not None and search_term != "":
            q_exp &= self.get_any_field_query(search_term)

        # 2. Add annotations (which can be used in search & sort)

        # Convert the date time field into a string.  This is used to render the imported timestamp so that searchers
        # users enter will match what they see.  The default django datetime format (i.e. what they see in the template)
        # when rendering the datetime object is not the same as what the stringified value looks like in a DB query.
        try:
            qs = qs.annotate(
                imported_timestamp_str=Func(
                    F("imported_timestamp"),
                    Value(self.DATETIME_FORMAT),
                    output_field=CharField(),
                    function=self.DBSTRING_FUNCTION,
                )
            )
        except Exception as e:
            self.timestamp_searchable = False
            print(
                f"ERROR: {type(e).__name__}: {e}\n"
                "Falling back to default.  Check that the database is postgres."
            )
            # The fallback is to have the template render the timestamp in the default manner.  Searching will be
            # imprecise however.
            qs = qs.annotate(imported_timestamp_str=F("imported_timestamp"))

        # Since the relationship between ArchiveFile and Study is M:M, we will annotate with just the first or last
        # ordered study.  We only need it for sorting.  In the template, we render all related studies with a sub-query.
        # "first_study" is a convenient misnomer.  It can be first or last depending on sort direction.
        if (
            order_by is not None
            and order_by == "first_study"
            and not order_dir.lower().startswith("d")
        ):
            qs = qs.annotate(
                first_study=Coalesce(
                    Min("peak_groups__msrun_sample__sample__animal__studies__name"),
                    Min("mz_to_msrunsamples__sample__animal__studies__name"),
                    Min("raw_to_msrunsamples__sample__animal__studies__name"),
                )
            )
        elif order_by is not None and order_by == "first_study":
            qs = qs.annotate(
                first_study=Coalesce(
                    Max("peak_groups__msrun_sample__sample__animal__studies__name"),
                    Max("mz_to_msrunsamples__sample__animal__studies__name"),
                    Max("raw_to_msrunsamples__sample__animal__studies__name"),
                )
            )

        # 3. Apply the search and filters

        if len(q_exp.children) > 0:
            qs = qs.filter(q_exp)

        # 4. Apply the sort

        # Sort the results, if sort has a value
        if order_by is not None:
            # We don't want to string-sort the timestamps.  We want to date-sort them, so...
            if order_by == "imported_timestamp_str":
                order_by = "imported_timestamp"
            if order_dir is not None and order_dir.lower().startswith("d"):
                # order_dir = "asc" or "desc"
                order_by = f"-{order_by}"

            qs = qs.order_by(order_by)

        # 4. Ensure distinct results (because annotations and/or sorting can cause the equivalent of a left join).

        qs = qs.distinct()

        # 5. Update the count

        # Set the total after the search
        self.total = qs.count()

        # NOTE: Pagination is controlled by the superclass and the override of the get_paginate_by method
        return qs

    def get_paginate_by(self, queryset):
        """An override of the superclass method to allow the user to change the rows per page."""

        limit = self.request.GET.get("limit", "")
        if limit == "":
            cookie_limit = get_cookie(self.request, "archive-file-limit")
            if cookie_limit is not None:
                limit = int(cookie_limit)
            else:
                limit = self.paginate_by
        else:
            limit = int(limit)

        # Setting the limit to 0 means "all", but returning 0 here would mean we wouldn't get a page object sent to the
        # template, so we set it to the number of results.  The template will turn that back into 0 so that we're not
        # adding an odd value to the rows per page select list and instead selecting "all".
        if limit == 0 or limit > queryset.count():
            limit = queryset.count()

        return limit

    def get_context_data(self, **kwargs):
        """This sets up django-compatible pagination, search, and sort"""

        context = super().get_context_data(**kwargs)

        # 1. Set context variables for initial defaults based on user-selections saved in cookies

        # Set search/sort context variables
        context["search_term"] = get_cookie(self.request, "archive-file-search")
        context["order_by"] = get_cookie(self.request, "archive-file-order-by")
        context["order_dir"] = get_cookie(self.request, "archive-file-order-dir")

        # Set limit context variable
        # limit can be 0 to mean unlimited/all, but the paginator's page is set to the number of results because if it's
        # set to 0, the page object and paginator object are not included in the context,
        context["limit"] = get_cookie(
            self.request, "archive-file-limit", self.paginate_by
        )

        # Set filter context variables
        filter_names = self.bst_data_fields.copy()
        for filter_name in filter_names:
            context[f"filter_{filter_name}"] = get_cookie(
                self.request, f"archive-file-filter-{filter_name}"
            )

        # Set default interface context variables
        context["limit_default"] = self.paginate_by
        context["total"] = self.total
        context["raw_total"] = self.raw_total
        context["timestamp_searchable"] = self.timestamp_searchable

        return context

    def get_any_field_query(self, term: str):
        """Given a string search term, returns a Q expression that does a case-insensitive search of all fields from
        the table displayed in the template.  Note, annotation fields must be generated in order to apply the query.
        E.g. `imported_timestamp` must be converted to an annotation field named `imported_timestamp_str`.

        Args:
            term (str): search term applied to all columns of the view
        Exceptions:
            None
        Returns:
            q_exp (Q): A Q expression that can be used in a django ORM filter
        """

        q_exp = Q()

        if term == "":
            return q_exp

        for fld in [
            "filename",
            "imported_timestamp_str",
            "data_type__name",
            "data_format__name",
            "peak_groups__msrun_sample__sample__animal__studies__name",
            "mz_to_msrunsamples__sample__animal__studies__name",
            "raw_to_msrunsamples__sample__animal__studies__name",
        ]:
            q_exp |= Q(**{f"{fld}__icontains": term})

        return q_exp


class ArchiveFileDetailView(DetailView):
    """Generic class-based detail view for an ArchiveFile"""

    model = ArchiveFile
    template_name = "DataRepo/archive_file_detail.html"
