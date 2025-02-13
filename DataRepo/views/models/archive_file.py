import json

from django.db.models import CharField, F, Func, Q, QuerySet, Value
from django.views.generic import DetailView, ListView

from DataRepo.models import ArchiveFile
from DataRepo.views.utils import get_cookie


class ArchiveFileListView(ListView):
    """Generic class-based view for a list of ArchiveFiles"""

    model = ArchiveFile
    context_object_name = "archive_file_list"
    template_name = "DataRepo/archive_file_list.html"
    paginate_by = 10
    DATETIME_FORMAT = (
        "Mon. DD, YYYY, h:MI a.m."  # TODO: postgres-specific. Disable if not postgres.
    )
    DBSTRING_FUNCTION = "to_char"  # TODO: postgres-specific. Disable if not postgres.

    # # TODO: Make a base class that supports ListView combined with bootstrap-table to make pages load faster, using
    # # server-side behavior for pagination

    def get_queryset(self):
        print("GETTING QS", flush=True)
        qs = super().get_queryset()
        print(f"SETTING TOTAL {qs.count()}")
        self.total = qs.count()
        self.raw_total = self.total
        print("GETTING PAGINATED QS", flush=True)
        return self.get_paginated_queryset(qs)

    def get_paginated_queryset(self, qs: QuerySet[ArchiveFile]):
        # See if there is search and/or sort criteria

        search_term = get_cookie(self.request, "archive-file-search", None)
        order_by = get_cookie(self.request, "archive-file-orderby", None)
        order_dir = get_cookie(self.request, "archive-file-orderdir", None)

        # We need the column names (which should correspond to fields we can use in a Q expression) to know how to check
        # each filter)
        filter_names_str = get_cookie(self.request, "archive-file-filternames", None)
        filter_names = []
        if filter_names_str is not None:
            filter_names = json.loads(filter_names_str)

        active_search = False
        q_exp = Q()

        for filter_name in filter_names:
            filter_value = get_cookie(
                self.request, f"archive-file-filter-{filter_name}", None
            )
            if filter_value is not None:
                active_search = True
                if filter_name == "studies":
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

        # Convert the date time field into a string.  This is used to render the imported timestamp so that searchers
        # users enter will match what they see.  The default django datetime format (i.e. what they see in the template)
        # when rendering the datetime object is not the same as what the stringified value looks like in a DB query.
        qs.annotate(
            imported_timestamp_str=Func(
                F("imported_timestamp"),
                Value(self.DATETIME_FORMAT),
                output_field=CharField(),
                function=self.DBSTRING_FUNCTION,
            )
        )

        # Perform a search if one is defined
        if search_term:
            active_search = True
            q_exp &= self.get_any_field_query(search_term)

        print(
            f"search q expression: {q_exp} order_by: {order_by} order_dir: {order_dir}",
            flush=True,
        )

        if active_search:
            qs = qs.filter(q_exp)

        # Set the total after the search
        print(f"SETTING QUERY TOTAL {qs.count()}")
        self.total = qs.count()

        # Sort the results, if sort has a value
        if order_by:
            if order_dir is not None and order_dir.lower().startswith(
                "d"
            ):  # "asc" or "desc"
                order_by = f"-{order_by}"
            qs = qs.order_by(order_by)

        # NOTE: Pagination is controlled by the override of the get_paginate_by method
        # # Limit the number of results returned using the offset and limit
        # if qs.count() < offset + 1:
        #     # In case the page/offset is invalid, reset to the beginning
        #     offset = 0

        # if limit <= 0:  # No limit - return all results
        #     if offset > 0:
        #         qs = qs[offset:]
        # else:
        #     qs = qs[offset:limit]

        print(f"RETURNING QS WITH {qs.count()} RECORDS", flush=True)

        return qs

    def get_paginate_by(self, queryset):
        limit = self.request.GET.get("limit", "")
        print(f"LIMIT RECEIVED: {limit}")
        if limit == "":
            cookie_limit = get_cookie(self.request, "archive-file-limit", None)
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
        """This sets up djang-compatible pagination, search, and sort"""

        # print("GETTING CONTEXT", flush=True)

        context = super().get_context_data(**kwargs)

        print(f"RAW TOTAL: {self.raw_total} TOTAL: {self.total}")

        # limit = self.request.GET.get("limit", "")
        # if limit == "":
        #     limit = self.paginate_by
        # else:
        #     limit = int(limit)

        # offset = self.request.GET.get("offset", "")
        # if offset == "":
        #     offset = 0
        # else:
        #     offset = int(offset)

        # page = self.request.GET.get("page", "")
        # if page == "":
        #     page = 1
        # else:
        #     page = int(page)

        # print("CREATING PAGINATOR", flush=True)

        # page_obj = context["page_obj"]

        # # Get pagination info
        # paginator = Paginator(page_obj.object_list, limit)

        # print("GETTING PAGE", flush=True)

        # # page_obj = paginator.get_page(page)
        # # context["page_obj"] = page_obj

        # # print("SETTING PAGE OBJ LIST", flush=True)

        # # # Ensure archive_file_list is paginated
        # # context[self.context_object_name] = page_obj.object_list

        print(
            f"RETURNING CONTEXT {context}\nPAGE_OBJ: "
            f"{[(k, getattr(context['page_obj'], k)) for k in dir(context['page_obj'])]}",
            flush=True,
        )

        return context

    def get_any_field_query(self, term: str):
        """Given a string search term, returns a Q expression that does a case-insensitive search of all fields from
        the table displayed in the template.  Note, annotation fields must be generated in order to apply the query.
        E.g. `imported_timestamp` must be converted to an annotation field named `imported_timestamp_str`.
        """

        print("GETTING QUERY", flush=True)

        q_exp = Q()
        for fld in [
            "filename",
            "imported_timestamp_str",
            "file_type__name",
            "file_format__name",
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
