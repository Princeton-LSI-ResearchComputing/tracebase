from django.core.paginator import Paginator
from django.db.models import Func, F, Q, Value, CharField, QuerySet
from django.views.generic import DetailView, ListView

from DataRepo.models import ArchiveFile


class ArchiveFileListView(ListView):
    """Generic class-based view for a list of ArchiveFiles"""

    model = ArchiveFile
    context_object_name = "archive_file_list"
    template_name = "DataRepo/archive_file_list.html"
    paginate_by = 10
    # DATETIME_FORMAT = "Mon. DD, YYYY, h:MI a.m."  # TODO: postgres-specific. Disable if not postgres.
    # DBSTRING_FUNCTION = "to_char"  # TODO: postgres-specific. Disable if not postgres.

    # # TODO: Make a base class that supports ListView combined with bootstrap-table to make pages load faster, using
    # # server-side behavior for pagination

    # def get_queryset(self):
    #     print("GETTING QS", flush=True)
    #     qs = super().get_queryset()
    #     print(f"SETTING TOTAL {qs.count()}")
    #     self.total = qs.count()
    #     self.raw_total = self.total
    #     print("GETTING PAGINATED QS", flush=True)
    #     return self.get_paginated_queryset(qs)

    # def get_paginated_queryset(self, qs: QuerySet[ArchiveFile]):
    #     # See if there is search and/or sort criteria
    #     search_term = self.request.GET.get("search")
    #     if search_term == "":
    #         search_term = None
    #     limit = self.request.GET.get("limit", "")
    #     if limit == "":
    #         limit = self.paginate_by
    #     else:
    #         limit = int(limit)
    #     offset = self.request.GET.get("offset", "")
    #     if offset == "":
    #         offset = 0
    #     else:
    #         offset = int(offset)
    #     sort = self.request.GET.get("sort")
    #     order = self.request.GET.get("order")

    #     print(f"search_term: {search_term} limit: {limit} offset: {offset} sort: {sort} order: {order}", flush=True)

    #     # Perform a search if one is defined
    #     if search_term:
    #         # Convert the date time field into a string to search with icontains from a bootstrap table search, using
    #         # the default django datetime format (i.e. what they see in the template)
    #         qs.annotate(imported_timestamp_str=Func(
    #             F("imported_timestamp"),
    #             Value(self.DATETIME_FORMAT),
    #             output_field=CharField(),
    #             function=self.DBSTRING_FUNCTION)
    #         )

    #         q_exp = self.get_any_field_query(search_term)

    #         qs = qs.filter(q_exp)

    #     # Set the total after the search
    #     print(f"SETTING QUERY TOTAL {qs.count()}")
    #     self.total = qs.count()

    #     # Sort the results, if sort has a value
    #     if sort:
    #         if order is not None and order.lower().startswith("d"):
    #             sort = f"-{sort}"
    #         qs = qs.order_by(sort)

    #     # Limit the number of results returned using the offset and limit
    #     qs = qs[offset:limit]

    #     print(f"RETURNING QS WITH {qs.count()} RECORDS", flush=True)

    #     return qs

    def get_paginate_by(self, queryset):
        limit = self.request.GET.get("limit", "")
        if limit == "":
            limit = self.paginate_by
        else:
            limit = int(limit)
        return limit

    # def get_context_data(self, **kwargs):
    #     """This sets up djang-compatible pagination, search, and sort"""

    #     print("GETTING CONTEXT", flush=True)

    #     context = super().get_context_data(**kwargs)

    #     limit = self.request.GET.get("limit", "")
    #     if limit == "":
    #         limit = self.paginate_by
    #     else:
    #         limit = int(limit)

    #     offset = self.request.GET.get("offset", "")
    #     if offset == "":
    #         offset = 0
    #     else:
    #         offset = int(offset)

    #     page = self.request.GET.get("page", "")
    #     if page == "":
    #         page = 1
    #     else:
    #         page = int(page)

    #     print("CREATING PAGINATOR", flush=True)

    #     page_obj = context["page_obj"]

    #     # Get pagination info
    #     paginator = Paginator(page_obj.object_list, limit)

    #     print("GETTING PAGE", flush=True)

    #     # page_obj = paginator.get_page(page)
    #     # context["page_obj"] = page_obj

    #     # print("SETTING PAGE OBJ LIST", flush=True)

    #     # # Ensure archive_file_list is paginated
    #     # context[self.context_object_name] = page_obj.object_list

    #     print(f"RETURNING CONTEXT {context}\nPAGE_OBJ: {[(k, getattr(context['page_obj'], k)) for k in dir(context['page_obj'])]}", flush=True)

    #     return context

    # def get_any_field_query(self, term: str):
    #     """Given a string search term, returns a Q expression that does a case-insensitive search of all fields from the
    #     table displayed in the template.  Note, annotation fields must be generated in order to apply the query.
    #     E.g. `imported_timestamp` must be converted to an annotation field named `imported_timestamp_str`."""

    #     print("GETTING QUERY", flush=True)

    #     q_exp = Q()
    #     for fld in ["filename", "imported_timestamp_str", "file_type__name", "file_format__name"]:
    #         q_exp |= Q(**{f"{fld}__icontains": term})
    #     return q_exp


class ArchiveFileDetailView(DetailView):
    """Generic class-based detail view for an ArchiveFile"""

    model = ArchiveFile
    template_name = "DataRepo/archive_file_detail.html"
