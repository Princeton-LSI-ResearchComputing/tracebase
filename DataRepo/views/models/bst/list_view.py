from typing import List
from warnings import warn
from django.conf import settings
from django.core.exceptions import FieldError
from django.db import ProgrammingError
from django.db.models import Q, QuerySet, Value
from django.db.models.functions import Coalesce

from DataRepo.views.models.bst.base import BSTBaseListView
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn


class BSTListView(BSTBaseListView):
    """Generic class-based view for a Model record list to make pages load faster, using server-side behavior for
    pagination.  This class (which inherits the client interface, Django ListView, and the auto-setup functionality of
    BSTBaseListView) is responsible for executing queries and serving up the results.  It uses the instance variables
    initialized by the BSTBaseListView class's constructor to narrow and sort the results based on user input.

    Usage:
        # Just create a class that inherits from BSTListView and sets a model
        class MyModelListView(BSTListView):
            model = MyModel

        # Set more fields
        class MyModelListView(BSTListView):
            model = MyModel
            column_ordering = ["field1", "field2", "related__field3", "reverse_related_field4"]

        # Customize the fields
        class MyModelListView(BSTListView):
            model = MyModel
            def __init__(self):
                # All of the other model fields are auto-added
                columns = {"field1": {"visible": False}}
                super().__init__(columns)
    """

    def get_manipulated_queryset(self, qs: QuerySet):
        """The superclass handles actual pagination, but the number of pages can be affected by applied filters, and the
        Content of those pages can be affected by sorting.  And both can be affected by annotated fields.  This method
        applies those criteria based on cookies.

        Limitations:
            1. Searching imported_timestamp is not supported if the database is not postgres
            2. Sorting based on study will only be based on the first or last related study, not the combined string of
               all related study names.
        Args:
            qs (QuerySet)
        Exceptions:
            None
        Returns:
            qs (QuerySet)
        """
        # 1. Retrieve search and sort settings (needed for annotations too, due to different annotations based on sort)

        # Search and filter criteria will be stored in a Q expression
        q_exp = Q()

        # Update the many-related sort settings in self.columns, based on self.groups, if the sortcol is in a group.
        # NOTE: These are used for sorting delimited values the same in the many-related columns that are in a group
        # (i.e. from the same many-related model).
        if self.ordered:
            for group in self.groups.values():
                if self.sortcol in group.columns:
                    group.set_sorters(self.sortcol, self.asc)

        # We need the column names (from the BST data-field attributes) to use in Q expressions
        model_paths: List[str] = []
        # prefetches = []
        for column in self.columns.values():
            # Put all fields' model paths into model_paths, to be evaluated for entry into prefetches
            if isinstance(column, BSTRelatedColumn):
                if column.related_model_path not in model_paths:
                    print(f"ADDING MODEL {column.related_model_path} FROM COLUMN {column.name} AND FIELD {column.field}")
                    model_paths.append(column.related_model_path)
                # else:
                #     print(f"NOT ADDING MODEL {column.related_model_path} FROM COLUMN {column.name} AND FIELD {column.field}")
                # # DEBUG: Testing if this speeds up ArchiveFileListView
                # if mdl is not None and column.many_related and len(mdl.split("__")) > 1:
                #     print(f"ADDING PREFETCH {mdl} FROM COLUMN {column.name} AND CREATING LIST IN ATTR {column.mm_list}")
                #     prefetches.append(Prefetch(mdl, to_attr=column.mm_list))
                # elif mdl is not None:
                #     prefetches.append(mdl)

            # Construct Q expressions for the filters (if any)
            if column.name in self.filter_terms.keys():
                filter_term = self.filter_terms[column.name]
                print(f"FILTERING COLUMN '{column.name}' WITH TERM '{filter_term}'")
                q_exp &= column.filterer.filter(filter_term)

        # Add a global search if one is defined
        if self.search_term is not None:
            q_exp &= self.search()

        # 2. Prefetch all required related fields to reduce the number of queries

        # DEBUG: Testing if the Prefetch object strategy above is faster
        prefetches: List[str] = []
        for model_path in sorted(
            model_paths,
            key=len,
            reverse=True,
        ):
            contained = False
            for upath in prefetches:
                if upath.startswith(model_path):
                    remainder = upath.replace(model_path, "", 1)
                    if remainder == "" or remainder.startswith("__"):
                        contained = True
                        break
            if not contained:
                prefetches.append(model_path)
        print(f"PREFETCHES: {prefetches} MODEL PATHS: {model_paths}")
        if len(prefetches) > 0:
            qs = qs.prefetch_related(*prefetches)

        # 3. Add annotations (which can be used in search & sort)

        annotations_before_filter = {}
        annotations_after_filter = {}
        for column in self.columns.values():
            if isinstance(column, BSTAnnotColumn):
                try:
                    print(f"ANNOTATING COLUMN {column.name}")
                    if isinstance(column.converter, Coalesce) and self.search_term is None and column.name not in self.filter_terms.keys():
                        annotations_after_filter[column.name] = column.converter
                    else:
                        annotations_before_filter[column.name] = column.converter
                except Exception as e:
                    column.searchable = False
                    column.sortable = False
                    msg = f"{type(e).__name__}: {e}\nConverter '{column.converter}' for column '{column.name}' failed."
                    warn(msg)
                    if settings.DEBUG:
                        self.warnings.append(msg)
                    annotations_after_filter[column.name] = Value("ERROR")

        if len(annotations_before_filter.keys()) > 0:
            print(f"BEFORE-ANNOTATIONS: {annotations_before_filter}")
            # print(f"COUNT: {qs.count()} BEFORE BEFORE-ANNOTATIONS: {annotations_before_filter}")
            qs = qs.annotate(**annotations_before_filter)

        # 4. Apply the search and filters

        if len(q_exp.children) > 0:
            try:
                print(f"FILTERS: {q_exp}")
                qs = qs.filter(q_exp)
            except FieldError as fe:
                fld_str = "\n\t".join(self.searchcols)
                fld_msg = f"One or more of {len(self.searchcols)} fields is misconfigured:\n\n\t{fld_str}"
                warning = (
                    f"Your search could not be executed.  {fld_msg}\n\n"
                    "Please report this error to the site administrators."
                )
                if settings.DEBUG:
                    warn(f"{warning}\nException: {type(fe).__name__}: {fe}")
                self.warnings.append(warning)
                self.reset_search_cookie()
                self.reset_filter_cookies()

        # 5. Apply coalesce annotations AFTER the filter, due to the inefficiency of WHERE interacting with COALESCE

        if len(annotations_after_filter.keys()) > 0:
            print(f"AFTER-ANNOTATIONS: {annotations_after_filter}")
            # print(f"COUNT: {qs.count()} BEFORE AFTER-ANNOTATIONS: {annotations_after_filter}")
            qs = qs.annotate(**annotations_after_filter)

        # 6. Apply the sort

        # Sort the results, if sort has a value
        if self.ordered:
            # print(f"COUNT BEFORE ORDERBY: {qs.count()} ORDER BY: {self.sortcol}")

            order_by_arg = self.columns[self.sortcol].sorter.order_by
            print(f"ORDERING ROWS BY {order_by_arg}")
            qs = qs.order_by(order_by_arg)

        # 7. Ensure distinct results (because annotations and/or sorting can cause the equivalent of a left join).

        print("DISTINCTING")
        # print(f"COUNT BEFORE DISTINCT: {qs.count()}")
        qs = qs.distinct()

        # 8. Update the count

        print("COUNTING")
        # Set the total after the search
        self.total = qs.count()
        print(f"COUNT BEFORE RETURN: {self.total}")

        if self.total > self.raw_total:
            if settings.DEBUG:
                if self.sortcol is None:
                    raise ProgrammingError(
                        f"An unknown bug is causing duplicate '{self.model.__name__}' records in the queryset, "
                        "inflating the number of results '{self.total}' over the number of records in the model "
                        f"'{self.raw_total}'."
                    )
                expression = self.columns[self.sortcol].sorter.expression
                raise ProgrammingError(
                    f"The number of records in the queryset '{self.total}' is greater than the number of records in "
                    f"the model '{self.raw_total}'.  This occurs when the sort expression uses a many-related field "
                    f"without an aggregation function.  The sort column was '{self.sortcol}' and the sort expression "
                    f"was '{expression}'.  Please set a sorter using an aggregate function."
                )

        return qs

    def search(self) -> Q:
        """Returns a Q expression for every searchable column using the self.search search term.

        NOTE: Annotation fields must be generated in order to apply the query is self.searchcol is a BSTAnnotColumn.

        Args:
            None
        Exceptions:
            None
        Returns:
            q_exp (Q): A Q expression that can be used in a django ORM filter
        """

        q_exp = Q()

        if self.search_term is None:
            return q_exp

        for column in self.columns.values():
            if column.searchable:
                # TODO: Consider making it possible to use icontains even if the input method is select (for the search
                # field)
                q_exp |= column.filterer.filter(self.search_term)

        return q_exp
