import traceback
from typing import Dict, List, Tuple
from warnings import warn

from django.conf import settings
from django.core.exceptions import FieldError
from django.db import ProgrammingError
from django.db.models import Q, QuerySet, Value
from django.db.models.expressions import Combinable

from DataRepo.utils.exceptions import DeveloperWarning
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
            column_ordering = ["field1", "field2", "related__field3", "reverse_related_field4", "annot_field"]
            annotations = {"annot_field": Upper("field5")}
            exclude = ["id", "field5"]

        # Customize the fields
        class MyModelListView(BSTListView):
            model = MyModel
            def __init__(self):
                # All of the other model fields are auto-added
                columns = {"field1": {"visible": False}}
                super().__init__(columns)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.total = 0
        self.raw_total = 0

        self.prefetches: List[str] = self.get_prefetches()
        self.filters = self.get_filters()
        (
            self.prefilter_annots,
            self.postfilter_annots,
        ) = self.get_annotations()

        # Update the many-related sort settings in self.columns, based on self.groups, if the sort col is in a group.
        # NOTE: These are used for sorting delimited values the same in the many-related columns that are in a group
        # (i.e. from the same many-related model).
        if self.ordered:
            for group in [
                g for g in self.groups.values() if self.sort_col.name in g.columns
            ]:
                group.set_sorters(self.sort_col.name, self.asc)
            else:
                self.sort_col.sorter = self.sort_col.create_sorter(asc=self.asc)

    def get_queryset(self):
        """An extension of the superclass method intended to only set total and raw_total instance attributes."""

        qs = super().get_queryset()

        self.raw_total = qs.count()

        try:
            qs = self.get_user_queryset(qs)

            self.total = qs.count()

        except Exception as e:
            if settings.DEBUG:
                tb = "".join(traceback.format_tb(e.__traceback__))
                warn(f"{tb}{type(e).__name__}: {e}", DeveloperWarning)

            self.total = self.raw_total
            self.clear_cookies = True
            self.warnings.append(
                "There was an error processing your request.  Your cookies have been cleared just in case a bad cookie "
                "is the reason.  If the error recurs, please report it to the administrators."
            )

        if self.total > self.raw_total:
            if settings.DEBUG:
                if self.sort_name is None:
                    raise ProgrammingError(
                        f"An unknown bug is causing duplicate '{self.model.__name__}' records in the queryset, "
                        f"inflating the number of results '{self.total}' over the number of records in the model "
                        f"'{self.raw_total}'."
                    )

                expression = self.columns[self.sort_name].sorter.expression
                raise ProgrammingError(
                    f"The number of records in the queryset '{self.total}' is greater than the number of records in "
                    f"the model '{self.raw_total}'.  This occurs when the sort expression uses a many-related field "
                    f"without an aggregation function.  The sort column was '{self.sort_name}' and the sort expression "
                    f"was '{expression}'.  Please set a sorter using an aggregate function."
                )

            warning = (
                f"A misconfiguration has caused the number of rows '{self.total}' to be greater than the number of "
                f"records in the database '{self.raw_total}'.  This can occur when a sort is applied to a column whose "
                f"data has a many-to-one relationship with the table '{self.model.__name__}'.  Note, some rows will "
                f"contain duplicate '{self.model.__name__}' data."
            )
            self.warnings.append(warning)

        return qs

    def get_user_queryset(self, qs: QuerySet):
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
        if len(self.prefetches) > 0:
            qs = qs.prefetch_related(*self.prefetches)

        if len(self.prefilter_annots.keys()) > 0:
            qs = self.apply_annotations(qs, self.prefilter_annots)

        if len(self.filters.children) > 0:
            qs = self.apply_filters(qs)

        if len(self.postfilter_annots.keys()) > 0:
            qs = self.apply_annotations(qs, self.postfilter_annots)

        if self.ordered:
            qs = qs.order_by(self.sort_col.sorter.order_by)

        qs = qs.distinct()

        return qs

    def get_prefetches(self):
        """Generate a list of strings that can be provided to Django's .prefetch_related() method, to speed up database
        interactions by reducing the number of queries necessary.

        Args:
            None
        Exceptions:
            None
        Returns:
            prefetches (List[str]): A list of model paths.  A model path is a dunderscore-delimited series of foreign
                keys that start from self.model.  Each model path is not contained in any other model path.
        """
        prefetches: List[str] = []
        # For all related_model_paths, by descending path length (i.e. number of dunderscore-delimited foreign keys)
        for model_path in sorted(
            [
                c.related_model_path
                for c in self.columns.values()
                if isinstance(c, BSTRelatedColumn)
            ],
            key=lambda p: len(p.split("__")),
            reverse=True,
        ):
            contained = False
            for prefetch in prefetches:
                if prefetch.startswith(model_path):
                    # Make sure that the match ends at either the end of the string or at a "__"
                    remainder = prefetch.replace(model_path, "", 1)
                    if remainder == "" or remainder.startswith("__"):
                        contained = True
                        break
            if not contained:
                prefetches.append(model_path)
            # # DEBUG: Test to see if using Prefetch() for many-related models speeds up ArchiveFileListView
            # if mdl is not None and column.many_related and len(mdl.split("__")) > 1:
            #     print(f"ADDING PREFETCH {mdl} FROM COLUMN {column.name} AND CREATING LIST IN ATTR {column.mm_list}")
            #     prefetches.append(Prefetch(mdl, to_attr=column.mm_list))
            # elif mdl is not None:
            #     prefetches.append(mdl)

        return prefetches

    def get_filters(self) -> Q:
        """Returns a Q expression for every filtered and searchable column using self.filter_terms and self.search_term.

        NOTE: Annotation fields must be generated in order to apply the query is self.searchcol is a BSTAnnotColumn.

        Args:
            None
        Exceptions:
            None
        Returns:
            q_exp (Q): A Q expression that can be used in a django ORM filter
        """

        # Search and filter criteria will be stored in a Q expression
        q_exp = Q()

        # Add individual filters, if any are defined
        for colname, filter_term in self.filter_terms.items():
            if colname in self.columns.keys():
                q_exp &= self.columns[colname].filterer.filter(filter_term)
            else:
                msg = f"Column '{colname}' filter '{filter_term}' failed.  Column not found.  Resetting filter cookie."
                self.warnings.append(msg)
                if settings.DEBUG:
                    warn(msg, DeveloperWarning)
                self.reset_column_cookie(colname, self.filter_cookie_name)

        # Add a global search if one is defined
        if self.search_term is not None:
            q_exp &= self.search()

        return q_exp

    def search(self) -> Q:
        """Returns a Q expression for every searchable column using self.search_term.

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

    def get_annotations(self) -> Tuple[Dict[str, Combinable], Dict[str, Combinable]]:
        """Generate 2 dicts of annotations that can each be provided to Django's .annotate() method.  The dicts are for
        before and after the call to .filter(), separated due to performance issues related to certain annotations'
        interactions with the WHERE clause.  If an annotation is not used in a filter, it is entered into the
        after-dict.

        Args:
            None
        Exceptions:
            None
        Returns:
            annotations_before_filter (Dict[str, Combinable]): A dict of Combinable objects keyed on annotation names,
                to be applied before filtering.
            annotations_after_filter (Dict[str, Combinable]): A dict of Combinable objects keyed on annotation names, to
                be applied after filtering.
        """
        annotations_before_filter: Dict[str, Combinable] = {}
        annotations_after_filter: Dict[str, Combinable] = {}
        for column in self.columns.values():
            if isinstance(column, BSTAnnotColumn):
                if (
                    self.search_term is None
                    and column.name not in self.filter_terms.keys()
                ):
                    annotations_after_filter[column.name] = column.converter
                else:
                    annotations_before_filter[column.name] = column.converter

        return annotations_before_filter, annotations_after_filter

    def apply_annotations(
        self, qs: QuerySet, annotations: Dict[str, Combinable]
    ) -> QuerySet:
        """This method exists to be able to try and recover from an exception about a specific annotation.

        Agrs:
            qs (QuerySet)
            annotations (Dict[str, Combinable])
        Exceptions:
            None
        Returns:
            qs (QuerySet)
        """
        for annot_name, expression in annotations.items():
            try:
                qs = qs.annotate(**{annot_name: expression})
            except Exception as e:
                # Attempt to recover by filling in "ERROR".  This will raise an exception if the name is the problem
                qs = qs.annotate(**{annot_name: Value("ERROR")})
                # The fallback is to have the template render the database values in the default manner.  Searching will
                # disabled.  Sorting will be a string sort (which is not ideal, e.g. if the value is a datetime).
                self.columns[annot_name].searchable = False
                self.columns[annot_name].sortable = False
                msg = f"Annotation column '{annot_name}' has a problem: {type(e).__name__}: {e}"
                if settings.DEBUG:
                    warn(msg)
                self.warnings.append(msg)
        return qs

    def apply_filters(self, qs: QuerySet) -> QuerySet:
        """This method exists to be able to try and recover from an exception about a Q expression.

        Agrs:
            qs (QuerySet)
        Exceptions:
            None
        Returns:
            qs (QuerySet)
        """
        try:
            qs = qs.filter(self.filters)
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
        return qs
