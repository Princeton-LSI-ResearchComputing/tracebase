import traceback
from enum import Enum
from typing import Dict, List, Optional, Tuple
from warnings import warn

from django.conf import settings
from django.core.exceptions import FieldError
from django.db import ProgrammingError
from django.db.models import Model, Q, QuerySet, Value
from django.db.models.expressions import Combinable

from DataRepo.models.utilities import (
    field_path_to_model_path,
    is_many_related_to_parent,
    model_path_to_model,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.base import BSTBaseListView
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn
from DataRepo.views.models.bst.column.sorter.many_related_field import (
    BSTManyRelatedSorter,
)
from DataRepo.views.utils import reduceuntil


class QueryMode(Enum):
    """This defines the query modes used to populate the many-related field values of a page of results"""

    iterate = 1
    subquery = 2


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

    QueryModes = ["iterate", "subquery"]

    def __init__(self, *args, query_mode: QueryMode = QueryMode.iterate, **kwargs):
        self.query_mode = query_mode

        super().__init__(*args, **kwargs)

        self.total = 0
        self.raw_total = 0

        self.prefetches: List[str] = self.get_prefetches()

        self.filters = self.get_filters()

        # Update the many-related sort settings in self.columns, based on self.groups, if the sort col is in a group.
        # NOTE: These are used for sorting delimited values the same in the many-related columns that are in a group
        # (i.e. from the same many-related model).
        # NOTE: This must be done BEFORE fetching the annotations, because if there is a sort column selected, the sort
        # will get an annotation (e.g. for lower-case).  It is not technically necessary for the regular sort, because
        # the only reason an annotation is necessary is to be able to supply it in a call to distinct, which is not done
        # for the row-sort, but it is for the delimited many-related columns' values sort (because the field path could
        # go through multiple many-related models).
        if self.ordered:
            # TODO: Set a default order-by
            for group in [
                g for g in self.groups.values() if self.sort_col.name in g.columns
            ]:
                group.set_sorters(self.sort_col.name, self.asc)
            else:
                self.sort_col.sorter = self.sort_col.create_sorter(asc=self.asc)

        (
            self.prefilter_annots,
            self.postfilter_annots,
        ) = self.get_annotations()

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

    def paginate_queryset(self, *args, **kwargs):
        """An extension of the superclass method intended to create attributes on the base model, each containing a list
        of many-related objects for its many-related column.

        Args:
            *args (Any): Superclass positional arguments
            **kwargs (Any): Superclass keyword arguments
        Exceptions:
            None
        Returns:
            paginator (Paginator)
            page (Page)
            object_list (QuerySet)
            is_paginated (bool)
        """
        paginator, page, object_list, is_paginated = super().paginate_queryset(
            *args, **kwargs
        )

        # If there are any many-related columns
        if any(isinstance(c, BSTManyRelatedColumn) for c in self.columns.values()):

            # For each record on this page, compile all of the many-related records and save them in an attribute off
            # the root model.
            # NOTE: Each iteration is at least 1 db query.  A subsequent query is issues for many-related prefetches
            for rec in object_list:

                # For each column object (order doesn't matter)
                for column in self.columns.values():

                    # If this is a many-related column
                    if isinstance(column, BSTManyRelatedColumn):

                        if self.query_mode == QueryMode.subquery:
                            subrecs = self.get_many_related_column_val_by_subquery(
                                rec, column
                            )
                        elif self.query_mode == QueryMode.iterate:
                            subrecs = self.get_column_val_by_iteration(rec, column)
                        else:
                            raise NotImplementedError(
                                f"QueryMode {self.query_mode} not implemented."
                            )

                        self.set_many_related_records_list(rec, column, subrecs)

        return paginator, page, object_list, is_paginated

    def set_many_related_records_list(
        self, rec: Model, column: BSTManyRelatedColumn, subrecs: List[Model]
    ):
        """Adds a list of many-related records as an attribute of the root model record.  Also truncates the list down
        to the size indicated in the column's settings (it's limit attribute) and "appends" an element with an ellipsis
        (if there are more records not shown).

        NOTE: The count of the total many-related records is already added to the records of the queryset, because it is
        added by get_user_queryset() as an annotation.

        Args:
            rec (Model): A record from self.model.
            column (BSTManyRelatedColumn): A column object that describes the many-related metadata.
            subrecs (List[Model]): A list of Model field values (or Model objects) from a model that is many-related
                with self.model.
        Exceptions:
            ProgrammingError when there is an attribute name collision
        Returns:
            None
        """
        if len(subrecs) >= (column.limit + 1):
            n = column.limit + 1
            limited_subrecs = subrecs[0:n]
            if hasattr(rec, column.count_attr_name):
                count = getattr(rec, column.count_attr_name)
                limited_subrecs[-1] = column.more_msg.format(count - column.limit)
            else:
                # The derived class must've eliminated the {colname}_mm_count column, so we cannot tell them how many
                # there are left to display.
                limited_subrecs[-1] = column.more_unknown_msg
        else:
            limited_subrecs = subrecs

        if hasattr(rec, column.list_attr_name):
            raise ProgrammingError(
                f"Attribute '{column.list_attr_name}' already exists on '{self.model.__name__}' object."
            )

        setattr(rec, column.list_attr_name, limited_subrecs)

    def get_paginate_by(self, qs: QuerySet):
        """An override of the superclass method to allow the user to change the rows per page.

        NOTE: self.limit was already set in the constructor based on both the URL param and cookie, but if it is 0
        (meaning "all"), we are going to update it based on the queryset.

        Assumptions:
            1. qs was obtained from get_queryset (or get_user_queryset).  This is for efficiency - to not issue a
               count() query.  In fact, I'm not sure why this method requires a queryset input.
        Args:
            qs (QuerySet)
        Exceptions:
            None
        Returns:
            self.limit (int): The number of table rows per page.
        """

        # Setting the limit to 0 means "all", but returning 0 here would mean we wouldn't get a page object sent to the
        # template, so we set it to the number of results.  The template will turn that back into 0 so that we're not
        # adding an odd value to the rows per page select list and instead selecting "all".
        if (
            self.limit == 0
            or (self.total > 0 and self.limit > self.total)
            or (self.total == 0 and self.limit > qs.count())
        ):
            if self.total > 0:
                # This avoids the count query, if self.total is already set
                self.limit = self.total
            else:
                self.limit = qs.count()

        return self.limit

    def get_prefetches(self, along_path: Optional[str] = None):
        """Generate a list of strings that can be provided to Django's .prefetch_related() method, to speed up database
        interactions by reducing the number of queries necessary.

        Args:
            along_path (Optional[str]): Only include paths in or after this path.
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
            if not contained and (
                along_path is None
                or model_path.startswith(along_path)
                or along_path.startswith(model_path)
            ):
                prefetches.append(model_path)

            # # TODO: Test to see if using Prefetch() for many-related models speeds up ArchiveFileListView
            # if mdl is not None and column.many_related and len(mdl.split("__")) > 1:
            #     prefetches.append(Prefetch(mdl, to_attr=column.mm_list))
            # elif mdl is not None:
            #     prefetches.append(mdl)

        return prefetches

    def get_filters(self) -> Q:
        """Returns a Q expression for every filtered and searchable column using self.filter_terms and self.search_term.

        NOTE: Annotation fields must be generated in order to apply the query if self.searchcol is a BSTAnnotColumn.

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

        NOTE: Annotation fields must be generated in order to apply the query if self.searchcol is a BSTAnnotColumn.

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
                # TODO: Consider making it possible to use icontains when the input method is select (for search field)
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
            if column.name == self.sort_col.name:
                annotations_before_filter[column.sorter.annot_name] = (
                    column.sorter.expression
                )
            # NOTE: The many-related sorter annot_name and many_expression are not added here.  They are added in a
            # subquery.

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

    def get_column_val_by_iteration(self, rec: Model, col: BSTBaseColumn):
        """Given a model record, i.e. row-data, e.g. from a queryset, and a column, return the column value(s).

        This method exists primarily to convert the column into arguments to the get_field_val_by_iteration
        method.  get_field_val_by_iteration returns either a single value or a list of values.

        Args:
            rec (Model)
            col (BSTBaseColumn)
        Exceptions:
            ProgrammingError when the value received is not a list of tuples or a tuple, as expected based on the column
                type
        Returns:
            (Union[Any, List[Any]]): Column value or values (if many-related).
        """
        # Defaults (which do not matter if this is not a BSTManyRelatedColumn, but allow us to make a single call)
        limit = 5
        sort_field_path = None
        asc = True
        if isinstance(col, BSTManyRelatedColumn):
            # In order for us to be able to sort the results and limit them to the TOP col.limit items, we need to get
            # all of them.  All records should have been prefetched, so even though we are getting all, it should
            # proceed expeditiously.  If the count annotation is absent from the rec, we go with the limit, even though
            # that could mean we don't get fully ordered results.

            # TODO: The above is apparently not true.  The mr_qs.all() in _recursive_many_rec_iterator() does perform a
            # query, so it would likely be faster to use an order_by in _recursive_many_rec_iterator and
            # _last_many_rec_iterator and set the related_limit here to 1 + the limit in the column object, instead of
            # to getattr(rec, col.count_attr_name).
            # Besides, it looks like it sorts anyway, given the model's ordering, based on the SQL output of
            # test__last_many_rec_iterator

            # We add 1 to col.limit so that we can display an ellipsis if more exist
            limit = getattr(rec, col.count_attr_name, -1)
            if limit == -1:
                limit = col.limit + 1
                if settings.DEBUG:
                    warn(
                        f"The count annotation for column {col} is absent.  "
                        f"Cannot guarantee the top {col.limit} records will include the the min/max sorted records.",
                        DeveloperWarning,
                    )
            if isinstance(col.sorter, BSTManyRelatedSorter):
                sort_field_path = col.sorter.field_path.split("__")
            else:
                raise ProgrammingError(
                    "BSTManyRelatedColumn encountered without a BSTManyRelatedSorter"
                )
            asc = col.asc
        elif isinstance(col, BSTAnnotColumn):
            return getattr(rec, col.name)

        # This method handles both singly-related and many-related column values and returns either a tuple (singly-
        # related) or a list of tuples (many-related)
        val = self.get_field_val_by_iteration(
            rec,
            col.name.split("__"),
            related_limit=limit,
            sort_field_path=sort_field_path,
        )

        # Many-related columns should return lists
        if isinstance(val, list) and all(
            isinstance(v, tuple) and len(v) == 3 for v in val
        ):
            return [
                # Returning the first value of the tuple - converting empty strings to None
                tpl[0] if not isinstance(tpl[0], str) or tpl[0] != "" else None
                # Sort based on the the sort value in the tuple (the second value at index 1)
                for tpl in sorted(val, key=lambda t: t[1], reverse=not asc)
            ]

        if isinstance(col, BSTManyRelatedColumn):
            raise ProgrammingError(
                f"List of 3-member tuples not returned for column '{col}'.  Got a '{type(val).__name__}': {val}."
            )
        elif not isinstance(val, tuple) or len(val) != 3:
            raise ProgrammingError(
                f"3-member tuple not returned for column '{col}'.  Got a '{type(val).__name__}': {val}."
            )

        # Convert empty strings in the tuple's return value to None
        return val[0] if not isinstance(val[0], str) or val[0] != "" else None

    def get_many_related_column_val_by_subquery(
        self, rec: Model, col: BSTManyRelatedColumn
    ) -> list:
        """Method to improve performance by grabbing the annotated value first and if it is not None, does an exhaustive
        search for all many-related values.

        Args:
            rec (Model)
            col (BSTColumn)
        Exceptions:
            ProgrammingError when the supplied column's sorter is not many-related.  This is mainly to satisfy mypy.
            TypeError when col is not a BSTManyRelatedColumn.
        Returns:
            (list): Sorted unique (to the many-related model) values.
        """
        if not isinstance(col, BSTManyRelatedColumn):
            raise TypeError(f"Column '{col}' is not many-related.")

        count = getattr(rec, col.count_attr_name, None)
        # It's faster to skip if we already know there are no records
        if count is not None and count == 0:
            return []

        if not isinstance(col.sorter, BSTManyRelatedSorter):
            raise ProgrammingError(
                f"Column '{col}' sorter must be a BSTManyRelatedSorter, not '{type(col.sorter).__name__}'."
            )

        # Set a limit to the retrieved records (count or supplied limit: whichever is lesser and not 0 [i.e. all])
        related_limit = col.limit + 1 if col.limit > 0 else 0
        if isinstance(count, int) and (related_limit == 0 or count < related_limit):
            related_limit = count

        annotations = col.sorter.get_many_annotations()
        order_bys = col.sorter.get_many_order_bys()
        distincts = col.sorter.get_many_distinct_fields()

        return self.get_many_related_field_val_by_subquery(
            rec,
            col.field_path,
            related_limit=related_limit,
            annotations=annotations,
            order_bys=order_bys,
            distincts=distincts,
        )

    def get_field_val_by_iteration(
        self,
        rec: Model,
        field_path: List[str],
        related_limit: int = 5,
        sort_field_path: Optional[List[str]] = None,
        _sort_val: Optional[List[str]] = None,
    ):
        """Private recursive method that takes a record and a path and traverses the record along the path to return
        whatever value is at the end of the path.  If it traverses through a many-related model, it returns a list of
        values.

        NOTE: If the end of the field_path is a foreign key itself, the value (or values) returned is not the key value
        (i.e. not an integer), but a model object (or objects).

        NOTE: The recursive calls are made via the supporting methods:
        - _get_field_val_by_iteration_onerelated_helper - Handles passing through singly-related foreign keys along the
          field_path
        - _get_field_val_by_iteration_manyrelated_helper - Handles passing through many-related foreign keys along the
          field_path

        The way this works is, _get_field_val_by_iteration_manyrelated_helper is called at any point along the
        field_path (possibly multiple points), where the foreign key being passed through is many-related.  Anytime a
        foreign key (of the end-value) along the field path is 1-related to its parent, it calls
        _get_field_val_by_iteration_onerelated_helper.  _get_field_val_by_iteration_onerelated_helper returns a 3-member
        tuple.  As those values are being passed back through the call stack, when they pass through the many-related
        step, those tuples are collected into a list of tuples.  The end result will either be a tuple (if there are no
        many-related relations along the path), or a list of tuples.  Each tuple is the value itself, a sort value, and
        a primary key.  In the case of there being no many-related component in the field_path, the second 2 values in
        the tuple are meaningless.

        Assumptions:
            1. The sort_field_path value will be a field under the associated column's related_model_path
        Args:
            rec (Model): A Model object.
            field_path (List[str]): A path from the rec object to a field/column value, that has been split by
                dunderscores.
            related_limit (int) [5]: Truncate/stop at this many (many-related) records.
            sort_field_path (Optional[List[str]]): A path from the rec object to a sort field, that has been split by
                dunderscores.  Only relevant if you know the field path to traverse through a many-related model.
            _sort_val (Optional[List[str]]): Do not supply.  This holds the sort value if the field path is longer than
                the sort field path.
            NOTE: We don't need to know if sorting is forward or reverse.  We are only returning tuples containing the
            sort value.  The sort must be done later, by the caller.
        Exceptions:
            ValueError when the sort field returns more than 1 value.
        Returns:
            (Union[List[Tuple[Any, Any, Any]]], Tuple[Any, Any, Any]): A list of 3-membered tuples or a 3-membered
                tuple.  Each tuple contains the value, a sort value, and a unique value.
        """
        if len(field_path) == 0 or rec is None:
            return None

        if is_many_related_to_parent(field_path[0], type(rec)):
            # This method handles only fields that are many-related to their immediate parent
            return self._get_field_val_by_iteration_manyrelated_helper(
                rec,
                field_path,
                related_limit=related_limit,
                sort_field_path=sort_field_path,
                _sort_val=_sort_val,
            )

        # This method handles only fields that are singly related to their immediate parent
        return self._get_field_val_by_iteration_onerelated_helper(
            rec,
            field_path,
            related_limit=related_limit,
            sort_field_path=sort_field_path,
            _sort_val=_sort_val,
        )

    def _get_field_val_by_iteration_onerelated_helper(
        self,
        rec: Model,
        field_path: List[str],
        related_limit: int = 5,
        sort_field_path: Optional[List[str]] = None,
        _sort_val: Optional[List[str]] = None,
    ):
        """Private recursive method that takes a field_path and a record (that is 1:1 related with the first element in
        the remaining field_path) and traverses the record along the path to return whatever ORM object's field value is
        at the end of the path.

        NOTE: Recursive calls go to get_field_val_by_iteration, which calls this method or the companion method
        (_get_field_val_by_iteration_manyrelated_helper) for many-related portions of the field_path.

        Assumptions:
            1. The related_sort_fld value will be a field under the related_model_path
        Args:
            rec (Model): A Model object.
            field_path (List[str]): A path from the rec object to a field/column value, that has been split by
                dunderscores.
            related_limit (int) [5]: Truncate/stop at this many (many-related) records.
            sort_field_path (Optional[List[str]]): A path from the rec object to a sort field, that has been split by
                dunderscores.  Only relevant if you know the field path to traverse through a many-related model.
            _sort_val (Optional[List[str]]): Do not supply.  This holds the sort value if the field path is longer than
                the sort field path.
        Exceptions:
            ValueError when the sort field returns more than 1 value.
        Returns:
            (Tuple[Any, Any, Any]): A tuple containing the value, a sort value, and a unique value.
        """
        if is_many_related_to_parent(field_path[0], type(rec)):
            raise TypeError(
                "_get_field_val_by_iteration_onerelated_helper called with a many-related field"
            )

        val_or_rec = getattr(rec, field_path[0])

        next_sort_field_path = (
            sort_field_path[1:] if sort_field_path is not None else None
        )
        # If we're at the end of the field path, we need to issue a separate recursive call to get the sort value
        if (
            sort_field_path is not None
            and len(sort_field_path) > 0
            and _sort_val is None
            and (
                sort_field_path[0] != field_path[0]
                or len(sort_field_path) == 1
                or len(field_path) == 1
            )
        ):
            # NOTE: Limiting to 2, because we only expect 1 and will raise ProgrammingError if multiple returned
            sort_val, _, _ = self.get_field_val_by_iteration(
                rec, sort_field_path, related_limit=2
            )
            if isinstance(sort_val, list):
                raise ProgrammingError(
                    "The sort value must not be many-related with the value for the column"
                )
            next_sort_field_path = None
            _sort_val = sort_val

        if len(field_path) == 1:
            uniq_val = val_or_rec
            if isinstance(val_or_rec, Model):
                uniq_val = val_or_rec.pk
            # NOTE: Returning the value, a value to sort by, and a value that makes it unique per record (or field)
            return val_or_rec, _sort_val, uniq_val

        return self.get_field_val_by_iteration(
            val_or_rec,
            field_path[1:],
            related_limit=related_limit,
            sort_field_path=next_sort_field_path,
            _sort_val=_sort_val,
        )

    def _get_field_val_by_iteration_manyrelated_helper(
        self,
        rec: Model,
        field_path: List[str],
        related_limit: int = 5,
        sort_field_path: Optional[List[str]] = None,
        _sort_val: Optional[List[str]] = None,
    ):
        """Private recursive method that takes a field_path and a record (that is many:1_or_many related with the first
        element in the remaining field_path) and traverses the record along the path to return values found at the end
        of the field_path.

        NOTE: Recursive calls go to get_field_val_by_iteration, which calls this method or the companion method
        (_get_field_val_by_iteration_single_helper) for singly-related portions of the field_path.

        NOTE: The recursive calls to get_field_val_by_iteration come from the 2 supporting methods:
        - _last_many_rec_iterator
        - _recursive_many_rec_iterator

        Assumptions:
            1. The sort_field_path value starts with the field_path
        Args:
            rec (Model): A Model object.
            field_path (List[str]): A path from the rec object to a field/column value, that has been split by
                dunderscores.
            related_limit (int) [5]: Truncate/stop at this many (many-related) records.
            sort_field_path (Optional[List[str]]): A path from the rec object to a sort field, that has been split by
                dunderscores.  Only relevant if you know the field path to traverse through a many-related model.
            _sort_val (Optional[List[str]]): Do not supply.  This holds the sort value if the field path is longer than
                the sort field path.
        Exceptions:
            ValueError when the sort field returns more than 1 value.
        Returns:
            (List[Tuple[Any, Any, Any]]): A list of tuples, the size of which corresponds to the number of unique
                records.
        """
        if rec is None:
            return []

        if not is_many_related_to_parent([field_path[0]], type(rec)):
            raise TypeError(
                "_get_field_val_by_iteration_many_related_helper called without a many-related field"
            )

        mr_qs: QuerySet = getattr(rec, field_path[0])

        next_sort_field_path = (
            sort_field_path[1:] if sort_field_path is not None else []
        )
        # If the sort_field_path has diverged from the field_path, retrieve its value
        if sort_field_path is not None and sort_field_path[0] != field_path[0]:
            sort_val, _, _ = self.get_field_val_by_iteration(
                rec,
                sort_field_path,
                # We only expect 1 value and are going to assume that the sort field was properly checked/generated to
                # not go through another many-related relationship.  Still, we specify the limit to be safe.
                related_limit=1,
            )
            if isinstance(sort_val, list):
                raise ProgrammingError(
                    "The sort value must not be many-related with the value for the column"
                )
            next_sort_field_path = []
            _sort_val = sort_val

        if len(field_path) == 1:

            uniq_vals = reduceuntil(
                lambda ulst, val: ulst + [val] if val not in ulst else ulst,
                lambda val: related_limit is not None and len(val) >= related_limit,
                self._last_many_rec_iterator(mr_qs, next_sort_field_path),
                [],
            )

            return uniq_vals

        uniq_vals = reduceuntil(
            lambda ulst, val: ulst + [val] if val not in ulst else ulst,
            lambda val: related_limit is not None and len(val) >= related_limit,
            self._recursive_many_rec_iterator(
                mr_qs,
                field_path[1:],
                next_sort_field_path,
                related_limit,
                _sort_val,
            ),
            [],
        )

        return uniq_vals

    def _last_many_rec_iterator(
        self,
        mr_qs: QuerySet,
        next_sort_field_path: List[str],
    ):
        """Private iterator to help _get_field_val_by_iteration_many_related_helper.  It iterates through the queryset,
        converting the many-related records to tuples of the record, the sort value, and the primary key.  This allows
        the caller to stop when it reaches its goal.  This is called when we're at the end of the field_path.  I.e. the
        end of the field_path is a foreign key to a many-related model.  It will make a recursive call if the
        sort_field_path is deeper than the field_path.

        NOTE: This lower-cases the sort value (if it is a str).

        Args:
            mr_qs: (QuerySet): A queryset of values that are many-related to self.model.
            next_sort_field_path (Optional[List[str]]): The next sort_field_path that can be supplied directly to
                recursive calls to get_field_val_by_iteration without slicing it.
        Exceptions:
            None
        Returns:
            (Tuple[Any, Any, Any]): The value, sort-value, and primary key of the many-related model
        """
        mr_rec: Model
        for mr_rec in mr_qs.all():
            yield (
                # Model object is the value returned
                mr_rec,
                # Each rec gets its own sort value.
                (
                    # TODO: See if this loop always causes a query.  If it does, then this iteration strategy may not be
                    # as efficient as I'd hoped and should be entirely removed, as should the _lower() method
                    self._lower(
                        self.get_field_val_by_iteration(mr_rec, next_sort_field_path)[0]
                    )
                    if len(next_sort_field_path) > 0
                    # Lower-case the string version of the many-related model object
                    else str(mr_rec).lower()
                ),
                # We don't need pk for uniqueness when including model objects, but callers expect it
                mr_rec.pk,
            )

    def _recursive_many_rec_iterator(
        self,
        mr_qs: QuerySet,
        next_field_path: List[str],
        next_sort_field_path: List[str],
        related_limit: int,
        _sort_val,
    ):
        """Private iterator to help _get_field_val_by_iteration_many_related_helper.  It iterates through the queryset,
        retrieving the values at the end of the path using recursive calls.  This allows the caller to stop when it
        reaches its goal.  This is called when a many-related model is encountered before we're at the end of the
        field_path.

        Args:
            mr_qs: (QuerySet): A queryset of values that are many-related to self.model.
            field_path (List[str])
            next_sort_field_path (List[str]): In order to simplify this method, instead of taking sort_field_path (which
                needs to be checked and converted to the next path, because it can diverge or be a different length from
                the field_path), that work must be done before calling this method.
            related_limit (int)
            _sort_val (Any)
        Exceptions:
            None
        Returns:
            (Tuple[Any, Any, Any]): The value, sort-value, and primary key of the many-related model
        """
        mr_rec: Model
        for mr_rec in mr_qs.all():
            val = self.get_field_val_by_iteration(
                mr_rec,
                next_field_path,
                related_limit=related_limit,
                sort_field_path=next_sort_field_path,
                _sort_val=_sort_val,
            )
            if isinstance(val, tuple):
                yield val
            else:
                # Must be a list
                for tpl in val:
                    yield tpl

    def get_many_related_field_val_by_subquery(
        self,
        rec: Model,
        field_path: str,
        related_limit: int = 5,
        annotations: dict = {},
        order_bys: list = [],
        distincts: list = [],
    ) -> list:
        """

        Args:
            rec (Model): A Model object whose Model corresponds to the start of the field_path.
            field_path (str): A dunderscore delimited field path whose starting field is a field of the rec's model.
            related_limit (int) [5]: Limit the size of the list returned to this many field values.
            annotations (dict) [{}]: Any annotations that need to be created.  Primarily, this is to support what is
                provided in the order_by list.
            order_bys (List[Union[str, OrderBy]]) [[]]: A list of OrderBy values (object or str).  This will be
                automatically appended to with the field_path and the primary key of the last many-related model foreign
                key in the field_path.
            distincts (List[str]) [[]]: A list of field paths.  Must match the fields contained in the order_bys list.
                This will be prepended with the field_path and appended with the primary key of the last many-related
                model foreign key in the field_path.
        Exceptions:
            None
        Returns:
            vals_list (list): A unique list of values from the many-related model at the end of the field path.
        """
        # This is used to ensure that the last many-related model is what is made distinct (not any other one-related
        # model that could be later in the field_path after the many-related model)
        many_related_model_path = field_path_to_model_path(
            rec.__class__, field_path, many_related=True
        )

        # If the field in the field_path is a foreign key (whether it is to the many-related model or another model that
        # is one-related with the many-related model), the primary key value returned is converted into model objects.
        related_model_path = field_path_to_model_path(rec.__class__, field_path)
        related_model = model_path_to_model(rec.__class__, related_model_path)
        is_fk = related_model_path == field_path

        # Append the actual field from the column (which may differ from the sort's field [c.i.p. as happens in column
        # groups])
        order_bys.append(field_path)
        # Append the many-related model's primary key in order to force a value for each distinct record
        order_bys.append(f"{many_related_model_path}__pk")

        # Prepend the actual field from the column (which may differ from the sort).  We put it first so we can easily
        # extract it in the values_list.  This is necessary because Django's combination of annotate, distinct, and
        # values_list have some quirks that prevent an intuitive usage of just flattening with the one value you want.
        # I tried many different versions of this before figuring this out.
        distincts.insert(0, field_path)
        # Append the many-related model's primary key in order to force a value for each distinct record
        distincts.append(f"{many_related_model_path}__pk")

        # We re-perform (essentially) the same query that generated the table, but for one root-table record, and with
        # all of the many-related values joined in to "split" the row, but we're only going to keep those many-related
        # values.  We can/will get repeated values if the field is not unique in the many-related model, but they each
        # will represent a unique many-related model record.  Furthermore, if the column is in a column group, the
        # order-bys will by based on the same field expression, because BSTColumnGroup modifies col.sorter.
        qs = (
            rec.__class__.objects
            # Filter for the current root model record
            .filter(pk=rec.pk)
            # The annotation is for use in the order_by, since it needs to be supplied to .distinct()
            .annotate(**annotations)
            # The order_by should be the annotation (e.g. the lower-cased version of the sort field) and 2 fields
            # necessary to be able to supply to distinct and the values_list: the primary key of the many-related model
            # (distinct) and the field_path of the field we want for the column (values_list)
            .order_by(*order_bys)
            # NOTE: This makes it distinct per the target many-related model record, even if there exist multiple many-
            # related models in the field path.
            .distinct(*distincts)
        )

        vals_list = [
            # Return an object, like an actual queryset does, if val is a foreign key field
            related_model.objects.get(pk=val) if is_fk else val
            for val in list(v[0] for v in qs.values_list(*distincts)[0:related_limit])
        ]

        return vals_list

    # TODO: This should be removed and the BSTManyRelatedColumn class should have a python sort method to take its place
    @classmethod
    def _lower(cls, val):
        """Intended for use in list comprehensions to lower-case the sort value, IF IT IS A STRING.
        Otherwise it returns the unmodified value."""
        if isinstance(val, str):
            return val.lower()
        return val
