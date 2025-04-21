import traceback
from functools import reduce
from typing import Any, Dict, List, Optional, Tuple
from warnings import warn

from django.conf import settings
from django.core.exceptions import FieldError
from django.db import ProgrammingError
from django.db.models import Model, Q, QuerySet, Value
from django.db.models.expressions import Combinable

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

# TODO: After performance testing, remove this global variable and any methods deemed to be inferior
QUERY_MODE = True


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

    def paginate_queryset(self, *args, **kwargs):
        """An extension of the superclass method intended to create attributes on the base model containing a list of
        related objects.

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
        for rec in object_list:
            for column in self.columns.values():
                if isinstance(column, BSTManyRelatedColumn):
                    if QUERY_MODE:
                        subrecs = self.get_many_related_rec_val_by_subquery(rec, column)
                    else:
                        subrecs = self.get_rec_val_by_iteration(rec, column)
                    self.set_many_related_records_list(rec, column, subrecs)
        return paginator, page, object_list, is_paginated

    def set_many_related_records_list(
        self, rec: Model, column: BSTManyRelatedColumn, subrecs: List[Model]
    ):
        """Adds the sub-list and metadata of many-related records as attributes off the root model record.  Also
        replaces the last list element with an ellipsis if there are more records not shown.

        Args:
            rec (Model): A record from self.model.
            column (BSTManyRelatedColumn): A column object that describes the many-related metadata.
            subrecs (List[Model]): A list of Model record fields or objects from a model that is many-related with
                self.model.
        Exceptions:
            None
        Returns:
            None
        """
        if len(subrecs) == (column.limit + 1):
            if hasattr(rec, column.count_attr_name):
                count = getattr(rec, column.count_attr_name)
                subrecs[-1] = column.more_msg.format(count)
            else:
                subrecs[-1] = "..."

        if hasattr(rec, column.list_attr_name):
            raise ProgrammingError(
                f"Attribute '{column.list_attr_name}' already exists on '{self.model.__name__}' object."
            )

        setattr(rec, column.list_attr_name, subrecs)

    def get_paginate_by(self, qs: QuerySet):
        """An override of the superclass method to allow the user to change the rows per page.

        self.limit was already set in the constructor based on both the URL param and cookie, but if it is 0 (meaning
        "all"), we are going to update it based on the queryset.

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
        if self.limit == 0 or self.limit > qs.count():
            self.limit = qs.count()

        return self.limit

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

    def get_rec_val_by_iteration(
        self, rec: Model, col: BSTBaseColumn, related_limit: int = 5
    ):
        """Given a model record, i.e. row-data, e.g. from a queryset, and a column, return the column value.

        NOTE: While this supports many-related columns, it is more efficient to call get_many_related_rec_val directly.

        Args:
            rec (Model)
            col (BSTBaseColumn)
            related_limit (int) [5]: Truncate/stop at this many (many-related) records.
        Exceptions:
            None
        Returns:
            (Any): Column value or values (if many-related).
        """
        # _get_rec_val_by_iteration_helper returns the value, which can be any value from a field in the DB or a list of
        # values (if the field in question is many-related with self.model).  If it is many-related, the second return
        # value is the sort value (which we do not need) and the third values is a primary key intended to have been
        # used to return the correct number of unique values (also which we do not need).  The last 2 values of the
        # return are only needed in the method's recursion.
        val, _, _ = self._get_rec_val_by_iteration_helper(
            rec, col.name.split("__"), related_limit=related_limit
        )
        return val if not isinstance(val, str) or val != "" else None

    def get_many_related_rec_val_by_subquery(
        self, rec: Model, col: BSTManyRelatedColumn
    ) -> list:
        """Method to improve performance by grabbing the annotated value first and if it is not None, does an exhaustive
        search for all many-related values.

        Args:
            rec (Model)
            col (BSTColumn)
        Exceptions:
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

        # Set a limit to the retrieved records (count or supplied limit: whichever is lesser and not 0 [i.e. all])
        related_limit = col.limit + 1 if col.limit > 0 else 0
        if isinstance(count, int) and (related_limit == 0 or count < related_limit):
            related_limit = count

        return self._get_many_related_rec_val_by_subquery_helper(
            rec,
            col,
            related_limit=related_limit,
        )

    def _get_rec_val_by_iteration_helper(
        self,
        rec: Model,
        field_path: List[str],
        related_limit: int = 5,
        sort_field_path: Optional[List[str]] = None,
        _sort_val: Optional[List[str]] = None,
    ):
        """Private recursive method that takes a record and a path and traverses the record along the path to return
        whatever ORM object's field value is at the end of the path.  If it traverses through a many-related model, it
        returns a list of such objects or None if empty.

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
            (Optional[Union[List[Any], Any]]): A list if passing through a populated many-related model or a field
                value.
        """
        if len(field_path) == 0 or rec is None:
            return None, None, None
        elif (
            type(rec).__name__ != "RelatedManager"
            and type(rec).__name__ != "ManyRelatedManager"
        ):
            return self._get_rec_val_by_iteration_single_helper(
                rec,
                field_path,
                related_limit,
                sort_field_path,
                _sort_val,
            )
        else:
            return self._get_rec_val_by_iteration_many_helper(
                rec,
                field_path,
                related_limit,
                sort_field_path,
                _sort_val,
            )

    def _get_rec_val_by_iteration_single_helper(
        self,
        rec: Model,
        field_path: List[str],
        related_limit: int = 5,
        sort_field_path: Optional[List[str]] = None,
        _sort_val: Optional[List[str]] = None,
    ):
        """Private recursive method that takes a record and a path and traverses the record along the path to return
        whatever ORM object's field value is at the end of the path.  If it traverses through a many-related model, it
        returns a list of such objects or None if empty.

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
            (Optional[Union[List[Any], Any]]): A list if passing through a populated many-related model or a field
                value.
        """
        if (
            type(rec).__name__ != "RelatedManager"
            and type(rec).__name__ != "ManyRelatedManager"
        ):
            val_or_rec = getattr(rec, field_path[0])
        else:
            raise TypeError(
                "_get_rec_val_by_iteration_single_helper called with a related manager"
            )

        if len(field_path) == 1:
            uniq_val = val_or_rec
            if isinstance(val_or_rec, Model):
                uniq_val = val_or_rec.pk
            return val_or_rec, _sort_val, uniq_val

        next_sort_field_path = (
            sort_field_path[1:] if sort_field_path is not None else None
        )
        # If we're at the end of the field path, we need to issue a separate recursive call to get the sort value
        if sort_field_path is not None and (
            sort_field_path[0] != field_path[0]
            or (len(sort_field_path) == len(field_path) and len(field_path) == 1)
        ):
            sort_val, _, _ = self._get_rec_val_by_iteration_helper(
                rec, sort_field_path, related_limit=2
            )
            if isinstance(sort_val, list):
                uniq_vals: List[Any] = reduce(
                    lambda lst, val: lst + [val] if val not in lst else lst,
                    sort_val,
                    [],
                )
                if len(uniq_vals) > 1:
                    raise ValueError("Multiple values returned")
                elif len(uniq_vals) == 1:
                    self._lower(uniq_vals[0])
                else:
                    sort_val = None
            next_sort_field_path = None
            _sort_val = sort_val

        return self._get_rec_val_by_iteration_helper(
            val_or_rec,
            field_path[1:],
            related_limit=related_limit,
            sort_field_path=next_sort_field_path,
            _sort_val=_sort_val,
        )

    def _get_rec_val_by_iteration_many_helper(
        self,
        rec: Model,
        field_path: List[str],
        related_limit: int = 5,
        sort_field_path: Optional[List[str]] = None,
        _sort_val: Optional[List[str]] = None,
    ):
        """Private recursive method that takes a record and a path and traverses the record along the path to return
        whatever ORM object's field value is at the end of the path.  If it traverses through a many-related model, it
        returns a list of such objects or None if empty.

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
            (Optional[Union[List[Any], Any]]): A list if passing through a populated many-related model or a field
                value.
        """
        if (
            type(rec).__name__ != "RelatedManager"
            and type(rec).__name__ != "ManyRelatedManager"
        ):
            raise TypeError(
                "_get_rec_val_by_iteration_many_related_helper called with a related manager"
            )

        if len(field_path) == 1:
            if rec is None or rec.count() == 0:
                return []

            uniq_vals = reduceuntil(
                lambda ulst, val: ulst + [val] if val not in ulst else ulst,
                lambda val: related_limit is not None and len(val) >= related_limit,
                self._last_many_rec_iterator(rec, sort_field_path),
                [],
            )

            return uniq_vals

        next_sort_field_path = (
            sort_field_path[1:] if sort_field_path is not None else None
        )
        # If the sort_field_path has diverged from the field_path, retrieve its value
        if sort_field_path is not None and sort_field_path[0] != field_path[0]:
            sort_val, _, _ = self._get_rec_val_by_iteration_helper(
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
                # uniq_vals = reduce(lambda lst, val: lst + [val] if val not in lst else lst, sort_val, [])
                # if len(uniq_vals) > 1:
                #     raise ValueError("Multiple values returned")
                # elif len(uniq_vals) == 1:
                #     sort_val = self.lower(uniq_vals[0])
                # else:
                #     sort_val = None
            next_sort_field_path = None
            _sort_val = sort_val

        if rec.exists() > 0:
            uniq_vals = reduceuntil(
                lambda ulst, val: ulst + [val] if val not in ulst else ulst,
                lambda val: related_limit is not None and len(val) >= related_limit,
                self._recursive_many_rec_iterator(
                    rec, field_path, next_sort_field_path, related_limit, _sort_val
                ),
                [],
            )
            return uniq_vals

        return []

    def _last_many_rec_iterator(
        self,
        mr_qs: QuerySet,
        sort_field_path,
    ):
        """Private method to help _get_rec_val_by_iteration_many_related_helper.  Allows it to stop when it reaches its
        goal.  This is called when we're at the end of the field_path.  It will make a recursive call if the
        sort_field_path is deeper than the field_path."""
        mr_rec: Model
        for mr_rec in mr_qs.all():
            yield (
                # Model object is the value returned
                mr_rec,
                # Each rec gets its own sort value.
                (
                    self._lower(
                        self._get_rec_val_by_iteration_helper(
                            mr_rec, sort_field_path[1:]
                        )[0]
                    )
                    if sort_field_path is not None and len(sort_field_path) > 1
                    else str(mr_rec).lower()
                ),
                # We don't need pk for uniqueness when including model objects, but callers expect it
                mr_rec.pk,
            )

    def _recursive_many_rec_iterator(
        self,
        mr_qs: QuerySet,
        field_path,
        next_sort_field_path,
        related_limit,
        _sort_val,
    ):
        """Private method to help _get_rec_val_by_iteration_many_related_helper.  Allows it to stop when it reaches its
        goal.  This is called when a many-related model is encountered before we're at the end of the field_path.
        """
        mr_rec: Model
        for mr_rec in mr_qs.all():
            for tpl in self._get_rec_val_by_iteration_helper(
                mr_rec,
                field_path[1:],
                related_limit=related_limit,
                sort_field_path=next_sort_field_path,
                _sort_val=_sort_val,
            ):
                yield tpl

    def _get_many_related_rec_val_by_subquery_helper(
        self,
        rec: Model,
        col: BSTManyRelatedColumn,
        related_limit: int = 5,
    ) -> list:
        """

        Args:
            rec (Model): A Model object.
            field (str): A path from the rec object to a field/column value, delimited by dunderscores.
            sort_field (str): A path from the rec object to a sort field, delimited by dunderscores.
            reverse (bool) [False]: Whether the many-related values should be reverse sorted.
        Exceptions:
            ProgrammingError when an exception occurs during the sorting of the value received from _get_rec_val_helper.
        Returns:
            vals_list (list): A unique list of values from the many-related model at the end of the field path.
        """

        if not isinstance(col.sorter, BSTManyRelatedSorter):
            raise ProgrammingError(
                "This conditional is here to assure mypy that col.sorter is indeed a BSTManyRelatedSorter."
            )

        qs = (
            rec.__class__.objects.filter(pk=rec.pk)
            .order_by(col.sorter.many_order_by, *col.distinct_fields)
            .distinct(*col.distinct_fields)
        )

        vals_list = [
            # Return an object, like an actual queryset does, if val is a foreign key field
            col.related_model.objects.get(pk=val) if col.is_fk else val
            for val in qs.values_list(col.field_path, flat=True)[0:related_limit]
            if val is not None
        ]

        return vals_list

    @classmethod
    def _lower(cls, val):
        """Intended for use in list comprehensions to lower-case the sort value, IF IT IS A STRING.
        Otherwise it returns the unmodified value."""
        if isinstance(val, str):
            return val.lower()
        return val
