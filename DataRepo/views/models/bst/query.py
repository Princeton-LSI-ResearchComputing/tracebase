import traceback
from enum import Enum
from typing import Dict, List, Optional, Tuple, Type
from urllib.parse import urlencode
from warnings import warn

from django.conf import settings
from django.core.exceptions import FieldError
from django.db import ProgrammingError
from django.db.models import Model, Q, QuerySet, Value
from django.db.models.expressions import Combinable
from django.shortcuts import redirect

from DataRepo.models.utilities import (
    field_path_to_manager_path,
    field_path_to_model_path,
    get_field_val_by_iteration,
    get_many_related_field_val_by_subquery,
    is_many_related_to_root,
)
from DataRepo.utils.exceptions import DeveloperWarning, trace
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn
from DataRepo.views.models.bst.column.sorter.base import BSTBaseSorter
from DataRepo.views.models.bst.column.sorter.many_related_field import (
    BSTManyRelatedSorter,
)
from DataRepo.views.models.bst.column_setup import (
    BSTBaseDetailView,
    BSTBaseListView,
)


# TODO: Figure out how to move this to .utils without a circular import error
class QueryMode(Enum):
    """This defines the query modes used to populate the many-related field values of a page of results"""

    iterate = 1
    subquery = 2


class BSTQueryView:
    """BSTBaseView is responsible for model queries.

    This class represents the common components of BSTDetailView and BSTListView queries.

    This base class uses the BSTBaseColumn objects configured in BSTBaseView and retrieves field values/annotations and
    performs the searches/sorts requested by the user.

    Classes derived from this class can configure the search/sort behavior and included annotations.

    Class Attributes:
        query_mode (QueryMode) [iterate] {iterate, subquery}: There are 2 query modes that can be selected based on
            performance.
            - *iterate* is the equivalent of traversing a dot notation field path, e.g. Sample.animal.treatment.name,
              but it adds the ability to also traverse through many-related relationships, e.g.
              Sample.animal.studies.name, where it returns a list of values (unique to the last many-related model
              traversed).  It uses the field paths in the BSTBaseColumn objects configured by the superclass.
            - *subquery* behaves the same as *iterate* except for its behavior with respect to many-related fields.  It
              returns a list of values the same way *iterate* does, but it retrieves them by including the many-related
              model's primary key in a distinct clause instead of traversing the field path.
    Instance Attributes:
        query_mode (QueryMode) [iterate] {iterate, subquery}: An override of the class attribute.
        prefetches (List[str]): A list of related model paths to prefetch.
        NOTE: Other class attributes are inherited, but re-declared for IDE functionality.
    """

    query_mode = QueryMode.iterate

    def __init__(self, query_mode: Optional[QueryMode] = None):
        # This assumes/requires that the self passed in has these instance attributes, which are (re-)declared here for
        # IDEs to know they exist in the instance methods
        self.columns: Dict[str, BSTBaseColumn]
        self.warnings: List[str]
        self.model: Type[Model]

        # Change the query mode based on the data-influenced performance
        if isinstance(query_mode, QueryMode):
            self.query_mode = query_mode
        else:
            self.query_mode = type(self).query_mode

        # We can get the prefetches and annotations right away, because none of it is based on cookies.
        self.prefetches: List[str] = self.get_prefetches()

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

        all_related_model_paths: List[str] = []
        for col in self.columns.values():
            if (
                isinstance(col, BSTRelatedColumn)
                and col.related_model_path not in all_related_model_paths
            ):
                all_related_model_paths.append(col.related_model_path)
            elif isinstance(col, BSTAnnotColumn) and len(col.related_model_paths) > 0:
                for rmp in col.related_model_paths:
                    if rmp not in all_related_model_paths:
                        all_related_model_paths.append(rmp)

        # For all related_model_paths, by descending path length (i.e. number of dunderscore-delimited foreign keys)
        for field_path in sorted(
            all_related_model_paths,
            key=lambda p: len(p.split("__")),
            reverse=True,
        ):
            # prefetch_related doesn't take field_paths.  It takes manager_paths (the manager objects that handle many-
            # related model relations).  The effective difference is just that if a foreign key field doesn't have a
            # related_name, the default related name is the name of the lower-cased related model name with "_set"
            # appended.  The field_path_to_manager_path function does this for us.
            model_path = field_path_to_manager_path(self.model, field_path)
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
                or field_path.startswith(along_path)
                or along_path.startswith(field_path)
            ):
                prefetches.append(model_path)

            # # TODO: Test to see if using Prefetch() for many-related models speeds up ArchiveFileListView
            # if mdl is not None and column.many_related and len(mdl.split("__")) > 1:
            #     prefetches.append(Prefetch(mdl, to_attr=column.mm_list))
            # elif mdl is not None:
            #     prefetches.append(mdl)

        return prefetches

    @classmethod
    def get_column_val_by_iteration(cls, rec: Model, col: BSTBaseColumn):
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

            if hasattr(rec, col.count_annot_name):
                # If a count annotation column (counting records/values in this column) was included in the table, set
                # the limit to it.  See the comments above for an explanation.
                limit = getattr(rec, col.count_annot_name, -1)
            if (not isinstance(limit, int) or limit == -1) and hasattr(
                rec, col.count_attr_name
            ):
                # If the count annotation column was excluded, the count should be stored in an attribute named as in
                # the variable: col.count_attr_name
                limit = getattr(rec, col.count_attr_name, -1)

            if not isinstance(limit, int):
                if isinstance(limit, str) and limit == "ERROR":
                    limit = -1
                else:
                    raise ValueError(
                        f"The count annotation for column {col} returned a {type(limit).__name__} instead of the "
                        f"expected int, with the value '{limit}'."
                    )

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
                    f"BSTManyRelatedColumn '{col.name}' does not have a BSTManyRelatedSorter"
                )
            asc = col.asc
        elif isinstance(col, BSTAnnotColumn):
            return getattr(rec, col.name)

        # This method handles both singly-related and many-related column values and returns either a tuple (singly-
        # related) or a list of tuples (many-related)
        return get_field_val_by_iteration(
            rec,
            col.name.split("__"),
            related_limit=limit,
            sort_field_path=sort_field_path,
            asc=asc,
            # If a many-related column is not in a column group (i.e. there are no other columns that pass through the
            # same many-related model), then make the displayed values unique.  Otherwise, show a value for every unique
            # many-related model record.
            value_unique=(
                not col._in_group
                or (isinstance(col, BSTManyRelatedColumn) and col.unique)
            ),
        )

    @classmethod
    def get_many_related_column_val_by_subquery(
        cls, rec: Model, col: BSTManyRelatedColumn
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

        count_annot_name = BSTManyRelatedColumn.get_count_name(
            col.many_related_model_path, col.model
        )

        count = getattr(rec, count_annot_name, None)
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

        return get_many_related_field_val_by_subquery(
            rec,
            col.field_path,
            related_limit=related_limit,
            annotations=annotations,
            order_bys=order_bys,
            distincts=distincts,
            # If a many-related column is not in a column group (i.e. there are no other columns that pass through the
            # same many-related model), then make the displayed values unique.  Otherwise, show a value for every unique
            # many-related model record.
            value_unique=not col._in_group or col.unique,
        )

    def apply_annotations(
        self,
        qs: QuerySet,
        annotations: Dict[str, Combinable],
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
                # Attempt to recover by filling in "ERROR".  This will raise an exception if the name is the problem.
                qs = qs.annotate(**{annot_name: Value("ERROR")})

                # Account for the fact that a sort annotation does not exist as a column object, but it does reference a
                # real column.  We will use the annotation name to detect if this is a sort annotation and if so, derive
                # the column name.
                if BSTBaseSorter.is_sort_annotation(annot_name):
                    colname = BSTBaseSorter.sort_annot_name_to_col_name(annot_name)
                else:
                    colname = annot_name

                # Disable searching, filtering, and sorting.
                self.columns[colname].searchable = False
                self.columns[colname].filterable = False
                self.columns[colname].sortable = False

                if colname == annot_name:
                    msg = f"The expression '{expression}' for the annotation column '{colname}' "
                else:
                    msg = f"The sort expression '{expression}' for column '{colname}' "
                msg += f"has a problem: {type(e).__name__}: {e}"

                if settings.DEBUG:
                    warn(trace(e) + msg, DeveloperWarning)
                self.warnings.append(msg)
        return qs


class BSTDetailView(BSTBaseDetailView, BSTQueryView):
    """Generic class-based view for a Model record.  This class is responsible mainly for adding annotatiomns to a
    record, but also for querying many-related fields.

    Instance Attributes:
        annots (Dict[str, Combinable])
    """

    def __init__(self, *args, **kwargs):
        BSTBaseDetailView.__init__(self, *args, **kwargs)
        BSTQueryView.__init__(self)

        # We can get the annotations right away, because none of it is based on cookies.
        self.annots: Dict[str, Combinable] = self.get_annotations()

    def get_object(self, **kwargs):
        object: Model = super().get_object(**kwargs)

        # We need to create a queryset to be able to add annotations
        qs: QuerySet = self.model.objects.filter(pk=object.pk)

        if len(self.prefetches) > 0:
            qs = qs.prefetch_related(*self.prefetches)

        qs = self.apply_annotations(qs, self.annots)

        if qs.count() != 1:
            raise ProgrammingError(
                f"The annotations of '{self.model.__name__}' record '{object}' resulted in multiple ({qs.count()}) "
                "objects.  This occurs when an annotation uses a many-related field without an aggregation function.  "
                f"The annotations are: {self.annots}.  Please find the annotation returning multiple values and apply "
                "an aggregate function."
            )

        object = qs.get()

        # If there are any many-related or annotated columns
        if any(
            isinstance(c, (BSTManyRelatedColumn, BSTAnnotColumn))
            for c in self.columns.values()
        ):

            # For each column object (order doesn't matter)
            for column in self.columns.values():

                # If this is a many-related column
                if isinstance(column, BSTManyRelatedColumn):

                    if self.query_mode == QueryMode.subquery:
                        subrecs = self.get_many_related_column_val_by_subquery(
                            object, column
                        )
                    elif self.query_mode == QueryMode.iterate:
                        subrecs = self.get_column_val_by_iteration(object, column)
                    else:
                        raise NotImplementedError(
                            f"QueryMode {self.query_mode} not implemented."
                        )

                    column.set_list_attr(object, subrecs)

                elif isinstance(column, BSTAnnotColumn) and column.is_fk:

                    # See if there are any foreign key annotations
                    model_obj: Model = column.get_model_object(
                        getattr(object, column.name)
                    )

                    if model_obj is not None:
                        setattr(object, column.name, model_obj)

        return object

    def get_annotations(self) -> Dict[str, Combinable]:
        """An override of the superclass method."""
        annotations: Dict[str, Combinable] = {}
        for column in self.columns.values():
            if isinstance(column, BSTAnnotColumn):
                annotations[column.name] = column.converter
        return annotations


class BSTListView(BSTBaseListView, BSTQueryView):
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

        # Customize the fields without extending the constructor
        class SampleList(BSTBaseListView):
            model = Sample
            column_ordering = ["name", "tissue", "animal", "time_collected", "handler"]
            exclude = ["id", "msrun_samples"]
            column_settings = {
                # You only need to include columns and their options when you want something other than the default
                "handler": {
                    "header": "Researcher",
                    filterer: {"choices": get_researchers}  # BSTBaseFilterer.__init__'s choices arg takes a callable
                }
            }

    ## Subsetting the list

    Any field path (whether it's one of the column's field paths or not) can be supplied as a URL parameter and value.
    The paths must be *from* self.model to a related model field.  Supplying a search field path and value will result
    in the display of a subset of self.model records.  Any supplied "search path" can be used (in combination) to link
    to this list view.  The supplied value must be an exact search term (case sensitive) for the related field.  The
    queryset will be limited to JUST records linked with that related model record/field and the title will be changed
    to specify the search fields and values.
    """

    def __init__(self, *args, query_mode=None, **kwargs):
        BSTBaseListView.__init__(self, *args, **kwargs)
        BSTQueryView.__init__(self, query_mode=query_mode)

        # The filters and annotations are initially empty.  Will be set in get()
        self.filters = Q()
        self.presubset_annots: Dict[str, Combinable] = {}
        self.prefilter_annots: Dict[str, Combinable] = {}
        self.postfilter_annots: Dict[str, Combinable] = {}

    def get(self, request, *args, **kwargs):
        """Extends BSTBaseListView.get, and is used here to set the filters, sorts, and annotations based on the cookies
        obtained from the request object.

        Args:
            request (HttpRequest): This is a superclass arg we need to initialize the cookies before passing on to
                super().get().
        Exceptions:
            None
        Returns:
            response (HttpResponse)
        """

        # Initialize the cookies for the interface.
        # NOTE: I had ideally wanted the BSTClientInterface to be entirely obscured from this class other than the
        # instance members it sets, but the call to super().get() triggers the call to get_queryset BEFORE the sort and
        # search criteria (including annotations needed for sorting), so I had to create a separate method to handle the
        # cookies and the query initialization.  This must be why Ken Whitesell in the Django forum said that grabbing
        # cookies was done all over the place instead of in one consolidated location.
        self.request = request
        self.init_subquery()

        if self.subquery_exists and self.subquery_ready is False:
            # When a subquery comes in, the URL has search fields and terms, but filter cookies have to be cleared when
            # the request first come in, so that the initial view is guaranteed to be all records from the subquery.
            # Subsequent user searches should be allowed however, and we indicate that filter cookies should not be
            # cleared on subsequent loads by appending "&subquery=true" to the URL parameters, which is accomplished by
            # the redirect below...
            base_url = self.request.get_full_path()
            subquery_param = urlencode({self.subquery_param_name: "true"})
            # There have to be URL parameters already since self.subquery_exists is True, so append with "&"
            subquery_url = f"{base_url}&{subquery_param}"
            return redirect(subquery_url)

        self.init_interface()

        # Now that the search criteria and other query elements are initialized from the cookies, trigger the query
        try:
            response = super().get(request, *args, **kwargs)
        except Exception as e:
            # Try to gracefully recover from a problematic cookie
            # Check to see if this is due to a cookie by clearing the cookies, reinitializing, and retrying the query
            bad_cookies = self.reset_all_cookies()
            # Reinitialize, but keep the clear_cookies state
            self.__init__(clear_cookies=True)
            self.init_interface()
            try:
                response = super().get(request, *args, **kwargs)
                warning = (
                    "A problem was encountered and your request could not be completed.\n"
                    f"The following cookies have been reset: {list(bad_cookies.keys())}.\n\n"
                    "Please report this error to the site administrators if it persists."
                )
                if settings.DEBUG:
                    warn(
                        f"{trace(e)}\n{warning}\nCookies: {bad_cookies}\nException: {type(e).__name__}: {e}",
                        DeveloperWarning,
                    )
                self.warnings.append(warning)
            except Exception:
                raise e

        return response

    def init_interface(self):
        """An extension of the BSTBastListView init_interface method, used here to set the filters, sort, and
        annotations.

        Args:
            None
        Exceptions:
            ProgrammingError the sort_col's representative field is invalid
        Returns:
            response (HttpResponse)
        """
        super().init_interface()

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

    def init_subquery(self):
        """Initializes the presubset_annots.  Some annotations' values can be affected by subquery search terms, so
        those annotations must be put in front of the subquery filter.

        Basically, this facillitates linking to subsets of data in this view.  Links can add field_paths and their
        search terms to the URL parameters in order to displat a subset of records in the list view.  When a subquery is
        active, at "sub title" below the page title appears showing the context of the subset.
        """
        # This initializes self.subquery_ready, self.subquery, and self.subtitles
        super().init_subquery()

        # Determine what annotations must occur before the subquery so as to leave their values unaltered (because many-
        # related model field filters can change aggregate annotation values)
        (self.presubset_annots, _) = self.get_annotations(self.subquery)

    def get_queryset(self):
        """An extension of the superclass method intended to only set the total instance attribute.  raw_total is set by
        the superclass (which also initializes total to raw_total)."""

        qs = super().get_queryset()

        try:
            # If the queryset is being sub-setted via URL parameters from a link, create a starting queryset that is
            # based on the subquery
            if self.subquery is not None:
                qs = self.get_sub_queryset(qs)
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

    def get_sub_queryset(self, qs: QuerySet):
        """This method subsets the queryset based on search terms in the self.subquery dict.

        Args:
            qs (QuerySet)
        Exceptions:
            None
        Returns:
            qs (QuerySet)
        """
        if self.subquery is None:
            return qs

        if len(self.prefetches) > 0:
            qs = qs.prefetch_related(*self.prefetches)

        if len(self.presubset_annots.keys()) > 0:
            qs = self.apply_annotations(qs, self.presubset_annots)

        qs = qs.filter(**self.subquery)

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
        if self.subquery is None and len(self.prefetches) > 0:
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

        # If there are any many-related or annotated columns
        if any(
            isinstance(c, (BSTManyRelatedColumn, BSTAnnotColumn))
            for c in self.columns.values()
        ):

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

                        column.set_list_attr(rec, subrecs)

                    elif isinstance(column, BSTAnnotColumn) and column.is_fk:

                        # See if there are any foreign key annotations
                        model_obj: Model = column.get_model_object(
                            getattr(rec, column.name)
                        )

                        if model_obj is not None:
                            setattr(rec, column.name, model_obj)

        return paginator, page, object_list, is_paginated

    # TODO: Figure out a way to move this to BSTQueryView without it having to know about the client interface elements
    # like cookeis and filter_terms.
    def get_filters(self) -> Q:
        """Returns a Q expression for every filtered and filterable column using self.filter_terms and self.search_term.

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
                q_exp &= self.columns[colname].filterer.create_q_exp(filter_term)
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

    # TODO: Figure out a way to move this to BSTQueryView without it having to know about the client interface elements
    # like cookeis and search_term.
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
                # TODO: Consider making it possible to use icontains when the input method is select (for a particular
                # search field)
                q_exp |= column.filterer.create_q_exp(self.search_term)

        return q_exp

    # TODO: Figure out a way to move this to BSTQueryView without it having to know about the client interface aspects
    # like pre- and post- filter annotations.
    def get_annotations(
        self, filter_dict: Optional[Dict[str, str]] = None
    ) -> Tuple[Dict[str, Combinable], Dict[str, Combinable]]:
        """An override of the superclass method.

        Generate 2 dicts of annotations that can each be provided to Django's .annotate() method.  The dicts are for
        before and after the call to .filter(), separated due to performance issues related to certain annotations'
        interactions with the WHERE clause.  If an annotation is not used in a filter, it is entered into the
        after-dict.

        Args:
            filter_dict (Optional[Dict[str, str]]) [self.filter_terms]: A dict of the active filters where the keys are
                the column names^ and the values are the filter/search terms.  This option exists to aid subsetting and
                linking to subsets of records.  You do not need to supply this option when the base queryset is not
                subsetted.
                ^The keys may optionally not be a column (when subsetting the list), but can still affect whether
                 annotations will be returned in the before or after filter dicts.
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

        if filter_dict is None:
            filter_dict = self.filter_terms

        # Obtain a list of many-related model paths that are being filtered, so that any annotation based on the same
        # many-related model can be put *before* the filter.  E.g. If there is a Count annotation for many-related
        # records, we want the count to be unaffected by the filter.
        filtered_mr_model_paths = []
        for cn in filter_dict.keys():
            if (
                cn in self.columns.keys()
                and isinstance(self.columns[cn], BSTManyRelatedColumn)
            ) or (
                # Subqueries can be based on fields that are not represented in a column, such as a "parent" model that
                # is linked to by multiple records in the current ListView.
                cn not in self.columns.keys()
                and is_many_related_to_root(cn, self.model)
            ):
                # Get up to the *first* many-related step in the field path so that we can put any affected many-related
                # annotations, such as count annotations *before* the filter, so as not to change the annotation value.
                # Note that the count annotations are used to limit many-related record/field value collection in order
                # to speed up the query, but if that count changes, it will alter the many-related columns' assortment
                # of values.
                mr_model_path = field_path_to_model_path(
                    self.model, cn, first_many_related=True
                )
                if mr_model_path not in filtered_mr_model_paths:
                    filtered_mr_model_paths.append(mr_model_path)

        for column in self.columns.values():
            # If this is an annotation column that wasn't already applied to the queryset via subsetting
            if (
                isinstance(column, BSTAnnotColumn)
                # Do not add annotations that were already added due to the subset
                and (
                    self.presubset_annots is None
                    or column.name not in self.presubset_annots.keys()
                )
            ):
                if (
                    self.search_term is None
                    and column.name not in filter_dict.keys()
                    and not any(
                        column.is_related_to_many_related_model_path(mrmp)
                        for mrmp in filtered_mr_model_paths
                    )
                ):
                    annotations_after_filter[column.name] = column.converter
                else:
                    annotations_before_filter[column.name] = column.converter
            if column.name == self.sort_col.name:
                annotations_after_filter[column.sorter.annot_name] = (
                    column.sorter.expression
                )
            # NOTE: The many-related sorter annot_name and many_expression are not added here.  They are added in a
            # subquery.

        return annotations_before_filter, annotations_after_filter

    # TODO: Figure out a way to move this to BSTQueryView without it having to know about the client interface elements
    # like cookeis and filters.
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
            searchcols = [
                c.name for c in self.columns.values() if c.filterable or c.searchable
            ]
            fld_str = "\n\t".join(searchcols)
            fld_msg = f"One or more of {len(searchcols)} fields is misconfigured:\n\n\t{fld_str}"
            warning = (
                f"Your search could not be executed.  {fld_msg}\n\n"
                "Please report this error to the site administrators."
            )
            if settings.DEBUG:
                warn(
                    f"{warning}\nException: {type(fe).__name__}: {fe}", DeveloperWarning
                )
            self.warnings.append(warning)
            self.reset_search_cookie()
            self.reset_filter_cookies()

        return qs
