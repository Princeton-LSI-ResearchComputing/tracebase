from warnings import warn

from django.conf import settings
from django.db.models import F, Field, Max, Min
from django.db.models.aggregates import Aggregate

from DataRepo.models.utilities import is_many_related_to_root
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.sorter.field import BSTSorter


class BSTManyRelatedSorter(BSTSorter):
    """Class that defines sorting rows for a column that contains delimited values from a many-related model.

    Sorting rows based on a many-related model field causes the number of objects returned in a queryset to increase,
    with duplicate root model records containing different many-related values, based on the number of related records.
    So we apply an aggregate function to prevent that.  We use Min and Max based on whether we want an ascending or
    descending sort, so that the position of this row in the context of the other rows always puts the least value first
    in ascending or the greatest value first in descending.

    Assumptions:
        None
    Limitations:
        1. Only supports row sort based on the min/max value.  Multiple rows containing the same min/max value will
            not further sort based on the next value.
    """

    def __init__(self, *args, **kwargs):
        """Constructor.  Extends BSTSorter.__init__.

        Args:
            *args (field_expression, model): This class uses self.expression set by field_expression and model in the
                superclass.  See superclass.
            **kwargs (asc, name, client_sorter, client_mode, _server_sorter): This class uses self.asc set by the asc
                arg.  See superclass.
        Exceptions:
            None
        Returns:
            None
        """
        # First, apply default sort metrics, like case insensitivity
        super().__init__(*args, **kwargs)

        # Figure out the aggregate function we need for sorting rows (Min or Max) based on asc or desc
        agg: Aggregate = Min if self.asc else Max

        if isinstance(self.field, Field) and not is_many_related_to_root(
            self.field_path, self.model
        ):
            raise ValueError(
                f"field_path '{self.field_path}' must be many-related with the model '{self.model.__name__}'."
            )

        self.many_annot_name = "_".join(self.field_path.split("__")) + "_bstcellsort"

        if isinstance(self.expression, Aggregate):
            if settings.DEBUG and not isinstance(self.expression, agg):
                warn(
                    f"Unable to apply aggregate function '{agg.__name__}' to the sorter for column '{self.name}' "
                    f"because the supplied field already has an aggregate function '{self.expression}'.  In order for "
                    "the delimited values to be sorted and for the row sort to be based on either the first or last "
                    "delimited value, the supplied field must not already be wrapped in an aggregate function.  "
                    "Sorting on this column will not base row position on the min/max related value and the sort of "
                    "the delimited values will be static and appear unordered until this is addressed.  If this is "
                    "intended to be an annotation column, use BSTAnnotColumn instead.",
                    DeveloperWarning,
                )
            # No need to change self.expression.  It is already an aggregate.  But, for the delimited values in the
            # column, there is no transform that we can apply that can be used for individual values, so:
            self.many_expression = self.SERVER_SORTERS.NONE(self.field_path)
        else:
            self.many_expression = self._server_sorter(self.field_path)
            self.expression = agg(self.expression)

    def get_many_order_bys(self):
        """Returns a list of OrderBy objects containing the annotation.

        Purpose:
            1. To be able to be used in conjunction with self.many_distinct_fields()
        Assumptions:
            1. self.field_path does not contain a foreign key at the end of the path.  This is by design.  Sorters are
               created intentionally with a non-foreign-key field, meaning that we don't need to consult the model's
               _meta.ordering.
            2. The caller has added the annotation whose name is stored in self.many_annot_name.
        Args:
            None
        Exceptions:
            None
        Returns:
            (List[OrderBy])
        """
        if self.asc:
            return [
                # Order by the expression first
                F(self.many_annot_name).asc(nulls_first=True),
            ]

        return [
            # Order by the expression first
            F(self.many_annot_name).desc(nulls_last=True),
        ]

    def get_many_distinct_fields(self):
        """Returns a list of field names containing the annotation.

        Assumptions:
            1. The caller has added the annotation whose name is stored in self.many_annot_name.
        Args:
            None
        Exceptions:
            None
        Returns:
            (List[str])
        """
        return [self.many_annot_name]

    def get_many_annotations(self):
        """Returns a dict containing the annotation name and expression.

        Add this annotation before using many_order_bys or many_distinct_fields.

        Args:
            None
        Exceptions:
            None
        Returns:
            (List[str])
        """
        return {self.many_annot_name: self.many_expression}
