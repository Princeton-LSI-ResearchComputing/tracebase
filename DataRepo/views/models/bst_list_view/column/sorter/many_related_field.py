from warnings import warn

from django.conf import settings
from django.db.models import Max, Min
from django.db.models.aggregates import Aggregate

from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst_list_view.column.sorter.field import BSTSorter


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
        """Constructor.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # First, apply default sort metrics, like case insensitivity
        super().__init__(*args, **kwargs)

        # Then, apply the aggregate Min/Max based on asc or desc
        agg: Aggregate = Min if self.asc else Max

        if (
            settings.DEBUG
            and isinstance(self.expression, Aggregate)
            and not isinstance(self.expression, agg)
        ):
            warn(
                f"Unable to apply aggregate function '{agg.__name__}' to the sorter for column '{self.name}' because "
                f"the supplied field already has an aggregate function '{self.expression}'.  In order for the "
                "delimited values to be sorted and for the row sort to be based on either the first or last delimited "
                "value, the supplied field must not already be wrapped in an aggregate function.  Sorting on this "
                "column will not base row position on the min/max related value and the sort of the delimited values "
                "will be static and appear unordered until this is addressed.  If this is intended to be an annotation "
                "column, use BSTAnnotColumn instead.",
                DeveloperWarning,
            )
        elif not isinstance(self.expression, Aggregate):
            self.expression = agg(self.expression)
