from warnings import warn

from django.conf import settings
from django.db.models import F, Field, Max, Min
from django.db.models.aggregates import Aggregate
from django.db.models.expressions import Expression

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
            if self.asc:
                self.many_expression = self.SERVER_SORTERS.NONE(self.field_path).asc(
                    nulls_first=True
                )
            else:
                self.many_expression = self.SERVER_SORTERS.NONE(self.field_path).desc(
                    nulls_last=True
                )
        elif not isinstance(self.expression, Aggregate):
            if isinstance(self.expression, Expression):
                if self.asc:
                    self.many_expression = self.expression.asc(nulls_first=True)
                else:
                    self.many_expression = self.expression.desc(nulls_last=True)
            elif isinstance(self.expression, F):
                if self.asc:
                    self.many_expression = self.expression.asc(nulls_first=True)
                else:
                    self.many_expression = self.expression.desc(nulls_last=True)
            else:
                raise NotImplementedError(
                    f"self.expression type '{type(self.expression).__name__}' not supported."
                )
            self.expression = agg(self.expression)

        if isinstance(self.field, Field) and not is_many_related_to_root(
            self.field_path, self.model
        ):
            raise ValueError(
                f"field_path '{self.field_path}' must be many-related with the model '{self.model.__name__}'."
            )

    @property
    def many_order_by(self):
        """Returns an expression that can be supplied to a Django order_by() call."""
        if isinstance(self.many_expression, Expression):
            if self.asc:
                return self.many_expression.asc(nulls_first=True)
            return self.many_expression.desc(nulls_last=True)
        elif isinstance(self.many_expression, F):
            if self.asc:
                return self.many_expression.asc(nulls_first=True)
            return self.many_expression.desc(nulls_last=True)
        raise NotImplementedError(
            f"self.many_expression type '{type(self.many_expression).__name__}' not supported."
        )
