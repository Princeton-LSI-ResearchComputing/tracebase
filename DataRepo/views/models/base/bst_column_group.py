from collections import defaultdict
from functools import reduce
from typing import Optional

from DataRepo.views.models.base import BSTColumn


class BootstrapTableColumnGroup:
    """Can be used in place of a BSTColumn to group columns contiguously.  Columns in a group that are not contiguous
    can still be achieved if you set BootstrapTableListView's instance attributes 'columns' and 'groups' and
    'groups_dict' explicitly."""

    sort_dirs = ["asc", "desc"]

    def __init__(
        self,
        *columns: BSTColumn,
        many_related_sort_fld: Optional[str] = None,
        many_related_sort_fwd: bool = True,
    ):
        """Construct an instance.

        Limitations:
            1. A list value for the many_related_sort_fld argument is (not yet) supported (for columns that need
               coalesce).
        Args:
            *columns (BSTColumn): Minimum of 2 BSTColumn objects.
        Exceptions:
            ValueError
        Returns:
            (BootstrapTableColumnGroup)
        """
        self.columns = columns

        if not all([c.many_related for c in columns]):
            one_to_ones = [c.name for c in columns if not c.many_related]
            raise ValueError(
                f"Invalid columns.  Every column must be a many-related column.  These columns are not many-related: "
                f"{one_to_ones}."
            )
        if len(columns) < 2:
            raise ValueError(f"Invalid columns.  There must be more than 1.  Supplied: {len(columns)}.")

        self.model = columns[0].many_related_model
        if not all([c.many_related_model == self.model for c in columns]):
            models = [c.many_related_model for c in columns]
            uniq_models = reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, models, [])
            raise ValueError(
                "All columns must belong to the same many-related model.  The following model mix was found: "
                f"{uniq_models}."
            )

        self.sort_fld = many_related_sort_fld
        if many_related_sort_fld is None:
            # Default to the first column's many_related_sort_fld
            self.sort_fld = columns[0].many_related_sort_fld
        sort_flds = dict((c.many_related_sort_fld, 0) for c in columns)
        for c in columns:
            sort_flds[c.many_related_sort_fld] += 1
        dupe_sort_flds = reduce(lambda dlst, val: dlst + [val] if sort_flds[val] > 1 else dlst, sort_flds, [])
        if len(dupe_sort_flds) > 0:
            raise ValueError(
                "All columns must initially have unique sort fields.  The following sort fields were found more than "
                f"once among columns {[c.name for c in columns]}: {dupe_sort_flds}."
            )

        self.sort_fwd = many_related_sort_fwd

        seen = defaultdict(int)
        for c in columns:
            seen[c.name] += 1
        dupes = [k for k in seen.keys() if seen[k] > 1]
        if len(dupes) > 0:
            raise ValueError(f"Each column name must be unique.  These were found to be redundant: {dupes}")

    def set_sort_fld(self, sort_fld: str, ignore_non_matches: bool = False):
        """Sets the many_related_sort_fld of every column in the group so that each column's many-related values will be
        sorted the same.

        Args:
            sort_fld (str): BSTColumn.name of the column selected to sort the rows.
            ignore_non_matches (bool) [False]: If True, it allows sorting on other columns not in the group.
        Exceptions:
            ValueError when the sort_fld is not a member of the group (and ignore_non_matches is False).
            You will encounter an undefined exception if you supply a sort_fld that is not under the many related model
                (BSTColumnGroup.model)
        Returns:
            None
        """
        if not ignore_non_matches and sort_fld not in self.columns:
            raise ValueError(
                f"Sort field '{sort_fld}' is not a name of any column in this group.  The options are: "
                f"{[c.name for c in self.columns]}"
            )

        for c in self.columns:
            c.many_related_sort_fld = sort_fld

    def set_sort_dir(self, sort_dir: str, ignore_non_matches=False):
        """Sets the many_related_sort_fwd of every column in the group so that each column's many-related values will be
        sorted the same.

        Args:
            sort_dir (str): See self.sort_dirs
            ignore_non_matches (bool) [False]: If True, invalid values are ignored.
        Exceptions:
            ValueError when the sort_dir is not a member of self.sort_dirs (and ignore_non_matches is False).
        Returns:
            None
        """
        if not ignore_non_matches and sort_dir.lower() not in self.sort_dirs:
            raise ValueError(f"Sort direction '{sort_dir}' is not a sort direction.  The options are: {self.sort_dirs}")

        for c in self.columns:
            c.many_related_sort_fwd = sort_dir.lower() == "asc"
