from __future__ import annotations

from collections import defaultdict
from typing import Optional

from DataRepo.models.utilities import field_path_to_model_path, is_many_related_to_root
from DataRepo.views.models.bst_list_view.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst_list_view.column.sorter.many_related_field import BSTManyRelatedSorter


class BSTColumnGroup:
    """Class to control multiple many-related columns' delimited sort values to sort the same so that they visually
    align.

    Limitations:
        1. Only sorting is supported (not filtering [yet]).
        2. Only contiguous (i.e. side-by-side) columns are supported.
           (Discontiguous columns can be achieved manually [i.e. without this class] by setting each column's sorter
           object.)
    """

    def __init__(
        self,
        *columns: BSTManyRelatedColumn,
        initial: Optional[str] = None,
        asc: Optional[bool] = None,
        related_model_path: Optional[str] = None,
    ):
        """Constructor.

        Example:
            # To create a column group in a Sample ListView consisting of tracer info (tracer name, compound, & conc):
            #   animal__infusate__tracer_links__tracer__name
            #   animal__infusate__tracer_links__tracer__compound
            #   animal__infusate__tracer_links__concentration
            # Call this constructor like this:
            colgroup = BSTColumnGroup(
                BSTManyRelatedColumn("animal__infusate__tracer_links__tracer__name", Sample),  # default initial colunm
                BSTManyRelatedColumn("animal__infusate__tracer_links__tracer__compound", Sample),
                BSTManyRelatedColumn("animal__infusate__tracer_links__concentration", Sample),
            )
            # Each column will be sorted based on the initial column (tracer name) by default.

            # To change all the sorts of each column to be based on descending concentration:
            colgroup.set_sorters(column="animal__infusate__tracer_links__concentration", asc=False)
        Args:
            *columns (BSTManyRelatedColumn): Minimum of 2 BSTManyRelatedColumn objects.
            initial (Optional[str]) [columns[0].name]: The name of the BSTManyRelatedColumn in columns that the sort
                will be based on.
            asc (Optional[bool]) [auto]: The initial sort direction (ascending or descending).  The default is that of
                the initial column.
            related_model_path (Optional[str]) [auto]: The many-related model path common among all of the columns.  The
                default is based on the last many-related model path of the first column.  You should only need to
                provide this if a column's field_path traverses mutliple many-related models and the last one in the
                first column is not common among all of the supplied columns (and there exists another that is common).
        Exceptions:
            TypeError when columns are not all BSTManyRelatedColumn objects.
            ValueError when
                - columns has fewer than 2 columns.
                - columns don't all have the same root model
                - columns don't all start with the same many-related model
                - related_model_path is not many-related to the root model
                - initial does not match any of the columns
        Returns:
            None
        """
        # Initial columns checks required for defaults set below
        not_bstmrcs = [
            f"column {i + 1}: {type(c).__name__}"
            for i, c in enumerate(columns)
            if not isinstance(c, BSTManyRelatedColumn)
        ]
        if len(not_bstmrcs) > 0:
            raise TypeError(
                f"Invalid columns.  {len(not_bstmrcs)} of the {len(columns)} columns are the wrong type: {not_bstmrcs}."
            )
        if len(columns) < 2:
            raise ValueError(f"Invalid columns.  There must be more than 1.  Supplied: {len(columns)}.")

        # Defaults
        self.columns = columns
        self.asc = asc
        self.initial = (initial if initial is not None else columns[0].name)
        self.model = columns[0].model

        self.related_model_path = (
            related_model_path
            if related_model_path is not None
            else field_path_to_model_path(columns[0].model, columns[0].related_model_path)
        )

        self.sorter: BSTManyRelatedSorter
        self.controlling_column: BSTManyRelatedColumn
        for c in columns:
            if c == initial:
                self.controlling_column = c
                self.sorter = c.create_sorter(asc=asc)
                break
        else:
            nlt = "\n\t"
            raise ValueError(
                f"Initial column '{initial}' does not match any of the supplied columns:{nlt}"
                f"{nlt.join([c.name for c in columns])}"
            )

        # Check that all columns have the same root model
        not_common_models = [
            f"{c.name}: {c.model.__name__}"
            for c in columns[1:]
            if c.model != self.model
        ]
        if len(not_common_models) > 0:
            raise ValueError(
                f"Invalid columns.  {len(not_common_models)} of the {len(columns)} columns do not have the same root "
                f"model '{self.model}' as the first column: {not_common_models}."
            )

        # Check that each column's field_path starts with the same many-related model
        # And set each column's related_model_path to match
        not_common_mr_models = []
        for c in columns:
            if not c.field_path.startswith(self.related_model_path):
                not_common_mr_models.append(c.field_path)
            c.set_related_model_path(self.related_model_path)
        if len(not_common_mr_models) > 0:
            nlt = "\n\t"
            raise ValueError(
                f"All columns' field_paths must start with the same many-related model '{self.related_model_path}'.  "
                "The following column field_path(s) do not match:\n"
                f"\t{nlt.join(not_common_mr_models)}\n"
                "Either supply different columns, adjust their field_paths, or supply related_model_path with the "
                "common many-related path."
            )

        # Make sure that self.related_model_path is many-related
        if not is_many_related_to_root(self.related_model_path, self.model):
            raise ValueError(
                f"The related_model_path '{self.related_model_path}' is not many-related to the root model "
                f"'{self.model}'."
            )

        # Check for duplicate column names
        seen = defaultdict(int)
        for c in columns:
            seen[c.name] += 1
        dupes = [k for k in seen.keys() if seen[k] > 1]
        if len(dupes) > 0:
            raise ValueError(f"Duplicate column names not allowed: {dupes}")

        # Finally, update all of the columns' sorters
        self.set_sorters()

    def set_sorters(self, column: Optional[str] = None, asc: Optional[bool] = None):
        """Sets the sorter of every column in the group so that each column's many-related values will be sorted the
        same.

        This also sets self.sorter if either argument is not None.

        Example:
            # For a column group consisting of:
            #   animal__infusate__tracer_links__tracer__name
            #   animal__infusate__tracer_links__tracer__compound
            #   animal__infusate__tracer_links__concentration
            # To sort them all by descending tracer concentration:
            column_group.set_sorters(column="animal__infusate__tracer_links__concentration", asc=False)
        Assumptions:
            1. self.controlling_column is defined
        Args:
            column (Optional[str]) [self.controlling_column]: BSTManyRelatedColumn.name of the column selected to sort
                the rows.
            asc (Optional[bool]) [auto]: Sort is ascending.  Default is based on column.create_sorter() if column is
                supplied.  The default is based on self.controlling_column.create_sorter() otherwise.
        Exceptions:
            ValueError when column is not the name of a column in self.columns.
        Returns:
            None
        """
        if column is not None and column != self.controlling_column:
            for c in self.columns:
                if c == column:
                    self.controlling_column = c
                    self.sorter = c.create_sorter(asc=asc)
                    break
            else:
                nlt = "\n\t"
                raise ValueError(
                    f"Column '{column}' does not match any of the columns in this group:{nlt}"
                    f"{nlt.join([c.name for c in self.columns])}"
                )
        elif asc is not None and asc != self.controlling_column.sorter.asc:
            self.sorter = self.controlling_column.create_sorter(asc=asc)

        for c in self.columns:
            c.sorter = self.sorter
