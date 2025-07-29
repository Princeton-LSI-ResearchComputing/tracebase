from __future__ import annotations

from collections import defaultdict
from typing import Optional

from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.sorter.many_related_field import (
    BSTManyRelatedSorter,
)


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
        name: Optional[str] = None,
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
            initial (Optional[str]) [columns[0].name]: The name of the BSTManyRelatedColumn in columns that the initial
                sort will be based on.
            name (Optional[str]) [auto]: Arbitrary object name.  Must not match the name of any included BSTBaseColumn
                objects.  The default is based on self.many_related_model_path, with dunderscores replaced with
                underscores, and "_group" appended.
                NOTE: self.many_related_model_path is set from the column's many_related_model_path (all are the same).
        Exceptions:
            TypeError when columns are not all BSTManyRelatedColumn objects.
            ValueError when
                - columns has fewer than 2 columns.
                - columns don't all have the same root model
                - columns don't all start with the same many-related model
                - initial does not match any of the columns
        Returns:
            None
        """
        # Initial columns checks required for defaults set below
        not_bstmrcs = [
            f"column {i + 1}, '{c.name}': {type(c).__name__}"
            for i, c in enumerate(columns)
            if not isinstance(c, BSTManyRelatedColumn)
        ]
        if len(not_bstmrcs) > 0:
            nlt = "\n\t"
            raise TypeError(
                f"Invalid columns.  {len(not_bstmrcs)} of the {len(columns)} columns are the wrong type:\n"
                f"\t{nlt.join(not_bstmrcs)}\nColumn objects must be BSTManyRelatedColumn instances."
            )
        if len(columns) < 2:
            raise ValueError(
                f"Invalid columns.  There must be more than 1.  Supplied: {len(columns)}."
            )

        # Defaults
        self.columns = list(columns)
        self.initial = initial if initial is not None else columns[0].name
        self.model = columns[0].model

        # NOTE: All contained columns must have the same many_related_model_path
        self.many_related_model_path = columns[0].many_related_model_path

        # Sanitize/generate an object name and base name on many_related_model_path (unless supplied)
        self.name = self.get_or_fix_name(name or self.many_related_model_path)

        self.sorter: BSTManyRelatedSorter
        self.controlling_column: BSTManyRelatedColumn
        for c in columns:
            if c == self.initial:
                self.controlling_column = c
                self.sorter = c.create_sorter()
                break
        else:
            nlt = "\n\t"
            raise ValueError(
                f"Initial column '{self.initial}' does not match any of the supplied columns:{nlt}"
                f"{nlt.join([c.name for c in columns])}"
            )

        self.asc = self.controlling_column.sorter.asc

        # Check that all columns have the same root model
        not_common_models = [
            f"{c.name}: {c.model.__name__}"
            for c in columns[1:]
            if c.model != self.model
        ]
        if len(not_common_models) > 0:
            raise ValueError(
                f"Invalid columns.  {len(not_common_models)} of the {len(columns)} columns do not have the same root "
                f"model '{self.model.__name__}' as the first column: {not_common_models}."
            )

        # Check that each column's field_path starts with the same many-related model
        # Set each column's many_related_model_path to match and indicate that the column is in a group.
        not_common_mr_models = []
        for c in columns:
            c._in_group = True
            if c.many_related_model_path != self.many_related_model_path:
                not_common_mr_models.append(f"{c.name}: {c.many_related_model_path}")

        if len(not_common_mr_models) > 0:
            nlt = "\n\t"
            raise ValueError(
                f"All columns' many_related_model_path must be the same: '{self.many_related_model_path}'.  "
                "The following column(s) have a many_related_model_path that does not match:\n"
                f"\t{nlt.join(not_common_mr_models)}\n"
                "Either supply different columns or adjust their field_paths to all start with the same many-related "
                "model path."
            )

        # Check for duplicate column names
        seen: dict = defaultdict(int)
        for c in columns:
            seen[c.name] += 1
        dupes = dict((k, v) for k, v in seen.items() if v > 1)
        if len(dupes.keys()) > 0:
            nlt = "\n\t"
            raise ValueError(
                f"Duplicate column names not allowed ({len(dupes.keys())} occurrence(s) in {len(columns)} columns):\n"
                f"\t{nlt.join([f'{k}: {v} occurrences' for k, v in dupes.items()])}"
            )

        # Finally, update all of the columns' sorters
        self.set_sorters()

    @staticmethod
    def get_or_fix_name(name: str) -> str:
        """This will take self.many_related_model_path (or self.name) and return a name that has been sanitized to
        (likely) not conflict with BSTBaseColumn.name.

        NOTE: This is a static method in order to generate a name before the object is created.

        Args:
            name (str): Supply an arbitrary name or (recommended) self.many_related_model_path.
        Exceptions:
            None
        Returns:
            (str): A sanitized object name intended for self.name
        """
        return name.replace("__", "_") + "_group"

    def __str__(self):
        return self.name

    def __eq__(self, other):
        """This is a convenience override to be able to compare a group name with a group object to see if the object
        is for that group.  It also enables the `in` operator to work between strings and objects.

        Args:
            other (Optional[Union[str, BSTColumnGroup]]): A value to equate with self
                NOTE: Cannot apply this type hint due to mypy superclass requirements that it be 'object'.
        Exceptions:
            NotImplementedError when the type of other is invalid
        Returns:
            (bool)
        """
        if isinstance(other, __class__):  # type: ignore
            return self.__class__ == other.__class__ and self.__dict__ == other.__dict__
        elif isinstance(other, str):
            return self.name == other
        elif other is None:
            return False
        else:
            raise NotImplementedError(
                f"Equivalence of {__class__.__name__} to {type(other).__name__} not implemented."  # type: ignore
            )

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
                    f"Column '{column}' does not match any of the columns in this group:\n"
                    f"\t{nlt.join([c.name for c in self.columns])}"
                )
        elif asc is not None and asc != self.controlling_column.sorter.asc:
            self.sorter = self.controlling_column.create_sorter(asc=asc)

        for c in self.columns:
            c.sorter = self.sorter
