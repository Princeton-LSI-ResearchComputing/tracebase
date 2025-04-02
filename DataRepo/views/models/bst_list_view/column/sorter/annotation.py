from DataRepo.views.models.bst_list_view.column.sorter.base import (
    BSTBaseSorter,
)


class BSTAnnotSorter(BSTBaseSorter):
    """This class manages sorting of rows/objects based on a column in the Bootstrap Table for a model object
    annotation field.

    All it does different from the BSTBaseSorter is it sets is_annotation to True.
    """

    is_annotation = True
