from DataRepo.views.models.bst_list_view.column.filterer.base import (
    BSTBaseFilterer,
)


class BSTAnnotFilterer(BSTBaseFilterer):
    """This class manages filtering of rows/objects based on a column in the Bootstrap Table for a model object
    annotation field.

    All it does different from the BSTBaseFilterer is it sets is_annotation to True.
    """

    is_annotation = True
