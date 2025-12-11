from DataRepo.views.models.bst.column.filterer.base import BSTBaseFilterer


class BSTAnnotFilterer(BSTBaseFilterer):
    """This class manages filtering of rows/objects based on a column in the Bootstrap Table for a model object
    annotation field.

    All it does different from the BSTBaseFilterer is it sets is_annotation to True.
    """

    is_annotation = True

    # TODO: Create a constructor that takes converter and uses its output_field to set input_method, choices, and
    # filterers automatically.  Right now, it's all manual.
