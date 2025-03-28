from typing import Optional, Type
from warnings import warn

from django.conf import settings
from django.db.models import Model
from django.db.models.expressions import Combinable
from django.db.models.functions import Coalesce

from DataRepo.views.models.bst_list_view.column import BSTColumn


class BSTAnnotColumn(BSTColumn):
    """Class to extend BSTColumn to support annotations.

    BSTAnnotColumn sets 'name' to the name of the annotation field, which is used for sorting and filtering based on the
    column values.

    Note that in order for some BSTColumns' search and sort to work as expected, a BSTAnnotColumn should be used to
    convert them to a simple string or number annotation that is compatible with django's annotate method.

    For example, as a DateTimeField, imported_timestamp, will sort correctly on the server side, but Bootstrap Table
    will sort the page's worth of results using alphanumeric sorting.  Filtering will also be quirky, because of the
    differences between how the database compares a table field that is a datetime to a search string using LIKE versus
    Django's rendering of a datetime in a template.

    Specifically, postgres does not pad a date with zeroes, but django does (by default), so if you filter the column
    based on what you see (a date with a padded '0'), you will not get what you expect from the database.

    You can make the searching and sorting behavior consistent by supplying a function using the converter argument in
    the constructor, like this:

        BSTColumn(
            "imported_timestamp_str",
            field="imported_timestamp",
            converter=Func(
                F("imported_timestamp"),
                Value("YYYY-MM-DD HH:MI a.m."),
                output_field=CharField(),
                function="to_char",
            ),
        )

    The BSTSorter and BSTFilterer provided by the base class will use the annotation field for their operations.
    """

    # Overrides BSTColumn.is_annotation
    is_annotation = True

    def __init__(
        self,
        name: str,
        converter: Combinable,
        *args,
        model: Optional[Type[Model]] = None,
        field_path: Optional[str] = None,
        **kwargs,
    ):
        """Constructor.

        Args:
            name (str): The name of the annotation (and name of the column).
            converter (Combinable): A method to combine, transform, or create database field values, e.g. to a
                CharField.  Converting to a CharField for example, can be necessary for searching and filtering because
                BST only does substring searches.  Note that BSTManyRelatedColumn uses an annotation/converter behind
                the scenes for many-related model fields to prevent sorting from increasing the number of resulting
                rows.
                Recommendations: Refrain from using Coalesce, as its performance is slow in certain contexts.  Consider
                    using a converter that utilizes Case/When instead.
                Example Functions to use (non-comprehensive):
                    Regular fields:
                        Lower
                        Upper
                        etc.
                    Many-related fields:
                        Count
                        Min
                        Max
                        etc.
                    Combining multiple fields:
                        Concat
                        Case/When
                        Coalesce (see recommendations above)
                        etc.
                    Build your own:
                        Func
                        etc.
            model (Optional[Type[Model]]): Passed to super().__init__().  Providing the model and field_path is used to
                infer the field type of the annotation in limited cases, for selecting default behaviors for filtering
                and sorting.  For example, to decide whether to match the entire value or a substring of the value.
                Numeric values match the entire value by default.  A BSTSorter and BSTFilterer can be supplied if the
                default is not ideal.
            field_path (Optional[str]): Passed to super().__init__().  Checked to make sure not the same as name.
                Providing the model and field_path is used to infer the field type of the annotation in limited cases,
                for selecting default behaviors for filtering and sorting.  For example, to decide whether to match the
                entire value or a substring of the value.  Numeric values match the entire value by default.  A
                BSTSorter and BSTFilterer can be supplied if the default is not ideal.
        Super Args Notes:
            model (Optional[Type[Model]]): Supply this if you want to have the annotation link to the detail
            field_path (Optional[str]): Name of the database field (including the field path) corresponding to the
                column.  A value must be supplied, but that value may be None (to support derived classes that do not
                use it).
                NOTE: Adding a many-related field will increase the number of resulting rows in the table.  See
                BSTManyRelatedColumn to prevent this (and display many-related records as delimited values).

            link (bool) [False]: Whether or not to link the value in the column to a detail page for the model record
                the row represents.  The model must have a "get_absolute_url" method.
        Exceptions:
            None
        Returns:
            None
        """
        self.name = name
        self.converter = converter

        if field_path is not None and name == field_path:
            raise ValueError(
                f"The annotation name '{name}' cannot be the same as the field_path '{field_path}'."
            )

        if settings.DEBUG and isinstance(self.converter, Coalesce):
            warn(
                "Usage of Coalesce in annotations is discouraged due to performance in searches and sorting.  Try "
                "changing the converter to a different function, such as 'Case'."
            )

        super().__init__(model, field_path, *args, **kwargs)
