from typing import Optional
from warnings import warn

from django.conf import settings
from django.db.models.expressions import Combinable
from django.db.models.functions import Coalesce

from DataRepo.views.models.bst_list_view.column.base import BSTBaseColumn
from DataRepo.views.models.bst_list_view.column.filterer.annotation import (
    BSTAnnotFilterer,
)
from DataRepo.views.models.bst_list_view.column.sorter.annotation import (
    BSTAnnotSorter,
)


class BSTAnnotColumn(BSTBaseColumn):
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

    # Overrides BSTBaseColumn.is_annotation
    is_annotation = True

    def __init__(
        self,
        name: str,
        converter: Combinable,
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
        Exceptions:
            TypeError when the provided sorter/filterer is the wrong type
        Returns:
            None
        """
        self.converter = converter
        sorter = kwargs.get("sorter")
        filterer = kwargs.get("filterer")

        if sorter is None:
            sorter = BSTAnnotSorter(name, name=name)
        elif isinstance(sorter, str):
            sorter = BSTAnnotSorter(name, name=name, client_sorter=sorter)
        elif not isinstance(sorter, BSTAnnotSorter):
            raise TypeError(
                f"sorter must be a BSTAnnotSorter, not {type(sorter).__name__}"
            )

        if filterer is None:
            filterer = BSTAnnotFilterer(name)
        elif isinstance(filterer, str):
            filterer = BSTAnnotFilterer(name, client_filterer=filterer)
        elif not isinstance(filterer, BSTAnnotFilterer):
            raise TypeError(
                f"filterer must be a BSTAnnotFilterer, not {type(filterer).__name__}"
            )

        if settings.DEBUG and isinstance(self.converter, Coalesce):
            warn(
                "Usage of Coalesce in annotations is discouraged due to performance in searches and sorting.  Try "
                "changing the converter to a different function, such as 'Case'."
            )

        kwargs.update(
            {
                "sorter": sorter,
                "filterer": filterer,
            }
        )

        super().__init__(name, **kwargs)

    def create_sorter(self, sorter: Optional[str] = None) -> BSTAnnotSorter:
        if sorter is None:
            sorter_obj = BSTAnnotSorter(self.name)
        elif isinstance(sorter, str):
            sorter_obj = BSTAnnotSorter(self.name, client_sorter=sorter)
        else:
            # Checks exact type bec. we don't want this to be a BSTRelatedSorter or BSTManyRelatedSorter
            raise TypeError(f"sorter must be a str, not {type(sorter).__name__}")
        return sorter_obj

    def create_filterer(self, filterer: Optional[str] = None) -> BSTAnnotFilterer:
        if filterer is None:
            filterer_obj = BSTAnnotFilterer(self.name)
        elif isinstance(filterer, str):
            filterer_obj = BSTAnnotFilterer(self.name, client_filterer=filterer)
        else:
            # Checks exact type bec. we don't want this to be a BSTRelatedFilterer or BSTManyRelatedFilterer
            raise TypeError(f"filterer must be a str, not {type(filterer).__name__}")
        return filterer_obj
