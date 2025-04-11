from typing import Dict, List, Optional, Union

from django.utils.functional import classproperty

from DataRepo.utils.text_utils import camel_to_title, underscored_to_title
from DataRepo.views.models.bst_list_view.client_interface import (
    BSTClientInterface,
)
from DataRepo.views.models.bst_list_view.column.base import BSTBaseColumn
from DataRepo.views.models.bst_list_view.column.many_related_group import (
    BSTColumnGroup,
)
from DataRepo.views.utils import GracefulPaginator


class BSTListView(BSTClientInterface):
    """Generic class-based view for a Model record list to make pages load faster, using server-side behavior for
    pagination.

    Usage:
        # Just create a class that inherits from BSTListView and sets a model
        class MyModelListView(BSTListView):
            model = MyModel

        # Set more fields
        class MyModelListView(BSTListView):
            model = MyModel
            ordering = ["field1", "field2", "related__field3", "reverse_related_field4"]

        # Customize the fields
        class MyModelListView(BSTListView):
            model = MyModel
            def __init__(self):
                # All of the other model fields are auto-added
                columns = {"field1": {"visible": False}}
                super().__init__(columns)
    """

    ordering: Optional[List[str]] = None  # default: self.model._meta.get_fields()
    exclude: Optional[List[str]] = ["id"]

    PER_PAGE_CHOICES = [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0]  # 0 = "ALL"

    paginator_class = GracefulPaginator
    paginate_by = 15

    template_name = "DataRepo/templates/models/bst_list_view/base.html"

    def __init__(
        self,
        columns: Optional[
            Union[
                List[Union[str, dict, BSTBaseColumn, BSTColumnGroup]],
                Dict[str, Union[str, dict, BSTBaseColumn, BSTColumnGroup]],
            ]
        ] = None,
        **kwargs,
    ):
        """An extension of the ListView constructor intended to initialize the columns.

        Args:
            columns (Optional[Union[
                List[Union[str, dict, BSTBaseColumn, BSTColumnGroup]],
                Dict[str, Union[str, dict, BSTBaseColumn, BSTColumnGroup]]
            ]]) [auto]: There are multiple ways to supply a columns specification, but all result in a dict of
                BSTBaseColumn objects.  See init_column_settings() for details.
                If columns are not supplied, default columns will be selected using self.ordering, self.exclude, and/or
                self.model._meta.get_fields().
            kwargs (dict): Passed to ListView superclass constructor.
        Exceptions:
            None
        Returns:
            None
        """

        super().__init__(**kwargs)

        # This is only here to silence Django Warnings.  It specifies the default order of the model objects, which is
        # already set in the model.
        self.ordering = self.model._meta.ordering

        # Initialize the values obtained from cookies
        self.search: Optional[str] = self.get_cookie("search")
        self.filters = self.get_column_cookie_dict("filter")
        self.visibles = self.get_boolean_column_cookie_dict("visible")
        self.sortcol: Optional[str] = self.get_cookie("sortcol")
        self.asc: bool = self.get_boolean_cookie("asc", True)
        self.ordered = self.sortcol != ""

        # Initialize values obtained from URL parameters (or cookies)
        self.limit = self.request.GET.get("limit", "")
        if self.limit == "":
            cookie_limit = self.get_cookie("limit")
            # Never set limit to 0 from a cookie, because if the page times out, the users will never be able to load it
            # without deleting their browser cookie.
            if (
                cookie_limit is not None
                and cookie_limit != ""
                and int(cookie_limit) != 0
            ):
                self.limit = int(cookie_limit)
            else:
                self.limit = self.paginate_by
        else:
            self.limit = int(self.limit)

        # Basics
        self.total = 0
        self.raw_total = 0
        self.warnings = self.cookie_warnings.copy()

        # Initialize columns
        self.column_settings: Dict[str, Union[dict, BSTBaseColumn, BSTColumnGroup]]
        self.init_column_settings(columns)
        self.columns: Dict[str, BSTBaseColumn] = {}
        self.groups: Dict[str, BSTColumnGroup] = {}
        self.init_columns()

    def init_column_settings(
        self,
        columns: Optional[
            Union[
                List[
                    Union[
                        str,  # Model field paths
                        dict,  # (args and) kwargs for the BSTBaseColumn constructors
                        BSTBaseColumn,
                        BSTColumnGroup,
                    ],
                ],
                Dict[
                    str,  # Model field paths or annotation names
                    Union[
                        str,  # Model field paths
                        dict,  # (args and) kwargs for the BSTBaseColumn constructors
                        BSTBaseColumn,
                        BSTColumnGroup,
                    ],
                ],
            ]
        ] = None,
    ):
        """Initializes self.column_settings.

        Args:
            columns (Optional[Union[
                List[Union[str, dict, BSTBaseColumn, BSTColumnGroup]],
                Dict[str, Union[str, dict, BSTBaseColumn, BSTColumnGroup]]]
            ]) [auto]: There are multiple ways to supply a columns specification, but all result in a dict of
                BSTBaseColumn objects.  A list of model field_paths or column or column group objects, or a dict keyed
                on the column object name (which is either a field path or annotation name) containing either column or
                column group objects or a dict specifying the kwargs that will be supplied to a BSTBaseColumn derived
                class constructor, selected based on the dict key (i.e. the column name).  Given that the column name is
                either a model field_path or annotation, the actual field can be obtained from self.model to determine
                whether the column object should be a BSTManyRelatedColumn, BSTRelatedColumn, BSTColumn, or
                BSTAnnotColumn (if the name does not exist as an attribute of the model).
        Exceptions:
            TypeError when a type is encountered in columns that is not supported.
            ValueError when a list is supplied containing a dict whose value is a str and does not match the key.  Or
                when a dict is supplied and the 'name' and/or 'field_path' key does not match the outer dict key.
                NOTE: Support for str values in a list of dicts is simply for convenience.  Both the key and value
                specify the field_path for a model field, intended to create a BSTColumn with all default settings.
            KeyError when a dict is supplied containing a dict and does not contain required positional arguments to the
                BST*Column constructors (e.g. required keys are 'name', 'field_path', and optionally 'model' [provided
                by this class's model class attribute]).  Note that the required 'converter' positional argument for
                BSTAnnotColumn is not checked and would raise an error downstream of this method, if missing.
        Returns:
            None
        """
        self.column_settings = {}
        if columns is None:
            pass
        elif isinstance(columns, list):
            # CASE 1: A list was supplied, potentially containing:
            # - str (for a field_path, to use defaults for any BSTColumn, BSTRelatedColumn, or BSTManyRelatedColumn)
            # - dict (like str, but also including BSTAnnotColumn and/or custom settings. Must contain positional args.)
            # - BSTBaseColumn (BSTBaseColumn.name will be the key in the returned dict)
            # - BSTColumnGroup (BSTColumnGroup.name will be the key in the returned dict)
            for colobj in columns:
                if isinstance(colobj, str):
                    # This only works for model field_paths.  Annotations must be supplied in one of the other types.
                    self.column_settings[colobj] = {}
                elif isinstance(colobj, dict):
                    # Remove positional "args" (with the exception of a BSTAnnotColumn 'converter') from the dict,
                    # so it can be used as kwargs
                    name_arg = colobj.pop("name", None)
                    field_path = colobj.pop("field_path", None)
                    colobj.pop(
                        "model", None
                    )  # Differences with self.model will be ignored
                    name = name_arg or field_path

                    if name is None:
                        raise KeyError(
                            f"When supplying a list of column settings containing a dict (such as {colobj}), either an "
                            "annotation 'name' or model 'field_path' key must be supplied."
                        )

                    self.column_settings[name] = colobj
                elif isinstance(colobj, BSTBaseColumn):
                    self.column_settings[colobj.name] = colobj
                elif isinstance(colobj, BSTColumnGroup):
                    self.column_settings[colobj.name] = colobj
                else:
                    raise TypeError(
                        "When supplying a list of column settings, the type must be one of [str, dict, BSTBaseColumn, "
                        f"or BSTColumnGroup], not '{type(colobj).__name__}'."
                    )
        elif isinstance(columns, dict):
            # CASE 2: A dict was supplied, potentially containing:
            # - str (for a field_path, to use defaults for any BSTColumn, BSTRelatedColumn, or BSTManyRelatedColumn.
            #   Must be identical to outer dict key.)
            # - dict (like str, but also including BSTAnnotColumn and/or custom settings. Must contain positional args.
            #   'name' must be identical to outer dict key.)
            # - BSTBaseColumn (BSTBaseColumn.name will be the key in the returned dict)
            # - BSTColumnGroup (BSTColumnGroup.name will be the key in the returned dict)
            for colname, colobj in columns.items():
                if isinstance(colobj, str):
                    # This only works for model field_paths.  Annotations must be supplied in one of the other types.
                    if colname != colobj:
                        raise ValueError(
                            f"The value (the column's model field_path) of the columns dict key '{colname}' (when it "
                            f"is a str) must be identical to the outer dict's key, but the value '{colobj}' does not "
                            "match."
                        )
                    self.column_settings[colobj] = {}
                elif isinstance(colobj, dict):
                    # Remove positional "args" (with the exception of a BSTAnnotColumn 'converter') from the dict, so it
                    # can be used as kwargs
                    name_arg = colobj.pop("name", None)
                    field_path = colobj.pop("field_path", None)
                    colobj.pop(
                        "model", None
                    )  # Differences with self.model will be ignored
                    name = name_arg or field_path

                    if name is not None and colname != name:
                        raise ValueError(
                            f"The value (the 'name' and/or 'field_path' key) of the columns dict key '{colname}' (when "
                            f"its value is a dict) must be identical to the outer dict's key, but the value '{name}' "
                            "does not match."
                        )

                    self.column_settings[colname] = colobj
                elif isinstance(colobj, BSTBaseColumn):
                    self.column_settings[colobj.name] = colobj
                elif isinstance(colobj, BSTColumnGroup):
                    self.column_settings[colobj.name] = colobj
                else:
                    raise TypeError(
                        f"The value associated with the columns dict key '{colname}' must be of type [str, dict, "
                        f"BSTBaseColumn, or BSTColumnGroup], not '{type(colobj).__name__}'."
                    )
        else:
            raise TypeError(
                f"Invalid columns type: '{type(columns).__name__}'.  Must be a dict or list."
            )

    @classproperty
    def model_title_plural(cls):  # pylint: disable=no-self-argument
        try:
            return underscored_to_title(cls.model._meta.__dict__["verbose_name_plural"])
        except Exception:
            return f"{camel_to_title(cls.model.__name__)}s"

    @classproperty
    def model_title(cls):  # pylint: disable=no-self-argument
        try:
            return underscored_to_title(cls.model._meta.__dict__["verbose_name"])
        except Exception:
            return camel_to_title(cls.model.__name__)

    def init_columns(self):
        # TODO: Implement this method as a part of issue:
        # https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/1505
        pass
