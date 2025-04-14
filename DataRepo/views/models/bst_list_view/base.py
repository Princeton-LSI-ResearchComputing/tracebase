from typing import Dict, List, Optional, Union, cast
from warnings import warn

from django.conf import settings
from django.utils.functional import classproperty

from DataRepo.models.utilities import is_many_related_to_root, is_related
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.utils.text_utils import camel_to_title, underscored_to_title
from DataRepo.views.models.bst_list_view.client_interface import (
    BSTClientInterface,
)
from DataRepo.views.models.bst_list_view.column.annotation import (
    BSTAnnotColumn,
)
from DataRepo.views.models.bst_list_view.column.base import BSTBaseColumn
from DataRepo.views.models.bst_list_view.column.field import BSTColumn
from DataRepo.views.models.bst_list_view.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst_list_view.column.many_related_group import (
    BSTColumnGroup,
)
from DataRepo.views.models.bst_list_view.column.related_field import (
    BSTRelatedColumn,
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
            column_ordering = ["field1", "field2", "related__field3", "reverse_related_field4"]

        # Customize the fields
        class MyModelListView(BSTListView):
            model = MyModel
            def __init__(self):
                # All of the other model fields are auto-added
                columns = {"field1": {"visible": False}}
                super().__init__(columns)
    """

    column_ordering: List[str] = []
    # column_ordering default comes from: self.model._meta.get_fields()
    exclude: List[str] = ["id"]

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
                If columns are not supplied, default columns will be selected using self.column_ordering, self.exclude,
                and/or self.model._meta.get_fields().
            kwargs (dict): Passed to ListView superclass constructor.
        Exceptions:
            None
        Returns:
            None
        """
        # TODO: Automatically detect and create column groups based on collumn related_model_path or otherwise allow
        # users to supply groups by column name and/or settings
        super().__init__(**kwargs)

        # This is only here to silence Django Warnings.  It specifies the default row ordering of the model objects,
        # which is already set in the model.
        self.ordering = self.model._meta.ordering if self.model is not None else []

        # Initialize the values obtained from cookies
        self.search: Optional[str] = self.get_cookie("search")
        self.filters = self.get_column_cookie_dict("filter")
        self.visibles = self.get_boolean_column_cookie_dict("visible")
        self.sortcol: Optional[str] = self.get_cookie("sortcol")
        self.asc: bool = self.get_boolean_cookie("asc", True)
        self.ordered = self.sortcol is not None

        # Initialize values obtained from URL parameters (or cookies)
        limit_param = self.get_param("limit")
        if limit_param is None:
            cookie_limit = self.get_cookie("limit")
            # Never set limit to 0 from a cookie, because if the page times out, the users will never be able to load it
            # without deleting their browser cookie.
            if cookie_limit is not None and int(cookie_limit) != 0:
                self.limit = int(cookie_limit)
            else:
                self.limit = self.paginate_by
        else:
            self.limit = int(limit_param)

        # Basics
        self.total = 0
        self.raw_total = 0
        self.warnings = self.cookie_warnings.copy()

        # Initialize column settings
        self.column_settings: Dict[str, Union[dict, BSTBaseColumn, BSTColumnGroup]] = {}
        self.init_column_settings(columns)

        # Initialize column order
        self.column_ordering: List[str] = self.__class__.column_ordering.copy()
        self.init_column_ordering()

        # Initialize columns
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
            for i, colobj in enumerate(columns):
                if isinstance(colobj, str):
                    # This only works for model field_paths.  Annotations must be supplied in one of the other types.
                    self.column_settings[colobj] = {}
                elif isinstance(colobj, dict):
                    settings_name = self.prepare_column_kwargs(colobj)
                    self.column_settings[settings_name] = colobj
                elif isinstance(colobj, BSTBaseColumn):
                    self.column_settings[colobj.name] = colobj
                elif isinstance(colobj, BSTColumnGroup):
                    self.column_settings[colobj.name] = colobj
                else:
                    raise TypeError(
                        "When supplying a list of all columns' settings, the value's type must be one of [str, dict, "
                        f"BSTBaseColumn, or BSTColumnGroup], but the value of the column settings at index '{i}' was "
                        f"'{type(colobj).__name__}'."
                    )
        elif isinstance(columns, dict):
            # CASE 2: A dict was supplied, potentially containing:
            # - str (for a field_path, to use defaults for any BSTColumn, BSTRelatedColumn, or BSTManyRelatedColumn.
            #   Must be identical to outer dict key.)
            # - dict (like str, but also including BSTAnnotColumn and/or custom settings. Must contain positional args.
            #   'name' must be identical to outer dict key.)
            # - BSTBaseColumn (BSTBaseColumn.name will be the key in the returned dict)
            # - BSTColumnGroup (BSTColumnGroup.name will be the key in the returned dict)
            for settings_name, colobj in columns.items():
                if isinstance(colobj, str):
                    # This only works for model field_paths.  Annotations must be supplied in one of the other types.
                    if settings_name != colobj:
                        raise ValueError(
                            f"The column settings key '{settings_name}' must be identical to the field_path string "
                            f"provided '{colobj}'."
                        )
                    self.column_settings[colobj] = {}
                elif isinstance(colobj, dict):
                    self.prepare_column_kwargs(colobj, settings_name)
                    self.column_settings[settings_name] = colobj
                elif isinstance(colobj, BSTColumnGroup):
                    self.column_settings[colobj.name] = colobj
                elif isinstance(colobj, BSTBaseColumn):
                    self.column_settings[colobj.name] = colobj
                else:
                    raise TypeError(
                        "When supplying a dict of all columns' settings, the value's type must be one of [str, dict, "
                        "BSTBaseColumn, or BSTColumnGroup], but the value of the column settings at key "
                        f"'{settings_name}' was '{type(colobj).__name__}'."
                    )
        else:
            raise TypeError(
                f"Invalid columns type: '{type(columns).__name__}'.  Must be a dict or list."
            )

    def prepare_column_kwargs(
        self, column_settings: dict, settings_name: Optional[str] = None
    ) -> str:
        """This method turns a dict of column settings into a kwargs dict meant for any BSTBaseColumn constructor (with
        the exception of handling the converter positional arg that the BSTAnnotColumn constructor requires).

        This method alters the supplied column_settings dict to act as the kwargs dict and returns the key that should
        be used in the outer dict containing all columns' settings.

        Args:
            column_settings (dict): A dict containing both kwargs and positional args for a BSTBaseColumnConstructor.
            settings_name (Optional[str]): The key for the ultimate output settings dict that will contain all of the
                columns' settings dicts.
        Exceptions:
            ValueError when a list is supplied containing a dict whose value is a str and does not match the key.  Or
                when a dict is supplied and the 'name' and/or 'field_path' key does not match the outer dict key.
                NOTE: Support for str values in a list of dicts is simply for convenience.  Both the key and value
                specify the field_path for a model field, intended to create a BSTColumn with all default settings.
            KeyError when a dict is supplied containing a dict and does not contain required positional arguments to the
                BST*Column constructors (e.g. required keys are 'name', 'field_path', and optionally 'model' [provided
                by this class's model class attribute]).  Note that the required 'converter' positional argument for
                BSTAnnotColumn is not checked and would raise an error downstream of this method, if missing.
        Returns:
            settings_name (str): The key to be used for the outer dict, which should be the 'field_path' for BSTColumn
                and its derived classes or 'name' for BSTAnnotColumn.
        """

        # Remove positional "args" (with the exception of a BSTAnnotColumn 'converter') from the dict,
        # so it can be used as kwargs
        orig = column_settings.copy()
        name_arg = column_settings.pop("name", None)
        field_path = column_settings.pop("field_path", None)
        # Differences with self.model will be ignored
        column_settings.pop("model", None)
        # Conflicts between name and field_path (which must be the same) will be left to the constructors to
        # raise
        name = name_arg or field_path or settings_name

        if name is None:
            raise KeyError(
                "When supplying column settings in a dict, that dict must have either a 'name' or 'field_path' key.  "
                f"Non-positional keys found: {list(orig.keys())}."
            )

        if settings_name is None:
            settings_name = name

        if settings_name != name:
            raise ValueError(
                "When supplying all columns' settings as a dict containing dicts, each column's settings dict must "
                f"contain a 'name' or 'field_path' key whose value '{name}' must be identical to the outer dict key "
                f"'{settings_name}'."
            )

        return cast(str, settings_name)

    @classproperty
    def model_title_plural(cls):  # pylint: disable=no-self-argument
        """Creates a title-case string from self.model, accounting for potentially set verbose settings.  Pays
        particular attention to pre-capitalized values in the model name, and ignores the potentially poorly automated
        title-casing in existing verbose values so as to not lower-case acronyms in the model name, e.g. MSRunSample.
        """
        try:
            vname = cls.model._meta.__dict__["verbose_name_plural"]
            if any([c.isupper() for c in vname]):
                return underscored_to_title(vname)
            else:
                return f"{camel_to_title(cls.model.__name__)}s"
        except Exception:
            return f"{camel_to_title(cls.model.__name__)}s"

    @classproperty
    def model_title(cls):  # pylint: disable=no-self-argument
        """Creates a title-case string from self.model, accounting for potentially set verbose settings.  Pays
        particular attention to pre-capitalized values in the model name, and ignores the potentially poorly automated
        title-casing in existing verbose values so as to not lower-case acronyms in the model name, e.g. MSRunSample.
        """
        try:
            vname = cls.model._meta.__dict__["verbose_name"]
            sanitized = vname.replace(" ", "")
            sanitized = sanitized.replace("_", "")
            if (
                any([c.isupper() for c in vname])
                and cls.model.__name__.lower() == sanitized
            ):
                return underscored_to_title(vname)
            else:
                return camel_to_title(cls.model.__name__)
        except Exception:
            return camel_to_title(cls.model.__name__)

    def init_column_ordering(self):
        """Initializes self.column_ordering.  It first filters the class attribute (if set) based on the excludes, then
        adds defaults from the model in the order they were defined (but putting many-related at the end), and finally
        adds any that are among the settings that are not already added.

        NOTE: The way to exclude defaults is using the self.exclude class attribute.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        if self.column_ordering is None:
            # Initialize if None
            self.column_ordering = self.__class__.column_ordering.copy()
        else:
            # Remove excludes
            self.column_ordering = [
                f for f in self.column_ordering if f not in self.exclude
            ]

        # Supporting the model being None to mimmick Django's ListView
        if self.model is not None:
            # Add the defaults
            for fld in sorted(
                self.model._meta.get_fields(),
                key=lambda f: is_many_related_to_root(f.name, self.model) is True,
            ):
                self.add_to_column_ordering(fld.name, _warn=False)

        # Add from the settings dict
        for colname, colobj in self.column_settings.items():
            if isinstance(colobj, BSTColumnGroup):
                for col in colobj.columns:
                    self.add_to_column_ordering(col.name)
            else:
                self.add_to_column_ordering(colname)

    def add_to_column_ordering(self, colname: str, _warn=True):
        """This takes a column name and adds it to self.column_ordering if it is not among the excludes and hasn't
        already been added.

        Args:
            colname (str): The column name (same as the key that could appear in the self.column_settings).
            _warn (bool): Whether or not a warning should be raised when the added column exists among the excludes.
        Exceptions:
            None
        Returns:
            None
        """
        if colname in self.exclude:
            if settings.DEBUG and _warn:
                warn(
                    f"Ignoring attempt to add an excluded column '{colname}'.  Override the 'exclude' class attribute "
                    f"to include this column.  Current excludes: {self.exclude}.",
                    DeveloperWarning,
                )
        elif colname not in self.column_ordering:
            self.column_ordering.append(colname)

    def init_columns(self):
        """Traverses self.column_ordering and populates self.columns with BSTBaseColumn objects.

        Assumptions:
            1. self.column_ordering has been initialized
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        for colname in self.column_ordering:
            self.init_column(colname)

    def init_column(self, colname: str):
        """Takes a column name, creates a BSTBaseColumn object, and adds it to self.columns.

        Assumptions:
            1. The legnth of the colname string is greater than 0.
        Args:
            colname (str)
        Exceptions:
            None
        Returns:
            None
        """
        # Initialize kwargs (or add a column object to self.columns and return if settings exist with an object already)
        kwargs = {}
        if colname in self.column_settings.keys():
            column_object = self.column_settings.get(colname)
            if isinstance(column_object, BSTColumnGroup):
                self.init_column_group(column_object)
                return
            elif isinstance(column_object, BSTBaseColumn):
                self.columns[colname] = column_object
                return
            elif isinstance(column_object, dict):
                # Copy, because we pop the converter if it's an annot field below
                kwargs = column_object.copy()
            # else: All default kwargs (i.e. empty kwargs)

        # Determine if this is a model field or annotation, and initialize a column object and add it to self.columns
        first_field = colname.split("__")[0]
        if hasattr(self.model, first_field):

            if is_many_related_to_root(colname, self.model):
                self.columns[colname] = BSTManyRelatedColumn(
                    colname, self.model, **kwargs
                )
            elif is_related(colname, self.model):
                self.columns[colname] = BSTRelatedColumn(colname, self.model, **kwargs)
            else:
                self.columns[colname] = BSTColumn(colname, self.model, **kwargs)

        elif colname is not None and "converter" in kwargs.keys():

            converter = kwargs.pop("converter")
            self.columns[colname] = BSTAnnotColumn(colname, converter, **kwargs)

        else:

            raise ValueError(
                f"Unable to determine column type for initialization of column named '{colname}'.  Either the name is "
                "not a field_path, the name is a field_path and the first field in the field_path is not an attribute "
                f"of model '{self.model.__name__}', or the name is an annotation and no 'converter' was provided in "
                f"kwargs: {kwargs}."
            )

    def init_column_group(self, colgroup: BSTColumnGroup):
        """Takes a column group object, and adds each contained column to self.columns and self.groups.

        NOTE: self.columns contains the individual columns contained in the group and self.groups provides access to the
        group from the column.

        Args:
            colgroup (BSTColumnGroup)
        Exceptions:
            None
        Returns:
            None
        """
        for col in colgroup.columns:
            self.columns[col.name] = col
            self.groups[col.name] = colgroup
