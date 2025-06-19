from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Optional, Union, cast
from warnings import warn

from django.conf import settings
from django.db import ProgrammingError
from django.db.models import IntegerField, Value
from django.db.models.aggregates import Count
from django.db.models.expressions import Combinable

from DataRepo.models.utilities import (
    is_many_related_to_root,
    is_related,
    model_path_to_model,
    select_representative_field,
)
from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.views.models.bst.client_interface import BSTClientInterface
from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.field import BSTColumn
from DataRepo.views.models.bst.column.many_related_field import (
    BSTManyRelatedColumn,
)
from DataRepo.views.models.bst.column.many_related_group import BSTColumnGroup
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn


class BSTBaseListView(BSTClientInterface):
    """This "base" class (which inherits the client interface and Django ListView) is responsible for all of the
    automatic initialization of the columns in the Bootstrap Table, based on the model (set by a derivation of this
    class).  It uses the client interface to set the values that will be used in BSTListView to perform queries and
    serve results.

    Generally, this is a generic class-based view for a Model record list to make pages load faster, using server-side
    behavior for pagination.

    Examples:
        Note that you should inherit from BSTListView, but this is here in this middle class because this is the class
        that does the column setup.

        Simplest fully working example:

            class SampleList(BSTBaseListView):
                model = Sample

        Custom column selection example:

            class SampleList(BSTBaseListView):
                model = Sample
                column_ordering = ["name", "tissue", "animal", "time_collected", "handler"]
                exclude = ["id", "msrun_samples"]

        Fully customized example:

            class SampleList(BSTBaseListView):
                model = Sample
                column_ordering = ["name", "tissue", "animal", "time_collected", "handler"]
                exclude = ["id", "msrun_samples"]
                column_settings = {
                    "handler": {
                        "header": "Researcher",
                        filterer: {"choices": get_researchers}
                    }
                }

    Class Attributes:
        column_ordering (List[str]) [[]]: This is a list of column names (which can be a field_path, an annotation name,
            or a column group name).  An instance attribute is created and initialized using a copy of this class
            attribute.  The exclude attribute overrides (i.e. removes) anything added here.  You will see a warning if a
            manually added column name (either from a derived class override or via the constructor) is in the exclude
            list.  The default is an empty list, but the instance attribute automatically gets fields from self.model
            added in the order defined in the model class (except with many-related fields put at the end).  It is
            further appended to by columns supplied to the constructor (from the column_settings dict keys).
        column_settings (Dict[str, Union[dict, BSTBaseColumn, BSTColumnGroup]]): A dict keyed on column (or column
            group) name.  The name will be either a field path or annotation name.  The value can either be a dict of
            keyword arguments to the column or column group's constructor or an instance of BSTBaseColumn or
            BSTColumnGroup.
        annotations (Dict[str, Combinable]) [{}]: The bare minumum definition of annotations defined by a dict where the
            keys are the annotation name and the values are Combinables, e.g. 'Lower("fieldname")'.  The annotation name
            may be in column_ordering and the value can be overridden by what is provided to the constructor.
        exclude (List[str]) ["id"]: This is a list of column names (which can be either a field_path or annotation
            name).  And column name added here will cause a matching value in the column_rdering to be skipped.
    Instance Attributes:
        column_settings (Dict[str, Union[dict, BSTBaseColumn, BSTColumnGroup]]): A dict, keyed on column or column group
            name that contains settings for that column or column group.  Those settings are either in the form of a
            dict of kwargs for the column or column group class's constructor, or an instance of that class.
        column_ordering (List[str]): A list of column and/or column group names in the desired order they should appear.
            It defaults to the class attribute version of this instance attribute of the same name.  If that is not
            defined, it defaults to a list of fields from the model in the order they were defined with one exception:
            many-related fields are moved to the end.  Additionally, all columns from many-related models trigger the
            automatic inclusion of an annotation column for the count of those related records.
        columns (Dict[str, BSTBaseColumn]): A dict of column objects keyed on column name.  This includes columns from
            any/all column groups.
        groups (Dict[str, BSTColumnGroup]): A dict of column groups keyed on column group name.  This is only used to
            sort the delimited values of multiple many-related columns the same.
        sort_col (BSTBaseColumn): A shortcut to the user-selected sort-column, found via cookie.
    """

    # column_ordering default comes from: self.model._meta.get_fields()
    column_ordering: List[str] = []
    column_settings: Dict[str, Union[dict, BSTBaseColumn, BSTColumnGroup]] = {}
    annotations: Dict[str, Combinable] = {}
    exclude: List[str] = ["id"]

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
            ProgrammingError the sort_col's representative field is invalid
        Returns:
            None
        """
        super().__init__(**kwargs)

        # Copy what's in the class attribute to start
        self.column_settings = deepcopy(type(self).column_settings)
        # Initialize column settings
        self.init_column_settings(columns)

        # Initialize column order
        self.column_ordering: List[str] = type(self).column_ordering.copy()
        self.init_column_ordering()

        # Initialize columns (NOTE: This could add a Details BSTAnnotColumn)
        self.representative_column: Optional[BSTBaseColumn] = None
        self.columns: Dict[str, BSTBaseColumn] = {}
        self.groups: Dict[str, BSTColumnGroup] = {}
        self.init_columns()

        # Default the sort_col to the best representative
        if self.model is not None:
            sort_field = select_representative_field(
                self.model, force=True, subset=self.column_ordering
            )
            if isinstance(sort_field, str):
                self.sort_col = self.columns[sort_field]
                self.ordered = True
            else:
                raise ProgrammingError(
                    "Invalid return from select_representative_field"
                )
        elif len(self.columns.keys()) > 0:
            # Arbitrary column - does not matter without a model
            self.sort_col = list(self.columns.values())[0]
            self.ordered = True

        # Go through the columns and make sure that multiple many-related columns from the same related model are in
        # groups so that their delimited values sort together.
        self.add_check_groups()

    def init_interface(self):
        """An extension of the BSTClientInterface init_interface method, used here to set the sort_col.

        Call this method after setting the class's request object in the get method, but before calling super().get().

        Example:
            class MyBSTListView(BSTListView):
                def get(request, *args, **kwargs):
                    self.request = request
                    self.init_interface()
                    return super().get(request, *args, **kwargs)
        Args:
            None
        Exceptions:
            None
        Returns:
            response (HttpResponse)
        """
        super().init_interface()

        # Initialize a sort column (sort_col) based on the saved sort cookie (sort_name)
        if isinstance(self.sort_name, str):
            self.sort_col = self.columns[self.sort_name]

        # Set initial filter terms
        bad_filter_entries = []
        for colname, filter_term in self.filter_terms.items():
            if colname not in self.columns.keys():
                bad_filter_entries.append(colname)
                self.reset_column_cookie(colname, self.filter_cookie_name)
                warning = (
                    f"Invalid '{self.filter_cookie_name}' column encountered: '{colname}'.  "
                    "Resetting filter cookie."
                )
                self.warnings.append(warning)
                if settings.DEBUG:
                    warn(
                        warning
                        + f"  '{self.get_column_cookie_name(colname, self.filter_cookie_name)}'",
                        DeveloperWarning,
                    )
            else:
                self.columns[colname].filterer.initial = filter_term

        # Delete any bad filter term so that it doesn't come up in BSTListView when creating a Q expression
        for colname in bad_filter_entries:
            del self.filter_terms[colname]

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
        clear: bool = False,
    ):
        """Initializes self.column_settings.  Makes recursive calls with the columns from BSTColumnGroups.

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
            clear (bool) [False]: Whether the column_settings should start off fresh (or be added to).
        Exceptions:
            TypeError when a type is encountered in columns that is not supported.
            ValueError when a list is supplied containing a dict whose value is a str and does not match the key.  Or
                when a dict is supplied and the 'name' and/or 'field_path' key does not match the outer dict key.
                NOTE: Support for str values in a list of dicts is simply for convenience.  Both the key and value
                specify the field_path for a model field, intended to create a BSTColumn with all default settings.
        Returns:
            None
        """
        if clear:
            self.column_settings = {}

        # Add any annotations that were defined in the class attribute
        # NOTE: This could get overridden by what's in columns
        for annot_name, annot_expression in self.annotations.items():
            if annot_name in self.column_settings.keys():
                raise ProgrammingError(
                    f"The annotation column '{annot_name}' has been defined twice: once in "
                    f"{type(self).__name__}.column_settings and once in {type(self).__name__}.annotations."
                )
            self.column_settings[annot_name] = {"converter": annot_expression}

        if isinstance(columns, list):
            # CASE 1: A list was supplied, potentially containing:
            # - str (for a field_path, to use defaults for any BSTColumn, BSTRelatedColumn, or BSTManyRelatedColumn)
            # - dict (like str, but also including BSTAnnotColumn and/or custom settings. Must contain positional args.)
            # - BSTBaseColumn (BSTBaseColumn.name will be the key in the returned dict)
            # - BSTColumnGroup (BSTColumnGroup.name will be the key in the returned dict)
            for i, colobj in enumerate(columns):
                colkey = self.get_column_name(colobj, i)
                self.init_column_setting(colobj, colkey)
        elif isinstance(columns, dict):
            # CASE 2: A dict was supplied, potentially containing:
            # - str (for a field_path, to use defaults for any BSTColumn, BSTRelatedColumn, or BSTManyRelatedColumn.
            #   Must be identical to outer dict key.)
            # - dict (like str, but also including BSTAnnotColumn and/or custom settings. Must contain positional args.
            #   'name' must be identical to outer dict key.)
            # - BSTBaseColumn (BSTBaseColumn.name will be the key in the returned dict)
            # - BSTColumnGroup (BSTColumnGroup.name will be the key in the returned dict)
            for colkey, colobj in columns.items():
                self.init_column_setting(colobj, colkey)
        elif columns is not None:
            raise TypeError(
                f"Invalid columns type: '{type(columns).__name__}'.  Must be a dict or list."
            )

        # Add some complementary metadata columns for any many-related columns present
        if self.model is not None:
            self.add_default_many_related_column_settings()

    def get_column_name(
        self,
        colobj: Union[str, dict, BSTBaseColumn, BSTColumnGroup],
        index: Optional[int] = None,
    ) -> str:
        """Extracts the column name from the supplied object (which came from a list).

        Args:
            colobj (Union[str, dict, BSTBaseColumn, BSTColumnGroup])
            index (Optional[int]): The list index where the colobj came from (only for error reporting).
        Exceptions:
            TypeError when a colobj type is not supported.
        Returns:
            None
        """
        if isinstance(colobj, str):
            colkey = colobj
        elif isinstance(colobj, dict):
            colkey = self.prepare_column_kwargs(colobj)
        elif isinstance(colobj, BSTBaseColumn):
            colkey = colobj.name
        elif isinstance(colobj, BSTColumnGroup):
            colkey = colobj.name
        else:
            raise TypeError(
                "When supplying a list of all columns' settings, the value's type must be one of [str, dict, "
                f"BSTBaseColumn, or BSTColumnGroup], but the value of the column settings at index '{index}' was "
                f"'{type(colobj).__name__}'."
            )
        return colkey

    def init_column_setting(
        self,
        colobj: Union[str, dict, BSTBaseColumn, BSTColumnGroup],
        colkey: str,
    ):
        """Initializes a single entry in the self.column_settings dict.

        Args:
            colobj (Union[str, dict, BSTBaseColumn, BSTColumnGroup]): There are multiple ways to supply a columns'
                specification, but all result in setting the dict values contained to a dict, BSTBaseColumn, or
                BSTColumnGroup.
            colkey (str): The self.column_settings key, which must match the object's name (in the case of a dict, the
                value of the "name" key, or in the case of a str, the value of the str).
        Exceptions:
            TypeError when a colobj type is not supported.
            ValueError when a name does not match the colkey or when duplicate conflicting settings encountered.
        Returns:
            None
        """
        # Check that the colkey is valid
        if isinstance(colobj, str):
            if colkey != colobj:
                raise ValueError(
                    f"The column settings key '{colkey}' must be identical to the field_path string provided "
                    f"'{colobj}'."
                )
        elif isinstance(colobj, dict):
            # This checks that the name key matches the colkey
            self.prepare_column_kwargs(colobj, colkey)
        elif isinstance(colobj, BSTColumnGroup) or isinstance(colobj, BSTBaseColumn):
            if colkey != colobj.name:
                raise ValueError(
                    f"The column settings key '{colkey}' must be identical to the provided {type(colobj).__name__} "
                    f"name '{colobj}'."
                )
        else:
            raise TypeError(
                "When supplying a dict of all columns' settings, the value's type must be one of [str, dict, "
                "BSTBaseColumn, or BSTColumnGroup], but the value of the column settings at key "
                f"'{colkey}' was '{type(colobj).__name__}'."
            )

        # If settings already exist for this column, look for conflicts or update, where appropriate.
        if colkey in self.column_settings.keys():
            if isinstance(colobj, str):
                if settings.DEBUG:
                    warn(
                        f"Ignoring duplicate column setting (with just the column name) for column {colkey}.  Silence "
                        "this warning by removing the duplicate setting."
                    )
            elif isinstance(colobj, dict):
                if isinstance(self.column_settings[colkey], str):
                    # Replace the str with the dict settings and issue a warning about the duplicate
                    self.column_settings[colkey] = colobj
                    if settings.DEBUG:
                        warn(
                            "Overwriting duplicate column settings (with just the column name) with the supplied dict "
                            f"for column {colkey}.  Silence this warning by removing the duplicate setting."
                        )
                if isinstance(self.column_settings[colkey], dict):
                    # If the settings are a supplied annotation and the only key in the settings is the converter
                    # Annotations and strings are the only ones allowed to pre-exist.
                    # NOTE: Invalid mypy error, so ignoring 'union-attr'.  The conditional above literally checks:
                    # isinstance(self.column_settings[colkey], dict)
                    # error: Item "BSTColumnGroup" of "Union[dict[Any, Any], BSTBaseColumn, BSTColumnGroup]" has no
                    # attribute "keys"  [union-attr]
                    if colkey in self.annotations.keys() and list(
                        self.column_settings[colkey].keys()  # type: ignore[union-attr]
                    ) == ["converter"]:
                        if "converter" in colobj.keys():
                            raise ValueError(
                                f"Multiple BSTAnnotColumn converters defined.  "
                                f"Class default: '{self.annotations[colkey]}'.  "
                                f"Supplied via the constructor: '{colobj['converter']}'."
                            )
                        # Update the dict without complaining about the duplicate (because self.annotations doesn't
                        # allow custom settings, thus supplying those settings in the constructor is the only way to do
                        # it).
                        # NOTE: Invalid mypy error, so ignoring 'union-attr'.  The conditional above literally checks:
                        # isinstance(self.column_settings[colkey], dict)
                        # error: Item "BSTColumnGroup" of "Union[dict[Any, Any], BSTBaseColumn, BSTColumnGroup]" has no
                        # attribute "update"  [union-attr]
                        self.column_settings[colkey].update(colobj)  # type: ignore[union-attr]
                    else:
                        raise ValueError(
                            f"Multiple column settings dicts defined for column {colkey}."
                        )
                else:
                    raise ValueError(
                        f"Multiple column settings defined for column {colkey}.  A "
                        f"'{type(self.column_settings[colkey]).__name__}' and 'dict' were supplied."
                    )
            else:
                raise ValueError(
                    f"Multiple column settings defined for column {colkey}.  A "
                    f"'{type(self.column_settings[colkey]).__name__}' and '{type(colobj).__name__}' were supplied."
                )
        else:
            if isinstance(colobj, str):
                self.column_settings[colkey] = {}
            elif isinstance(colobj, dict):
                self.column_settings[colkey] = colobj
            elif isinstance(colobj, BSTBaseColumn):
                self.column_settings[colkey] = colobj
            elif isinstance(colobj, BSTColumnGroup):
                self.column_settings[colkey] = colobj
                # If the derived class provides settings for individual columns in a group, producing a conflict, an
                # error will be raised.
                # NOTE: Recursive call
                # NOTE: Invalid mypy error, so ignoring 'arg-type'.  'list[BSTManyRelatedColumn]' ISA:
                # 'list[Union[str, dict[Any, Any], BSTBaseColumn, BSTColumnGroup]]' because 'BSTManyRelatedColumn' is a
                # subclass of 'BSTBaseColumn'.
                # error: Argument 1 to "init_column_settings" of "BSTBaseListView" has incompatible type
                # "list[BSTManyRelatedColumn]"; expected "Union[list[Union[str, dict[Any, Any], BSTBaseColumn,
                # BSTColumnGroup]], dict[str, Union[str, dict[Any, Any], BSTBaseColumn, BSTColumnGroup]], None]"  [arg-
                # type]
                self.init_column_settings(colobj.columns)  # type: ignore[arg-type]

    def add_default_many_related_column_settings(self):
        """Adds an automatically generated count column to complement each many-related column (if that column is not
        excluded).

        NOTE: The count added here is distinct and results may not be what you might expect.  For example:
        Animal.last_serum_sample links to Sample using ForeignKey.  You might think this is a 1:1 relationship, but that
        is wrong.  When gathering many-related fields for the Sample model, "Animal.last_serum_sample's related name is
        "Sample.animals".  There are other serum samples that are not the last that link to that animal and in turn,
        link to the last serum sample record, making the Sample.animals relationship many to 1.  They all just happen to
        link to the same animal.  This is why we supply distinct=True.

        Assumptions:
            1. If a column already exists with the generated annotation name, assume it contains a distinct count.
        Args:
            None:
        Exceptions:
            None
        Returns:
            None
        """
        mmfields = [
            f
            for f in self.model._meta.get_fields()
            if is_many_related_to_root(f.name, self.model)
        ]
        for fld in mmfields:
            count_annot_name = BSTManyRelatedColumn.get_count_name(fld.name, self.model)

            if (
                (
                    fld.name not in self.exclude
                    or self.many_related_columns_exist(fld.name)
                )
                and count_annot_name not in self.exclude
                and count_annot_name not in self.column_settings.keys()
            ):
                self.column_settings[count_annot_name] = BSTAnnotColumn(
                    count_annot_name,
                    Count(fld.name, output_field=IntegerField(), distinct=True),
                    header=(
                        underscored_to_title(
                            BSTManyRelatedColumn.get_attr_stub(fld.name, self.model)
                        )
                        + " Count"
                    ),
                    filterer="strictFilterer",
                    sorter="numericSorter",
                )

    def many_related_columns_exist(self, mr_model_path: str):
        """Checks if a supplied many-related model path is the parent of any field among the column_settings keys or
        column_ordering field paths.

        Assumptions:
            1. mr_model_path ends in a foreign key field
            2. The self.column_settings have been initialized
        Args:
            mr_model_path (str): A dunderscore-delimited path to a foreign key that is many-related to the root model.
        Exceptions:
            None
        Returns:
            (bool): True if any field defined among the column_settings or column_ordering starts with the mr_model_path
        """
        return any(
            f.startswith(f"{mr_model_path}__") for f in self.column_ordering
        ) or any(
            f.startswith(f"{mr_model_path}__") for f in self.column_settings.keys()
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
            self.column_ordering = type(self).column_ordering.copy()
        else:
            # Remove excludes
            self.column_ordering = [
                f for f in self.column_ordering if f not in self.exclude
            ]

        # Supporting the model being None to mimmick Django's ListView
        if self.model is not None:
            # Add the defaults

            for fld, many_related in sorted(
                [
                    (f, is_many_related_to_root(f.name, self.model))
                    for f in self.model._meta.get_fields()
                ],
                key=lambda tpl: tpl[1] is True,
            ):
                if many_related is True:
                    # Add an automatically generated count column to complement many-related columns
                    count_annot_name = BSTManyRelatedColumn.get_count_name(
                        fld.name, self.model
                    )
                    if fld.name not in self.exclude:
                        self.add_to_column_ordering(count_annot_name, _warn=False)
                self.add_to_column_ordering(fld.name, _warn=False)

        # Add from the annotations
        for annot_name in self.annotations.keys():
            self.add_to_column_ordering(annot_name)

        # Add from the settings dict
        for colname, obj in self.column_settings.items():
            self.add_to_column_ordering(colname)
            if isinstance(obj, BSTColumnGroup):
                for col in obj.columns:
                    self.add_to_column_ordering(col.name)

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
        """Traverses self.column_ordering and populates self.columns with BSTBaseColumn objects and self.groups with
        BSTColumnGroup objects by calling init_column for each column name in self.column_ordering.

        Assumptions:
            1. Every column name in self.column_ordering is present in self.column_settings.
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        details_link_exists = False
        for colname in self.column_ordering:
            self.init_column(colname)

            # See if the derived class specified a linked column (to the row's details page)
            if colname not in self.groups.keys() and self.columns[colname].linked:
                details_link_exists = True
                # Only make the first linked column the representative (if there are multiple)
                if not details_link_exists:
                    self.representative_column = self.columns[colname]

        # If no representative exists and the model has a detail page, automatically set a representative
        if not details_link_exists and BSTBaseColumn.has_detail(self.model):
            rep_colname = select_representative_field(
                self.model, subset=self.column_ordering
            )
            # If no representative could be chosen
            if rep_colname is None:
                # Append a details column containing only the linked text "details"
                details = BSTAnnotColumn("details", Value("details"), linked=True)
                self.column_ordering.append(details.name)
                self.columns[details.name] = details
            else:
                self.columns[rep_colname].linked = True
                self.representative_column = self.columns[rep_colname]

    def init_column(self, colname: str):
        """Takes a column name, creates a derived instance of the BSTBaseColumn class, and adds it to self.columns.

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
            column_kwargs_or_obj = self.column_settings.get(colname)
            if isinstance(column_kwargs_or_obj, BSTColumnGroup):
                # A group's columns are individually added to self.column_settings
                self.groups[colname] = column_kwargs_or_obj
                return
            elif isinstance(column_kwargs_or_obj, BSTBaseColumn):
                self.columns[colname] = column_kwargs_or_obj
                return
            elif isinstance(column_kwargs_or_obj, dict):
                # Copy, because we pop the converter if it's an annot field below
                kwargs = column_kwargs_or_obj.copy()
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

        elif len(kwargs.keys()) > 0 and not hasattr(self.model, first_field):
            raise ValueError(
                f"Unable to determine column type for column '{colname}'.  The column name does not appear to be a "
                "field path (no occurrences of '__') or annotation (no converter[/expression] in the column settings) "
                f"and '{first_field}' is not a field in the model '{self.model.__name__}'."
            )
        else:
            raise ValueError(
                f"Unable to determine column type for column '{colname}'.  There was no 'converter' provided in the "
                f"kwargs: {kwargs}."
            )

        # Collect a unique set of javascripts needed by the columns
        for script in self.columns[colname].javascripts:
            if script not in self.javascripts:
                self.javascripts.append(script)

    def add_check_groups(self):
        """Go through the columns and make sure that multiple columns from the same related model are in a column group
        so that their delimited value sorts are synchronized.  Adds BSTColumnGroup objects to self.groups (if missing).

        Assumptions:
            1. Multiple groups from the same related model path do not exist.
        Limitations:
            1. Multiple groups from the same related model path are not supported.
            2. This does not add new column groups to self.column_ordering
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # The column group object name can be chosen arbitrarily by the developer of the derived class, so we can't rely
        # on the automatically generated column group name to equate groups.  We must use the related model.
        existing_groups_by_model: Dict[str, BSTColumnGroup] = {}
        for group in self.groups.values():
            existing_groups_by_model[group.many_related_model_path] = group

        cols_by_model: Dict[str, List[BSTManyRelatedColumn]] = defaultdict(list)
        for colname in self.column_ordering:
            # There can be group names in the column_rdering (to allow developers to shortcut its creation)
            if colname not in self.groups.keys():
                col = self.columns[colname]
                # If this is a many-related column that has a field_path longer than 1 foreign key (i.e. ignore sole
                # many-related foreign keys that the developer should have removed)
                if isinstance(col, BSTManyRelatedColumn) and "__" in col.field_path:
                    cols_by_model[col.many_related_model_path].append(col)

        # Loop through the automatically generated groups and if the group doesn't exist, create it.  If it does exist,
        # check it.
        for mr_model_path, columns in cols_by_model.items():
            if (
                mr_model_path not in existing_groups_by_model.keys()
                and len(columns) > 1
            ):
                group_name = BSTColumnGroup.get_or_fix_name(mr_model_path)

                if group_name in self.groups.keys():
                    raise ProgrammingError(
                        f"Automatically generated group column name conflict: '{group_name}' for columns: {columns}"
                    )

                self.groups[group_name] = BSTColumnGroup(
                    *columns,
                    initial=self._get_group_representative(mr_model_path, columns),
                )

            elif settings.DEBUG and len(columns) > 1:
                custom = set(
                    [c.name for c in existing_groups_by_model[mr_model_path].columns]
                )
                expected = set([c.name for c in columns])

                missing = expected - custom
                unexpected = custom - expected

                if len(missing) > 0 or len(unexpected) > 0:
                    msg = ""
                    if len(missing) > 0:
                        msg += (
                            f"  {len(missing)} column(s) that go through the same many-related model were not in the "
                            f"group: {missing}.  Please add them."
                        )
                    if len(unexpected) > 0:
                        msg += (
                            f"  {len(unexpected)} column(s) were unexpected: {unexpected}.  There must be a bug.  "
                            "Please report this warning."
                        )
                    warn(
                        f"Manually created column group '{existing_groups_by_model[mr_model_path]}' for related model "
                        f"'{mr_model_path}' is not as expected.{msg}",
                        DeveloperWarning,
                    )

    def _get_group_representative(
        self, mr_model_path: str, columns: List[BSTManyRelatedColumn]
    ):
        """Takes a many-related model path and a list of BSTManyRelatedColumns from that model path and returns a
        selected representative field_path of one of the columns.  The field path will be the best for sorting the
        delimited values in the columns."""
        # Trim off the mr_model_path from the field_paths of each column
        relative_subset = []
        orig_subset_lookup = {}
        for col in columns:
            if col.field_path.startswith(f"{mr_model_path}__"):
                relative_fld = col.field_path.replace(f"{mr_model_path}__", "", 1)
                relative_subset.append(relative_fld)
                orig_subset_lookup[relative_fld] = col.field_path
            else:
                raise ProgrammingError(
                    f"The field path in column '{col}' does not have the expected length."
                )

        # use the relative field paths to select the best representative field in the group to sort its
        # delimited values by
        relative_representative = select_representative_field(
            model_path_to_model(self.model, mr_model_path),
            force=True,
            subset=relative_subset,
        )
        # This assumes that there are not duplicate columns
        representative = orig_subset_lookup[str(relative_representative)]

        return representative

    def get_context_data(self, **_):
        """An override of the superclass method to provide context variables to the page.  All of the values are
        specific to pagination and BST operations."""

        context = super().get_context_data()

        # The column objects contain all of the column details
        context.update({self.columns_var_name: self.columns})

        return context
