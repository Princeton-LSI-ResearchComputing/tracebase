from functools import reduce
from typing import Callable, Dict, List, Optional, Union

from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.widgets import BSTHeader, BSTValue


class BSTSorters(dict):
    def __init__(self, ALPHANUMERIC: str, NUMERIC: str, HTML: str):
        self.ALPHANUMERIC = ALPHANUMERIC
        self.NUMERIC = NUMERIC
        self.HTML = HTML
        sorters = {
            "ALPHANUMERIC": self.ALPHANUMERIC,
            "NUMERIC": self.NUMERIC,
            "HTML": self.HTML,
        }
        super().__init__(**sorters)


class BootstrapTableColumn:
    """Class to represent the interface between a bootstrap column and a Model field.

    Usage: Use this class to populate the BootstrapTableListView.columns list, like this:

        BootstrapTableListView(
            BSTColumn("filename"),
            BSTColumn("imported_timestamp"),
            BSTColumn("data_format__name"),
            BSTColumn("data_type__name"),
        )

    Use django "field paths" relative to the base model, or model annotation names for the name arguments to the
    constructor, as BootstrapTableListView uses these for server-side pagination, filtering, and sorting.

    Alter whatever settings you want in the constructor calls.  In the BootstrapTableListView's template, all you have
    to do to render the th tag for each column is just use the name:

        {{ filename }}
        {{ imported_timestamp }}
        {{ data_format__name }}
        {{ data_type__name }}

    It will render the column headers (by default) using a title version of the last 2 values in django's dunderscore-
    delimited field path.  For example, the header generated from the above objects would be:

        Filename
        Imported Timestamp
        Data Format Name
        Data Type Name

    It's also important to note that in order for BootstrapTableListView's search and sort to work as expected, each
    column should be converted to a simple string or number annotation that is compatible with django's annotate method.
    For example, as a DateTimeField, imported_timestamp, will sort correctly on the server side, but Bootstrap Table
    will sort the page's worth of results using alphanumeric sorting.  You can make the sorting behavior consistent by
    supplying a function using the converter argument, like this:

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
    """

    FILTER_CONTROL_CHOICES = {
        "INPUT": "input",  # default
        "SELECT": "select",
        "DATEPICKER": "datepicker",
        "DISABLED": "",
    }
    SORTER_CHOICES = BSTSorters(
        ALPHANUMERIC="alphanumericSorter",  # default.  See static/js/htmlSorter.js
        NUMERIC="numericSorter",  # See static/js/htmlSorter.js
        HTML="htmlSorter",  # See static/js/htmlSorter.js
    )
    NAME_IS_FIELD = "__same__"

    DEF_DELIM = "; "  # For many-related fields

    def __init__(
        self,
        name: str,  # field path or annotation name (see converter)
        field: Optional[Union[str, List[str]]] = NAME_IS_FIELD,
        header: str = None,
        is_fk: bool = False,
        many_related: bool = False,
        sortable: bool = True,
        sorter: Optional[str] = None,
        visible: bool = True,
        exported: bool = True,
        searchable: Optional[bool] = None,
        filter_control: Optional[str] = None,
        select_options: Optional[Union[Dict[str, str], List[str]]] = None,

        # Advanced (some assign reasonable defaults)
        converter: Optional[Callable] = None,
        related_model_path: Optional[Union[str, List[str]]] = None,  # default = field's immediate parent
        related_sort_fld: Optional[Union[str, List[str]]] = None,  # default = {related_model_path}__pk
        related_sort_fwd: bool = True,
        sort_nocase: bool = False,  # Case insensitive sort
        many_related_delim: str = DEF_DELIM,
        mm_list_name: str = None,
        mm_count_annot_name: int = None,
        strict_select: Optional[bool] = None,
        # Tell BSTListView to link the column's values the detail page via object.get_absolute_url
        link_to_detail: Optional[bool] = None,
        th_template: Optional[str] = "DataRepo/widgets/bst_th.html",  # The template gets context variables "column": self, "object": model record, and "related_objects": dict of {object.pk: {column.name: list}}
        td_template: Optional[str] = "DataRepo/widgets/bst_td.html",  # The template gets context variables "column": self, "object": model record, and "related_objects": dict of {object.pk: {column.name: list}}
        value_template: Optional[str] = "DataRepo/widgets/bst_value.html",  # The template gets context variables "column": self, "object": model record, and "related_objects": dict of {object.pk: {column.name: list}}
    ):
        """Defines options used to populate the bootstrap table columns for a BootstrapListView and a single reference
        model.

        Args:
            name (str): The data-field attribute of a th tag.  Used for cookies too.  If filter_control is not None,
                this value should be a valid Django annotation name or database field filter path relative to the
                BootstrapListView.model that contains this instance.
            field (Optional[Union[str, List[str]]]) ["__same__"]: Name of the database field or fields corresponding to
                the column.  Supply this explicitly if the name argument is an annotation or set to None if no
                corresponding model field exists (or is desired for search, filter, or sort).  "__same__" means that
                name is a model field.  If a list, the related fields must all be CharFields.
            converter (Optional[Callable]): A method to convert a database field to a CharField.  This is necessary for
                searching and filtering because BST only does substring searches.  It also prevents sorting from
                increasing the number of resulting rows if there is a many-related field (i.e. when field is a list).
            is_fk (bool) [False]: The field at the end of the field's path is a foreign key.  This is not used in this
                class, but it is used in conjunction with the related_sort_fld in BSTListView.  This differs from
                many_related in that the field at the end of a field path of a many_related column is not necessarily a
                foreign key, but it contains a many-related foreign key *in* its path.
            many_related (bool) [False]: If this field is a reverse relation, e.g. the link resides in another model
                that links to "this" model or the link is a ManyToManyField, setting this value to True ensures that the
                number of rows in the table accurately reflects the number of records in the reference model when
                sorting is performed on this column.  It does this by sorting on the single annotated value instead of
                the related field.  Django has this left-join side effect when you sort on a many-related field.  By
                sorting on the annotated field, that row increase side-effect is prevented.  Setting this to True means
                that an annotation will be automatically created (unless a converter is supplied).  If there are
                multiple linked records, either a Min() or Max() will be applied if the user selects to sort on this
                column.  You can render whatever you link in the column, but to have each page sorted correctly by
                bootstrap, you should set the annotated value as a hidden element.  The overall DB sort in the query
                will be based on the annotated value.  Ideally, that value would be a joined string of all of the
                related values, but all those functions are postgres-specific. Example for AnimalListView:
                    BootstrapTableColumn("study", field="studies__name", many_related=True) If an animal belongs to
                    multiple studies and the user selects to do an ascending sort on the study column, the "study" field
                    will be defined as and the order_by in the query will look like:
                        animal_queryset.annotate("study"=Min("studies__name")).order_by("study").distinct()
                    It's also notable that any filtering will still be on the DB field (and it will not affect the
                    number of resulting records), so a search for a study name "My Study":
                        animal_queryset.filter(studies__name__icontains="My Study")
                    Together, it looks like this:
                        animal_queryset
                            .filter(studies__name__icontains="My Study") .annotate("study"=Min("studies__name"))
                            .order_by("study") .distinct()
                Note, if many_related is true, name must differ from field.
            exported (bool) [True]: Adds to BST's exportOptions' ignoreColumn attribute if False.
            searchable (Optional[bool]) [True]: Whether or not a column is searchable.  Searchable being True is
                mutually exclusive with filter_control being None.  Automatically set to True if filter_control is not
                None.  It is set to False is filter_control is None.
            filter_control (Optional[str]) ["input"]: Set to "" to disable.  Must be in FILTER_CONTROL_CHOICES.
                This cannot be None if searchable is True.
            select_options (Optional[Union[Dict[str, str], List[str]]]): A dict or a list of select list options when
                filter_control is "select".  Supplying this argument will default filter_control to "select".
            strict_select (Optional[bool]): Ignored if select_options is not defined.  This apps an entire table
                attribute 'data-filter-strict-search' to the the filter_data context variable.
            sortable (bool) [True]
            sorter (Optional[str]) ["alphanum"]: Must be in SORTER_CHOICES.
            visible (bool) [True]: Controls whether a column is initially visible.
            header (Optional[str]) [auto]: The column header to display in the template.  Will be automatically
                generated using the title case conversion of the last (2, if present) dunderscore-delimited name values.
            related_model_path (Optional[Union[str, List[str]]]) [field]: This is a dunderscore-delimited path to a
                foreign key field for a related model (relative to the base model for which the path can be used in a
                filter).  Its only utility in this class is for making other fields under the same many_related model
                sort the same way (but it is also used outside this class to sort rows in the BSTListView in a specific
                field instead of by the value of the primary key).  All many_related columns will be delimited and
                sorted by their own value.  You can optionally sort based on any field under this parent model, and even
                via relations to that model, but there must be no many-to-many relations occurring in the field's path
                after that model.  Every field that should sort the same way must also have the same value for
                related_model_path.  E.g. if you have these 3 fields:
                    animal__infusate__tracer_links__tracer__name
                    animal__infusate__tracer_links__tracer__compound__id
                    animal__infusate__tracer_links__concentration
                and you want their delimited values to sort the same way, you can supply
                    related_model_path="animal__infusate__tracer_links"
                but note that both field paths MUST each start with that model, i.e. they must all start with:
                    "animal__infusate__tracer_links"
                The result will be if the user has sorted on the field/column for
                "animal__infusate__tracer_links__concentration", the other two columns will also be sorted by the values
                in that concentration field. This option is ignored if many_related is False.
            related_sort_fld (Optional[str]) [related_model_path + "__pk"]: The default sort field for foreigh key
                fields.  This should always be relevant to THIS column.  It is how you want THIS column to sort in
                BSTListView.  See BSTColumnGroup for default sorting based on another column.
            related_sort_fwd (bool) [True]: Set to False to reverse sort by default.
            sort_nocase (bool) [False]: If True, it makes the sort case-insensitive.
            link_to_detail (Optional[bool]): Tell BSTListView to link the value in this column to its detail page.  The
                BSTListView.model must have a "get_absolute_url" method and the column cannot be a relation.
            td_template (Optional[str]) ["DataRepo/widgets/bst_td.html"]: The template path to a template file to use
                for rendering a td vag with a value from a Model object.
        Exceptions:
            ValueError when: - Either many_related must be True or a converter must be supplied if the BST column name
            is not equal to
              the model field name.
            - The BST column name must differ from the model field name when either a converter is supplied or
              many_related is True.
            - filter_control value is not among FILTER_CONTROL_CHOICES.
            - sorter value is not among SORTER_CHOICES.
            - searchable and filter_control values are conflicting.
        Returns:
            instance (BootstrapTableColumn)
        """
        self.name = name
        self.header_orig = header
        self.header = (
            underscored_to_title("_".join(name.split("__")[-2:]))
            if header is None
            else header
        )

        # The field is the same as the name unless explicitly set differently
        # A field should be a list if multiple models link to the table's base model.  A dynamic converter will be
        # dynamically created if not explicitly set to a static converter.  E.g. for a field in a many-related model, a
        # Min or Max function converter will be used based on sort order.  If field is a list, Coalesce will be used.
        # And if both field is a list and many-related, Coalesce will be combined with Min and Max.
        self.field_orig = field
        self.field = (
            field
            if field is None or isinstance(field, list) or field != self.NAME_IS_FIELD
            else name
        )
        self.converter = converter

        # Some columns are from fields in many-related models that are either many to 1 or many to many.  They are
        # displayed with delimeters in a single table cell, and if multiple columns have fields from the same
        # many-related model, they are sorted the same relative to one another, in their delimited order.
        self.is_fk = is_fk
        self.many_related = many_related
        self.related_model_path_orig = related_model_path
        self.related_model_path = related_model_path
        self.related_sort_fld = related_sort_fld
        self.related_sort_fld_orig = related_sort_fld  # Used for updating sort fields in column groups
        self.related_sort_fwd = related_sort_fwd
        self.sort_nocase = sort_nocase
        self.delim = many_related_delim
        self.init_related()

        # A column is an annotation on a queryset if it has a converter, is many-related, or field is a list (i.e.
        # multiple models link to the table's base model)
        self.is_annotation = (
            self.converter is not None
            or isinstance(self.field, list)
            or self.many_related
        )

        # Export/display
        self.exported = exported
        self.visible = "true" if visible else "false"
        # DEBUG: Experimenting with sending the column to the widget's constructor to see if it is updated
        self.header_widget = BSTHeader(self)
        self.cell_widget = BSTValue(self)
        # TODO: Change this to a value_template, defaulting to DataRepo/widgets/bst_value.html
        self.th_template = th_template
        self.value_template = value_template
        self.td_template = td_template
        self.link_to_detail = link_to_detail

        # Filtering
        # - inputs and select lists (The input elements under each column header)
        self.filter_control_orig = filter_control
        self.searchable_orig = searchable
        self.select_options_orig = select_options
        self.filter = ""
        self.strict_select = strict_select
        self.init_filters()

        # Sorting
        # - The triangles to sort by a column next to each header
        self.sortable = "true" if sortable else "false"
        self.sorter = sorter
        if self.sorter is None:
            self.sorter = self.SORTER_CHOICES.ALPHANUMERIC
        elif self.sorter not in self.SORTER_CHOICES.values():
            raise ValueError(
                f"Invalid sorter value: '{sorter}'.  "
                f"Valid choices are: {list(self.SORTER_CHOICES.values())}."
            )

        # Validate the settings
        self.validate()

        self.mm_list = mm_list_name
        self.mm_count = mm_count_annot_name

    def __str__(self):
        return self.th
        # return self.render_th()

    def render_th(self, attrs=None):
        return self.header_widget.render()
        # return self.header_widget.render(
        #     f"{self.name}_header",
        #     self,
        #     attrs=attrs,
        # )

    @property
    def th(self):
        print(f"Rendering th for {self.name}")
        th = self.header_widget.render()
        print(f"GOT: {th}")
        return self.header_widget.render()

    @property
    def td(self):
        return self.td_template
        # return self.cell_widget.render()

    def init_related(self):
        """Initializes attributes related to the field for the column being many-related to the base table or when the
        field is a foreign key.

        Args:
            None
        Exceptions:
            ValueError when a field path is invalid.
        Returns:
            None
        """
        if (self.is_fk or self.many_related) and self.related_model_path is None and self.field is not None:
            # Default the related_model_path to the immediate parent model
            if isinstance(self.field, list):
                self.related_model_path = []
                for f in self.field:
                    path = f.split("__")
                    if len(path) > 1:
                        self.related_model_path.append("__".join(path[0:-1]))
                    elif len(path) == 1:
                        # assume it ends in a foreign key
                        self.related_model_path.append(f)
                    else:
                        raise ValueError(f"Invalid field path '{f}' in column '{self.name}'.")
            else:
                path = self.field.split("__")
                if len(path) > 1:
                    self.related_model_path = "__".join(path[0:-1])
                elif len(path) == 1:
                    # assume it ends in a foreign key
                    self.related_model_path = self.field
                else:
                    raise ValueError(f"Invalid field path '{self.field}' in column '{self.name}'.")

        if (self.is_fk or self.many_related) and self.related_sort_fld is None:
            # Default to sorting M:M field values using the actual field itself.  Note, this will effectively be the
            # primary key of the related model if the field is a foreign key.  BSTColumnGroup will overwrite this on the
            # fly if sorting by a neighboring column in the group, hence this also ends up being saved in
            # related_sort_fld_orig.
            if isinstance(self.related_model_path, list):
                self.related_sort_fld = self.field[:]
                self.related_sort_fld_orig = self.field[:]
            else:
                print(f"SETTING related_sort_fld TO FIELD: {self.field}")
                self.related_sort_fld = self.field
                self.related_sort_fld_orig = self.field

    def init_filters(self):
        """Initializes attributes related to row filtering mechanisms in BST.

        Args:
            None
        Exceptions:
            ValueError when a value is invalid or multiple values are conflicting.
        Returns:
            None
        """
        default_filter_control = (
            self.FILTER_CONTROL_CHOICES["INPUT"]
            if self.select_options_orig is None
            else self.FILTER_CONTROL_CHOICES["SELECT"]
        )
        if self.filter_control_orig is None:
            if self.searchable_orig is None or self.searchable_orig is True:
                # Full default settings
                self.filter_control = default_filter_control
                self.searchable = True
            else:
                print(f"DISABLING FILTER CONTROL BECAUSE self.searchable_orig = {self.searchable_orig}")
                self.searchable = False
                self.filter_control = self.FILTER_CONTROL_CHOICES["DISABLED"]
        elif self.filter_control_orig in self.FILTER_CONTROL_CHOICES.values():
            self.filter_control = (
                self.filter_control_orig
                if self.filter_control_orig is not None and self.filter_control_orig != ""
                else ""
            )
            tmp_searchable = self.filter_control_orig is not None and self.filter_control_orig != ""
            if self.searchable_orig is None:
                self.searchable = tmp_searchable
            elif self.searchable_orig != tmp_searchable:
                raise ValueError(
                    f"Conflict between searchable '{self.searchable_orig}' and filter_control "
                    f"'{self.filter_control_orig}'.  searchable must be False if filter_control is not None."
                )
            else:
                self.searchable = self.searchable_orig
            self.filter_control = (
                self.filter_control_orig
                if self.filter_control_orig is not None and self.filter_control_orig != ""
                else ""
            )
        else:
            raise ValueError(
                f"Invalid filter_control value: '{self.filter_control_orig}'.  "
                f"Valid choices are: {list(self.FILTER_CONTROL_CHOICES.values())}."
            )

        if self.select_options_orig is None:
            self.select_options = None
        elif isinstance(self.select_options_orig, dict):
            self.select_options = self.select_options_orig
        else:  # list
            # NOTE: This filters for a unique case-insensitive sorted dict, and arbitrarily uses the first case instance
            # encountered.  I.e., it does not produce a lower-cased output dict - just a dict that is unique when case
            # is ignored for uniqueness.
            self.select_options = dict(
                (opt, opt)
                for opt in sorted(
                    reduce(
                        lambda lst, val: lst + [val] if str(val).lower() not in [str(v).lower() for v in lst] else lst,
                        self.select_options_orig,
                        [],
                    ),
                    key=str.casefold,
                )
            )
        print(f"self.select_options = {self.select_options}, self.filter_control = {self.filter_control}")

    def validate(self):
        """Validates instance attributes.

        Args:
            None
        Exceptions:
            TypeError when different arguments must have a consistent type.
            ValueError
                Conflicting arguments
                Invalid arguments
                Inconsistent arguments
                Required arguments
        Returns:
            None
        """
        if self.field is not None:
            if (
                isinstance(self.field, str)
                and self.name != self.field
                and self.converter is None
                and not self.many_related
                and not self.is_fk
            ):
                raise ValueError(
                    "Either many_related or is_fk must be True or a converter must be supplied if the BST column name "
                    f"'{self.name}' is not equal to the model field name '{self.field_orig}'."
                )
            elif (
                (self.converter is not None or self.many_related)
                and isinstance(self.field, str)
                and self.name == self.field_orig
            ):
                raise ValueError(
                    f"The BST column name '{self.name}' must differ from the model field name '{self.field_orig}' when "
                    "either a converter is supplied or many_related is True.\n"
                    "In the case of 'many_related', the name must differ in order to create an annotated field for "
                    "sorting, so as to prevent artificially increasing the number of rows in the resulting table due "
                    "to a left-join side-effect of sorting in the ORM."
                )
            elif (
                (self.converter is not None or self.many_related)
                and isinstance(self.field, str)
                and self.field_orig == self.NAME_IS_FIELD
            ):
                raise ValueError(
                    f"The BST column field must be supplied and set to a different value from the name '{self.name}' "
                    "when either a converter is supplied or many_related is True.\n"
                    "In the case of 'many_related', the name must differ in order to create an annotated field for "
                    "sorting, so as to prevent artificially increasing the number of rows in the resulting table due "
                    "to a left-join side-effect of sorting in the ORM."
                )

        if self.related_model_path_orig is not None:
            if type(self.related_model_path_orig) != type(self.field) or (
                isinstance(self.field, list) and len(self.related_model_path_orig) != len(self.field)
            ):
                raise TypeError(
                    f"field and related_model_path must be the same type and size (if list type).  "
                    f"[{type(self.related_model_path_orig).__name__} != {type(self.field).__name__}]"
                )
            if isinstance(self.field, list):
                for i, fld in enumerate(self.field):
                    sort_fld_mdl = self.related_model_path_orig[i] + "__"
                    if not fld.startswith(sort_fld_mdl):
                        raise ValueError(
                            f"The field path of column '{self.name}': '{fld}' must start with related_model_path "
                            f"('{self.related_model_path_orig[i]}') in order to link the sort of delimited values in "
                            "this column with those in other columns."
                        )
            else:
                sort_fld_mdl = self.related_model_path_orig + "__"
                if not self.related_sort_fld.startswith(sort_fld_mdl):
                    raise ValueError(
                        f"The field path of column '{self.name}': '{self.field}' must start with related_model_path "
                        f"('{self.related_model_path_orig}') in order to link the sort of delimited values in this "
                        "column with those in other columns."
                    )

        if self.related_sort_fld is not None:
            if type(self.related_sort_fld) != type(self.field) or (
                isinstance(self.field, list) and len(self.related_sort_fld) != len(self.field)
            ):
                raise TypeError(
                    f"field and related_sort_fld's must be the same type and size (if they are lists).  "
                    f"[{type(self.related_sort_fld).__name__} != {type(self.field).__name__}]"
                )
            if isinstance(self.field, list):
                for i, fld in enumerate(self.related_sort_fld):
                    if not fld.startswith(self.related_model_path[i]):
                        raise ValueError(
                            f"The default sort field path of column '{self.name}': '{fld}' must start with "
                            f"related_model_path ('{self.related_model_path[i]}') in order to link the sort of "
                            "delimited values in this column with those in other columns."
                        )
            else:
                if not self.related_sort_fld.startswith(self.related_model_path):
                    raise ValueError(
                        f"The default sort field path of column '{self.name}': '{self.related_sort_fld}' must "
                        f"start with related_model_path ('{self.related_model_path}') in order to link the sort "
                        "of delimited values in this column with those in other columns."
                    )

        if self.link_to_detail is True and (self.is_fk or self.many_related):
            raise ValueError(
                f"link_to_detail for column '{self.name}' is mutually exclusive with foreign key and/or many-related "
                "columns."
            )

    @classmethod
    def field_to_related_model_path(cls, field: str, many_related=False):
        """Turns a django field path into a related model path.

        Example:
            mz_to_msrunsamples__sample__animal__studies__id ->
            mz_to_msrunsamples__sample__animal__studies
        Limitations:
            1. Field paths in a many-related models not ending in a foreign key are not truncated when the path is 1
                item long.  This method has no means of confirming whether a value in the path is a foreign key or not,
                so this trick will not work if there is a path longer than 1 item that ends in a foreign key.
        Args:
            field (str): A django dunderscore delimited field path.
            many_related (bool) [False]: Making this True causes a single path item to not be chopped off if it's the
                only item.
        Exceptions:
            None
        Returns:
            field (Optional[str]): The input field path with the field chopped off the end or not if many related and
                there's a sinle dunderscore-delimited value.
        """
        path = field.split("__")
        if len(path) > 1:
            return "__".join(path[0:-1])
        elif many_related and len(path) == 1:
            # assume it ends in a foreign key
            return field
        return None

    def __eq__(self, other):
        """This is a convenience override to be able to compare a column name with a column object to see if the object
        is for that column.  It also enables the `in` operator to work between strings and objects."""
        if isinstance(other, __class__):
            print("TESTING COLUMN CLASSES EQUAL")
            return self.name == other.name
        elif isinstance(other, str):
            print("TESTING COLUMN CLASS AND STRING EQUAL")
            return self.name == other
        elif other is None:
            print("TESTING COLUMN CLASS AND NONE EQUAL")
            return False
        else:
            raise NotImplementedError(f"Equivalence of {__class__.__name__} to {type(other).__name__} not implemented.")
