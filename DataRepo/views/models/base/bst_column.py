from collections import defaultdict
from functools import reduce
from typing import Callable, Dict, List, Optional, Union

from DataRepo.utils.text_utils import underscored_to_title
from DataRepo.widgets import BSTHeader


class BootstrapTableColumn:
    """Class to represent the interface between a bootstrap column and a Model field.

    Usage: Use this class to populate the BootstrapTableListView.columns list, like this:

        self.columns = [
            BSTColumn("filename"),
            BSTColumn("imported_timestamp"),
            BSTColumn("data_format__name"),
            BSTColumn("data_type__name"),
        ]

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

    Instance Attributes:
        name
        converter
        delim
        exported
        field
        filter
        filter_control
        header
        many_related
        searchable
        select_options
        sortable
        sorter
        strict_select
        visible
        widget
    """

    FILTER_CONTROL_CHOICES = {
        "INPUT": "input",  # default
        "SELECT": "select",
        "DATEPICKER": "datepicker",
        "DISABLED": "",
    }
    SORTER_CHOICES = {
        "ALPHANUMERIC": "alphanum",  # default
        "NUMERIC": "numericOnly",
        "HTML": "htmlSorter",  # See static/js/htmlSorter.js
    }
    NAME_IS_FIELD = "__same__"

    # TODO: Make this into an instance attribute
    DEF_DELIM = "; "  # For many-related fields

    def __init__(
        self,
        name: str,  # field path or annotation name (see converter)
        field: Optional[Union[str, List[str]]] = NAME_IS_FIELD,
        header: str = None,
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
        many_related_model: Optional[Union[str, List[str]]] = None,  # default = field's immediate parent
        many_related_sort_fld: Optional[Union[str, List[str]]] = None,  # default = {many_related_model}__pk
        many_related_sort_fwd: bool = True,
        many_related_delim: str = DEF_DELIM,
        strict_select: Optional[bool] = None,
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
                related values, but all thos functions are postgres-specific.
                Example for AnimalListView:
                    BootstrapTableColumn("study", field="studies__name", many_related=True)
                    If an animal belongs to multiple studies and the user selects to do an ascending sort on the study
                    column, the "study" field will be defined as and the order_by in the query will look like:
                        animal_queryset.annotate("study"=Min("studies__name")).order_by("study").distinct()
                    It's also notable that any filtering will still be on the DB field (and it will not affect the
                    number of resulting records), so a search for a study name "My Study":
                        animal_queryset.filter(studies__name__icontains="My Study")
                    Together, it looks like this:
                        animal_queryset
                            .filter(studies__name__icontains="My Study")
                            .annotate("study"=Min("studies__name"))
                            .order_by("study")
                            .distinct()
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
            many_related_model (Optional[Union[str, List[str]]]) [field]: All many_related columns will be delimited
                and sorted by their own value.  You can optionally sort based on any field under this parent model, and
                even via relations to that model, but there must be no many-to-many relations occurring in the field's
                path after that model.  Every field that should sort the same way must also have the same value for
                many_related_model.  E.g. if you have these 3 fields:
                    animal__infusate__tracer_links__tracer__name
                    animal__infusate__tracer_links__tracer__compound__id
                    animal__infusate__tracer_links__concentration
                and you want their delimited values to sort the same way, you can supply
                    many_related_model="animal__infusate__tracer_links"
                but note that both field paths MUST each start with that model, i.e. they must all start with:
                    "animal__infusate__tracer_links"
                The result will be if the user has sorted on the field/column for
                "animal__infusate__tracer_links__concentration", the other two columns will also be sorted by the values
                in that concentration field.
                This option is ignored if many_related is False.
            many_related_sort_fld (Optional[str]) [many_related_model + "__pk"]: The default sort field for many-
                related fields.
            many_related_sort_fwd (bool) [True]: Set to False to reverse sort by default.
        Exceptions:
            ValueError when:
            - Either many_related must be True or a converter must be supplied if the BST column name is not equal to
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
        self.many_related = many_related
        self.many_related_model_orig = many_related_model
        self.many_related_model = many_related_model
        self.many_related_sort_fld = many_related_sort_fld
        self.many_related_sort_fwd = many_related_sort_fwd
        self.delim = many_related_delim
        self.init_many_related()

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
        self.widget = BSTHeader()

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
            self.sorter = self.SORTER_CHOICES["ALPHANUMERIC"]
        elif self.sorter not in self.SORTER_CHOICES.values():
            raise ValueError(
                f"Invalid sorter value: '{sorter}'.  "
                f"Valid choices are: {list(self.SORTER_CHOICES.values())}."
            )

        # Validate the settings
        self.validate()

    def __str__(self):
        return self.as_widget()

    def as_widget(self, attrs=None):
        return self.widget.render(
            self.name,
            self,
            attrs=attrs,
        )

    def init_many_related(self):
        if self.many_related_model is None and self.field is not None:
            # Default to sorting M:M field values by the primary key of the many_related_model model
            if isinstance(self.field, list):
                self.many_related_model = []
                for f in self.field:
                    path = f.split("__")
                    if len(path) > 1:
                        self.many_related_model.append("__".join(path[0:-1]))
                    elif len(path) == 1:
                        # assume it ends in a foreign key
                        self.many_related_model.append(f)
                    else:
                        raise ValueError(f"Invalid field path '{f}' in column '{self.name}'.")
            else:
                path = self.field.split("__")
                if len(path) > 1:
                    self.many_related_model = "__".join(path[0:-1])
                elif len(path) == 1:
                    # assume it ends in a foreign key
                    self.many_related_model = self.field
                else:
                    raise ValueError(f"Invalid field path '{self.field}' in column '{self.name}'.")

        if self.many_related and self.many_related_sort_fld is None:
            # Default to sorting M:M field values by the primary key of the many_related_model model
            if isinstance(self.many_related_model, list):
                self.many_related_sort_fld = [f"{f}__pk" for f in self.many_related_model]
            else:
                self.many_related_sort_fld = self.many_related_model + "__pk"

    def init_filters(self):
        default_filter_control = (
            self.FILTER_CONTROL_CHOICES["INPUT"]
            if self.select_options_orig is None and self.select_options_orig is None
            else self.FILTER_CONTROL_CHOICES["SELECT"]
        )
        if self.filter_control_orig is None:
            if self.searchable_orig is None or self.searchable_orig is True:
                # Full default settings
                self.filter_control = default_filter_control
                self.searchable = True
            else:
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
            self.select_options = dict((opt, opt) for opt in self.select_options_orig)

    def validate(self):
        if self.field is not None:
            if (
                isinstance(self.field, str)
                and self.name != self.field
                and self.converter is None
                and not self.many_related
            ):
                raise ValueError(
                    "Either many_related must be True or a converter must be supplied if the BST column name "
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

        if self.many_related_model_orig is not None:
            if type(self.many_related_model_orig) != type(self.field) or (
                isinstance(self.field, list) and len(self.many_related_model_orig) != len(self.field)
            ):
                raise TypeError(
                    f"field and many_related_model must be the same type and size (if list type).  "
                    f"[{type(self.many_related_model_orig).__name__} != {type(self.field).__name__}]"
                )
            if isinstance(self.field, list):
                for i, fld in enumerate(self.field):
                    sort_fld_mdl = self.many_related_model_orig[i] + "__"
                    if not fld.startswith(sort_fld_mdl):
                        raise ValueError(
                            f"The field path of column '{self.name}': '{fld}' must start with many_related_model "
                            f"('{self.many_related_model_orig[i]}') in order to link the sort of delimited values in "
                            "this column with those in other columns."
                        )
            else:
                sort_fld_mdl = self.many_related_model_orig + "__"
                if not self.many_related_sort_fld.startswith(sort_fld_mdl):
                    raise ValueError(
                        f"The field path of column '{self.name}': '{self.field}' must start with many_related_model "
                        f"('{self.many_related_model_orig}') in order to link the sort of delimited values in this "
                        "column with those in other columns."
                    )

        if self.many_related and self.many_related_sort_fld is not None:
            if type(self.many_related_sort_fld) != type(self.field) or (
                isinstance(self.field, list) and len(self.many_related_sort_fld) != len(self.field)
            ):
                raise TypeError(
                    f"field and many_related_sort_fld must be the same type and size (if list type).  "
                    f"[{type(self.many_related_sort_fld).__name__} != {type(self.field).__name__}]"
                )
            if isinstance(self.field, list):
                for i, fld in enumerate(self.many_related_sort_fld):
                    sort_fld_mdl = self.many_related_model[i] + "__"
                    if not fld.startswith(sort_fld_mdl):
                        raise ValueError(
                            f"The default sort field path of column '{self.name}': '{fld}' must start with "
                            f"many_related_model ('{self.many_related_model[i]}') in order to link the sort of "
                            "delimited values in this column with those in other columns."
                        )
            else:
                sort_fld_mdl = self.many_related_model + "__"
                if not self.many_related_sort_fld.startswith(sort_fld_mdl):
                    raise ValueError(
                        f"The default sort field path of column '{self.name}': '{self.many_related_sort_fld}' must "
                        f"start with many_related_model ('{self.many_related_model}') in order to link the sort "
                        "of delimited values in this column with those in other columns."
                    )

    @classmethod
    def field_to_related_model(cls, field: str):
        """Turns a django field path into a related model path, e.g. mz_to_msrunsamples__sample__animal__studies__id ->
        mz_to_msrunsamples__sample__animal__studies"""
        path = field.split("__")
        if len(path) > 1:
            return "__".join(path[0:-1])
        elif len(path) == 1:
            # assume it ends in a foreign key
            return field
        return None

    def __eq__(self, other):
        if isinstance(other, __class__):
            return self.name == other.name
        elif isinstance(other, str):
            return self.name == other
        else:
            raise NotImplementedError(f"Equivalence of {__class__.__name__} to {type(other).__name__} not implemented.")


class BootstrapTableColumnGroup:
    sort_dirs = ["asc", "desc"]

    def __init__(self, columns: List[BootstrapTableColumn]):
        self.columns = columns

        if not all([c.many_related for c in columns]):
            one_to_ones = [c.name for c in columns if not c.many_related]
            raise ValueError(
                f"Invalid columns.  Every column must be a many-related column.  These columns are not many-related: "
                f"{one_to_ones}."
            )
        if len(columns) < 2:
            raise ValueError(f"Invalid columns.  There must be more than 1.  Supplied: {len(columns)}.")

        self.model = columns[0].many_related_model
        if not all([c.many_related_model == self.model for c in columns]):
            models = [c.many_related_model for c in columns]
            uniq_models = reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, models, [])
            raise ValueError(
                "All columns must belong to the same many-related model.  The following model mix was found: "
                f"{uniq_models}."
            )

        self.sort_fld = columns[0].many_related_sort_fld
        if not all([c.many_related_sort_fld == self.sort_fld for c in columns]):
            sort_flds = [c.many_related_sort_fld for c in columns]
            uniq_sort_flds = reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, sort_flds, [])
            raise ValueError(
                "All columns must have the same sort field.  The following sort field mix was found: "
                f"{uniq_sort_flds}."
            )

        self.sort_fwd = columns[0].many_related_sort_fwd
        if not all([c.many_related_sort_fwd == self.sort_fwd for c in columns]):
            sort_fwds = [c.many_related_sort_fwd for c in columns]
            uniq_sort_fwds = reduce(lambda ulst, val: ulst + [val] if val not in ulst else ulst, sort_fwds, [])
            raise ValueError(
                "All columns must belong to the same many-related sort_fwd.  The following sort_fwd mix was found: "
                f"{uniq_sort_fwds}."
            )

        seen = defaultdict(int)
        for c in columns:
            seen[c.name] += 1
        dupes = [k for k in seen.keys() if seen[k] > 1]
        if len(dupes) > 0:
            raise ValueError(f"Each column name must be unique.  These were found to be redundant: {dupes}")

    def set_sort_fld(self, sort_fld: str, ignore_non_matches=False):
        if not ignore_non_matches and sort_fld not in self.columns:
            raise ValueError(
                f"Sort field '{sort_fld}' is not a name of any column in this group.  The options are: "
                f"{[c.name for c in self.columns]}"
            )

        for c in self.columns:
            c.many_related_sort_fld = sort_fld

    def set_sort_dir(self, sort_dir: str, ignore_non_matches=False):
        if not ignore_non_matches and sort_dir.lower() not in self.sort_dirs:
            raise ValueError(f"Sort direction '{sort_dir}' is not a sort direction.  The options are: {self.sort_dirs}")

        for c in self.columns:
            c.many_related_sort_fwd = sort_dir.lower() == "asc"
