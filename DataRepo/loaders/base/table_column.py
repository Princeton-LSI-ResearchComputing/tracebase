import sys
from typing import Callable, List, Optional

from django.db.models import Field

from DataRepo.utils.exceptions import (
    ConditionallyRequiredArgs,
    ConditionallyRequiredOptions,
    MutuallyExclusiveOptions,
)


class ColumnReference:
    """This class allows a Loader class to cross-reference a TableColumn.

    The purpose depends on where the reference is used.  In the ColumnHeader class, it is used to populate a header
    comment, saying something like "Values must match those in column Y of sheet X".  In the ColumnValue class, it is
    (/will) be used to create drop-down lists for selecting values from a column in another sheet.
    """

    def __init__(
        self,
        header: Optional[str] = None,
        sheet: Optional[str] = None,
        loader_class=None,  # No typehint.  See below.
        loader_header_key: Optional[str] = None,
    ):
        """ColumnReference constructor.

        - Either loader_class or (sheet and header) are required.
        - loader_class and sheet are mutually exclusive.
        - loader_header_key and header are mutually exclusive.
        - loader_header_key or header is required.

        Args:
            header (string): The name of the header being referenced.  Must match the ColumnHeader.name of that header.
                Mutually exclusive/required with loader_header_key.
            sheet (string): The name of the excel sheet being referenced.  Mutually exclusive/required with loader_class
            loader_class (TableLoader): A concrete TableLoader class where the sheet can be derived.  Mutually exclusive
                /required with sheet.
            loader_header_key (string): A header key defined in the loader_class.  Mutually exclusive/required with
                header.

        Exceptions:
            ConditionallyRequiredOptions
            MutuallyExclusiveOptions

        Returns:
            instance
        """
        self.header = header
        self.sheet = sheet
        self.loader_class = loader_class
        self.loader_header_key = loader_header_key

        # Cannot import loader class at compile(/"interpretation") time, because the import would be circular.  Instead,
        # we must evaluate the class to check its type at execution time.  There are 2 ways to do this:
        # - getattr(sys.modules[__name__], "Foo")
        # - globals()["Foo"]  # When both are in the same file but reference one another (which is prohibited as well)
        # Below, I use the fact that both table_column.py and table_loader.py are in the same directory...
        table_loader_pypath = self.__module__.rsplit(".", 1)[0] + ".table_loader"
        TableLoader = getattr(sys.modules[table_loader_pypath], "TableLoader")
        if loader_class is not None and not issubclass(loader_class, TableLoader):
            bases = ", ".join([bc.__name__ for bc in loader_class.__bases__])
            raise TypeError(
                f"loader_class must be a derived class of {TableLoader.__name__}, not {bases}."
            )

        if loader_class is None:
            if header is None or sheet is None:
                raise ConditionallyRequiredOptions(
                    "header and sheet are required when loader is undefined."
                )
        else:
            if sheet is not None:
                raise MutuallyExclusiveOptions(
                    "loader_class and sheet are mutually exclusive."
                )
            self.sheet = loader_class.DataSheetName
            if loader_header_key is not None and header is not None:
                raise MutuallyExclusiveOptions(
                    "loader_header_key and header are mutually exclusive when loader_class is defined."
                )
            elif loader_header_key is None and header is None:
                raise ConditionallyRequiredOptions(
                    "loader_header_key or header is required when loader_class is defined."
                )

            if loader_header_key is not None:
                # Note, this is not the same as getattr(loader_class.DataHeaders, loader_header_key)
                # TODO: Refactor TableLoader to consolidate loader_class.DataHeaders and
                # loader_class.DataColumnMetadata.KEY.header.name - THIS WILL SIMPLIFY TableLoader somewhat
                self.header = getattr(
                    loader_class.DataColumnMetadata, loader_header_key
                ).header.name


class ColumnHeader:
    """Defines metadata associated with an excel column header, e.g. for decorating the header."""

    def __init__(
        self,
        name: Optional[str] = None,
        field: Optional[Field] = None,
        guidance: Optional[str] = None,
        format: Optional[str] = None,
        reference: Optional[ColumnReference] = None,
        dynamic_choices: Optional[ColumnReference] = None,
        readonly: bool = False,
        formula: Optional[str] = None,
        # ColumnHeader arguments set by field, but can be supplied (e.g. if None or to override)
        help_text: Optional[str] = None,
        required: Optional[bool] = None,
        unique: Optional[bool] = None,
        # Option for the generated name (when field is supplied)
        include_model_in_header: bool = True,
    ):
        """ColumnHeader constructor.

        Args:
            name (str): Header name.  Can override the field derived header.  Required if no field.
            field (Field): Model field from which to derive data, if provided.  Required if no name.
            guidance (str): Details added to Model.field.help_text when it differs from the model.
            format (str): Add to provide notes about units, delimiting strings, etc.
            reference (ColumnReference): Used to add to a header comment when columns are related.
            help_text (str): Description of the column data.  Can override the field derived value.
            required (bool): Whether the header is required to be present.  Derived from field.blank, but can be
                overridden.
            unique (bool): Whether the values in the column must be unique.  Derived from field.unique, but can be
                overridden.
            include_model_in_header (bool): Option to include the field's model name in the header (if name not
                supplied).
            readonly (bool): Whether the column may be only read (i.e. not edited).
            formula (string): Excel formula to use to populate the column.

        Exceptions:
            ConditionallyRequiredOptions

        Returns:
            instance
        """
        if name is None and field is None:
            raise ConditionallyRequiredOptions("name or field is required")

        # field is not saved here, but it can be supplied to automatically set some attributes
        if field is not None:
            # Convert to the class
            field = field.field

            if name is None:
                if include_model_in_header:
                    name = make_title(field.model.__name__, field.name)
                else:
                    name = make_title(field.name)

            # Handle overrides and unset/missing attributes.  If anything is invalid, it should be caught downstream.
            if (
                hasattr(field, "help_text")
                and field.help_text is not None
                and help_text is None
            ):
                help_text = field.help_text
            if hasattr(field, "blank") and field.blank is not None and required is None:
                required = not field.blank
            if hasattr(field, "unique") and field.unique is not None:
                unique = field.unique

        # Default values
        if required is None:
            required = True
        if unique is None:
            unique = False

        self.name = name
        self.required = required
        self.help_text = help_text
        self.format = format
        self.guidance = guidance
        self.reference = reference
        self.unique = unique
        self.dynamic_choices = dynamic_choices
        self.readonly = readonly
        self.formula = formula

    @property
    def comment(self):
        """Content of the header comment, composed based on instance attributes.
        Args:
            None
        Exceptions:
            None
        Returns:
            comment (string)
        """
        if (
            self.help_text is None
            and self.guidance is None
            and self.format is None
            and self.reference is None
            and self.unique is None
            and (self.required is None or not self.required)
        ):
            return None

        comment = ""
        if self.readonly:
            comment += "Readonly."
            if self.formula is not None:
                comment += "  (Calculated by formula.)"
            comment += "\n\n"
        elif self.formula is not None:
            comment += "Calculated by formula.\n\n"
        if self.help_text is not None:
            comment += self.help_text
        if self.guidance is not None:
            if comment != "":
                comment += "\n\n"
            comment += self.guidance
        if self.format is not None:
            if comment != "":
                comment += "\n\n"
            comment += self.format
        if self.reference is not None:
            if comment != "":
                comment += "\n\n"
            comment += f"Must match a value in column '{self.reference.header}' in sheet: {self.reference.sheet}."
        # TODO: Add a note about static_choices (and current_choices)
        if self.dynamic_choices is not None:
            if comment != "":
                comment += "\n\n"
            comment += (
                f"Select a '{self.name}' from the dropdowns in this column.  The dropdowns are populated by the "
                f"'{self.dynamic_choices.header}' column in the '{self.dynamic_choices.sheet}' sheet, so if the "
                f"dropdowns are empty, add rows to the '{self.dynamic_choices.sheet}' sheet."
            )
        if self.unique is not None and self.unique:
            if comment != "":
                comment += "\n\n"
            comment += "Must be unique."
        if self.required:
            if comment != "":
                comment += "\n\n"
            comment += "Required."
        else:
            if comment != "":
                comment += "\n\n"
            comment += "Optional."

        return comment


class ColumnValue:
    """This is a *template* for any cell in a table column (except the header cell).  The attributes here are
    collectively applied to every (non-header) cell in a column.

    For example, all the values must be of a single type.  They can be required to be unique or be derived via formula.
    They can be derived from a set of static choices, or via the values from a column in another sheet.
    """

    def __init__(
        self,
        field: Optional[Field] = None,
        default=None,
        required: Optional[bool] = None,
        type: Optional[type] = str,
        static_choices: Optional[List[tuple]] = None,
        dynamic_choices: Optional[ColumnReference] = None,
        current_choices: bool = False,
        current_choices_converter: Optional[Callable] = None,
        unique: Optional[bool] = None,
        formula: Optional[str] = None,
        readonly: bool = False,
    ):
        """ColumnValue constructor.

        Args:
            field (Field): Model field from which to derive data, if provided.  Required if no name.
            default (object): Details added to Model.field.help_text when it differs from the model.
            required (bool): Whether a value is required to be present.  Derived from field.blank, but can be
                overridden.
            type (type) [str]: The type of data expected in the excel column.
            static_choices (Optional[List[tuple]|Callable[[], List[tuple]]]): (Will be) used to populate a drop-down
                list.  Derived from the model field, but can be overridden.
            dynamic_choices (ColumnReference): (Will be) used to populate a drop-down list using the contents of a
                column in another sheet.  Overrides static_choices.
            current_choices (bool): Whether to set static_choices to a distinct list of what's currently in the db.
                Requires field to be supplied.
            current_choices_converter (function): A converter function for use by current_choices.
            unique (bool): Whether the values in the column must be unique.  Derived from field.unique, but can be
                overridden.
            formula (string): Excel formula to use to populate the column.
            readonly (bool): Whether the column may be only read (i.e. not edited).

        Exceptions:
            None

        Returns:
            instance
        """
        self.model = None
        self.field = None
        if field is not None:
            # Convert to the class
            self.field = field.field
            self.model = field.field.model

            # Handle overrides and unset/missing attributes.  If anything is invalid, it should be caught downstream.
            if (
                hasattr(self.field, "unique")
                and self.field.unique is not None
                and unique is None
            ):
                unique = self.field.unique
            if (
                hasattr(self.field, "default")
                and self.field.default is not None
                and default is None
            ):
                default = self.field.default
            if (
                hasattr(self.field, "blank")
                and self.field.blank is not None
                and required is None
            ):
                required = not self.field.blank
            if (
                hasattr(self.field, "choices")
                and self.field.choices is not None
                and static_choices is None
            ):
                static_choices = self.field.choices
        elif current_choices:
            raise ConditionallyRequiredArgs(
                "ColumnValue requires a field argument if current_choices is True"
            )

        # Default values
        if required is None:
            required = True
        if unique is None:
            unique = False

        self.default = default
        self.required = required
        self._static_choices = static_choices
        self.dynamic_choices = dynamic_choices
        self.current_choices = current_choices
        self.current_choices_converter = current_choices_converter
        self.type = type
        self.unique = unique
        self.formula = formula
        self.readonly = readonly

    @property
    def static_choices(self):
        """Made this into a property to support "current_choices".

        This data could not be compiled during the instantiation of an object because the migrations check ends up
        evaluating classes that use a TableColumn object as a value of class attributes, which causes a database query
        to try to execute before the database is setup.  This property avoids that situation.

        Args:
            None
        Exceptions:
            None
        Returns:
            choices (List[tuple])
        """
        choices = self._static_choices
        if self.current_choices:
            fldnm = self.field.name
            choices = [
                (
                    (fs, fs)
                    if self.current_choices_converter is None
                    else (
                        self.current_choices_converter(fs),
                        self.current_choices_converter(fs),
                    )
                )
                for fs in list(
                    self.model.objects.order_by(fldnm)
                    .values_list(fldnm, flat=True)
                    .distinct(fldnm)
                )
                if fs is not None
            ]
            if len(choices) == 0:
                choices = None
        return choices


class TableColumn:
    """A container of ColumnHeader and ColumnValue (template).  This provides the interface into those objects."""

    def __init__(
        self,
        field: Optional[Field] = None,
        header: Optional[ColumnHeader] = None,
        value: Optional[ColumnValue] = None,
        readonly: bool = False,
    ):
        """TableColumn constructor.
        Args:
            header (ColumnHeader): Object describing header metadata.
            value (ColumnValue): Object describing metadata associated with every cell below the header.
            field (Field): Model field associated with the table column.
            readonly (bool): Whether the column may be only read (i.e. not edited).
        Exceptions:
            None
        Returns:
            instance (TableColumn)
        """
        self.field = field
        self.model = None
        if field is not None:
            # Convert to the class
            self.field = field.field
            self.model = field.field.model
        self.readonly = readonly

        if header is None:
            if field is None:
                raise ConditionallyRequiredOptions("header or field is required")
            header = ColumnHeader(field=field)
        self.header = header

        if value is None:
            value = ColumnValue(field=field)
        self.value = value

    @classmethod
    def init_flat(
        cls,
        field: Optional[Field] = None,
        readonly: bool = False,
        # ColumnHeader arguments not set by field
        name: Optional[str] = None,
        guidance: Optional[str] = None,
        format: Optional[str] = None,
        reference: Optional[ColumnReference] = None,
        # ColumnValue arguments not set by field
        type: Optional[type] = str,
        unique: Optional[bool] = None,
        dynamic_choices: Optional[ColumnReference] = None,
        formula: Optional[str] = None,
        # ColumnHeader arguments set by field (to override or if None)
        help_text: Optional[str] = None,
        header_required: Optional[bool] = None,
        # ColumnValue arguments set by field (to override or if None)
        default=None,
        value_required: Optional[bool] = None,
        static_choices: Optional[List[tuple]] = None,
        current_choices: bool = False,
        current_choices_converter: Optional[Callable] = None,
    ):
        """Alternate TableColumn constructor.  (This is the pythonic was to implement multiple constructors.)
        Args:
            field (Field): A model field that can be used to derive some instance attributes.
            readonly (bool): Whether the column can be edited or not.
            name (str): Header.
            guidance (str): Additional notes to help_text, if the excel version differs from the model field.
            format (str): Notes on units or delimited values.
            reference (ColumnReference): Reference to a column in another sheet for populating the header comment.
            type (type): The type of data expected in the excel column.
            unique (bool): Whether the column values must be unique.
            dynamic_choices (ColumnReference): The excel column used to populate a dropdown.
            formula (str): The formula used to populate a column.
            help_text (str): Details about the data contained in the column (derived from field).
            header_required (bool): Whether the column is required to be present (derived from field).
            default (object): A default value for the column.
            value_required (bool): Whether every cell must have a value (derived from field).
            static_choices (list of tuples): Possible values in the column (derived from field).
            current_choices (bool): Whether to set static_choices to a distinct list of what's currently in the db.
            current_choices_converter (function): A converter function for use by current_choices.
        Exceptions:
            None
        Returns:
            instance (TableColumn)
        """
        return cls(
            field=field,
            readonly=readonly,
            header=ColumnHeader(
                field=field,
                name=name,
                guidance=guidance,
                format=format,
                reference=reference,
                dynamic_choices=dynamic_choices,
                readonly=readonly,
                formula=formula,
                # Set by field (but provided for overriding)
                help_text=help_text,
                required=header_required,
            ),
            value=ColumnValue(
                field=field,
                type=type,
                unique=unique,
                dynamic_choices=dynamic_choices,
                formula=formula,
                readonly=readonly,
                # Set by field (but provided for overriding)
                default=default,
                required=value_required,
                static_choices=static_choices,
                current_choices=current_choices,
                current_choices_converter=current_choices_converter,
            ),
        )


def make_title(*args: str):
    """Takes any number of string arguments, splits each one on space, and joins the flattened result together, with
    each word getting title-case applied unless they are already mixed-case or a single character.
    """
    mixed_case_strings = []
    for word in [word for a in args for word in a.split(" ")]:
        # If mixed case or a single character
        if (word != word.lower() and word != word.upper()) or len(word) == 1:
            mixed_case_strings.append(word)
        else:
            mixed_case_strings.append(word.title())
    return " ".join(mixed_case_strings)
