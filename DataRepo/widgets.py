from typing import List, Tuple, Union

from django.db import ProgrammingError
from django.forms import Widget
from django.forms.widgets import ClearableFileInput, Select, TextInput
from django.template import Context, Template
from django.utils.safestring import mark_safe


class AutoCompleteTextInput(TextInput):
    """This defines a widget for text form input with autocomplete using an HTML5 datalist element."""

    def __init__(
        self,
        datalist_id: str,
        datalist_values: Union[List[str], List[Tuple[str, str]]],
        *args,
        datalist_manual=False,
        **kwargs,
    ):
        """Constructor for the widget.

        Args:
            attrs (dict) with the following recognized (additional) keys:
                datalist_id (str): Required ID for the datalist element and the "list" attribute of the input element.
                datalist_values (List[str]): Required list of values for the datalist element.
                datalist_manual (bool): Whther to automatically include the datalist element in the rendered html.
                    Note, if this is true, you must call the datalist method in your template to render its html.
        Exceptions:
            ProgrammingError
        Returns:
            object (AutoCompleteTextInput)
        """
        super().__init__(*args, **kwargs)
        if (
            datalist_id is not None
            and "list" in self.attrs.keys()
            and self.attrs["list"] != datalist_id
        ):
            raise ProgrammingError(
                "A 'list' attribute must not be supplied if 'datalist_id' is provided."
            )

        self.datalist_id = datalist_id
        self.datalist_values = datalist_values
        self.attrs["list"] = self.datalist_id
        self.datalist_manual = datalist_manual
        # This makes tabbing to the field reveal the dropdown.  Without this, it only ever drops down on click.
        self.attrs["onfocus"] = "this.click();"

    def render(self, *args, **kwargs):
        """This renders the HTML for the widget (the input element and an associated datalist element).

        Args:
            None
        Exceptions:
            None
        Returns:
            html (str)
        """
        html = super().render(*args, **kwargs)
        if not self.datalist_manual:
            html += self.datalist()
        return html

    def datalist(self):
        vals = ""
        for val in self.datalist_values:
            if vals != "":
                vals += " "
            if isinstance(val, str):
                vals += f"<option>{val}</option>"
            elif isinstance(val, tuple):
                if val[0].lower().replace(" ", "") == val[1].lower().replace(" ", ""):
                    vals += f"<option>{val[0]}</option>"
                else:
                    vals += f"<option value='{val[0]}'>{val[1]}</option>"
            else:
                raise TypeError(
                    f"datalist_values must be a str or tuple, not {type(self.datalist_values).__name__}"
                )
        return mark_safe(f'<datalist id="{self.datalist_id}">{vals}</datalist>')


class RowsPerPageSelectWidget(Select):
    template_name = "DataRepo/widgets/rowsperpage_select.html"
    option_template_name = "DataRepo/widgets/rowsperpage_select_option.html"


class ListViewRowsPerPageSelectWidget(Select):
    template_name = "DataRepo/widgets/listview_rowsperpage_select.html"
    option_template_name = "DataRepo/widgets/listview_rowsperpage_select_option.html"


class MultipleFileInput(ClearableFileInput):
    """Subclass of ClearableFileInput that specifically allows multiple selected files"""

    allow_multiple_selected = True


class BSTHeader(Widget):
    template_name = "DataRepo/widgets/bst_th.html"
    td_template = "DataRepo/widgets/bst_td.html"
    value_template = "DataRepo/widgets/bst_value.html"

    def __init__(self, column, template_name=None):
        super().__init__()
        if template_name is not None:
            self.template_name = template_name
        self.column = column

    def get_context(self, *args, **kwargs):
        context = super().get_context(*args, **kwargs)
        context["column"] = self.column
        context["td_template"] = self.td_template
        context["value_template"] = self.value_template
        return context

    def render(self, *args, **kwargs):
        return super().render(self.column.name, self.column.header)


class BSTValue(Widget):
    # Template received context variables: 'object' (Model), 'value', and 'column' (BSTColumn)
    template_name = "DataRepo/widgets/bst_td.html"
    value_template = "DataRepo/widgets/bst_value.html"

    def __init__(self, column, value_template=None, template_name=None):
        if template_name is not None:
            self.template_name = template_name
        if value_template is not None:
            self.value_template = value_template
        super().__init__()
        self.column = column

    def get_context(self, *args, **kwargs):
        context = super().get_context(*args, **kwargs)
        context["column"] = self.column
        context["value_template"] = self.value_template
        return context

    def get_nested_template(self):
        return Template(f'{{% include "{self.template_name}" %}}')

    def render(self, context, *args, **kwargs):
        # return super().render(self.column.name, self.column.header)
        # context = self.get_context(self.column.name, None, context)
        # template = Template(f'{{% dynamic_include "{self.template_name}" with object="object" %}}').render(Context(context))
        template = Template(f'{{% include "{self.template_name}" %}}')
        # print(f"RENDERED TEMPLATE: {template}\n\nCONTEXT: {context}")
        return template
