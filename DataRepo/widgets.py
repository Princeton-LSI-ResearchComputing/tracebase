from typing import List, Tuple, Union

from django.db import ProgrammingError
from django.forms import Widget
from django.forms.widgets import ClearableFileInput, Select, TextInput
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

    def get_context(self, name, column, attrs=None):
        # Called via DataRepo.views.models.base.BootstrapTableColumn.render_th(), which passes 'self' as the 'column'
        # argument, but it's also called from Django internals where this 'column' argument is referred to as 'value',
        # so all this is doing is it's taking that value and putting it in a context variable named "column" for
        # convenience and readability in the template.  I could pass along that 'value' in the super call, in which
        # case, you could access it in the template as 'widget.value'.
        context = super().get_context(name, None, attrs)
        context["column"] = column
        return context


class BSTValue(Widget):
    # Template received context variables: 'object' (Model), 'value', and 'column' (BSTColumn)
    default_template = "DataRepo/widgets/bst_td.html"

    def __init__(self, template_name=default_template):
        self.template_name = template_name
        super().__init__()

    def get_context(self, name, value: dict, attrs=None):
        # Called via DataRepo.views.models.base.BootstrapTableColumn.td(), which passes 'self' as the 'column' argument,
        # but it's also called from Django internals where this 'column' argument is referred to as 'value', so all this
        # is doing is it's taking that value and putting it in a context variable named "column" for convenience and
        # readability in the template.  I could pass along that 'value' in the super call, in which case, you could
        # access it in the template as 'widget.value'.
        context = super().get_context(name, None, attrs)
        context.update(value)
        return context
