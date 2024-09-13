from typing import List

from django.db import ProgrammingError
from django.forms.widgets import TextInput
from django.utils.safestring import mark_safe


class AutoCompleteTextInput(TextInput):
    """This defines a widget for text form input with autocomplete using an HTML5 datalist element."""

    def __init__(
        self,
        datalist_id: str,
        datalist_values: List[str],
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
        vals = " ".join([f"<option>{val}</option>" for val in self.datalist_values])
        return mark_safe(f'<datalist id="{self.datalist_id}">{vals}</datalist>')