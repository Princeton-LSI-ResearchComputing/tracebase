from django.forms.widgets import Select


class RowsPerPageSelectWidget(Select):
    template_name = "widgets/base/rowsperpage_select.html"
    option_template_name = "widgets/base/rowsperpage_select_option.html"
