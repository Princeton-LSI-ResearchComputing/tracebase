from django.forms.widgets import Select


class RowsPerPageSelectWidget(Select):
    template_name = "widgets/search/rowsperpage_select.html"
    option_template_name = "widgets/search/rowsperpage_select_option.html"
