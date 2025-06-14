from typing import List, Optional, Union

from django.forms.widgets import Select


class BSTRowsPerPageSelect(Select):
    """Extends the Select widget to provide a select list that supports an "all" option at the bottom and a list of page
    sizes that is responsive to the total number of rows/results.

    Class Attributes:
        template_name (str) [widgets/bst/rowsperpage_select.html]
        option_template_name (str) [widgets/bst/rowsperpage_select_option.html]
        all_page_sizes (List[int]) [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0]
        all_label (str) ["ALL"]
    Instance Attributes:
        total_rows (int): Total rows across all pages
        selected (int) [1]: The selected number of rows per page.  If not present in all_page_sizes, it will be added.
        _selected_label (Union[int, str]): This is either the selected page size or "ALL" if the selected page size is
            0.  This is to support the way the superclass sets the selected option.
        select_name (str) ["paginate_by"]: Name of the select list HTML element.
        option_name (str) ["rows-per-page-option"]: The common name of every select list option element.
        page_sizes (List[int]): A subset of all_page_sizes that are less than or equal to total_rows.
        smallest (int) [all_page_sizes[0]]: The smallest page size (greater than 0).  This is usually all_page_sizes[0],
            but if the user requests a custom page size using the 'limit' URL parameter, it can be smaller.
    """

    template_name = "widgets/bst/rowsperpage_select.html"
    option_template_name = "widgets/bst/rowsperpage_select_option.html"
    all_page_sizes: List[int] = [5, 10, 15, 20, 25, 50, 100, 200, 500, 1000, 0]
    all_label = "ALL"
    default_option_name = "rows-per-page-option"

    def __init__(
        self,
        total_rows: int,
        selected: Optional[int] = None,
        select_name: str = "paginate_by",
        option_name: Optional[str] = None,
    ):
        """Constructor

        Args:
            total_rows (int): Total rows across all pages
            selected (Optional[int]) [1]: The selected number of rows per page
            select_name (str) ["paginate_by"]: Name of the select list HTML element
            option_name (str) [default_option_name]: The common name of every select list option element.
        Exceptions:
            None
        Returns:
            None
        """
        self.select_name = select_name
        self.option_name = (
            option_name if option_name is not None else self.default_option_name
        )
        self.total_rows = total_rows

        # Filter the page sizes
        self.page_sizes = self.filter_page_sizes()

        # Selected rows per page
        self.selected = selected if selected is not None else self.page_sizes[0]
        self._selected_label: Union[int, str] = (
            self.selected if self.selected > 0 else self.all_label
        )

        if self.selected not in self.page_sizes:
            self.page_sizes.append(self.selected)

        # Sort the page sizes
        self.page_sizes = sorted(
            self.page_sizes,
            # Put 0 last
            key=lambda v: (v == 0, v),
        )

        # The smallest page size can be a custom size using the 'limit' URL parameter
        self.smallest = (
            self.page_sizes[0] if self.page_sizes[0] > 0 else self.all_page_sizes[0]
        )

        page_size_tuples = (
            (size, (self.all_label if size == 0 else size)) for size in self.page_sizes
        )

        super().__init__(choices=page_size_tuples)

    def filter_page_sizes(self):
        """Filters all_page sizes using self.total_rows.  Keeps 0, which represents "all rows"."""
        return [s for s in self.all_page_sizes if s == 0 or s < self.total_rows]

    def __str__(self):
        """This allows the select list to be rendered via the paginator object in the template by simple including
        `{{ page_obj.paginator.page_size_select }}`."""
        return self.render(self.select_name, self._selected_label)

    def get_context(self, *args, **kwargs):
        context = super().get_context(*args, **kwargs)
        # This cycles through each option element (index j) contained in "optgroups".  (I'm not sure what "optgroups"
        # is, but my guess would be sub-select-lists.)  Each of those contains a 3-member tuple, the second of which (at
        # index 1), is the list of option elements, to which we want to set the element name for use in the option's
        # template.  This is needed to attach javascript listeners to be able to trigger page reloads when a user
        # decides to change the rows per page.
        for i, (_, group_choices, _) in enumerate(context["widget"]["optgroups"]):
            for j in range(len(group_choices)):
                context["widget"]["optgroups"][i][1][j][
                    "option_name"
                ] = self.option_name
        # For convenience, we also assign the name at the first level of the widget's content dict.
        context["widget"]["option_name"] = self.option_name
        return context
