from typing import Optional

from DataRepo.views.utils import GracefulPaginator
from DataRepo.widgets.bst.rows_per_page_select import BSTRowsPerPageSelect


class SizedPaginator(GracefulPaginator):
    """An extension GracefulPaginator that adds page context and a page size select list.

    Speficially, this class:
    - Reports the total number of rows across all pages
    - Reports the start and stop row relative to the total
    - Provides a total number of unfiltered rows
    - Provides a rows-per-page select list
    - Provides data to decide whether to
        - Render next/previous arrows
        - Render an ellipsis how many pages to link to either side of the current page
        - Render a separate first/last page shortcut

    Much of this functionality could exist without an added class, by having all the logic and math embedded in the
    template, however, the general wisdom in Django MVC design is to not have logic and math in the templates, but
    rather to have it in the view code, which is what this accomplishes.

    Class Attributes:
        template_name (str) ["models/bst/paginator.html"]
        script_name (str) ["js/bst/paginator.js"]: Where the javascript code lives that is called from the template.
        flank (int) [3]: The number of pages to link to either side of the current page in the pagination controls.
    Instance Attributes:
        total (int): Total number of (filtered) rows across all pages.
        raw_total (int) [total]: Total number of unfiltered rows across all pages.
        cur_page (int) [1]: The current page number.
        size_select_list (BSTRowsPerPageSelect): A select list widget that will render in HTML in string context.
        first_row (int): The number of the first visible row on the current page relative to total.
        last_row (int): The number of the last visible row on the current page relative to total.
        show_first_shortcut (bool): Whether to include a shortcut to the first page in the pagination controls.
        show_last_shortcut (bool): Whether to include a shortcut to the last page in the pagination controls.
        show_left_ellipsis (bool): Whether to include a ellipsis to the right of the first page in the pagination
            controls, that triggers a javascript prompt for the user to jump to any page.
        show_right_ellipsis (bool): Whether to include a shortcut to the left of the last page in the pagination
            controls, that triggers a javascript prompt for the user to jump to any page.
        page_name (str) ["page"]: The URL parameter name for a page number.
        limit_name (str) ["limit"]: The URL parameter name for the number of rows per page.
        can_be_resized (bool): Whether the total and the smallest page size option allow for resizing the rows per page.
    """

    template_name = "models/bst/paginator.html"
    script_name = "js/bst/paginator.js"
    # Contains askForPage(), initPaginator(), and validatePageNum()

    flank: int = 3
    # The number of pages to link to on either side of the current page

    def __init__(
        self,
        total: int,
        *args,
        raw_total: Optional[int] = None,
        page: int = 1,
        page_name: str = "page",
        limit_name: str = "limit",
        **kwargs,
    ):
        """Constructor

        Args:
            total (int): Total number of (filtered) rows across all pages
            raw_total (Optional[int]) [total]: Total number of unfiltered rows across all pages
            page (int) [1]: The current page number.
            page_name (str) ["page"]: The URL parameter name for a page number.
            limit_name (str) ["limit"]: The URL parameter name for the number of rows per page.
        Exceptions:
            None
        Returns:
            None
        """
        super().__init__(*args, **kwargs)

        self.total: int = total
        self.raw_total: int = raw_total if raw_total is not None else total
        self.size_select_list = BSTRowsPerPageSelect(self.total, self.per_page)
        self.can_be_resized = self.total > self.size_select_list.smallest
        self.page_name = page_name
        self.limit_name = limit_name

        # Validate the supplied page.
        # NOTE: A potential refacor *might* improve this design to extend Django's Page class either in addition to this
        # Paginator extension or in lieu of this class, and put all of the page-relative values initialized below there,
        # similar to Page.has_next, etc.
        self.cur_page = self.page(page).number

        self.first_row = (self.cur_page - 1) * self.per_page + 1
        last_row = page * self.per_page
        self.last_row = last_row if last_row <= total else total

        self.show_first_shortcut = (self.cur_page - self.flank) > 1
        self.show_last_shortcut = (self.cur_page + self.flank) < self.num_pages

        self.show_left_ellipsis = (self.cur_page - self.flank) > 2
        self.show_right_ellipsis = (self.cur_page + self.flank) < (self.num_pages - 1)

    @property
    def linked_page_range(self):
        start = self.cur_page - self.flank
        if start < 1:
            start = 1
        end = self.cur_page + self.flank
        if end > self.num_pages:
            end = self.num_pages
        return range(start, end + 1)
