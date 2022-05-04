import math


class Pager:
    def __init__(
        self,
        page_form_class,
        action,
        form_id_field,
        rows_per_page_choices,
        page_field,
        rows_per_page_field,
        order_by_field,
        order_dir_field,
        num_buttons=5,
        other_field_ids=None,  # {fld_name: id}
        # Default form values
        default_rows=10,
        # Default form element attributes
        page_input_id="pager-page-elem",
        rows_input_id="pager-rows-elem",
        orderby_input_id="pager-orderby-elem",
        orderdir_input_id="pager-orderdir-elem",
        form_id="custom-paging",
        rows_attrs={
            "class": "btn btn-primary dropdown-toggle",
            "type": "button",
            "data-bs-toggle": "dropdown",
        },
    ):
        self.form_id_field = form_id_field
        self.action = action
        self.num_buttons = num_buttons
        self.page_form_class = page_form_class
        self.page_input_id = page_input_id
        self.rows_input_id = rows_input_id
        self.orderby_input_id = orderby_input_id
        self.orderdir_input_id = orderdir_input_id
        self.rows_attrs = rows_attrs
        self.other_field_ids = other_field_ids
        self.page_form = self.page_form_class()
        self.page_form.update(
            self.page_input_id,
            self.rows_input_id,
            self.orderby_input_id,
            self.orderdir_input_id,
            self.rows_attrs,
            self.other_field_ids,
        )
        self.rows_per_page_choices = rows_per_page_choices
        self.page_field = page_field
        self.rows_per_page_field = rows_per_page_field
        self.default_rows = default_rows
        self.order_by_field = order_by_field
        self.order_dir_field = order_dir_field
        self.form_id = form_id

        self.min_rows_per_page = None
        for atuple in self.rows_per_page_choices:
            num = int(atuple[0])
            if self.min_rows_per_page is None or num < self.min_rows_per_page:
                self.min_rows_per_page = num

    def update(
        self,
        tot=None,
        page=1,
        rows=None,
        start=None,
        end=None,
        order_by=None,
        order_dir=None,
        other_field_inits=None,  # {fld_name: init_val,...}
    ):
        """
        This method is used to update the pager object for each new current page being sent to the pagination template
        """

        # Make sure rows, start, and end are set
        if rows is None:
            rows = self.default_rows
        if start is None:
            self.start = (page - 1) * rows + 1
        else:
            self.start = start
        if end is None:
            self.end = self.start - 1 + rows
            if tot is not None and self.end > tot:
                self.end = tot
        else:
            self.end = end

        # Set the member variables
        self.page = page
        self.rows = rows
        self.tot = tot
        self.order_by = order_by
        self.order_dir = order_dir
        self.pages = []

        # Validate
        if self.num_buttons % 2 == 0 or self.num_buttons < 3:
            raise Exception(
                f"The minimum number of buttons [{self.num_buttons}] must be an odd number and greater than 2."
            )
        if page < 1 or (tot is not None and tot != 0 and page > tot):
            raise Exception(
                f"Invalid page number [{page}] must be a number between 1 and {tot}."
            )

        # Prepare the form
        init_dict = {
            self.page_field: page,
            self.rows_per_page_field: rows,
            self.order_by_field: order_by,
            self.order_dir_field: order_dir,
        }
        # Set an arbitrary initial value - doesn't matter what
        init_dict.setdefault(self.form_id_field, 1)
        if other_field_inits is not None:
            for fld in other_field_inits.keys():
                init_dict[fld] = other_field_inits[fld]
        kwargs = {"initial": init_dict}
        self.page_form = self.page_form_class(**kwargs)
        self.page_form.update(
            self.page_input_id,
            self.rows_input_id,
            self.orderby_input_id,
            self.orderdir_input_id,
            self.rows_attrs,
            self.other_field_ids,
        )

        # Set up the paging controls
        if tot is not None:
            totpgs = math.ceil(tot / rows)

            # The number of pages that are shown to either side of the current page
            num_side_controls = int(self.num_buttons / 2)
            left_leftovers = 0
            right_leftovers = 0

            # Initially, this is the total possible number of pages to the left
            num_left_controls = self.page - 1
            if num_left_controls > num_side_controls:
                num_left_controls = num_side_controls

            # Initially, this is the total possible number of pages to the right
            num_right_controls = totpgs - self.page
            if num_right_controls > num_side_controls:
                num_right_controls = num_side_controls

            startpg = self.page - num_left_controls
            if startpg < 1:
                startpg = 1
                num_left_controls = self.page - startpg
            if num_left_controls < num_side_controls:
                left_leftovers = num_side_controls - num_left_controls

            endpg = self.page + num_right_controls
            if endpg > totpgs:
                endpg = totpgs
                num_right_controls = endpg - self.page
            if num_right_controls < num_side_controls:
                right_leftovers = num_side_controls - num_right_controls

            # Append leftovers
            startpg -= right_leftovers
            if startpg < 1:
                startpg = 1
            endpg += left_leftovers
            if endpg > totpgs:
                endpg = totpgs

            if self.page > 1:
                self.pages.append(
                    {"navigable": True, "val": (self.page - 1), "name": "<"}
                )

            if startpg > 1:
                self.pages.append({"navigable": True, "val": 1, "name": 1})
                self.pages.append({"navigable": False, "val": "", "name": "..."})
                startpg += 1

            # If the ending page in the range is not the last page, decrement the ending page so that we can use the
            # last page control for the last page (after an ellipsis)
            if endpg < totpgs:
                endpg -= 1

            # Need to be 1 past for the range function
            endpg += 1

            for pg in range(startpg, endpg):
                if pg == self.page:
                    self.pages.append({"navigable": False, "val": pg, "name": pg})
                else:
                    self.pages.append({"navigable": True, "val": pg, "name": pg})

            # While endpg is 1 larger than the number of pages that were drawn, we can use that to decide whether to
            # print an ellipsis and the last page control
            if endpg < totpgs:
                self.pages.append({"navigable": False, "val": "", "name": "..."})
                self.pages.append({"navigable": True, "val": totpgs, "name": totpgs})

            if self.page < totpgs:
                self.pages.append(
                    {"navigable": True, "val": (self.page + 1), "name": ">"}
                )

        return self
