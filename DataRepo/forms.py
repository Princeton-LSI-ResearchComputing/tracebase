from typing import Dict, Optional

from django import forms
from django.forms import formset_factory

from DataRepo.formats.dataformat import Format
from DataRepo.formats.fluxcirc_dataformat import FluxCircFormat
from DataRepo.formats.peakdata_dataformat import PeakDataFormat
from DataRepo.formats.peakgroups_dataformat import PeakGroupsFormat
from DataRepo.formats.search_group import SearchGroup

# IMPORTANT NOTE ABOUT THE pos & posprefix FIELDS IN EACH AdvSearch FORM CLASSES:

# The 'pos' field contains a hierarchical path created by javascript (see static/js/hierarchical_formsets.js). Each
# row form displayed in the hierarchy on the page is at a leaf of the hierarchy. 'pos' is one of the (hidden) fields
# in those forms containing the hierarchical path to that leaf. The path is of the form 0.0.0... Internal nodes are
# either an 'all'(/'and') group or an 'any'(/'or') group. Inner nodes don't have an html form, so the path for a
# leaf-form will contain each group's type, e.g. 'all0.any0.0' represents the and-group at the root, it's first child
# is an or-group, and it's child is the leaf, i.e. 'first form'. Lastly, information about the form type is prepended
# to the root in for form posprefix-format_name[-selected]. The posprefix must match the start of the string for form
# validation to work. Thus, the final format example of a value for this field would be:
# pgtemplate-PeakGroups-selected.all0.any0.0. This represents the depth-first leaf under an and-group and or-group.

# If the field name ("pos") is changed in any Form class below, it must be changed to a matching field in all other
# AdvSearch form classes and it must be updated in the javascript code which uses the field name to know when to
# increnemt the formset index in the saveSearchQueryHierarchyHelper function using the variable "count".

# posprefix is a static data member indicating the template ID from which to obtain empty forms. It also is used to
# match against the root node in the pos field so that form validation operates on the correct form data in a
# mixed-form environment.


class BaseAdvSearchForm(forms.Form):
    """
    Advanced search form base class that will be used inside a formset.
    """

    # This is the class used to populate posprefix value and the fld choices. format_class is set in the
    # derived class (not here.  Here, it's just declared for mypy).
    format_class: Format

    # This class is used to initialize the fld select list choices to a flat tuple encompassing every format
    # The format-specific list of fields is pared down by javascript using the format_class above
    # The initial list needs to be comprehensive for form validation so that unselected formats retain their user-
    # selections after a search is peformed.  See issue #229.
    advsrch_view_class = SearchGroup()

    # See important note above about the pos & posprefix fields above
    posprefix: Optional[str] = None
    pos = forms.CharField(widget=forms.HiddenInput())

    # Saves whether this is a static search form or not (for uneditable queries prepended to searches (see fctemplate))
    static = forms.CharField(widget=forms.HiddenInput())

    fld = forms.ChoiceField(required=True, widget=forms.Select())

    ncmp = forms.ChoiceField(required=True, widget=forms.Select())

    # Note: the placeholder attribute solves issue #135
    val = forms.CharField(widget=forms.TextInput(attrs={"placeholder": "search term"}))

    # This can be used to indicate the units or format of the term supplied to val, e.g. "minutes" for a DurationField
    units = forms.ChoiceField(required=True, widget=forms.Select())

    def clean(self):
        """This override of super.clean is so we can reconstruct the search inputs upon form_invalid in views.py"""
        self.saved_data = self.cleaned_data
        return self.cleaned_data

    def is_valid(self):
        data = self.cleaned_data
        fields = self.base_fields.keys()
        # Only validate if the pos field starts with the posprefix - otherwise, it belongs to a different form class
        if "pos" in data and data["pos"].startswith(self.posprefix + "-"):
            self.selected = True
            for field in fields:
                if field not in data:
                    return False
        elif len(data.keys()) == 0:
            return False
        return True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.posprefix = self.format_class.id
        self.fields["fld"].choices = self.advsrch_view_class.getAllSearchFieldChoices()
        self.fields["ncmp"].choices = self.advsrch_view_class.getAllComparisonChoices()
        self.fields["units"].choices = self.advsrch_view_class.getAllFieldUnitsChoices()


class AdvSearchPeakGroupsForm(BaseAdvSearchForm):
    """
    Advanced search form for the peakgroups output format that will be used inside a formset.
    """

    format_class = PeakGroupsFormat()


class AdvSearchPeakDataForm(BaseAdvSearchForm):
    """
    Advanced search form for the peakdata output format that will be used inside a formset.
    """

    format_class = PeakDataFormat()


class AdvSearchFluxCircForm(BaseAdvSearchForm):
    """
    Advanced search form for the fcirc output format that will be used inside a formset.
    """

    format_class = FluxCircFormat()


class AdvSearchForm:
    """
    A group of advanced search form classes
    """

    form_classes: Dict[str, BaseAdvSearchForm] = {}
    # This form field is actually (currently) created in javascript.
    format_select_list_name = "fmt"

    def __init__(self, *args, **kwargs):
        for form_class in (
            AdvSearchPeakGroupsForm(),
            AdvSearchPeakDataForm(),
            AdvSearchFluxCircForm(),
        ):
            id = form_class.format_class.id
            self.form_classes[id] = formset_factory(form_class.__class__)


class AdvSearchDownloadForm(forms.Form):
    """
    Advanced search download form for any advanced search data.
    """

    qryjson = forms.JSONField(widget=forms.HiddenInput())

    def clean(self):
        """This override of super.clean is so we can reconstruct the search inputs upon form_invalid in views.py"""
        self.saved_data = self.cleaned_data
        return self.cleaned_data


class RowsPerPageSelectWidget(forms.Select):
    template_name = "DataRepo/widgets/rowsperpage_select.html"
    option_template_name = "DataRepo/widgets/rowsperpage_select_option.html"


class AdvSearchPageForm(forms.Form):
    """
    Advanced search download form for any advanced search data.
    """

    ROWS_PER_PAGE_CHOICES = (
        ("10", "10"),
        ("25", "25"),
        ("50", "50"),
        ("100", "100"),
        ("200", "200"),
        ("500", "500"),
        ("1000", "1000"),
    )

    qryjson = forms.JSONField(widget=forms.HiddenInput())
    rows = forms.ChoiceField(
        choices=ROWS_PER_PAGE_CHOICES,
        widget=RowsPerPageSelectWidget(),
    )
    page = forms.CharField(widget=forms.HiddenInput())
    order_by = forms.CharField(widget=forms.HiddenInput())
    order_direction = forms.CharField(widget=forms.HiddenInput())
    paging = forms.CharField(
        widget=forms.HiddenInput()
    )  # This field's name ("paging") is used to distinguish pager form submissions from other form submissions
    show_stats = forms.BooleanField(widget=forms.HiddenInput())
    stats = forms.JSONField(widget=forms.HiddenInput())

    def clean(self):
        """
        This override of super.clean is so we can reconstruct the search inputs upon form_invalid in views.py
        """
        self.saved_data = self.cleaned_data
        return self.cleaned_data

    def update(
        self, page_id, rows_id, orderby_id, orderdir_id, rows_attrs={}, other_ids=None
    ):
        """
        Adds IDs and other attributes to form elements.
        """
        # Allow IDs for the inputs to be set for javascript to find the inputs and change them
        page = self.fields.get("page")
        rows = self.fields.get("rows")
        order_by = self.fields.get("order_by")
        order_direction = self.fields.get("order_direction")

        #
        # Make sure any future hard-coded settings are not silently over-ridden
        #

        # page input
        if (
            page.widget.attrs
            and "id" in page.widget.attrs
            and page.widget.attrs["id"] != page_id
        ):
            raise Exception(
                "ERROR: AdvSearchPageForm class already has an ID set for the page input"
            )
        page.widget.attrs["id"] = page_id

        # rows input
        if (
            rows.widget.attrs
            and "id" in rows.widget.attrs
            and rows.widget.attrs["id"] != rows_id
        ):
            raise Exception(
                "ERROR: AdvSearchPageForm class already has an ID set for the rows input"
            )
        rows.widget.attrs["id"] = rows_id

        # order_by input
        if (
            order_by.widget.attrs
            and "id" in order_by.widget.attrs
            and order_by.widget.attrs["id"] != orderby_id
        ):
            raise Exception(
                "ERROR: AdvSearchPageForm class already has an ID set for the order_by input"
            )
        order_by.widget.attrs["id"] = orderby_id

        # order_direction input
        if (
            order_direction.widget.attrs
            and "id" in order_direction.widget.attrs
            and order_direction.widget.attrs["id"] != orderdir_id
        ):
            raise Exception(
                "ERROR: AdvSearchPageForm class already has an ID set for the order_direction input"
            )
        order_direction.widget.attrs["id"] = orderdir_id

        # Allow setting of additional attributes for appearance of the rows select list. Others are assumed to be
        # hidden and page control is assumed to be accomplished using submit buttons that run javascript
        for key, val in rows_attrs.items():
            if (
                rows.widget.attrs
                and key in rows.widget.attrs
                and rows.widget.attrs[key] != val
            ):
                raise Exception(
                    "ERROR: AdvSearchPageForm class already has a [{key}] set for the rows input"
                )
            rows.widget.attrs[key] = val

        if other_ids is not None:
            for fld_name in other_ids.keys():
                fld = self.fields.get(fld_name)
                if (
                    fld.widget.attrs
                    and "id" in fld.widget.attrs
                    and fld.widget.attrs["id"] != other_ids[fld_name]
                ):
                    raise Exception(
                        f"ERROR: AdvSearchPageForm class already has an ID set for the {fld_name} input"
                    )
                fld.widget.attrs["id"] = other_ids[fld_name]

    def is_valid(self):
        # This triggers the setting of self.cleaned_data
        super().is_valid()
        data = self.cleaned_data
        fields = self.base_fields.keys()
        ignore_missing_fields = [
            "order_by",
            "order_direction",
            "show_stats",
            "stats",
        ]
        # Make sure all fields besides the order fields are present
        for field in fields:
            if field not in ignore_missing_fields and field not in data:
                return False
        return True


class DataSubmissionValidationForm(forms.Form):
    """
    Form for users to validate their Animal and Sample Table with Accucor files
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for visible in self.visible_fields():
            visible.field.widget.attrs["class"] = "form-control"

    animal_sample_table = forms.FileField(
        required=True, widget=forms.ClearableFileInput(attrs={"multiple": False})
    )
    accucor_files = forms.FileField(
        required=False, widget=forms.ClearableFileInput(attrs={"multiple": True})
    )
