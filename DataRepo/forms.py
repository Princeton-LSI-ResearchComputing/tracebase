from typing import Dict, Optional

from django import forms
from django.forms import formset_factory

from DataRepo.compositeviews import (
    BaseAdvancedSearchView,
    BaseSearchView,
    FluxCircSearchView,
    PeakDataSearchView,
    PeakGroupsSearchView,
)

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

    # This is the class used to populate posprefix value and the fld choices. composite_view_class is set in the
    # derived class (not here.  Here, it's just declared for mypy).
    composite_view_class: BaseSearchView

    # This class is used to initialize the fld select list choices to a flat tuple encompassing every format
    # The format-specific list of fields is pared down by javascript using the composite_view_class above
    # The initial list needs to be comprehensive for form validation so that unselected formats retain their user-
    # selections after a search is peformed.  See issue #229.
    advsrch_view_class = BaseAdvancedSearchView()

    # See important note above about the pos & posprefix fields above
    posprefix: Optional[str] = None
    pos = forms.CharField(widget=forms.HiddenInput())

    # Saves whether this is a static search form or not (for uneditable queries prepended to searches (see fctemplate))
    static = forms.CharField(widget=forms.HiddenInput())

    fld = forms.ChoiceField(required=True, widget=forms.Select())

    ncmp = forms.ChoiceField(required=True, widget=forms.Select())

    # Note: the placeholder attribute solves issue #135
    val = forms.CharField(widget=forms.TextInput(attrs={"placeholder": "search term"}))

    def clean(self):
        """This override of super.clean is so we can reconstruct the search inputs upon form_invalid in views.py"""
        self.saved_data = self.cleaned_data
        return self.cleaned_data

    def is_valid(self):
        data = self.cleaned_data
        fields = self.base_fields.keys()
        # Only validate if the pos field contains the posprefix - otherwise, it belongs to a different form class
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
        self.posprefix = self.composite_view_class.id
        self.fields["fld"].choices = self.advsrch_view_class.getAllSearchFieldChoices()
        self.fields[
            "ncmp"
        ].choices = self.composite_view_class.getAllComparisonChoices()


class AdvSearchPeakGroupsForm(BaseAdvSearchForm):
    """
    Advanced search form for the peakgroups output format that will be used inside a formset.
    """

    composite_view_class = PeakGroupsSearchView()


class AdvSearchPeakDataForm(BaseAdvSearchForm):
    """
    Advanced search form for the peakdata output format that will be used inside a formset.
    """

    composite_view_class = PeakDataSearchView()


class AdvSearchFluxCircForm(BaseAdvSearchForm):
    """
    Advanced search form for the fcirc output format that will be used inside a formset.
    """

    composite_view_class = FluxCircSearchView()


class AdvSearchForm:
    """
    A group of advanced search form classes
    """

    form_classes: Dict[str, BaseAdvSearchForm] = {}
    # These form field elements are actually (currently) created in javascript.
    format_select_list_name = "fmt"
    hierarchy_path_field_name = "pos"

    def __init__(self, *args, **kwargs):
        for form_class in (
            AdvSearchPeakGroupsForm(),
            AdvSearchPeakDataForm(),
            AdvSearchFluxCircForm(),
        ):
            id = form_class.composite_view_class.id
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
        # TODO: Can probably get the caret in the button image using:
        #   https://stackoverflow.com/questions/45424162/listing-a-choicefield-in-django-as-button
        widget=forms.Select(),
    )
    page = forms.CharField(widget=forms.HiddenInput())
    order_by = forms.CharField(widget=forms.HiddenInput())
    order_direction = forms.CharField(widget=forms.HiddenInput())
    adv_search_page_form = forms.CharField(
        widget=forms.HiddenInput()
    )  # Used to distinguish pager form submissions from advanced search submissions

    def clean(self):
        """
        This override of super.clean is so we can reconstruct the search inputs upon form_invalid in views.py
        """
        self.saved_data = self.cleaned_data
        return self.cleaned_data

    def update(self, page_id, rows_id, orderby_id, orderdir_id, rows_attrs={}):
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
                "ERROR: AdvSearchPageForm class already has an ID set for the page input"
            )
        rows.widget.attrs["id"] = rows_id

        # order_by input
        if (
            order_by.widget.attrs
            and "id" in order_by.widget.attrs
            and order_by.widget.attrs["id"] != orderby_id
        ):
            raise Exception(
                "ERROR: AdvSearchPageForm class already has an ID set for the page input"
            )
        order_by.widget.attrs["id"] = orderby_id

        # order_direction input
        if (
            order_direction.widget.attrs
            and "id" in order_direction.widget.attrs
            and order_direction.widget.attrs["id"] != orderdir_id
        ):
            raise Exception(
                "ERROR: AdvSearchPageForm class already has an ID set for the page input"
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

    def is_valid(self):
        # This triggers the setting of self.cleaned_data
        super().is_valid()
        data = self.cleaned_data
        fields = self.base_fields.keys()
        # Make sure all fields besides the order fields are present
        for field in fields:
            if field != "order_by" and field != "order_direction" and field not in data:
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
