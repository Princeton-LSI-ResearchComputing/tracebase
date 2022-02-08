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

    qryjson = forms.JSONField(widget=forms.HiddenInput())
    rows = forms.CharField(widget=forms.HiddenInput())
    page = forms.CharField(widget=forms.HiddenInput())
    order_by = forms.CharField(widget=forms.HiddenInput())
    order_direction = forms.CharField(widget=forms.HiddenInput())

    def clean(self):
        """This override of super.clean is so we can reconstruct the search inputs upon form_invalid in views.py"""
        self.saved_data = self.cleaned_data
        return self.cleaned_data


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
