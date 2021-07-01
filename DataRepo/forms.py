from django import forms


class AdvSearchPeakGroupsForm(forms.Form):
    """
    Advanced search form for the peakgroups output format that will be used inside a formset.
    """

    # "pos" keeps track of a form's hierarchical position, managed in javascript
    # It encodes grouptypes as "any" or "all", indicating whether the members are joined with a logical "or" or "and"
    # Example: "all0.0" indicates the (first group (type "and")) . (first form), i.e. "0.0" where the first 0 is an
    # "and" group
    # IMPORTANT: If the field name ("pos") is changed, it must be updated in the javascript code which uses the field
    # name to know when to increnemt the formset index in the saveSearchQueryHierarchyHelper function using the
    # variable "count".
    pos = forms.CharField(widget=forms.HiddenInput())

    fld = forms.ChoiceField(
        choices=(
            # PeakData Searchable Fields in the PeakGroups Output Format
            ("labeled_element", "Atom"),
            ("labeled_count", "Label Count"),
            ("fraction", "Fraction"),
            # PeakData Searchable Fields in the PeakGroups Output Format
            ("peak_group__name", "Presumed Output Compound"),
            ("peak_group__total_abundance", "TIC"),
            ("peak_group__normalized_labeling", "Norm Fraction"),
            # PeakData Searchable Fields in the PeakGroups Output Format
            ("peak_group__ms_run__sample__name", "Sample"),
            # PeakData Searchable Fields in the PeakGroups Output Format
            ("peak_group__ms_run__sample__tissue__name", "Tissue"),
            # PeakData Searchable Fields in the PeakGroups Output Format
            ("peak_group__ms_run__sample__animal__name", "Animal"),
            ("peak_group__ms_run__sample__animal__feeding_status", "Feeding Status"),
            (
                "peak_group__ms_run__sample__animal__tracer_infusion_rate",
                "Infusion Rate",
            ),
            (
                "peak_group__ms_run__sample__animal__tracer_infusion_concentration",
                "[Infusion]",
            ),
            # PeakData Searchable Fields in the PeakGroups Output Format
            (
                "peak_group__ms_run__sample__animal__tracer_compound__name",
                "Input Compound",
            ),
            # PeakData Searchable Fields in the PeakGroups Output Format
            ("peak_group__ms_run__sample__animal__studies__name", "Study"),
        ),
        widget=forms.Select(),
    )

    ncmp = forms.ChoiceField(
        choices=(
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("icontains", "contains"),
            ("not_icontains", "does not contain"),
            ("istartswith", "starts with"),
            ("not_istartswith", "does not start with"),
            ("iendswith", "ends with"),
            ("not_iendswith", "does not end with"),
            ("gt", ">"),
            ("gte", ">="),
            ("lt", "<"),
            ("lte", "<="),
            ("not_isnull", "has a value *"),
            ("isnull", "does not have a value *"),
            # ToDo: This is a placeholder until dynamic form updating & validation is implemented
            ("* - Ignores text field", ()),
            ("* - but must enter any value", ()),
        ),
        widget=forms.Select(),
    )

    # TODO: Currently, I am only providing this one field type.  Eventually, I will work out a way to dynamically
    # update this based on the model's field type
    val = forms.CharField(widget=forms.TextInput())

    def clean(self):
        """This override of super.clean is so we can reconstruct the search inputs upon form_invalid in views.py"""
        self.saved_data = self.cleaned_data
        return self.cleaned_data
