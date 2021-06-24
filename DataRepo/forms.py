from django import forms


class AdvSearchPeakGroupsForm(forms.Form):
    """
    Advanced search form for the peakgroups output format that will be used inside a formset.
    """
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
            ("peak_group__ms_run__sample__animal__tracer_infusion_rate", "Infusion Rate"),
            ("peak_group__ms_run__sample__animal__tracer_infusion_concentration", "[Infusion]"),

            # PeakData Searchable Fields in the PeakGroups Output Format
            ("peak_group__ms_run__sample__animal__tracer_compound__name", "Input Compound"),

            # PeakData Searchable Fields in the PeakGroups Output Format
            ("peak_group__ms_run__sample__animal__studies__name", "Study")
        ),
        widget=forms.Select()
    )

    # TODO: Currently, I am only providing these 2 options.  Eventually, I will work out a way to dynamically update this based on the model's field type
    ncmp = forms.ChoiceField(
        choices=(
            ('iexact', 'is'),
            ('not_iexact', 'is not')
        ),
        widget=forms.Select()
    )

    # TODO: Currently, I am only providing this one field type.  Eventually, I will work out a way to dynamically update this based on the model's field type
    val = forms.CharField(widget=forms.TextInput())
