import datetime
import os
from typing import Dict, Optional, Type

from django.forms import (
    BooleanField,
    CharField,
    ChoiceField,
    ClearableFileInput,
    FileField,
    Form,
    HiddenInput,
    JSONField,
    Select,
    TextInput,
    ValidationError,
    formset_factory,
)

from DataRepo.formats.dataformat import Format
from DataRepo.formats.fluxcirc_dataformat import FluxCircFormat
from DataRepo.formats.peakdata_dataformat import PeakDataFormat
from DataRepo.formats.peakgroups_dataformat import PeakGroupsFormat
from DataRepo.formats.search_group import SearchGroup
from DataRepo.models.lc_method import LCMethod
from DataRepo.models.msrun_sequence import MSRunSequence
from DataRepo.models.researcher import get_researchers
from DataRepo.utils.file_utils import date_to_string, is_excel
from DataRepo.widgets import AutoCompleteTextInput

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


class BaseAdvSearchForm(Form):
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
    pos = CharField(widget=HiddenInput())

    # Saves whether this is a static search form or not (for uneditable queries prepended to searches (see fctemplate))
    static = CharField(widget=HiddenInput())

    fld = ChoiceField(required=True, widget=Select())

    ncmp = ChoiceField(required=True, widget=Select())

    # Note: the placeholder attribute solves issue #135
    val = CharField(widget=TextInput(attrs={"placeholder": "search term"}))

    # This can be used to indicate the units or format of the term supplied to val, e.g. "minutes" for a DurationField
    units = ChoiceField(required=True, widget=Select())

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
            # TODO: These should not be creating instances.  `format_class` is a class attribute.  Remove the parens and
            # __class__ below
            AdvSearchPeakGroupsForm(),
            AdvSearchPeakDataForm(),
            AdvSearchFluxCircForm(),
        ):
            id = form_class.format_class.id
            self.form_classes[id] = formset_factory(form_class.__class__)


class AdvSearchDownloadForm(Form):
    """
    Advanced search download form for any advanced search data.
    """

    qryjson = JSONField(widget=HiddenInput())

    def clean(self):
        """This override of super.clean is so we can reconstruct the search inputs upon form_invalid in views.py"""
        self.saved_data = self.cleaned_data
        return self.cleaned_data


class RowsPerPageSelectWidget(Select):
    template_name = "DataRepo/widgets/rowsperpage_select.html"
    option_template_name = "DataRepo/widgets/rowsperpage_select_option.html"


class AdvSearchPageForm(Form):
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

    qryjson = JSONField(widget=HiddenInput())
    rows = ChoiceField(
        choices=ROWS_PER_PAGE_CHOICES,
        widget=RowsPerPageSelectWidget(),
    )
    page = CharField(widget=HiddenInput())
    order_by = CharField(widget=HiddenInput())
    order_direction = CharField(widget=HiddenInput())
    # This field's name ("paging") is used to distinguish pager form submissions from other form submissions
    paging = CharField(widget=HiddenInput())
    show_stats = BooleanField(widget=HiddenInput())
    stats = JSONField(widget=HiddenInput())

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
            raise ValueError(
                "ERROR: AdvSearchPageForm class already has an ID set for the page input"
            )
        page.widget.attrs["id"] = page_id

        # rows input
        if (
            rows.widget.attrs
            and "id" in rows.widget.attrs
            and rows.widget.attrs["id"] != rows_id
        ):
            raise ValueError(
                "ERROR: AdvSearchPageForm class already has an ID set for the rows input"
            )
        rows.widget.attrs["id"] = rows_id

        # order_by input
        if (
            order_by.widget.attrs
            and "id" in order_by.widget.attrs
            and order_by.widget.attrs["id"] != orderby_id
        ):
            raise ValueError(
                "ERROR: AdvSearchPageForm class already has an ID set for the order_by input"
            )
        order_by.widget.attrs["id"] = orderby_id

        # order_direction input
        if (
            order_direction.widget.attrs
            and "id" in order_direction.widget.attrs
            and order_direction.widget.attrs["id"] != orderdir_id
        ):
            raise ValueError(
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
                raise ValueError(
                    f"ERROR: AdvSearchPageForm class already has a [{key}] set for the rows input"
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
                    raise ValueError(
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


class MultipleFileInput(ClearableFileInput):
    """Subclass of ClearableFileInput that specifically allows multiple selected files"""

    allow_multiple_selected = True


class MultipleFileField(FileField):
    """Subclass of FileField that validates multiple files submitted in a single form field"""

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("widget", MultipleFileInput())
        super().__init__(*args, **kwargs)

    def clean(self, data, initial=None):
        single_file_clean = super().clean
        if isinstance(data, (list, tuple)):
            result = [single_file_clean(d, initial) for d in data]
        else:
            result = single_file_clean(data, initial)
        return result


def create_BuildSubmissionForm() -> Type[Form]:
    """This class works around a problem that raises an exception when migrations are created or checked and there exist
    database calls at the class level.  See: https://stackoverflow.com/a/56878154"""

    class BuildSubmissionForm(Form):
        """
        Form for users to compile and validate their study with Peak Annotation files
        """

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # The form-control class is used to identify form fields among cloned form instances
            for visible in self.visible_fields():
                visible.field.widget.attrs["class"] = "form-control"
            for hidden in self.hidden_fields():
                hidden.field.widget.attrs["class"] = "form-control"

        # mode is "autofill" or "validate"
        mode = CharField(widget=HiddenInput())

        # "Study doc" (hidden on the start tab)
        study_doc = FileField(
            required=False, widget=ClearableFileInput(attrs={"multiple": False})
        )

        # This following fields are for a single file's form, but will be replicated using javascript.  The fields above
        # should only occur once, but all should be a part of the same form submission.

        peak_annotation_file = FileField(
            required=False,
            widget=ClearableFileInput(attrs={"multiple": False}),
        )

        # The following fields are for specifying sequence details for each peak annotation file (and will also be
        # replicated using javascript)
        operator = CharField(
            required=False,
            widget=AutoCompleteTextInput(
                "operators_datalist",
                get_researchers() if len(get_researchers()) > 0 else ["Anonymous"],
                datalist_manual=True,
                attrs={
                    "title": "Mass Spec Operator, e.g. 'John Doe'",
                    "placeholder": "ms operator",
                    "autocomplete": "off",
                },
            ),
        )
        protocol = CharField(
            required=False,
            widget=AutoCompleteTextInput(
                "protocols_datalist",
                (
                    list(LCMethod.objects.values_list("name", flat=True))
                    if LCMethod.objects.count() > 0
                    else [LCMethod.create_name(LCMethod.DEFAULT_TYPE)]
                ),
                datalist_manual=True,
                attrs={
                    "title": (
                        "Liquid Chromatography Protocol, e.g. '"
                        + LCMethod.create_name(LCMethod.DEFAULT_TYPE)
                        + "'"
                    ),
                    "placeholder": MSRunSequence.get_most_used_protocol(
                        default="protocol"
                    ),
                    "autocomplete": "off",
                },
            ),
        )
        instrument = CharField(
            required=False,
            widget=AutoCompleteTextInput(
                "instruments_datalist",
                MSRunSequence.INSTRUMENT_CHOICES,
                datalist_manual=True,
                attrs={
                    "title": "MS Instrument Model, e.g. '"
                    + MSRunSequence.INSTRUMENT_DEFAULT
                    + "'",
                    "placeholder": MSRunSequence.get_most_used_instrument(
                        default="instrument"
                    ),
                    "autocomplete": "off",
                },
            ),
        )
        run_date = CharField(
            required=False,
            widget=AutoCompleteTextInput(
                "date_datalist",
                [
                    date_to_string(
                        datetime.date.today() - datetime.timedelta(days=days)
                    )
                    for days in range(14)
                ],
                datalist_manual=True,
                attrs={
                    "title": "MS Run Date, e.g. '"
                    + date_to_string(datetime.date.today())
                    + "'",
                    "placeholder": "run date",
                    "autocomplete": "off",
                },
            ),
        )

        @property
        def operator_datalist(self):
            return self.fields["operator"].widget.datalist()

        @property
        def protocol_datalist(self):
            return self.fields["protocol"].widget.datalist()

        @property
        def instrument_datalist(self):
            return self.fields["instrument"].widget.datalist()

        @property
        def run_date_datalist(self):
            return self.fields["run_date"].widget.datalist()

        def is_valid(self):
            super().is_valid()

            allowed_delimited_exts = ["csv", "tsv"]

            # Fields we need to check
            study_doc = self.cleaned_data.get("study_doc", None)
            peak_annotation_file = self.cleaned_data.get("peak_annotation_file", None)
            mode = self.cleaned_data.get("mode", None)

            if (study_doc is None and peak_annotation_file is None) or (
                study_doc is not None and not is_excel(study_doc)
            ):
                return False

            if peak_annotation_file is not None:
                peak_annot_filepath = peak_annotation_file.temporary_file_path()
                if not is_excel(peak_annot_filepath):
                    # Excel files do not need a specific extension, but delimited files do...
                    ext = list(os.path.splitext(peak_annotation_file))[1]
                    if ext not in allowed_delimited_exts:
                        return False

                # Note, all the sequence metadata fields should allow novel entries (even the instrument)

            # All Sequence details are required when a peak annotation file is supplied
            if mode is None or mode.strip() == "":
                return False

            return True

        def clean(self):
            """Ensure that at least a study doc or peak annotation file is supplied and that the study doc is an excel
            file and the peak annotation file is an excel, csv, or tsv.

            Args:
                None
            Exceptions:
                Raises:
                    None
                Buffers:
                    ValidationError
            Returns:
                self.cleaned_data (dict)
            """
            super().clean()
            allowed_delimited_exts = ["csv", "tsv"]

            # Fields (excluding mode)
            study_doc = self.cleaned_data.get("study_doc", None)
            peak_annotation_file = self.cleaned_data.get("peak_annotation_file", None)
            mode = self.cleaned_data.get("mode", None)

            # NOTE: All of these errors are added as non-field-errors because the form is processed as a form inside a
            # formset, but the formset is constructed using javascript from a regular form, so the original formset
            # object containing errors cannot associate field errors with fields that do not get re-populated (and
            # they're not bothered to be repopulated because you cannot repopulate files anyway).

            if study_doc is None and peak_annotation_file is None:
                if mode is None or mode not in ["autofill", "validate"]:
                    self.add_error(
                        None,
                        ValidationError(
                            "At least 1 file is required.",
                            code="TooFewFiles",
                        ),
                    )
                elif mode == "autofill":
                    self.add_error(
                        None,
                        ValidationError(
                            "At least 1 peak annotation file is required.",
                            code="TooFewFiles",
                        ),
                    )
                else:
                    self.add_error(
                        None,
                        ValidationError(
                            "At least 1 study doc is required.",
                            code="TooFewFiles",
                        ),
                    )
            elif study_doc is not None and not is_excel(study_doc):
                self.add_error(
                    None,
                    ValidationError(
                        "Study doc must be an excel file.",
                        code="InvalidStudyFile",
                    ),
                )

            if peak_annotation_file is not None:
                peak_annot_filepath = peak_annotation_file.temporary_file_path()
                if not is_excel(peak_annot_filepath):
                    # Excel files do not need a specific extension, but delimited files do...
                    _, ext = os.path.splitext(peak_annotation_file)
                    if ext not in allowed_delimited_exts:
                        self.add_error(
                            None,
                            ValidationError(
                                f"Peak annotation files must be excel or {allowed_delimited_exts}.",
                                code="InvalidPeakAnnotFile",
                            ),
                        )

                # Note, all the sequence metadata fields should allow novel entries (even the instrument)

            return self.cleaned_data

    return BuildSubmissionForm
