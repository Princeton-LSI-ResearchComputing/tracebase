from typing import Dict, List, Optional

from django.forms import Form
from django.http.response import HttpResponseForbidden, HttpResponseRedirect
from django.views.generic.base import ContextMixin, TemplateResponseMixin
from django.views.generic.edit import ProcessFormView

# Based on:
#   https://stackoverflow.com/questions/15497693/django-can-class-based-views-accept-two-forms-at-a-time
#   https://gist.github.com/jamesbrobb/748c47f46b9bd224b07f

# TODO: Issue #370 refactor multiforms.py and views.AdvSearchView


class MultiFormMixin(ContextMixin):
    """
    This class identifies, retrieves, binds, and validates multiple types of form found on a page from a single view.
    Note, the purpose is not to handle formsets (multiple forms of the same type) though that case is also supported*.
    It can call multiple custom form_valid/form_invalid, create, and initial methods.  It does not process submitted
    forms (see ProcessMultipleFormsView).  There are 4 categories into which forms are identified: individual, grouped,
    mixed, and "all".  All form classes should be set by the user in the form_classes dict.  The keys of the dict
    depend on the category:

        - Individual: The name of the form field intended to identify the form
        - Grouped: Arbitrary key defined by the user, also contained in the grouped_forms list value.  The
          grouped_forms key instead, must be the name of the form field intended to identify the form as being part of
          a group of form types expected to be processed together.
        - Mixed: Arbitrary key defined by the user, also contained in the mixed_forms list value.  The mixed_forms key
          instead, must be the name of the form field intended to identify the form as being part of a mix of form
          types expected to be processed together.  The value of the identifying field must be one of the form_classes
          keys to indicate the form type to validate.
        - All: The keys in this case do not matter.  If the form contains no field the identifies it as "individual",
          "grouped", or "mixed", all forms in the form_classes dict are used.^

    In the mixed case, it is assumed that the field names are the same for each form type and the forms are
    differentiated based on things like autocomplete, choices in select lists, etc..  It is intended to be used with
    formsets combined with a single select list that allows the user to select the form type.  One example is an
    advanced search where the user selects a schema to search and the 3 fields in each form are for "database field",
    "condition" (e.g. "equals or "contains"), and "search term".

    * - I'm not entirely certain formsets are supported in the "individual" and "grouped" form cases because I haven't
        tested those, but it is supported in the mixed forms case.  The user is required to handle formsets in their
        code (e.g. in their `*form_valid` and `*form_invalid` methods).
    ^ - I suspect that this could only ever really work if there was only a single form type, but I'm not sure.
    """

    prefixes: Dict[str, str] = {}
    success_urls: Dict[str, str] = {}
    default_formid_fieldname = "action"

    # identifying_fields: Key is a field name and the value indicates whether to look in form_classes ("individual"),
    # grouped_forms ("grouped"), or mixed_forms ("mixed") to determine the form type(s)
    identifying_fields: Dict[str, str] = {}

    # form_classes: Key is the value in the identifying field that defines an individualform.  Value is a form type.
    form_classes: Dict[str, Form] = {}

    # grouped_forms: Key is the value in the identifying field that defines the group.  Value is a list of form_classes
    # keys.
    grouped_forms: Dict[str, List] = {}

    # mixed_forms: Key is the value in the identifying field that defines the mixed type.  Value is a list of
    # form_classes keys.  Note, this class does not differentiate the forms in any particular mix.  Each form's
    # is_valid method must recognize and ignore forms of other types.  All forms in a mixed form type must have the
    # same fields.
    mixed_forms: Dict[str, List] = {}

    debug = True  # Reports when an expected method is not defined

    # I know that initial is Dict[str, function], but I don't know what the initial function takes or
    # returns, as I don't use it, so I'm leaving it as ignore.
    initial = {}  # type: ignore
    prefix: Optional[str] = None
    success_url: Optional[str] = None

    def add_individual_form(self, identifying_field_name, form_class):
        """
        Add a form class that is identified simply by the presence of a field in the form with a specific name.
        """
        # Error-check the identifying_field_name
        if (
            identifying_field_name in self.identifying_fields.keys()
            and form_class.__name__
            != self.form_classes[identifying_field_name].__name__
        ):
            raise MultiformIdentifierConflict(
                f"Identifying form field {identifying_field_name} already exists as a ["
                f"{self.identifying_fields[identifying_field_name]}] form.  Cannot add an individual form identified "
                "by the same field name."
            )

        # Error-check the form_class
        if (
            identifying_field_name in self.form_classes.keys()
            and form_class.__name__
            != self.form_classes[identifying_field_name].__name__
        ):
            raise MultiformIdentifierConflict(
                f"A different form class [{self.form_classes[identifying_field_name].__name__}] already exists under "
                f"key [{identifying_field_name}].  Cannot add form class [{form_class.__name__}]."
            )

        self.identifying_fields[identifying_field_name] = "individual"
        if identifying_field_name not in self.form_classes.keys():
            self.form_classes[identifying_field_name] = form_class

    def add_grouped_forms(self, identifying_field_name, form_classes_dict):
        """
        Add a set of form classes that are grouped together.  The group is identified by the presence of a field in the
        form with a specific name.
        """
        # Error-check the identifying_field_name
        if (
            identifying_field_name in self.identifying_fields.keys()
            and self.identifying_fields[identifying_field_name] != "grouped"
        ):
            raise MultiformIdentifierConflict(
                f"Identifying form field {identifying_field_name} already exists as a ["
                f"{self.identifying_fields[identifying_field_name]}] form.  Cannot add grouped forms identified by "
                "the same field name."
            )

        # Error-check the form_classes_dict
        for key in form_classes_dict.keys():
            if (
                key in self.form_classes.keys()
                and form_classes_dict[key].__name__ != self.form_classes[key].__name__
            ):
                raise MultiformIdentifierConflict(
                    f"A different form class [{self.form_classes[key].__name__}] already exists under key [{key}].  "
                    f"Cannot add form class [{form_classes_dict[key].__name__}] for group [{identifying_field_name}]."
                )
            if key not in self.form_classes.keys():
                self.form_classes[key] = form_classes_dict[key]

        self.identifying_fields[identifying_field_name] = "grouped"
        if identifying_field_name not in self.grouped_forms:
            self.grouped_forms[identifying_field_name] = {}
        self.grouped_forms[identifying_field_name] = list(form_classes_dict.keys())

    def add_mixed_forms(self, identifying_field_name, form_classes_dict):
        """
        Add a set of form classes that are part of a mixed form.  The mix is identified by the presence of a field in
        the form with a specific name.  The identifying field is the field whose value will indicate the selected form
        type.
        """
        # Error-check the identifying_field_name
        if (
            identifying_field_name in self.identifying_fields.keys()
            and self.identifying_fields[identifying_field_name] != "mixed"
        ):
            raise MultiformIdentifierConflict(
                f"Identifying form field {identifying_field_name} already exists as a ["
                f"{self.identifying_fields[identifying_field_name]}] form.  Cannot add mixed forms identified "
                "by the same field name."
            )

        # Error-check the form_classes_dict
        for key in form_classes_dict.keys():
            if (
                key in self.form_classes.keys()
                and form_classes_dict[key].__name__ != self.form_classes[key].__name__
            ):
                raise MultiformIdentifierConflict(
                    f"A different form class [{self.form_classes[key].__name__}] already exists under key [{key}].  "
                    f"Cannot add form class [{form_classes_dict[key].__name__}]."
                )
            if key not in self.form_classes.keys():
                self.form_classes[key] = form_classes_dict[key]

        self.identifying_fields[identifying_field_name] = "mixed"
        if identifying_field_name not in self.mixed_forms:
            self.mixed_forms[identifying_field_name] = {}
        self.mixed_forms[identifying_field_name] = list(form_classes_dict.keys())

    def __init__(self, *args, **kwargs):
        """
        This ensures everything is configured correctly.
        """
        super().__init__(*args, **kwargs)
        seen_forms = {}

        for group_key in self.grouped_forms.keys():
            form_classes_dict = {}
            for form_key in self.grouped_forms[group_key]:
                seen_forms[form_key] = 1
                if form_key not in self.form_classes:
                    raise MultiformMissingFormClass(
                        f"Group [{group_key}]'s form key: [{form_key}] is not in the form_classes dict."
                    )
                else:
                    form_classes_dict[form_key] = self.form_classes[form_key]
            self.add_grouped_forms(group_key, form_classes_dict)

        for mix_key in self.mixed_forms.keys():
            form_classes_dict = {}
            for form_key in self.mixed_forms[mix_key]:
                seen_forms[form_key] = 1
                if form_key not in self.form_classes:
                    raise MultiformMissingFormClass(
                        f"Mix [{mix_key}]'s form key: [{form_key}] is not in the form_classes dict."
                    )
                else:
                    form_classes_dict[form_key] = self.form_classes[form_key]
            self.add_mixed_forms(mix_key, form_classes_dict)

        for form_key in self.form_classes.keys():
            if form_key in seen_forms.keys():
                self.add_individual_form(form_key, self.form_classes[form_key])

    def get_form_classes(self):
        return self.form_classes

    def get_forms(self, form_classes, form_names=None, bind_all=False):
        return dict(
            [
                (
                    key,
                    self._create_form(
                        key, klass, (form_names and key in form_names) or bind_all
                    ),
                )
                for key, klass in form_classes.items()
            ]
        )

    def get_form_kwargs(self, form_name, bind_form=False):
        kwargs = {}
        init = self.get_initial(form_name)
        if init:
            kwargs.update({"initial": self.get_initial(form_name)})
        pfx = self.get_prefix(form_name)
        if pfx:
            kwargs.update({"prefix": self.get_prefix(form_name)})

        if bind_form:
            bound = self._bind_form_data()
            if len(bound.keys()) > 0:
                kwargs.update(bound)

        return kwargs

    def forms_valid(self, forms, one_form_name=None):
        calls = []
        if one_form_name is not None:
            if one_form_name not in forms:
                raise MultiformFormNotFound(
                    "Form [{one_form_name}] not in the forms submitted."
                )
            form_valid_method = "%s_form_valid" % one_form_name
            if hasattr(self, form_valid_method):
                calls.append([form_valid_method, forms[one_form_name], one_form_name])
        else:
            for form_name in forms.keys():
                form_valid_method = "%s_form_valid" % form_name
                if hasattr(self, form_valid_method):
                    calls.append([form_valid_method, forms[form_name], form_name])

        if len(calls) == 0 and hasattr(self, "form_valid"):
            # Will call form_valid with all forms
            calls.append(["form_valid", forms, None])

        if len(calls) == 1:
            form_valid_method, form_or_forms, form_name = calls[0]
            surl = self.get_success_url(form_name)

            # if there is a success URL
            if surl is not None and surl != "":
                getattr(self, form_valid_method)(form_or_forms)
                return HttpResponseRedirect(surl)

            return getattr(self, form_valid_method)(form_or_forms)
        else:
            # calls must have >1 members. Call each form valid method
            for call in calls:
                form_valid_method, form, form_name = call
                getattr(self, form_valid_method)(form)

            if self.debug and len(calls) == 1 and calls[0][0] == "form_valid":
                print(
                    f"WARNING: ({','.join(forms.keys())})_form_valid method(s) not found"
                )

            # If one_form_name has a value but calls is empty, we are here because no calls were idenified (i.e. we're
            # in the `else` instead of the above `if`).  There was no generic form_valid method, nor was there a
            # specific method.  Returning (whether the success url is defined or not) implies successful validation, so
            # it makes no sense.
            if one_form_name is not None and len(calls) == 0:
                raise MultiformNoValidationMethodFound(
                    f"There is neither a specific `{one_form_name}_form_valid` method nor a generic `form_valid` "
                    f"method to handle form type [{one_form_name}]."
                )

            # Get common success URL for all validated forms
            surl = self.get_success_url()

            # if there is a success URL (ignoring potentially multiple success URLs)
            if surl is not None and surl != "":
                return HttpResponseRedirect(surl)

            # Default to "same/self" view
            return self.render_to_response(self.get_context_data(forms=forms))

    def forms_invalid(self, forms):
        calls = []
        for form_name in forms.keys():
            form_invalid_method = "%s_form_invalid" % form_name
            if hasattr(self, form_invalid_method):
                calls.append([form_invalid_method, forms[form_name]])

        if self.debug and len(calls) == 0:
            print(
                f"WARNING: ({','.join(forms.keys())})_form_invalid method(s) not found"
            )

        if len(calls) == 0 and hasattr(self, "form_invalid"):
            # Will call form_invalid with all forms
            calls.append(["form_invalid", forms])

        if len(calls) == 1:
            form_invalid_method, form_or_forms = calls[0]
            return getattr(self, form_invalid_method)(form_or_forms)
        else:
            for call in calls:
                form_invalid_method, form = call
                getattr(self, form_invalid_method)(form)
                # Not entirely sure this is correct, but with the current code, this should never execute

            return self.render_to_response(self.get_context_data(forms=forms))

    def get_initial(self, form_name):
        initial_method = "get_%s_initial" % form_name
        if hasattr(self, initial_method):
            return getattr(self, initial_method)()
        else:
            return self.initial.copy()

    def get_prefix(self, form_name):
        """
        Tries to get a specific prefix (for use in element IDs) for an optional given form_name, defaulting to a single
        prefix.
        """
        return self.prefixes.get(form_name, self.prefix)

    def get_success_url(self, form_name=None):
        """
        Tries to get a specific URL for the optional given form_name, defaulting to a single success URL.
        """
        return self.success_urls.get(form_name, self.success_url)

    def _create_form(self, form_name, klass, bind_form):
        form_kwargs = self.get_form_kwargs(form_name, bind_form)
        form_create_method = "create_%s_form" % form_name
        if hasattr(self, form_create_method):
            form = getattr(self, form_create_method)(**form_kwargs)
        elif len(form_kwargs.keys()) > 0:
            try:
                form = klass(**form_kwargs)
            except TypeError as te:
                print(te)
                form = klass.__new__()
        else:
            form = klass()
        return form

    def _bind_form_data(self):
        if self.request.method in ("POST", "PUT"):
            retdict = {}
            data = self.request.POST
            if data:
                retdict["data"] = data
            files = self.request.FILES
            if files:
                retdict["files"] = files
            return retdict
        return {}


class ProcessMultipleFormsView(ProcessFormView):
    def get(self, request, *args, **kwargs):
        form_classes = self.get_form_classes()
        forms = self.get_forms(form_classes)
        return self.render_to_response(self.get_context_data(forms=forms))

    def post(self, request, *args, **kwargs):
        form_classes = self.get_form_classes()
        form_type, form_key = self._get_forms_type(request)

        if form_type == "individual":
            return self._process_individual_form(form_key, form_classes)
        elif form_type == "grouped":
            return self._process_grouped_forms(form_key, form_classes)
        elif form_type == "mixed":
            return self._process_mixed_forms(form_key, form_classes)
        elif form_type == "all":
            return self._process_all_forms(form_classes)

    def _get_forms_type(self, request):
        """
        This looks through the fields that identify the form types (individual, grouped, mixed, or all) and returns the
        type (individual, grouped, mixed, or all) and the associated field that identifies an instance of that type.
        """
        forms_type = "all"
        identifying_field = None
        for id_field in self.identifying_fields.keys():
            if id_field in request.POST:
                if identifying_field is None:
                    forms_type = self.identifying_fields[id_field]
                    identifying_field = id_field
                else:
                    raise MultiformAmbiguousFormIdentification(
                        f"Form matches multiple form types: [{identifying_field}] and [{id_field}]"
                    )
        if identifying_field is None and self.default_formid_fieldname in request.POST:
            identifying_field = self.default_formid_fieldname
            if self._individual_exists(identifying_field):
                forms_type = "individual"
            elif self._group_exists(identifying_field):
                forms_type = "grouped"
            elif self._mixed_exists(identifying_field):
                forms_type = "mixed"
        return forms_type, identifying_field

    def _individual_exists(self, form_name):
        return form_name in self.form_classes

    def _group_exists(self, group_name):
        return group_name in self.grouped_forms

    def _mixed_exists(self, mix_name):
        return mix_name in self.mixed_forms

    def _process_individual_form(self, form_name, form_classes):
        forms = self.get_forms(form_classes, (form_name,))
        form = forms.get(form_name)
        if not form:
            return HttpResponseForbidden()
        elif form.is_valid():
            return self.forms_valid(forms)
        else:
            return self.forms_invalid(forms)

    def _process_grouped_forms(self, group_name, form_classes):
        form_names = self.grouped_forms[group_name]
        forms = self.get_forms(form_classes, form_names)
        if all([forms.get(form_name).is_valid() for form_name in form_names.values()]):
            return self.forms_valid(forms)
        else:
            return self.forms_invalid(forms)

    def _process_mixed_forms(self, mix_name, form_classes):
        """
        This processes all forms in the mixed forms using the validation code of the selected form type only (the value
        contained in the `mix_name` form field).  I.e. the selected form class's `is_valid` method must be able to tell
        the difference between each form type.
        """
        # Get the selected form type from the form using the mix_name input (e.g. a select list)
        form_kwargs = self.get_form_kwargs("", True)
        selected_form = form_kwargs["data"][mix_name]
        if selected_form not in self.form_classes:
            raise MultiformInvalidSelectedMixedFormType(
                f"Form type [{selected_form}] obtained from form field [{mix_name}] is not in the form_classes dict.  "
                f"The form type select list must be populated with one of: [{','.join(self.form_classes.keys())}]."
            )

        # I only want to get the selected form type.  I want the selected form class to be bound to kwargs.
        # This should result in a call to forms_valid with all the form data (including the not-selected form data -
        # which is what we want, so that the user's entered searches are retained.
        selected_form_classes = {}
        selected_form_classes[selected_form] = form_classes[selected_form]
        forms = self.get_forms(selected_form_classes, None, True)

        # Only validate with the above selected form type
        # Note, the selected form must ignore forms of other types (i.e. always return true)
        if all([form.is_valid() for form in forms.values()]):
            return self.forms_valid(forms)
        else:
            return self.forms_invalid(forms)

    def _process_all_forms(self, form_classes):
        forms = self.get_forms(form_classes, None, True)
        if all([form.is_valid() for form in forms.values()]):
            return self.forms_valid(forms)
        else:
            return self.forms_invalid(forms)


class BaseMultipleFormsView(MultiFormMixin, ProcessMultipleFormsView):
    """
    A base view for displaying several forms.
    """


class MultiFormsView(TemplateResponseMixin, BaseMultipleFormsView):
    """
    A view for displaying several forms, and rendering a template response.
    """


class MultiformIdentifierConflict(Exception):
    pass


class MultiformMissingFormClass(Exception):
    pass


class MultiformFormNotFound(Exception):
    pass


class MultiformNoValidationMethodFound(Exception):
    pass


class MultiformAmbiguousFormIdentification(Exception):
    pass


class MultiformInvalidSelectedMixedFormType(Exception):
    pass
