from typing import Dict, Optional

from django import forms
from django.http.response import HttpResponseForbidden, HttpResponseRedirect
from django.views.generic.base import ContextMixin, TemplateResponseMixin
from django.views.generic.edit import ProcessFormView

# Based on:
#   https://stackoverflow.com/questions/15497693/django-can-class-based-views-accept-two-forms-at-a-time
#   https://gist.github.com/jamesbrobb/748c47f46b9bd224b07f


class MultiFormMixin(ContextMixin):

    form_classes: Dict[str, forms.Form] = {}
    prefixes: Dict[str, str] = {}
    success_urls: Dict[str, str] = {}
    grouped_forms: Dict[str, str] = {}

    # The mixed form type is a single form submission containing any number (>0) of formsets of any
    #   number (>1) of types
    # Only 1 formset type (based on a selected form field) will be validated
    # To use a mixed form, the following must have values: form_classes, mixedform_selected_formtype,
    #   mixedform_prefix_field

    # mixedform_selected_formtype defines the name of a form field (added e.g. via javascript) whose
    # value upon submit contains a value indicating the selected form type to be validated (e.g. a key
    # from form_classes)
    mixedform_selected_formtype = ""
    # mixedform_prefix_field defines the name of a form field whose value upon submit must start with
    # the value found in the mixedform_selected_formtype field.  Only forms with this match will be
    # validated (though all forms' values will be submitted).
    mixedform_prefix_field = ""

    # I know that initial is Dict[str, function], but I don't know what the initial function takes or
    # returns, as I don't use it, so I'm leaving it as ignore.
    initial = {}  # type: ignore
    prefix: Optional[str] = None
    success_url: Optional[str] = None

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
        kwargs.update({"initial": self.get_initial(form_name)})
        kwargs.update({"prefix": self.get_prefix(form_name)})

        if bind_form:
            kwargs.update(self._bind_form_data())

        return kwargs

    def forms_valid(self, forms):
        num_valid_calls = 0
        for form_name in forms.keys():
            form_valid_method = "%s_form_valid" % form_name
            print("CALLING",form_valid_method)
            if hasattr(self, form_valid_method):
                #### TODO: THIS NEEDS TO BE SUSTAINABLE, I.E. DO IT RIGHT.  PROB SHOULD RETURN USING SUCCESS URL
                #### Added return here just for proof of concept
                return getattr(self, form_valid_method)(forms[form_name])
                num_valid_calls += 1
        if self._mixed_exists() and num_valid_calls == 0:
            return self.form_valid(forms)
        else:
            return HttpResponseRedirect(self.get_success_url(form_name))

    def forms_invalid(self, forms):
        num_invalid_calls = 0
        for form_name in forms.keys():
            form_invalid_method = "%s_form_valid" % form_name
            if hasattr(self, form_invalid_method):
                getattr(self, form_invalid_method)(forms[form_name])
                num_invalid_calls += 1
        if self._mixed_exists() and num_invalid_calls == 0:
            return self.form_invalid(forms)
        else:
            return self.render_to_response(self.get_context_data(forms=forms))

    def get_initial(self, form_name):
        initial_method = "get_%s_initial" % form_name
        if hasattr(self, initial_method):
            return getattr(self, initial_method)()
        else:
            return self.initial.copy()

    def get_prefix(self, form_name):
        return self.prefixes.get(form_name, self.prefix)

    def get_success_url(self, form_name=None):
        return self.success_urls.get(form_name, self.success_url)

    def _create_form(self, form_name, klass, bind_form):
        form_kwargs = self.get_form_kwargs(form_name, bind_form)
        form_create_method = "create_%s_form" % form_name
        if hasattr(self, form_create_method):
            form = getattr(self, form_create_method)(**form_kwargs)
        else:
            form = klass(**form_kwargs)
        return form

    def _bind_form_data(self):
        if self.request.method in ("POST", "PUT"):
            return {
                "data": self.request.POST,
                "files": self.request.FILES,
            }
        return {}

    def _mixed_exists(self):
        if self.mixedform_prefix_field or self.mixedform_selected_formtype:
            if not self.mixedform_prefix_field or not self.mixedform_selected_formtype:
                print(
                    "ERROR: Both mixedform_prefix_field and mixedform_selected_formtype must be set to use mixed ",
                    "forms.  The value of the form field defined by mixedform_selected_formtype must be contained in ",
                    "the value of the form field defined by mixedform_prefix_field.",
                )
        elif (
            self.mixedform_prefix_field
            and self.mixedform_selected_formtype
            and len(self.form_classes.keys()) < 2
        ):
            print(
                "ERROR: form_classes must contain at least 2 form classes to used the mixed form type."
            )
        return (
            len(self.form_classes.keys()) > 1
            and self.mixedform_prefix_field
            and self.mixedform_selected_formtype
        )


class ProcessMultipleFormsView(ProcessFormView):
    def get(self, request, *args, **kwargs):
        form_classes = self.get_form_classes()
        forms = self.get_forms(form_classes)
        return self.render_to_response(self.get_context_data(forms=forms))

    def post(self, request, *args, **kwargs):
        form_classes = self.get_form_classes()
        form_name = request.POST.get("action")
        if self._individual_exists(form_name) and not self._mixed_exists():
            return self._process_individual_form(form_name, form_classes)
        elif self._group_exists(form_name) and not self._mixed_exists():
            return self._process_grouped_forms(form_name, form_classes)
        elif self._mixed_exists():
            print("FORM NAME: ", form_name)
            print("FORM CLASSES: ", form_classes)
            return self._process_mixed_forms(form_classes)
        else:
            return self._process_all_forms(form_classes)

    def _individual_exists(self, form_name):
        return form_name in self.form_classes

    def _group_exists(self, group_name):
        return group_name in self.grouped_forms

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

    def _process_mixed_forms(self, form_classes):
        # Get the selected form type using the mixedform_selected_formtype
        form_kwargs = self.get_form_kwargs("", True)
        selected_formtype = form_kwargs["data"][self.mixedform_selected_formtype]

        # I only want to get the forms in the context of the selected formtype.  That is managed by the content
        #   of the dict passed to get_forms.  And I want that form data to be bound to kwargs.  That is
        #   accomplished by supplying the desired key in the second argument to get_forms.
        # These 2 together should result in a call to forms_valid with all the form data (including the not-
        #   selected form data - which is what we want, so that the user's entered searches are retained.
        selected_form_classes = {}
        selected_form_classes[selected_formtype] = form_classes[selected_formtype]
        formsets = self.get_forms(selected_form_classes, [selected_formtype])

        # Only validate with the selected form type
        myall = [form.is_valid() for form in formsets.values()]
        myallall = all(myall)
        if myallall:
            return self.forms_valid(formsets)
        else:
            return self.forms_invalid(formsets)

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
