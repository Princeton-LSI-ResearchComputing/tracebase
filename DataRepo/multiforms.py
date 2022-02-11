from typing import Dict, Optional

from django import forms
from django.http.response import HttpResponseForbidden, HttpResponseRedirect
from django.views.generic.base import ContextMixin, TemplateResponseMixin
from django.views.generic.edit import ProcessFormView

# Based on:
#   https://stackoverflow.com/questions/15497693/django-can-class-based-views-accept-two-forms-at-a-time
#   https://gist.github.com/jamesbrobb/748c47f46b9bd224b07f

# TODO: Re-reading the above stack post after having learned a bunch about various components of form processing, I
# have realized a few things that I could clean up in here.  First of all, the original code requires that the submit
# button be named "action" and its "value" must match a form_name that is used as keys in the form_classes dict.
# However, that form processing only works if the submit button is what submits the form.  E.g. if a user hits enter in
# another form field, it won't work correctly.  The reason my changes work is because first, I haven't configured the
# pre-existing code correctly, so it never gets into the various form processing methods and gets to my mixed form
# check.  I am checking data member values for a "mixed" form.  A "mixed" form is 3 different form classes (all with
# the same form fields) that are all wrapped in one form tag.  Now, I have added another form type (AdvSearchPageForm)
# and it needs to be called as an individual form.  Code I added to views to use an additional paging form
# (AdvSearchPageForm) uses a strategy similar to the original intent of this code.  It identifies a form by one of its
# input names.  I need to refactor a number of things to make multiforms work correctly and handle my mixed form case
# AND the page form.


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

    def forms_valid(self, forms):
        calls = []
        for form_name in forms.keys():
            form_valid_method = "%s_form_valid" % form_name
            if hasattr(self, form_valid_method):
                calls.append([form_valid_method, forms[form_name]])
                # Originally, there was not a return here. I added it to validate my theory, and it worked, so I added an elif below to handle this case. The line below this one just originally called the form valid method and expected it to not return anything
                #return getattr(self, form_valid_method)(forms[form_name])
        if self._mixed_exists() and len(calls) == 0:
            return self.form_valid(forms)
        elif len(calls) == 1:
            form_valid_method, form = calls[0]
            return getattr(self, form_valid_method)(form)
        else:
            for call in calls:
                form_valid_method, form = call
                getattr(self, form_valid_method)(form)
            if len(self.success_urls) == 0:
                if self.success_url == "":
                    # Not entirely sure this is correct, but with the current code, this should never execute
                    return self.render_to_response(self.get_context_data(forms=forms))
                else:
                    return HttpResponseRedirect(self.success_url)
            else:
                return HttpResponseRedirect(self.get_success_url(form_name))

    def forms_invalid(self, forms):
        calls = []
        for form_name in forms.keys():
            form_invalid_method = "%s_form_invalid" % form_name
            if hasattr(self, form_invalid_method):
                calls.append([form_invalid_method, forms[form_name]])
                # Originally, there was not a return here. I added it to validate my theory, and it worked, so I added an elif below to handle this case. The line below this one just originally called the form valid method and expected it to not return anything
                #return getattr(self, form_invalid_method)(forms[form_name])
        if self._mixed_exists() and len(calls) == 0:
            return self.form_invalid(forms)
        elif len(calls) == 1:
            form_invalid_method, form = calls[0]
            return getattr(self, form_invalid_method)(form)
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
        return self.prefixes.get(form_name, self.prefix)

    def get_success_url(self, form_name=None):
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
