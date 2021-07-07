from django.views.generic.base import ContextMixin, TemplateResponseMixin
from django.views.generic.edit import ProcessFormView
from django.http.response import HttpResponseRedirect, HttpResponseForbidden

# Based on:
#   https://stackoverflow.com/questions/15497693/django-can-class-based-views-accept-two-forms-at-a-time
#   https://gist.github.com/jamesbrobb/748c47f46b9bd224b07f

class MultiFormMixin(ContextMixin):

    form_classes = {}
    prefixes = {}
    success_urls = {}
    grouped_forms = {}

    # A mixed form is a form submission containing any number of forms (i.e. formsets) and form types
    # Only 1 form type (based on a selected form field) will be validated
    mixedform_prefixes = {} # A dict keyed on the same keys as form_classes
    mixedform_selected_formtype = "" # This is a form field superficially added (e.g. via javascript) that contains a prefix of the form classes the user has selected from the mixed forms
    mixedform_prefix_field = "" # This is a form field included in each of the form_classes whose value will start with one of the mixedform_prefixes
    
    initial = {}
    prefix = None
    success_url = None

    def get_form_classes(self):
        return self.form_classes
     
    def get_forms(self, form_classes, form_names=None, bind_all=False):
        return dict([(key, self._create_form(key, klass, (form_names and key in form_names) or bind_all)) \
            for key, klass in form_classes.items()])
    
    def get_form_kwargs(self, form_name, bind_form=False):
        kwargs = {}
        kwargs.update({'initial':self.get_initial(form_name)})
        kwargs.update({'prefix':self.get_prefix(form_name)})
        
        if bind_form:
            kwargs.update(self._bind_form_data())

        return kwargs
    
    def forms_valid(self, forms):
        num_valid_calls = 0
        for form_name in forms.keys():
            form_valid_method = '%s_form_valid' % form_name
            if hasattr(self, form_valid_method):
                getattr(self, form_valid_method)(forms[form_name])
                num_valid_calls += 1
        if num_valid_calls == 0:
            return self.form_valid(forms)
        else:
            return HttpResponseRedirect(self.get_success_url(form_name))
     
    def forms_invalid(self, forms):
        num_invalid_calls = 0
        for form_name in forms.keys():
            form_invalid_method = '%s_form_valid' % form_name
            if hasattr(self, form_invalid_method):
                getattr(self, form_invalid_method)(forms[form_name])
                num_invalid_calls += 1
        if num_invalid_calls == 0:
            return self.form_invalid(forms)
        else:
            return self.render_to_response(self.get_context_data(forms=forms))
    
    def get_initial(self, form_name):
        initial_method = 'get_%s_initial' % form_name
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
        form_create_method = 'create_%s_form' % form_name
        if hasattr(self, form_create_method):
            form = getattr(self, form_create_method)(**form_kwargs)
        else:
            print("kwargs from _create_form(",form_name,",",bind_form,"): ",form_kwargs)
            form = klass(**form_kwargs)
        return form
           
    def _bind_form_data(self):
        if self.request.method in ('POST', 'PUT'):
            return{'data': self.request.POST,
                   'files': self.request.FILES,}
        return {}


class ProcessMultipleFormsView(ProcessFormView):

    def get(self, request, *args, **kwargs):
        form_classes = self.get_form_classes()
        forms = self.get_forms(form_classes)
        return self.render_to_response(self.get_context_data(forms=forms))
     
    def post(self, request, *args, **kwargs):
        form_classes = self.get_form_classes()
        form_name = request.POST.get('action')
        print("FORM NAME: ",form_name)
        if self._individual_exists(form_name):
            return self._process_individual_form(form_name, form_classes)
        elif self._group_exists(form_name):
            return self._process_grouped_forms(form_name, form_classes)
        elif self._mixed_exists():
            return self._process_mixed_forms(form_classes)
        else:
            return self._process_all_forms(form_classes)
        
    def _individual_exists(self, form_name):
        return form_name in self.form_classes
    
    def _group_exists(self, group_name):
        return group_name in self.grouped_forms

    def _mixed_exists(self):
        return self.mixedform_prefixes and self.mixedform_selected_formtype

    def _process_individual_form(self, form_name, form_classes):
        forms = self.get_forms(form_classes, (form_name,))
        form = forms.get(form_name)
        if not form:
            return HttpResponseForbidden()
        elif form.is_valid():
            return self.forms_valid(forms, form_name)
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
        selected_formtype = form_kwargs['data'][self.mixedform_selected_formtype]
        print("SELECTED FORMTYPE:",selected_formtype)
        
        formsets = self.get_forms(form_classes, selected_formtype, False)

        print("FORMSETS:",formsets)
        print("SELF IS: ",self)

        # Only validate the selected form type
        if all([form.is_valid() for form in formsets.values()]):
            print("Calling forms_valid from mixed_forms with: ",formsets)
            return self.forms_valid(formsets)
        else:
            print("Calling forms_invalid from mixed_forms with: ",formsets)
            return self.forms_invalid(formsets)
        
    def _process_all_forms(self, form_classes):
        forms = self.get_forms(form_classes, None, True)
        if all([form.is_valid() for form in forms.values()]):
            print("Calling forms_valid with: ",forms)
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