from django import template
from django.template.defaultfilters import stringfilter
import sys, inspect, re

register = template.Library()

@register.filter
def value_from_model(model, field):
    """
    Obtain a field value from a record, given the model instance and the field name
    """
    return getattr(model, field)

@register.filter
@stringfilter
def template_exists(template_filename):
    """
    Determine whether a template (i.e. html file) exists
    """
    try:
        template.loader.get_template(template_filename)
        return True
    except template.TemplateDoesNotExist:
        return False

@register.filter
def index(indexable, i):
    """
    Index a list from a template
    """
    return indexable[i]

@register.filter
def get_listviews(dummy):
    """
    Retrieve all the list views (and their table names) from views.py

    This is so that you don't have to pass them all to the template
    """
    list_views = []

    # Obtain a list of this module's ListView classes and the names of their Models
    # https://stackoverflow.com/questions/1796180/how-can-i-get-a-list-of-all-classes-within-current-module-in-python
    for listview_name, class_ref in inspect.getmembers(sys.modules['DataRepo.views']):
        if inspect.isclass(class_ref) and re.search('_list$', listview_name):
            # Create an instance to get the model name
            model_name = class_ref().model._meta.verbose_name_plural
            list_views.append([listview_name, model_name])
    
    # This returns a nested list that looks like this:
    # [['compound_list','Compound'], ['study_list', 'Study']]
    # ...that is based on the class content of DataRepo.views (classes matching *_list)
    return list_views
