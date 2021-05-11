from django import template
from django.template.defaultfilters import stringfilter
import sys, inspect, re

register = template.Library()

# This obtains a value from a field of a record of the model
@register.filter
def value_from_model(model, field):
    return getattr(model, field)

# This obtains a value from a field of a record of the model
@register.filter
def verbose_name_from_model(model, field):

    str = model._meta.get_field(field).verbose_name.title()

    # THE FOLLOWING IS REDUNDANT CODE WITH WHAT'S IN VIEWS. TODO: Move to a common location
    # https://www.geeksforgeeks.org/python-split-camelcase-string-to-individual-strings/
    words = [[str[0]]]

    for i, c in enumerate(str[1:]):
        # i starts from 0, but the string index starts from 1, so the index of the following character is:
        j = i+2
        d = ''
        if j < len(str):
            d = str[j]

        if (words[-1][-1].islower() and c.isupper()) or (c.isupper() and j < len(str) and d.islower()):
            words.append(list(c))
        else:
            words[-1].append(c)

    vname = ' '.join(''.join(word) for word in words)

    return vname

# This determines whether a template (i.e. html file) exists
@register.filter
@stringfilter
def template_exists(value):
    try:
        template.loader.get_template(value)
        return True
    except template.TemplateDoesNotExist:
        return False

# This allows indexing a list
@register.filter
def index(indexable, i):
    return indexable[i]

# Retrieve all the list views and their table names (so that you don't have to pass them all to the template)
@register.filter
def get_listviews(dummy):
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
