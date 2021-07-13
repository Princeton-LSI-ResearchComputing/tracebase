from django import template

from DataRepo.views import getAllBrowseData

register = template.Library()


@register.simple_tag
def define(the_val):
    return the_val


@register.simple_tag
def populateAllBrowseData(format):
    return getAllBrowseData(format)
