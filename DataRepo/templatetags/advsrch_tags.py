from django import template

from DataRepo.views import getAllPeakGroupsFmtData

register = template.Library()


@register.tag(name="get_all_peakgroups")
def do_get_all_peakgroups():
    return getAllPeakGroupsFmtData()


@register.simple_tag
def define(the_val):
    return the_val


@register.simple_tag
def populatePeakGroups(queryset):
    return getAllPeakGroupsFmtData()
