from abc import ABCMeta, abstractmethod

from django.http import Http404
from django.shortcuts import render
from django.views.generic import DetailView, ListView

from DataRepo.models import (
    Animal,
    Compound,
    MSRun,
    PeakData,
    PeakGroup,
    Protocol,
    Sample,
    Study,
    Tissue,
)

# Home view


def home(request):
    return render(request, "home.html")


# Abstract classes


class genericlist(ListView, metaclass=ABCMeta):
    """
    This class displays all list views of every model.  It is an abstract class.
    """

    @abstractmethod
    def __init__(self, model):
        self.model = model
        self.template_name = "listview.html"
        # pself.aginate_by = 10
        self.allow_empty = True
        if hasattr(model._meta, "ordering"):
            if isinstance(model._meta.ordering, str):
                queryset = model.objects.order_by(model._meta.ordering)
            elif (
                isinstance(model._meta.ordering, list) and len(model._meta.ordering) > 0
            ):
                queryset = model.objects.order_by(model._meta.ordering[0])

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        add_model_description(self.model, context)
        return context


class genericdetail(DetailView, metaclass=ABCMeta):
    """
    This class displays all detail views of every model.  It is an abstract class.
    """

    @abstractmethod
    def __init__(self, model):
        self.model = model
        self.template_name = "detailview.html"
        self.slug_field = "id"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        add_model_description(self.model, context)
        return context


# List views


class compound_list(genericlist):
    def __init__(self):
        super().__init__(Compound)


class study_list(genericlist):
    def __init__(self):
        super().__init__(Study)


class animal_list(genericlist):
    def __init__(self):
        super().__init__(Animal)


class tissue_list(genericlist):
    def __init__(self):
        super().__init__(Tissue)


class sample_list(genericlist):
    def __init__(self):
        super().__init__(Sample)


class protocol_list(genericlist):
    def __init__(self):
        super().__init__(Protocol)


class msrun_list(genericlist):
    def __init__(self):
        super().__init__(MSRun)


class peakgroup_list(genericlist):
    def __init__(self):
        super().__init__(PeakGroup)


class peakdata_list(genericlist):
    def __init__(self):
        super().__init__(PeakData)


# Detail Views


class study_detail(genericdetail):
    def __init__(self):
        super().__init__(Study)


class compound_detail(genericdetail):
    def __init__(self):
        super().__init__(Compound)


class animal_detail(genericdetail):
    def __init__(self):
        super().__init__(Animal)


class tissue_detail(genericdetail):
    def __init__(self):
        super().__init__(Tissue)


class sample_detail(genericdetail):
    def __init__(self):
        super().__init__(Sample)


class protocol_detail(genericdetail):
    def __init__(self):
        super().__init__(Protocol)


class msrun_detail(genericdetail):
    def __init__(self):
        super().__init__(MSRun)


class peakgroup_detail(genericdetail):
    def __init__(self):
        super().__init__(PeakGroup)


class peakdata_detail(genericdetail):
    def __init__(self):
        super().__init__(PeakData)


# Supporting functions


# https://www.geeksforgeeks.org/python-split-camelcase-string-to-individual-strings/
def verbosify(str):
    """Creates a table or field name "title" by splitting camelcase words and applies title() if it contains only lower case characters"""

    if str.islower():
        dotitle = True
    else:
        dotitle = False

    words = [[str[0]]]

    for i, c in enumerate(str[1:]):
        # i starts from 0, but the string index starts from 1, so the index of the following character is:
        j = i + 2
        d = ""
        if j < len(str):
            d = str[j]

        if (words[-1][-1].islower() and c.isupper()) or (
            c.isupper() and j < len(str) and d.islower()
        ):
            words.append(list(c))
        else:
            words[-1].append(c)

    sstr = " ".join("".join(word) for word in words)

    if dotitle:
        cstr = sstr.title()
    else:
        cstr = sstr

    return cstr.replace("_", " ")


def is_shown_field(dbfield):
    """Takes a database field from a model and returns whether it should be displayed in a view"""
    shown = dbfield.get_internal_type() != "AutoField" and not getattr(
        dbfield, "is_relation"
    )
    return shown


def is_relation(dbfield):
    """Takes a database field from a model and returns whether it is a link to another table"""
    return getattr(dbfield, "is_relation")


def add_model_description(model, context):
    """
    Adds a model description to the supplied context dictionary.

    This data is used by generic templates that render views for every model.
    """
    # Representations of the table name
    context["table"] = model.__name__
    context["table_verbose"] = verbosify(model.__name__)
    context["table_verbose_plural"] = verbosify(model._meta.verbose_name_plural)

    # This is the tablie field used to link to detail pages
    context["slugfield"] = "id"

    # Representations of the field names
    all_fields = model._meta.get_fields()
    local_fields = list(filter(lambda x: is_shown_field(x), all_fields))
    context["fieldnames"] = [field.name for field in local_fields]
    context["fieldnames_verbose"] = [
        verbosify(field.verbose_name) for field in local_fields
    ]

    # Represenation of relationships
    rel_fields = list(filter(lambda x: is_relation(x), all_fields))
    context["rel_fieldnames"] = [field.name for field in rel_fields]
    context["rel_fieldnames_verbose"] = [verbosify(field.name) for field in rel_fields]
    context["rel_fieldnames_lookups"] = [
        field.related_model._meta.verbose_name for field in rel_fields
    ]

    # Representation of the sub-table fields to display
    context["relations"] = {}
    for rel in rel_fields:
        rel_model = rel.related_model
        rel_model_name = rel.related_model._meta.verbose_name
        rel_all_fields = rel_model._meta.get_fields()
        rel_local_fields = list(filter(lambda x: is_shown_field(x), rel_all_fields))
        context["relations"][rel_model_name] = {}
        context["relations"][rel_model_name]["subtable"] = rel_model_name
        context["relations"][rel_model_name]["subtable_verbose"] = verbosify(
            rel_model_name
        )
        context["relations"][rel_model_name]["subfieldnames"] = [
            field.name for field in rel_local_fields
        ]
        context["relations"][rel_model_name]["subfieldnames_verbose"] = [
            verbosify(field.name) for field in rel_local_fields
        ]

    return context
