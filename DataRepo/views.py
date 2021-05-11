from django.http import Http404
from django.shortcuts import render
from django.views.generic import ListView
from abc import ABCMeta, abstractmethod

from DataRepo.models import Compound, Study, Animal, Tissue, Sample, Protocol, MSRun, PeakGroup, PeakData


def home(request):
    return render(request, "home.html")


class genericlist(ListView, metaclass=ABCMeta):
    """
    This class displays all list views of every model.  It is an abstract class.
    """
    @abstractmethod
    def __init__(self, model):
        self.model = model
        self.template_name = 'listview.html'
        #pself.aginate_by = 10
        self.allow_empty = True
        if hasattr(model._meta, 'ordering'):
            if isinstance(model._meta.ordering, str):
                queryset = model.objects.order_by(model._meta.ordering)
            elif isinstance(model._meta.ordering, list) and len(model._meta.ordering) > 0:
                queryset = model.objects.order_by(model._meta.ordering[0])

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        model = self.model
        context['table'] = model.__name__
        context['table_verbose'] = self.verbosify(model.__name__)
        context['table_verbose_plural'] = self.verbosify(model._meta.verbose_name_plural)
        all_fields = model._meta.get_fields()
        # Alternative, if necessary:
        # all_fields = model._meta.get_fields(include_parents=False, include_hidden=False)
        filt_fields = list(filter(lambda x:self.is_shown_field(x), all_fields))
        context['fieldnames'] = [field.name for field in filt_fields]
        context['fieldnames_verbose'] = [self.verbosify(field.verbose_name.title()) for field in filt_fields]
        return context

    def is_shown_field(self, field):
        shown = (field.get_internal_type() != 'AutoField' and
            not getattr(field, "is_relation"))
        return shown
    
    def verbosify(self, str):
        """Creates a table or field name "title by splitting camelcase words"""
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

        return ' '.join(''.join(word) for word in words)





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






def compound_detail(request, cpd_id):
    try:
        cpd = Compound.objects.get(id=cpd_id)
    except Compound.DoesNotExist:
        raise Http404("compound not found")
    return render(request, "compound_detail.html", {"cpd": cpd})
