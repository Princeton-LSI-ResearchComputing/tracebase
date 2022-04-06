import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.utils.functional import cached_property

from .animal import Animal
from .peakgroup import PeakGroup
from .study import Study
from .utilities import get_all_fields_named


def get_researchers(database=settings.TRACEBASE_DB):
    """
    Get a list of distinct researcher names that is the union of values in researcher fields from any model
    """
    target_field = "researcher"
    researchers = []
    # Get researcher names from any model containing a "researcher" field
    fields = get_all_fields_named(target_field)
    for field_info in fields:
        model = field_info[0]
        researchers += list(
            map(
                lambda x: x[target_field],
                model.objects.using(database).values(target_field).distinct(),
            )
        )
    unique_researchers = list(pd.unique(researchers))
    return unique_researchers


class Researcher:
    """
    Non-model class that provides various researcher related methods
    """

    def __init__(self, name):
        """
        Create a researcher object that will lookup items by name
        """
        if name not in get_researchers():
            raise ObjectDoesNotExist('Researcher "{name}" not found')
        else:
            self.name = name

    @cached_property
    def studies(self):
        """
        Returns QuerySet of Studies that contain samples "owned" by this Researcher
        """
        return Study.objects.filter(animals__samples__researcher=self.name).distinct()

    @cached_property
    def animals(self):
        """
        Returns QuerySet of Animals that contain samples "owned" by this Researcher
        """
        return Animal.objects.filter(samples__researcher=self.name).distinct()

    @cached_property
    def peakgroups(self):
        """
        Returns QuerySet of Peakgroups that contain samples "owned" by this Researcher
        """
        return PeakGroup.objects.filter(msrun__sample__researcher=self.name).distinct()

    def __eq__(self, other):
        if isinstance(other, Researcher):
            return self.name == other.name
        return False

    def __str__(self):
        return self.name
