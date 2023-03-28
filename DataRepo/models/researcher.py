import pandas as pd
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.utils.functional import cached_property

from DataRepo.models.animal import Animal
from DataRepo.models.peak_group import PeakGroup
from DataRepo.models.study import Study
from DataRepo.models.utilities import get_all_fields_named


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
    unique_researchers = list(pd.unique(list(filter(None, researchers))))
    return unique_researchers


def validate_researchers(
    input_researchers,
    known_researchers=None,
    skip_flag=None,
    database=settings.TRACEBASE_DB,
):
    """
    Raises an exception if any researchers are not already in the database (and the database has more than 0
    researchers already in it).
    """
    if not known_researchers:
        known_researchers = get_researchers(database)

    # Accept any input researchers if there are no known researchers
    if len(known_researchers) > 0:
        unknown_researchers = [
            researcher
            for researcher in input_researchers
            if researcher not in known_researchers and researcher.lower() != "anonymous"
        ]
        if len(unknown_researchers) > 0:
            raise UnknownResearcherError(
                unknown_researchers,
                input_researchers,
                known_researchers,
                skip_flag,
            )


class Researcher:
    """
    Non-model class that provides various researcher related methods
    """

    def __init__(self, name):
        """
        Create a researcher object that will lookup items by name
        """
        if name not in get_researchers():
            raise ObjectDoesNotExist(f'Researcher "{name}" not found')
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


class UnknownResearcherError(Exception):
    def __init__(self, unknown, new, known, skip_flag=None):
        nlt = "\n\t"  # Put \n\t in a var to join in an f string
        message = (
            f"{len(unknown)} researchers: [{','.join(sorted(unknown))}] out of {len(new)} do not exist in the "
            f"database.  Current researchers are:{nlt}{nlt.join(sorted(known))}"
        )
        if skip_flag is not None:
            message += f"\nIf all researchers are valid new researchers, add {skip_flag} to your command."
        super().__init__(message)
        self.unknown = unknown
        self.new = new
        self.known = known
        self.skip_flag = skip_flag
