from typing import Optional

import pandas as pd
from django.core.exceptions import ObjectDoesNotExist
from django.utils.functional import cached_property

from DataRepo.models.animal import Animal
from DataRepo.models.peak_group import PeakGroup
from DataRepo.models.study import Study
from DataRepo.models.utilities import get_all_fields_named


def get_researchers():
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
                model.objects.values(target_field).distinct(),
            )
        )
    unique_researchers = list(pd.unique(list(filter(None, researchers))))
    return sorted(unique_researchers)


def validate_researchers(input_researchers, known_researchers=None, skip_flag=None):
    """
    Raises an exception if any researchers are not already in the database (and the database has more than 0
    researchers already in it).
    """
    if not known_researchers:
        known_researchers = get_researchers()

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


def could_be_variant_researcher(
    researcher: str, known_researchers: Optional[list] = None
) -> bool:
    """Check if a researcher could potentially be a variant of one already existing in the database.

    Known can be supplied for efficiency.
    """
    if known_researchers is None:
        known_researchers = get_researchers()
    return (
        len(known_researchers) > 0
        and researcher not in known_researchers
        and researcher.lower() != "anonymous"
    )


class Researcher:
    """
    Non-model class that provides various researcher related methods
    """

    # Note, RESEARCHER_DEFAULT is not used as a "default" value for loading.  It is used for the following reasons:
    # 1. To allow the validation page to proceed without complaining about a missing researcher value
    # 2. As a placeholder value in order to proceed when a problem with a researcher value is encountered.  Whenever
    #    such a problem is encountered, an error is buffered and eventually raised at the end of a failed load.
    # 3. To avoid hard-coding static "magic" values in multiple places.
    RESEARCHER_DEFAULT = "anonymous"

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
        return PeakGroup.objects.filter(
            msrun_sample__sample__researcher=self.name
        ).distinct()

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
