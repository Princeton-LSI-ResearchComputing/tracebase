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


def validate_researchers(researchers, skip_flag=None, database=settings.TRACEBASE_DB):
    """
    Raises an exception if any researchers are not already in the database (and the database has more than 0
    researchers already in it).
    """
    if isinstance(researchers, str):
        input_researchers = [researchers]
    else:
        input_researchers = researchers
    known_researchers = get_researchers(database)
    # Accept all input researchers if there are no known researchers
    if len(known_researchers) > 0:
        unknown_researchers = [
            researcher
            for researcher in input_researchers
            if researcher not in known_researchers and researcher != "anonymous"
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
        nl = "\n"  # Put \n in a var to join in an f string
        message = (
            f"{len(unknown)} researchers: [{','.join(sorted(unknown))}] out of {len(new)} do not exist in the "
            f"database.  Current researchers are:{nl}{nl.join(sorted(known))}"
        )
        if skip_flag is not None:
            message += f"{nl}If all researchers are valid new researchers, add {skip_flag} to your command."
        super().__init__(message)
        self.unknown = unknown
        self.new = new
        self.known = known
        self.skip_flag = skip_flag

        # The following are used by the loading code to decide if this exception should be fatal or treated as a
        # warning, depending on the mode in which the loader is run.

        # load_warning governs whether this exception should be treated as a warning when validate is false.
        self.load_warning = False
        # validate_warning governs whether this exception should be treated as a warning when validate is true.
        self.validate_warning = True

        # These 2 values can differ based on whether this is something the user can fix or not.  For example, the
        # validation interface does not enable the user to verify that the researcher is indeed a new researcher, so
        # they cannot quiet an unknown researcher exception.  A curator can, so when the curator goes to load, it
        # should be treated as an exception (curator_warning=False).
