from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models

from DataRepo.models.maintained_model import (
    MaintainedModel,
    are_autoupdates_enabled,
    maintained_field_function,
)
from DataRepo.models.multi_db_mixin import MultiDBMixin
from DataRepo.models.utilities import get_model_by_name

if TYPE_CHECKING:
    from DataRepo.utils.infusate_name_parser import InfusateData


CONCENTRATION_SIGNIFICANT_FIGURES = 3
TRACER_DELIMETER = ";"
TRACERS_LEFT_BRACKET = "{"
TRACERS_RIGHT_BRACKET = "}"


class InfusateQuerySet(models.QuerySet):
    def get_or_create_infusate(
        self, infusate_data: InfusateData
    ) -> tuple[Infusate, bool]:
        """Get Infusate matching the infusate_data, or create a new infusate"""
        db = self._db or settings.DEFAULT_DB

        # Search for matching Infusate
        infusate = self.using(db).get_infusate(infusate_data)
        created = False

        # Matching record not found, create new record
        if infusate is None:
            print(f"Inserting infusate {infusate_data['unparsed_string']}")

            # create infusate
            infusate = self.using(db).create(
                tracer_group_name=infusate_data["infusate_name"]
            )

            # create tracers
            Tracer = get_model_by_name("Tracer")
            InfusateTracer = get_model_by_name("InfusateTracer")
            for infusate_tracer in infusate_data["tracers"]:
                tracer = Tracer.objects.using(db).get_tracer(infusate_tracer["tracer"])
                if tracer is None:
                    (tracer, _) = Tracer.objects.using(db).get_or_create_tracer(
                        infusate_tracer["tracer"]
                    )
                # associate tracers with specific conectrations
                InfusateTracer.objects.using(db).create(
                    infusate=infusate,
                    tracer=tracer,
                    concentration=infusate_tracer["concentration"],
                )
            infusate.full_clean()
            infusate.save(using=db)
            created = True
        return (infusate, created)

    def get_infusate(self, infusate_data: InfusateData) -> Optional[Infusate]:
        """Get Infusate matching the infusate_data"""
        db = self._db or settings.DEFAULT_DB
        matching_infusate = None

        # Check for infusates with the same name and same number of tracers
        infusates = (
            Infusate.objects.using(db)
            .annotate(num_tracers=models.Count("tracers"))
            .filter(
                tracer_group_name=infusate_data["infusate_name"],
                num_tracers=len(infusate_data["tracers"]),
            )
        )
        # Check that the tracers match
        for infusate_tracer in infusate_data["tracers"]:
            Tracer = get_model_by_name("Tracer")
            tracer = Tracer.objects.using(db).get_tracer(infusate_tracer["tracer"])
            infusates = infusates.filter(
                tracer_links__tracer=tracer,
                tracer_links__concentration=infusate_tracer["concentration"],
            )
        if infusates.count() == 1:
            matching_infusate = infusates.first()
        return matching_infusate


class Infusate(MaintainedModel, MultiDBMixin):
    objects = InfusateQuerySet().as_manager()

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        null=True,
        editable=False,
        help_text="A unique name or lab identifier of the infusate 'recipe' containing 1 or more tracer compounds at "
        "specific concentrations.",
    )
    tracer_group_name = models.CharField(
        max_length=20,
        unique=False,
        null=True,
        blank=True,
        help_text="A short name or lab identifier of refering to a group of tracer compounds, e.g '6eaas'.  There may "
        "be multiple infusate records with this group name, each referring to the same tracers at different "
        "concentrations.",
    )
    tracers = models.ManyToManyField(
        "DataRepo.Tracer",
        through="InfusateTracer",
        help_text="Tracers included in this infusate 'recipe' at specific concentrations.",
        related_name="infusates",
    )

    class Meta:
        verbose_name = "infusate"
        verbose_name_plural = "infusates"
        ordering = ["name"]

    def __str__(self):
        return str(self.get_name)

    @maintained_field_function(
        generation=0, update_field_name="name", update_label="name"
    )
    def _name(self):
        # Format: `tracer_group_name{tracername;tracername}`

        # Need to check self.id to see if the record exists yet or not, because if it does not yet exist, we cannot use
        # the reverse self.tracers reference until it exists (besides, another update will trigger when the
        # InfusateTracer records are created).  Otherwise, the following exception is thrown:
        # ValueError: "<Infusate: >" needs to have a value for field "id" before this many-to-many relationship can be
        # used.
        if self.id is None or self.tracers is None or self.tracers.count() == 0:
            return self.tracer_group_name

        link_recs = self.tracers.through.objects.filter(infusate__id__exact=self.id)

        name = TRACER_DELIMETER.join(
            sorted(
                map(
                    lambda o: o.tracer._name()
                    + f"[{o.concentration:.{CONCENTRATION_SIGNIFICANT_FIGURES}g}]",
                    link_recs.all(),
                )
            )
        )

        if self.tracer_group_name is not None:
            name = f"{self.tracer_group_name} {TRACERS_LEFT_BRACKET}{name}{TRACERS_RIGHT_BRACKET}"

        return name

    @property
    def get_name(self):
        """
        Returns the name field if populated.  If it's not populated, it populates it (in the same manner that the old
        cache mechanism worked)
        """
        display_name = None

        # Get the name.  Initialize if not set and auto-updates are on.
        if self.name:
            display_name = self.name
        elif are_autoupdates_enabled():
            # This triggers an auto-update
            self.save(update_fields=["name"])
            display_name = self.name

        # If it's still not set, call the method that generates the name.  It just won't be saved.
        if not display_name:
            display_name = self._name()

        return display_name

    @property
    def pretty_name(self):
        """
        Returns the name with hard-returns inserted
        """
        display_name = self.get_name

        if display_name:
            display_name = display_name.replace(
                TRACER_DELIMETER, f"{TRACER_DELIMETER}\n"
            )
            display_name = display_name.replace(
                TRACERS_LEFT_BRACKET, f"{TRACERS_LEFT_BRACKET}\n"
            )
            display_name = display_name.replace(
                TRACERS_RIGHT_BRACKET, f"\n{TRACERS_RIGHT_BRACKET}"
            )

        return display_name

    @property
    def short_name(self):
        """
        Returns the tracer_group_name field if populated.  If it's not populated, it returns the output of get_name()
        """
        if self.tracer_group_name:
            return self.tracer_group_name
        else:
            return self.get_name

    @property
    def pretty_short_name(self):
        """
        Returns the short_name with hard-returns inserted (if tracer_group_name is null)
        """
        if self.tracer_group_name:
            return self.tracer_group_name
        else:
            # This will do 2 unsuccessful replacements of curlies, but re-use is prioritized
            return self.pretty_name

    def clean(self, *args, **kwargs):
        """
        This is an override of clean to validate the tracer_group_name of new records
        """
        self.validate_tracer_group_names(name=self.tracer_group_name)
        super().clean(*args, **kwargs)

    @classmethod
    def validate_tracer_group_names(cls, name=None):
        """
        Validation method that raises and exception if two infusate records share a tracer_group_name but are not
        composed of the same group of tracers.  If you want to check an object instance's name only, you must supply
        the name as an argument.
        """
        if name is None:
            grouped_recs = cls.objects.filter(tracer_group_name__isnull=False)
        else:
            grouped_recs = cls.objects.filter(tracer_group_name__iexact=name)

        cls.validate_tracer_group_names_helper(grouped_recs)

    @classmethod
    def validate_tracer_group_names_helper(cls, grouped_recs):
        # Build a 2-level dict whose keys are the tracer_group_name and a "tracer key".  The "tracer key" is the sorted
        # IDs of tracer records concatenated together with a delimiting comma.  The value is a list of Infusate record
        # IDs.
        group_map_dict = {}
        for group_rec in grouped_recs:
            grp_name = group_rec.tracer_group_name
            if grp_name not in group_map_dict:
                group_map_dict[grp_name] = {}
            tracer_key = ",".join(
                sorted(map(lambda r: str(r.id), group_rec.tracers.all()))
            )
            if tracer_key not in group_map_dict[grp_name]:
                group_map_dict[grp_name][tracer_key] = []
            group_map_dict[grp_name][tracer_key].append(group_rec.id)

        # For each tracer_group name, if it refers to multiple groups of tracers, append an error message to the
        # problems array that identifies the ambiguous tracer_group_names, the number of different groupings of
        # tracers, a description of the different sets of tracers, and a single example list of the infusate record IDs
        # with the problematic tracer_group_names.
        problems = []
        for grp_name in group_map_dict:
            if len(group_map_dict[grp_name].keys()) != 1:
                num_groupings = len(group_map_dict[grp_name].keys())
                stats = "\n"
                i = 0
                for tracer_key in sorted(
                    group_map_dict[grp_name],
                    key=lambda x: len(group_map_dict[grp_name][x]),
                    reverse=True,
                ):
                    stats += (
                        f"Tracer IDs: {tracer_key} are refered to by {len(group_map_dict[grp_name][tracer_key])} "
                        "infusate records"
                    )
                    i += 1
                    if i == num_groupings:
                        stats += ", this last one with the following IDs: " + ",".join(
                            str(group_map_dict[grp_name][tracer_key])
                        )
                    else:
                        stats += "\n"
                msg = (
                    f"Tracer group name {grp_name} is inconsistent.  There are {num_groupings} different groupings of "
                    "different tracer records.  This group name refers to the following list of tracer IDs by the "
                    f"indicated number of infusate records: {stats}"
                )
                problems.append(msg)
        if len(problems) > 0:
            raise ValidationError("\n".join(problems))

    def tracer_labeled_elements(self):
        """
        This method returns a unique list of the labeled elements that exist among the tracers.
        """
        from DataRepo.models.tracer_label import TracerLabel

        # TODO: See issue #580.  "using" will be unnecessary.
        db = self.get_using_db()

        return list(
            TracerLabel.objects.using(db)
            .filter(tracer__infusates__id=self.id)
            .order_by("element")
            .distinct("element")
            .values_list("element", flat=True)
        )
