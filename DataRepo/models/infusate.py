from __future__ import annotations

import math
from typing import TYPE_CHECKING, Optional

from django.core.exceptions import ValidationError
from django.db import models, transaction

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.utilities import get_model_by_name

if TYPE_CHECKING:
    from DataRepo.utils.infusate_name_parser import InfusateData


class InfusateQuerySet(models.QuerySet):
    @transaction.atomic
    def get_or_create_infusate(
        self,
        infusate_data: InfusateData,
    ) -> tuple[Infusate, bool]:
        """Get Infusate matching the infusate_data, or create a new infusate"""

        # Search for matching Infusate
        infusate = self.get_infusate(infusate_data)
        created = False

        # Matching record not found, create new record
        if infusate is None:
            # create infusate
            infusate = self.create(tracer_group_name=infusate_data["infusate_name"])

            # create tracers
            Tracer = get_model_by_name("Tracer")
            InfusateTracer = get_model_by_name("InfusateTracer")
            for infusate_tracer in infusate_data["tracers"]:
                tracer = Tracer.objects.get_tracer(infusate_tracer["tracer"])
                if tracer is None:
                    (tracer, _) = Tracer.objects.get_or_create_tracer(
                        infusate_tracer["tracer"],
                    )
                # associate tracers with specific conectrations
                InfusateTracer.objects.create(
                    infusate=infusate,
                    tracer=tracer,
                    concentration=infusate_tracer["concentration"],
                )
            infusate.full_clean()
            infusate.save()
            created = True
        return (infusate, created)

    def get_infusate(self, infusate_data: InfusateData) -> Optional[Infusate]:
        """Get Infusate matching the infusate_data"""
        matching_infusate = None

        # Check for infusates with the same name and same number of tracers
        infusates = Infusate.objects.annotate(
            num_tracers=models.Count("tracers")
        ).filter(
            tracer_group_name=infusate_data["infusate_name"],
            num_tracers=len(infusate_data["tracers"]),
        )
        # Check that the tracers match
        for infusate_tracer in infusate_data["tracers"]:
            Tracer = get_model_by_name("Tracer")
            tracer = Tracer.objects.get_tracer(infusate_tracer["tracer"])
            infusates = infusates.filter(
                tracer_links__tracer=tracer,
                tracer_links__concentration=infusate_tracer["concentration"],
            )
        if infusates.count() == 1:
            matching_infusate = infusates.first()
        return matching_infusate


class Infusate(MaintainedModel, HierCachedModel):
    objects = InfusateQuerySet().as_manager()

    CONCENTRATION_SIGNIFICANT_FIGURES = 3
    TRACER_DELIMETER = ";"
    TRACERS_LEFT_BRACKET = "{"
    TRACERS_RIGHT_BRACKET = "}"

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
        return str(self._name())

    @MaintainedModel.setter(generation=0, update_field_name="name", update_label="name")
    def _name(self):
        # Format: `tracer_group_name {tracername[concentration];tracername[concentration]}`

        # Need to check self.id to see if the record exists yet or not, because if it does not yet exist, we cannot use
        # the reverse self.tracers reference until it exists (besides, another update will trigger when the
        # InfusateTracer records are created).  Otherwise, the following exception is thrown:
        # ValueError: "<Infusate: >" needs to have a value for field "id" before this many-to-many relationship can be
        # used.
        if self.id is None or self.tracers is None or self.tracers.count() == 0:
            return self.tracer_group_name

        link_recs = self.tracers.through.objects.filter(infusate__id__exact=self.id)

        name = self.TRACER_DELIMETER.join(
            sorted(
                map(
                    lambda o: o.tracer._name()
                    + f"[{o.concentration:.{self.CONCENTRATION_SIGNIFICANT_FIGURES}g}]",
                    link_recs.all(),
                )
            )
        )

        if self.tracer_group_name is not None:
            name = f"{self.tracer_group_name} {self.TRACERS_LEFT_BRACKET}{name}{self.TRACERS_RIGHT_BRACKET}"

        return name

    def name_and_concentrations(self):
        """Create an infusate name without concentrations and return that name and a list of concentrations in the
        corresponding tracer order.

        Args:
            None:

        Exceptions:
            None

        Returns:
            name (string): Same as returned from _name(), but without the concentrations
            concentrations (list of floats): Concentrations in the order of the names (not significant digits)
        """
        if self.id is None or self.tracers is None or self.tracers.count() == 0:
            return self.tracer_group_name

        link_recs = self.tracers.through.objects.filter(infusate__id__exact=self.id)

        tracer_names_and_concentrations = sorted(
            [[o.tracer._name(), o.concentration] for o in link_recs.all()],
            key=lambda item: item[0],
        )

        name = self.TRACER_DELIMETER.join(
            [item[0] for item in tracer_names_and_concentrations]
        )
        if self.tracer_group_name is not None:
            name = f"{self.tracer_group_name} {self.TRACERS_LEFT_BRACKET}{name}{self.TRACERS_RIGHT_BRACKET}"

        concentrations = [item[1] for item in tracer_names_and_concentrations]

        return name, concentrations

    def infusate_name_equal(self, supplied_name, supplied_concs):
        """Determines if a supplied infusate name and concentrations are the same as the record.

        Note, the reason for this is that the sorting of the tracers and isotopes in a valid name can differ.  The
        number of allowed spaces between the tracer group name and list of tracers can differ.  Also, equating floats
        (i.e. concentrations) is not reliable.  This method ignores those differences and compares the corresponding
        data to return True or False.

        Comparing a supplied name and concentrations could either be accomplished by converting the arguments to a
        record or converting both the record and the arguments into TypedDicts using the parser.  This strategy uses the
        latter so that the database is unaffected.

        Args:
            supplied_name (string)
            supplied_concs (list of floats)

        Exceptions:
            None

        Returns:
            equal (boolean): Whether the supplied name and concentration are equivalent to the record.
        """
        from DataRepo.utils.infusate_name_parser import (
            InfusateParsingError,
            parse_infusate_name,
        )

        # Any infusate name string (e.g. as supplied from a file) may not have the tracers in the same order
        rec_name, rec_concentrations = self.name_and_concentrations()

        rec_data = parse_infusate_name(rec_name, rec_concentrations)
        try:
            sup_data = parse_infusate_name(supplied_name, supplied_concs)
        except InfusateParsingError as ipe:
            # If the name and concs is invalid due to unmatching numbers of tracers and concentrations, return False
            if (
                "Unable to match" in str(ipe)
                and "tracers to" in str(ipe)
                and "concentration values" in str(ipe)
            ):
                return False
            raise ipe

        if rec_data["infusate_name"] != sup_data["infusate_name"] or len(
            rec_data["tracers"]
        ) != len(sup_data["tracers"]):
            return False

        # This assumes that compounds in an infusate are unique
        rec_tracers_data_sorted_by_compound = sorted(
            rec_data["tracers"], key=lambda item: item["tracer"]["compound_name"]
        )
        sup_tracers_data_sorted_by_compound = sorted(
            sup_data["tracers"], key=lambda item: item["tracer"]["compound_name"]
        )

        for i, _ in enumerate(rec_tracers_data_sorted_by_compound):
            if not math.isclose(
                rec_tracers_data_sorted_by_compound[i]["concentration"],
                sup_tracers_data_sorted_by_compound[i]["concentration"],
            ):
                return False
            elif (
                rec_tracers_data_sorted_by_compound[i]["tracer"]["compound_name"]
                != sup_tracers_data_sorted_by_compound[i]["tracer"]["compound_name"]
            ):
                return False
            elif (
                # At this point, we can equate the dicts, bec. their contents are not fragile to order or type
                rec_tracers_data_sorted_by_compound[i]["tracer"]["isotopes"]
                != sup_tracers_data_sorted_by_compound[i]["tracer"]["isotopes"]
            ):
                return False

        return True

    def get_tracer_group_infusates(self):
        """Get other infusates with the same assortment of tracers (i.e. the same tracer group), but in different
        concentrations, or potentially different group names (in order to catch group name inconsistencies).

        Args:
            None

        Exceptions:
            None

        Returns:
            infusates (QuerySet): Infusates with the same assortment of tracers
        """
        # Check for infusates with the same number of tracers
        infusates = Infusate.objects.annotate(
            num_tracers=models.Count("tracers")
        ).filter(
            num_tracers=self.tracers.count(),
        )
        # Check that the tracers match
        for tracer in self.tracers.all():
            infusates = infusates.filter(
                tracer_links__tracer=tracer,
            )
        return infusates.exclude(pk=self.pk)

    @property
    def pretty_name(self):
        """
        Returns the name with hard-returns inserted
        """
        display_name = self._name()

        if display_name:
            display_name = display_name.replace(
                self.TRACER_DELIMETER, f"{self.TRACER_DELIMETER}\n"
            )
            display_name = display_name.replace(
                self.TRACERS_LEFT_BRACKET, f"{self.TRACERS_LEFT_BRACKET}\n"
            )
            display_name = display_name.replace(
                self.TRACERS_RIGHT_BRACKET, f"\n{self.TRACERS_RIGHT_BRACKET}"
            )

        return display_name

    @property
    def short_name(self):
        """
        Returns the tracer_group_name field if populated.  If it's not populated, it returns the output of _name()
        """
        if self.tracer_group_name:
            return self.tracer_group_name
        else:
            return self._name()

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
        self.validate_tracer_groups()
        super().clean(*args, **kwargs)

    def validate_tracer_groups(self):
        """
        Validation method that raises and exception if two infusate records have the same assortment of tracers, but
        whose group names do not match.  And, it will raise an exception if there exists 2 infusates with the same
        tracers at the same concentrations (i.e. a duplicate).
        """
        # This is here to avoid circular import
        from DataRepo.utils.exceptions import TracerGroupsInconsistent

        dupes = []
        group_names_differ = []
        for infusate in self.get_tracer_group_infusates():
            # Same tracers, but different tracer group names:
            if infusate.tracer_group_name != self.tracer_group_name:
                group_names_differ.append(infusate)

            # Same tracers and same concentrations:
            concs_same = True
            for infusate_tracer in self.tracer_links.all():
                if (
                    infusate.tracer_links.filter(
                        tracer=infusate_tracer.tracer,
                        concentration=infusate_tracer.concentration,
                    ).count()
                    != 1
                ):
                    concs_same = False
                    break

            if concs_same:
                dupes.append(infusate)

        # If any issues
        if len(dupes) > 0 or len(group_names_differ) > 0:
            raise TracerGroupsInconsistent(self, dupes, group_names_differ)

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

        # For each tracer_group_name, if it refers to multiple groups of tracers, append an error message to the
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

    @property
    @cached_function
    def tracer_labeled_elements(self):
        """
        This method returns a unique list of the labeled elements that exist among the tracers.
        """
        from DataRepo.models.tracer_label import TracerLabel

        return list(
            TracerLabel.objects.filter(tracer__infusates__id=self.id)
            .order_by("element")
            .distinct("element")
            .values_list("element", flat=True)
        )
