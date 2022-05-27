from DataRepo.models.maintained_model import (
    MaintainedModel,
    field_updater_function,
)
from DataRepo.models.tracer import Tracer
from django.db import models

CONC_SIG_FIGS = 3


class Infusate(MaintainedModel):

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
        help_text="A (non-unique) short name or lab identifier of a specific assortment of tracer compounds "
        "regardless of concentration, e.g '6eaas'.",
    )
    tracers = models.ManyToManyField(
        Tracer,
        through="InfusateTracer",
        help_text="Tracers included in this infusate 'recipe'.",
        related_name="infusates",
    )

    class Meta:
        verbose_name = "infusate"
        verbose_name_plural = "infusates"
        ordering = ["name"]

    def __str__(self):
        return str(self._name())

    @field_updater_function(generation=0, update_field_name="name", update_label="name")
    def _name(self):
        # Format: `short_name{tracername;tracername}`

        # Need to check self.id to see if the record exists yet or not, because if it does not yet exist, we cannot use
        # the reverse self.tracers reference until it exists (besides, another update will trigger when the
        # InfusateTracer records are created).  Otherwise, the following exception is thrown:
        # ValueError: "<Infusate: >" needs to have a value for field "id" before this many-to-many relationship can be
        # used.
        if self.id is None or self.tracers is None or self.tracers.count() == 0:
            return self.tracer_group_name

        link_recs = self.tracers.through.objects.filter(infusate__exact=self.id)

        nickname = ""
        if self.tracer_group_name is not None:
            nickname = self.tracer_group_name

        return (
            nickname
            + "{"
            + ";".join(
                sorted(
                    map(
                        lambda o: o.tracer._name()
                        + f"[{o.concentration:.{CONC_SIG_FIGS}g}]",
                        link_recs.all(),
                    )
                )
            )
            + "}"
        )

    @classmethod
    def validate_all_tracer_group_names(cls):
        """
        This raises an exception if the tracer_group_names are not consistent across infusate records, i.e. they refer
        to different groups of tracers.
        """
        grouped_recs = cls.objects.filter(tracer_group_name__isnull=False)
        cls.validate_tracer_group_names_helper(grouped_recs, {})

    @classmethod
    def validate_one_tracer_group_name(cls, name):
        """
        This raises an exception if the supplied tracer_group_name is not consistent across infusate records, i.e. they
        refer to different groups of tracers.
        """
        grouped_recs = cls.objects.filter(tracer_group_name__iexact=name)
        cls.validate_tracer_group_names_helper(grouped_recs, {})

    @classmethod
    def validate_tracer_group_names_helper(cls, grouped_recs, group_map_dict):
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
                        stats += (
                            f", this last one with the following IDs: "
                            f"{','.join(group_map_dict[grp_name][tracer_key])}"
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
            raise InvalidTracerGroupNames(problems)

    def validate_my_tracer_group_name(self):
        grp_name = self.tracer_group_name
        grouped_recs = self.objects.filter(tracer_group_name__iexact=grp_name).filter(
            tracer_group_name__isnull=False
        )
        group_map_dict = {grp_name: {}}
        self.validate_tracer_group_names_helper(grouped_recs, group_map_dict)


class InvalidTracerGroupNames(Exception):
    def __init__(self, messages):
        message = "\n".join(messages)
        super().__init__(message)
