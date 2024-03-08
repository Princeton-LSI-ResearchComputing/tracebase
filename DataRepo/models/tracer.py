from __future__ import annotations

from typing import Optional

from django.db import models, transaction

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.utilities import get_model_by_name
from DataRepo.utils.infusate_name_parser import TracerData


class TracerQuerySet(models.QuerySet):
    @transaction.atomic
    def get_or_create_tracer(self, tracer_data: TracerData) -> tuple[Tracer, bool]:
        """Get Tracer matching the tracer_data, or create a new tracer"""

        tracer = self.get_tracer(tracer_data)
        created = False
        if tracer is None:
            TracerLabel = get_model_by_name("TracerLabel")
            Compound = get_model_by_name("Compound")
            compound = Compound.compound_matching_name_or_synonym(
                tracer_data["compound_name"]
            )
            tracer = self.create(compound=compound)
            for isotope_data in tracer_data["isotopes"]:
                TracerLabel.objects.create_tracer_label(tracer, isotope_data)
            tracer.full_clean()
            tracer.save()
            created = True
        return (tracer, created)

    def get_tracer(self, tracer_data: TracerData) -> Optional[Tracer]:
        """Get Tracer matching the tracer_data"""
        matching_tracer = None

        # First, check if the compound is found
        Compound = get_model_by_name("Compound")
        compound = Compound.compound_matching_name_or_synonym(
            tracer_data["compound_name"]
        )
        if compound:
            # Check for tracers of the compound with same number of labels
            tracers = Tracer.objects.annotate(num_labels=models.Count("labels")).filter(
                compound=compound, num_labels=len(tracer_data["isotopes"])
            )
            # Check that the labels match
            for tracer_label in tracer_data["isotopes"]:
                tracers = tracers.filter(
                    labels__element=tracer_label["element"],
                    labels__mass_number=tracer_label["mass_number"],
                    labels__count=tracer_label["count"],
                    labels__positions=tracer_label["positions"],
                )
            if tracers.count() == 1:
                matching_tracer = tracers.first()
        return matching_tracer


class Tracer(MaintainedModel, ElementLabel):
    objects = TracerQuerySet().as_manager()

    COMPOUND_DELIMITER = "-"
    LABELS_DELIMITER = ","
    LABELS_LEFT_BRACKET = "["
    LABELS_RIGHT_BRACKET = "]"

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        null=True,
        editable=False,
        help_text="A unique name or lab identifier of the tracer, e.g. 'lysine-C14'.",
    )
    compound = models.ForeignKey(
        to="DataRepo.Compound",
        on_delete=models.RESTRICT,
        null=False,
        related_name="tracers",
    )

    class Meta:
        verbose_name = "tracer"
        verbose_name_plural = "tracers"
        ordering = ["name"]

    def __str__(self):
        return str(self._name())

    @MaintainedModel.setter(
        generation=2,
        update_field_name="name",
        parent_field_name="infusates",
        update_label="name",
    )
    def _name(self):
        # format: `compound-[labelname,labelname,...]`, e.g. lysine-[13C6,15N2]
        if self.id is None or self.labels is None or self.labels.count() == 0:
            return self.compound.name
        labels_string = self.LABELS_DELIMITER.join([str(label) for label in self.labels.all()])
        return (
            f"{self.compound.name}{self.COMPOUND_DELIMITER}"
            f"{self.LABELS_LEFT_BRACKET}{labels_string}{self.LABELS_RIGHT_BRACKET}"
        )
