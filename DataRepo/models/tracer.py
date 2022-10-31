from __future__ import annotations

from typing import Optional

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.maintained_model import (
    MaintainedModel,
    maintained_field_function,
)
from DataRepo.models.utilities import get_model_by_name
from DataRepo.utils.infusate_name_parser import TracerData


class TracerQuerySet(models.QuerySet):
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
            tracer = self.using(self._db).create(compound=compound)
            for isotope_data in tracer_data["isotopes"]:
                TracerLabel.objects.using(self._db).create_tracer_label(
                    tracer, isotope_data
                )
            if self._db == settings.DEFAULT_DB:
                tracer.full_clean()
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
            tracers = (
                Tracer.objects.using(self._db)
                .annotate(num_labels=models.Count("labels"))
                .filter(compound=compound, num_labels=len(tracer_data["isotopes"]))
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

    @maintained_field_function(
        generation=2,
        update_field_name="name",
        parent_field_name="infusates",
        update_label="name",
    )
    def _name(self):
        # format: `compound - [ labelname,labelname,... ]` (but no spaces)
        if self.id is None or self.labels is None or self.labels.count() == 0:
            return self.compound.name
        labels_string = ",".join([str(label) for label in self.labels.all()])
        return f"{self.compound.name}-[{labels_string}]"

    def clean(self):
        """
        Validate this Tracer record.
        """
        for label in self.labels.all():
            atom_count = self.compound.atom_count(label.element)
            # Ensure isotope elements exist in compound formula
            if atom_count == 0:
                raise ValidationError(
                    f"Labeled element {label.element} does not exist in "
                    f"{self.compound} formula ({self.compound.formula})"
                )

            # Ensure isotope count does not exceed count of that element in formula
            if label.count > atom_count:
                raise ValidationError(
                    f"Count of labeled element {label.element} exceeds the number of "
                    f"{label.element} atoms in {self.compound} formula ({self.compound.formula})"
                )

            # Ensure positions exist if count < count of that element in formula
            if label.count < atom_count and not label.positions:
                raise ValidationError(
                    f"Positions required for partially labeled tracer compound {self.compound.name}. "
                    f"Labeled count ({label.count}) is less than number of {label.element} atoms "
                    f"({atom_count}) in formula ({self.compound.formula})."
                )
