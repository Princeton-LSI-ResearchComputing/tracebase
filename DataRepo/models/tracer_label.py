from django.contrib.postgres.fields import ArrayField
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models

# from django.utils.functional import cached_property
from DataRepo.models.maintained_model import (
    MaintainedModel,
    field_updater_function,
)
from DataRepo.models.tracer import Tracer
from DataRepo.models.tracer_labeled_class import TracerLabeledClass


class TracerLabel(MaintainedModel, TracerLabeledClass):

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        editable=False,
        help_text="An automatically maintained identifier of a tracer label.",
    )
    tracer = models.ForeignKey(
        Tracer,
        on_delete=models.CASCADE,
        related_name="labels",
    )
    element = models.CharField(
        max_length=1,
        null=False,
        blank=False,
        choices=TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
        default=TracerLabeledClass.CARBON,
        help_text='The type of atom that is labeled in the tracer compound (e.g. "C", "H", "O").',
    )
    count = models.PositiveSmallIntegerField(
        null=False,
        blank=False,
        validators=[
            MinValueValidator(1),
            MaxValueValidator(TracerLabeledClass.MAX_LABELED_ATOMS),
        ],
        help_text="The number of labeled atoms (M+) in the tracer compound supplied to this animal.  Note that the "
        "labeled_count must be greater than or equal to the number of labeled_positions.",
    )
    positions = ArrayField(
        models.PositiveSmallIntegerField(
            null=False,
            blank=False,
            validators=[
                MinValueValidator(1),
                MaxValueValidator(TracerLabeledClass.MAX_COMPOUND_POSITION),
            ],
        ),
        null=True,
        blank=True,
        default=list,
        help_text="The known labeled atom positions in the compound.  Note that the number of known labeled positions "
        "must be less than or equal to the labeled_count.",
    )
    mass_number = models.PositiveSmallIntegerField(
        null=False,
        blank=False,
        validators=[
            MinValueValidator(TracerLabeledClass.MIN_MASS_NUMBER),
            MaxValueValidator(TracerLabeledClass.MAX_MASS_NUMBER),
        ],
        help_text="The sum of the number of protons and neutrons of the labeled atom, a.k.a. 'isotope', e.g. Carbon "
        "14.  The number of protons identifies the element that this tracer is an isotope of.  The number of neutrons "
        "in the element equals the number of protons, but in an isotope, the number of neutrons will be less than "
        "or greater than the number of protons.  Note, this differs from the 'atomic number' which indicates the "
        "number of protons only.",
    )

    class Meta:
        verbose_name = "tracer label"
        verbose_name_plural = "tracer labels"
        ordering = ["tracer", "element", "mass_number", "count", "positions"]
        constraints = [
            models.UniqueConstraint(
                fields=["tracer", "element", "mass_number", "count", "positions"],
                name="unique_tracerlabel",
            )
        ]

    def __str__(self):
        return str(self._name())

    @field_updater_function(
        generation=3, update_field_name="name", parent_field_name="tracer"
    )
    def _name(self):
        # format: `position,position,... - weight element count` (but no spaces) positions optional
        pos_str = ""
        if len(self.positions) > 0:
            pos_str = (
                ",".join(list(map(lambda p: str(p), sorted(self.positions)))) + "-"
            )
        return "".join([pos_str, str(self.mass_number), self.element, str(self.count)])
