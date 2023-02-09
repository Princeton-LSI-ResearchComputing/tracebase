from chempy.util.periodic import atomic_number
from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models

from DataRepo.models import Tracer
from DataRepo.models.element_label import ElementLabel
from DataRepo.models.maintained_model import (
    MaintainedModel,
    maintained_field_function,
)
from DataRepo.utils.infusate_name_parser import IsotopeData


class TracerLabelQuerySet(models.QuerySet):
    def create_tracer_label(self, tracer: Tracer, isotope_data: IsotopeData):
        db = self._db or settings.DEFAULT_DB
        tracer_label = self.using(db).create(
            tracer=tracer,
            element=isotope_data["element"],
            count=isotope_data["count"],
            positions=isotope_data["positions"],
            mass_number=isotope_data["mass_number"],
        )
        tracer_label.full_clean()
        return tracer_label


class TracerLabel(MaintainedModel, ElementLabel):

    objects = TracerLabelQuerySet().as_manager()

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        null=True,
        editable=False,
        help_text="An automatically maintained identifier of a tracer label.",
    )
    tracer = models.ForeignKey(
        "DataRepo.Tracer",
        on_delete=models.CASCADE,
        related_name="labels",
    )
    element = models.CharField(
        max_length=1,
        null=False,
        blank=False,
        choices=ElementLabel.LABELED_ELEMENT_CHOICES,
        default=ElementLabel.CARBON,
        help_text='The type of atom that is labeled in the tracer compound (e.g. "C", "H", "O").',
    )
    count = models.PositiveSmallIntegerField(
        null=False,
        blank=False,
        validators=[MinValueValidator(1)],
        help_text="The number of labeled atoms (M+) in the tracer compound supplied to this animal.  Note that the "
        "count must be greater than or equal to the number of positions.",
    )
    positions = ArrayField(
        models.PositiveSmallIntegerField(
            null=False,
            blank=False,
            validators=[MinValueValidator(1)],
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
        validators=[MinValueValidator(ElementLabel.MIN_MASS_NUMBER)],
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

    @maintained_field_function(
        generation=3,
        update_field_name="name",
        parent_field_name="tracer",
        update_label="name",
    )
    def _name(self):
        # format: `position,position,... - weight element count` (but no spaces) positions optional
        positions_string = ""
        if self.positions and len(self.positions) > 0:
            positions_string = ",".join([str(p) for p in sorted(self.positions)]) + "-"
        return f"{positions_string}{self.mass_number}{self.element}{self.count}"

    def clean(self, *args, **kwargs):
        super().clean(*args, **kwargs)

        # Ensure positions match count
        if self.positions and len(self.positions) != self.count:
            raise ValidationError(
                "Length of labeled positions list "
                f"({len(self.positions)}) must match labeled element count ({self.count})"
            )

        atom_count = self.tracer.compound.atom_count(self.element)
        # Ensure isotope elements exist in compound formula
        if atom_count == 0:
            raise ValidationError(
                f"Labeled element {self.element} does not exist in "
                f"{self.tracer.compound} formula ({self.tracer.compound.formula})"
            )

        # Ensure isotope count does not exceed count of that element in formula
        if self.count > atom_count:
            raise ValidationError(
                f"Count of labeled element {self.element} exceeds the number of "
                f"{self.element} atoms in {self.tracer.compound} formula ({self.tracer.compound.formula})"
            )

        # Ensure positions exist if count < count of that element in formula
        if self.count < atom_count and not self.positions:
            raise ValidationError(
                f"Positions required for partially labeled tracer compound {self.tracer.compound.name}. "
                f"Labeled count ({self.count}) is less than number of {self.element} atoms "
                f"({atom_count}) in formula ({self.tracer.compound.formula})."
            )

        # Ensure that the mass number is at least the number of nuetrons (which is the same as the number of protons)
        num_neutrons = atomic_number(self.element)
        if self.mass_number < num_neutrons:
            raise ValidationError(
                f"The mass number ({self.mass_number}) of element {self.element} must be greater than or equal to the "
                f"number of {self.element}'s neutrons: {num_neutrons}."
            )

        # As a reasonable upper limit, ensure that the mass number of the isotope is not greater than the non-isotope
        # mass number plus the number of protons
        dbl_prot_mass_num = 3 * num_neutrons
        if self.mass_number > dbl_prot_mass_num:
            raise ValidationError(
                f"The mass number ({self.mass_number}) of element {self.element} must not be greater than the non-"
                f"isotope element with double the number of protons: {dbl_prot_mass_num}."
            )
