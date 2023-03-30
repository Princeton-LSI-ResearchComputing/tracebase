from chempy.util.periodic import atomic_number
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.utilities import atom_count_in_formula


class PeakDataLabel(models.Model, ElementLabel):
    """
    PeakDataLabel is a single observation of MS-detected labels in measured compounds.
    """

    id = models.AutoField(primary_key=True)
    peak_data = models.ForeignKey(
        to="DataRepo.PeakData",
        on_delete=models.CASCADE,
        null=False,
        related_name="labels",
    )
    element = models.CharField(
        max_length=1,
        null=False,
        blank=False,
        choices=ElementLabel.LABELED_ELEMENT_CHOICES,
        default=ElementLabel.CARBON,
        help_text='The type of element that is labeled in this observation (e.g. "C", "H", "O").',
    )
    count = models.PositiveSmallIntegerField(
        null=False,
        blank=False,
        validators=[MinValueValidator(0)],
        help_text="The number of labeled atoms (M+) observed relative to the "
        "presumed compound referred to in the peak group.",
    )
    mass_number = models.PositiveSmallIntegerField(
        null=False,
        blank=False,
        validators=[MinValueValidator(ElementLabel.MIN_MASS_NUMBER)],
        help_text="The sum of the number of protons and neutrons of the labeled atom, a.k.a. 'isotope', e.g. Carbon "
        "14.  The number of protons identifies the element that this tracer is an isotope of.  The number of neutrons "
        "in the element equals the number of protons, but in an isotope, the number of neutrons will be less than or "
        "greater than the number of protons.  Note, this differs from the 'atomic number' which indicates the number "
        "of protons only.",
    )

    class Meta:
        verbose_name = "label"
        verbose_name_plural = "labels"
        ordering = ["element", "count", "mass_number", "peak_data"]
        constraints = [
            models.UniqueConstraint(
                # We could additionally have "count" and "mass_number", but we only want 1 instance of "C" linked to 1
                # peak_data record. And while we could theoretically have both 13C and 14C labels in the same compound,
                # the tracer code doesn't currently support that, so there's no point in allowing that here.
                fields=["peak_data", "element"],
                name="unique_peakdata",
            )
        ]

    def clean(self, *args, **kwargs):
        super().clean(*args, **kwargs)

        atom_count = atom_count_in_formula(
            self.peak_data.peak_group.formula, self.element
        )
        # Ensure isotope elements exist in compound formula
        if atom_count == 0:
            raise ValidationError(
                f"Labeled element {self.element} does not exist in {self.tracer.compound} formula "
                f"({self.tracer.compound.formula})"
            )

        # Ensure isotope count does not exceed count of that element in formula
        if self.count > atom_count:
            raise ValidationError(
                f"Count of labeled element {self.element} exceeds the number of "
                f"{self.element} atoms in compound(s): [{', '.join(self.peak_data.peak_group.compounds)}] with "
                f"formula ({self.peak_data.peak_group.formula})."
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
