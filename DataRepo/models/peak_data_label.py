from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models

from .element_label import ElementLabel


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
        validators=[
            MinValueValidator(0),
            MaxValueValidator(ElementLabel.MAX_LABELED_ATOMS),
        ],
        help_text="The number of labeled atoms (M+) observed relative to the "
        "presumed compound referred to in the peak group.",
    )
    mass_number = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        validators=[
            MinValueValidator(1),
            MaxValueValidator(ElementLabel.MAX_MASS_NUMBER),
        ],
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

        # composite key
        constraints = [
            models.UniqueConstraint(
                fields=["peak_data", "element", "count"],
                name="unique_peakdata",
            )
        ]
