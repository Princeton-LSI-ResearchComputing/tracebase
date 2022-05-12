from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models

from .tracer_labeled_class import TracerLabeledClass


class PeakDataLabel(models.Model, TracerLabeledClass):
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
    labeled_element = models.CharField(
        max_length=1,
        null=True,
        choices=TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
        default=TracerLabeledClass.CARBON,
        blank=True,
        help_text='The type of element that is labeled in this observation (e.g. "C", "H", "O").',
    )
    labeled_count = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        validators=[
            MinValueValidator(0),
            MaxValueValidator(TracerLabeledClass.MAX_LABELED_ATOMS),
        ],
        help_text="The number of labeled atoms (M+) observed relative to the "
        "presumed compound referred to in the peak group.",
    )
    mass_number = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        validators=[
            MinValueValidator(1),
            MaxValueValidator(TracerLabeledClass.MAX_MASS_NUMBER),
        ],
        help_text="The sum of the number of protons and neutrons of the labeled atom, a.k.a. 'isotope', e.g. Carbon "
        "14.  The number of protons identifies the element that this tracer is an isotope of.  The number of neutrons "
        "in the element equals the number of protons, but in an isotope, the number of neutrons will be less than or "
        "greater than the number of protons.  Note, this differs from the 'atomic number' which indicates the number "
        "of protons only.",
    )

    class Meta:
        verbose_name = "label observation"
        verbose_name_plural = "label observations"
        ordering = ["labeled_element", "labeled_count", "mass_number", "peak_data"]

        # composite key
        constraints = [
            models.UniqueConstraint(
                fields=["peak_data", "labeled_element", "labeled_count"],
                name="unique_peakdata",
            )
        ]
