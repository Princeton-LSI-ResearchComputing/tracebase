from datetime import timedelta

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models


class ChromatographicTechniqueChoices(models.TextChoices):
    """
    The ChromatographicTechniqueChoices class is provides the controlled value
    choices for LCMethod.chromatographic_technique
    """

    HILIC_TECHNIQUE = "HILIC", "HILIC"
    REVERSE_PHASE_TECHNIQUE = "Reverse Phase", "Reverse Phase"
    OTHER_TECHNIQUE = "Other", "Other"


class LCMethod(models.Model):
    """
    The LCMethod class is a Django model of the concept of a liquid
    chromatography methodology
    """

    MINIMUM_VALID_RUN_LENGTH = timedelta(seconds=0)
    MAXIMUM_VALID_RUN_LENGTH = timedelta(days=1)

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    chromatographic_technique = models.CharField(
        max_length=256,
        choices=ChromatographicTechniqueChoices.choices,
        help_text="Laboratory-defined type of the liquid chromatography method.",
        unique=True,
    )
    description = models.TextField(
        blank=True,
        null=True,
        help_text="Full text of the liquid chromatography method.",
    )
    run_length = models.DurationField(
        validators=[
            MinValueValidator(MINIMUM_VALID_RUN_LENGTH),
            MaxValueValidator(MAXIMUM_VALID_RUN_LENGTH),
        ],
        help_text="Time duration to complete the mass spectrometry sequence.",
    )

    class Meta:
        verbose_name = "liquid chromatography method"
        verbose_name_plural = "liquid chromatography methods"
        ordering = ["chromatographic_technique"]
        constraints = [
            models.UniqueConstraint(
                fields=["chromatographic_technique", "description", "run_length"],
                name="%(class)s_record_unique",
            ),
            models.CheckConstraint(
                name="%(class)s_technique_valid",
                check=models.Q(
                    chromatographic_technique__in=ChromatographicTechniqueChoices.values
                ),
            ),
        ]

    def __str__(self):
        return f"{self.chromatographic_technique}-{self.run_length}"
