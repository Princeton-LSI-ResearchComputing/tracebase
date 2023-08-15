from datetime import timedelta

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models


class LCMethod(models.Model):
    """
    The LCMethod class is a Django model of the concept of a liquid
    chromatography methodology
    """

    MINIMUM_VALID_RUN_LENGTH = timedelta(seconds=0)
    MAXIMUM_VALID_RUN_LENGTH = timedelta(days=1)

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        unique=True,
        blank=False,
        null=False,
        max_length=256,
        help_text=(
            "Unique laboratory-defined name of the liquid chromatography method."
            "(e.g. HILIC-0:25:00 minutes, Reverse Phase-0:25:00 minutes)"
        ),
    )
    type = models.CharField(
        blank=False,
        null=False,
        max_length=128,
        help_text=(
            "Laboratory-defined type of the liquid chromatography method."
            "(e.g. HILIC, Reverse Phase)"
        ),
    )
    description = models.TextField(
        unique=True,
        blank=False,
        null=False,
        help_text="Unique full-text description of the liquid chromatography method.",
    )
    run_length = models.DurationField(
        blank=True,
        null=True,
        validators=[
            MinValueValidator(MINIMUM_VALID_RUN_LENGTH),
            MaxValueValidator(MAXIMUM_VALID_RUN_LENGTH),
        ],
        help_text=(
            "Time duration to complete a sample run "
            "through the liquid chromatography method.",
        ),
    )

    class Meta:
        verbose_name = "liquid chromatography method"
        verbose_name_plural = "liquid chromatography methods"
        ordering = ["name"]
        constraints = [
            models.CheckConstraint(
                name="DataRepo_lcmethod_name_not_empty",
                check=~models.Q(name=""),
            ),
            models.CheckConstraint(
                name="DataRepo_lcmethod_type_not_empty",
                check=~models.Q(type=""),
            ),
            models.CheckConstraint(
                name="DataRepo_lcmethod_description_not_empty",
                check=~models.Q(description=""),
            ),
        ]

    def __str__(self):
        return str(self.name)
