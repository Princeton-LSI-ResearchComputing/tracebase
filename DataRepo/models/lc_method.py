from datetime import timedelta

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models.functions import Length

# so we can call description__length__gt in the constraints
models.TextField.register_lookup(Length)


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
        blank=False,
        null=False,
        max_length=256,
        help_text=(
            "Laboratory-defined type of the liquid chromatography method."
            "(e.g. HILIC, Reverse Phase)"
        ),
    )
    description = models.TextField(
        blank=False,
        null=False,
        help_text="Full text of the liquid chromatography method.",
    )
        blank=True,
        null=True,
        validators=[
                name="%(app_label)s_%(class)s_record_unique",
            ),
            models.CheckConstraint(
                name="%(app_label)s_%(class)s_description_not_empty",
                check=models.Q(description__length__gt=0),
            ),
        ]

    def __str__(self):
        if self.run_length:
            return f"{self.chromatographic_technique}-{self.run_length}"
        return self.chromatographic_technique
