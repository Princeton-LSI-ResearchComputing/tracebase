from datetime import timedelta

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models


class LCMethodManager(models.Manager):
    def get_by_natural_key(self, name):
        """Allows Django to get objects by a natural key instead of the primary key"""
        return self.get(name=name)


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
            "(e.g. polar-HILIC-25-min)"
        ),
    )
    type = models.CharField(
        blank=False,
        null=False,
        max_length=128,
        help_text=(
            "Laboratory-defined type of the liquid chromatography method."
            "(e.g. polar-HILIC)"
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

    objects = LCMethodManager()

    def natural_key(self):
        """Django can use the natural_key() method to serialize any foreign
        key reference to objects of the type that defines the method.

        Must return a tuple."""
        return (self.name,)

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
