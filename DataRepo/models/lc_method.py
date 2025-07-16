from datetime import timedelta

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.forms import ValidationError


class LCMethodManager(models.Manager):
    def get_by_natural_key(self, name):
        """Allows Django to get objects by a natural key instead of the primary key"""
        return self.get(name=name)


class LCMethod(models.Model):
    """
    The LCMethod class is a Django model of the concept of a liquid
    chromatography methodology
    """

    detail_name = "lcmethod_detail"

    DEFAULT_TYPE = "unknown"
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
            "through the liquid chromatography method."
        ),
    )

    objects = LCMethodManager()

    @classmethod
    def create_name(cls, type=None, run_length=None):
        """
        Class method to create a name using the supplied type and run_length.

        run_length can either be an integer of minutes or a timedelta.
        """
        if type is None:
            type = cls.DEFAULT_TYPE

        if run_length is None:
            return type
        elif isinstance(run_length, timedelta):
            run_length = int(run_length.total_seconds() / 60)

        return f"{type}-{run_length}-min"

    def get_name(self):
        """Generates a name using type and run_length."""
        return self.create_name(self.type, self.run_length)

    def natural_key(self):
        """Django can use the natural_key() method to serialize any foreign
        key reference to objects of the type that defines the method.

        Must return a tuple."""
        return (self.name,)

    class Meta:
        verbose_name = "LC Protocol"
        verbose_name_plural = "LC Protocols"
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

    def clean(self, *args, **kwargs):
        try:
            super().clean(*args, **kwargs)
        except ValidationError as ve:
            raise ve

        if self.name is not None and self.name != self.get_name():
            raise ValidationError(
                f"Invalid name: {self.name}.  The name must match the type and run length, e.g.: {self.get_name()}"
            )

    @classmethod
    def parse_lc_protocol_name(cls, name: str):
        """Parse a given LC Protocol name string and return the type and run_length."""
        try:
            vals = name.split("-")
            vals.pop()  # "min"
            runlen = vals.pop()
            typ = "-".join(vals)
        except ValueError as ve:
            raise ValueError(f"{ve} from input: {name}")
        return typ, int(runlen)

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})
