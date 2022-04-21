from django.db import models


class Tissue(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text='The laboratory standardized name for this tissue type (e.g. "serum", "brain", "liver").',
    )
    description = models.TextField(
        blank=True,
        help_text="Description of this tissue type.",
    )

    class Meta:
        verbose_name = "tissue"
        verbose_name_plural = "tissues"
        ordering = ["name"]

    SERUM_TISSUE_PREFIX = "serum"

    def __str__(self):
        return str(self.name)
