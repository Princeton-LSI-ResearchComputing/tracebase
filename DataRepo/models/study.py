from django.db import models

from .protocol import Protocol

class Study(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="A succinct name for the study, which is a collection of "
        "one or more series of animals and their associated data.",
    )
    description = models.TextField(
        blank=True,
        help_text="A long form description for the study which may include "
        "the experimental design process, citations, and other relevant details.",
    )

    class Meta:
        verbose_name = "study"
        verbose_name_plural = "studies"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)

