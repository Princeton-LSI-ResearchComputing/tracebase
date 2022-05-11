from django.core.validators import MinValueValidator
from django.db import models

from .infusate import Infusate
from .tracer import Tracer


class TracerIngredient(models.Model):
    id = models.AutoField(primary_key=True)
    infusate = models.ForeignKey(Infusate, on_delete=models.CASCADE)
    tracer = models.ForeignKey(Tracer, on_delete=models.CASCADE)
    concentration = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The millimolar concentration of the tracer in a specific infusate 'recipe' (mM).",
    )

    class Meta:
        verbose_name = "ingredient"
        verbose_name_plural = "ingredients"
        ordering = ["infusate", "tracer"]
