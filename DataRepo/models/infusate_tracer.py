from django.core.validators import MinValueValidator
from django.db import models

from DataRepo.models.infusate import Infusate
from DataRepo.models.tracer import Tracer


class InfusateTracer(models.Model):
    id = models.AutoField(primary_key=True)
    infusate = models.ForeignKey(Infusate, on_delete=models.CASCADE)
    tracer = models.ForeignKey(Tracer, on_delete=models.CASCADE)
    concentration = models.FloatField(
        null=False,
        blank=False,
        validators=[MinValueValidator(0)],
        help_text="The millimolar concentration of the tracer in a specific infusate 'recipe' (mM).",
    )

    class Meta:
        verbose_name = "infusate_tracer_link"
        verbose_name_plural = "infusate_tracer_links"
        ordering = ["infusate", "tracer"]
