from django.db import models

from .tracer import Tracer


class Infusate(models.Model):

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="A unique name or lab identifier of the infusate 'recipe'.",
    )
    tracers = models.ManyToManyField(
        Tracer,
        through="TracerIngredient",
        help_text="Tracers included in this infusate 'recipe'.",
    )

    class Meta:
        verbose_name = "infusate"
        verbose_name_plural = "infusates"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)
