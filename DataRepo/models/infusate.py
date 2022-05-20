from django.db import models

# from django.utils.functional import cached_property
from DataRepo.models.maintained_model import field_updater_function
from DataRepo.models.tracer import Tracer


class Infusate(models.Model):

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="A unique name or lab identifier of the infusate 'recipe' containing 1 or more tracer compounds at "
        "specific concentrations.",
    )
    short_name = models.CharField(
        max_length=20,
        unique=True,
        null=True,
        blank=True,
        help_text="A unique short name or lab identifier of the infusate 'recipe' containing 1 or more tracer "
        "compounds at specific concentrations, e.g '6eaas'.",
    )
    tracers = models.ManyToManyField(
        Tracer,
        through="InfusateTracer",
        help_text="Tracers included in this infusate 'recipe'.",
        related_name="infusates",
    )

    class Meta:
        verbose_name = "infusate"
        verbose_name_plural = "infusates"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)

    @field_updater_function("name")
    def _name(self):
        # Format: `short_name { tracername ; tracername }` (no spaces)
        if self.tracers is None or self.tracers.count() == 0:
            return self.short_name
        return (
            self.short_name
            + "{"
            + ";".join(sorted(map(lambda o: o._name, self.tracers.all())))
            + "}"
        )
