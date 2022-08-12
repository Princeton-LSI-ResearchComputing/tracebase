import warnings

from django.db import models

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function

class AnimalTracer(HierCachedModel):
    parent_related_key_name = "animal"
    # Leaf

    id = models.AutoField(primary_key=True)
    animal = models.ForeignKey(
        "DataRepo.Animal",
        on_delete=models.CASCADE,
        related_name="animal_tracers",
    )
    tracer = models.ForeignKey(
        "DataRepo.Tracer",
        on_delete=models.CASCADE,
    )

    class Meta:
        verbose_name = "animal_tracer"
        verbose_name_plural = "animal_tracers"
        ordering = ["animal", "tracer"]
        constraints = [
            models.UniqueConstraint(
                fields=["animal", "tracer"],
                name="unique_animal_tracer",
            )
        ]
