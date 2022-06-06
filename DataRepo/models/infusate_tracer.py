from django.core.validators import MinValueValidator
from django.db import models

from DataRepo.models.infusate import Infusate
from DataRepo.models.maintained_model import (
    MaintainedModel,
    field_updater_function,
)
from DataRepo.models.tracer import Tracer


class InfusateTracer(MaintainedModel):
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
        ordering = ["infusate", "tracer", "concentration"]
        constraints = [
            models.UniqueConstraint(
                fields=["infusate", "tracer", "concentration"],
                name="unique_infusate_tracer",
            )
        ]

    @field_updater_function(
        generation=1, parent_field_name="infusate", update_label="name"
    )
    def _name(self):
        """
        No name field to update, but we want to propagate changes to Infusate.name when links are created, changed, or
        deleted.  This method is not called when the decorator above does not supply update_field_name
        """
        pass
