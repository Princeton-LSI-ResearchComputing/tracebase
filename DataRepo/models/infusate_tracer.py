from django.core.validators import MinValueValidator
from django.db import models

from DataRepo.models.maintained_model import MaintainedModel


class InfusateTracer(MaintainedModel):
    id = models.AutoField(primary_key=True)
    infusate = models.ForeignKey(
        "DataRepo.Infusate",
        on_delete=models.CASCADE,
        related_name="tracer_links",
    )
    tracer = models.ForeignKey(
        "DataRepo.Tracer",
        on_delete=models.CASCADE,
        related_name="infusate_links",
    )
    concentration = models.FloatField(
        null=False,
        blank=False,
        validators=[MinValueValidator(0)],
        help_text="The millimolar concentration of the tracer in a specific infusate 'recipe' (mM).",
        verbose_name="Tracer Concentration (mM)",
    )

    class Meta:
        verbose_name = "infusate_tracer_link"
        verbose_name_plural = "infusate_tracer_links"
        ordering = ["infusate", "tracer", "concentration"]
        constraints = [
            models.UniqueConstraint(
                fields=["infusate", "tracer"],
                name="unique_infusate_tracer",
            )
        ]

    def __str__(self):
        return f"Infusate {self.infusate.pk}, Tracer {self.tracer.pk}, at {self.concentration} mM"

    @MaintainedModel.setter(
        generation=1,
        parent_field_name="infusate",
        update_label="name",
    )
    def _name(self):
        """No name field to update, but we want changes to these records (i.e. their creation) to trigger Infusate.name
        to update.  That happens in this class's override to .save() in the MaintainedModel class, from which this
        class is derived.  But we don't actually want this method to be called because there is no field to update, so
        we leave out the update_field_name argument to the decorator.
        """
        pass

    @MaintainedModel.setter(
        generation=2,
        parent_field_name="infusate",
        update_label="label_combo",
    )
    def _label_combo(self):
        """No label_combo field to update, but we want changes to these records (i.e. their creation) to trigger
        Infusate.label_combo to update.  That happens in this class's override to .save() in the MaintainedModel class,
        from which this class is derived.  But we don't actually want this method to be called because there is no field
        to update, so we leave out the update_field_name argument to the decorator.
        """
        pass
