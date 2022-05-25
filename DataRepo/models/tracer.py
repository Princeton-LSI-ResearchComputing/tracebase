from django.db import models

# from django.utils.functional import cached_property
from DataRepo.models.maintained_model import (
    MaintainedModel,
    field_updater_function,
)
from DataRepo.models.element_label import ElementLabel


class Tracer(MaintainedModel, ElementLabel):

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        null=True,
        editable=False,
        help_text="A unique name or lab identifier of the tracer, e.g. 'lysine-C14'.",
    )
    compound = models.ForeignKey(
        to="DataRepo.Compound",
        on_delete=models.RESTRICT,
        null=False,
        related_name="tracer",
    )

    class Meta:
        verbose_name = "tracer"
        verbose_name_plural = "tracers"
        ordering = ["name"]

    def __str__(self):
        return str(self._name())

    @field_updater_function(
        generation=2, update_field_name="name", parent_field_name="infusates"
    )
    def _name(self):
        # format: `compound - [ labelname,labelname,... ]` (but no spaces)
        if self.id is None or self.labels is None or self.labels.count() == 0:
            return self.compound.name
        print(
            f"Returning tracer name including {self.labels.count()} labels from record IDs: "
            f"{','.join(list(map(lambda l: str(l.id), self.labels.all())))}"
        )
        return (
            self.compound.name
            + "-["
            + ",".join(list(map(lambda l: str(l), self.labels.all())))
            + "]"
        )
