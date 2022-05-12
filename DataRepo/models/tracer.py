from django.db import models

from .tracer_labeled_class import TracerLabeledClass


class Tracer(models.Model, TracerLabeledClass):

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
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
        return str(self.name)

    def _name(self):
        # format: `compound - [ labelname,labelname,... ]` (but no spaces)
        return self.compound.name + "-[" + ",".join(self.labels.name()) + "]"
