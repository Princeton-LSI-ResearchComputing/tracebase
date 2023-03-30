from django.core.exceptions import ValidationError
from django.db import models

from DataRepo.models.hier_cached_model import HierCachedModel
from DataRepo.models.maintained_model import (
    MaintainedModel,
    maintained_model_relation,
)
from DataRepo.models.protocol import Protocol


@maintained_model_relation(
    generation=2,
    parent_field_name="sample",
    # child_field_names=["peak_groups"],  # Only propagate up
    update_label="fcirc_calcs",
)
class MSRun(HierCachedModel, MaintainedModel):
    parent_related_key_name = "sample"
    child_related_key_names = ["peak_groups"]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    researcher = models.CharField(
        max_length=256,
        help_text="The name of the researcher who ran the mass spectrometer.",
    )
    date = models.DateField(
        help_text="The date that the mass spectrometer was run.",
    )
    # Don't delete a Protocol if an MSRun that links to it is deleted
    protocol = models.ForeignKey(
        to="DataRepo.Protocol",
        on_delete=models.RESTRICT,
        limit_choices_to={"category": Protocol.MSRUN_PROTOCOL},
        help_text="The protocol that was used for this mass spectrometer run.",
    )
    # If an MSRun is deleted, delete its samples
    sample = models.ForeignKey(
        to="DataRepo.Sample",
        on_delete=models.CASCADE,
        related_name="msruns",
        help_text="The sample that was run on the mass spectrometer.",
    )

    class Meta:
        verbose_name = "mass spectrometry run"
        verbose_name_plural = "mass spectrometry runs"
        ordering = ["date", "researcher", "sample__name", "protocol__name"]

        # MS runs that share researcher, date, protocol, and sample would be
        # indistinguishable, thus we restrict the database to ensure that
        # combination is unique. Constraint below assumes a researcher runs a
        # sample/protocol combo only once a day.
        constraints = [
            models.UniqueConstraint(
                fields=["researcher", "date", "protocol", "sample"],
                name="unique_msrun",
            )
        ]

    def __str__(self):
        return str(
            f"MS run of sample {self.sample.name} with {self.protocol.name} by {self.researcher} on {self.date}"
        )

    def clean(self, *args, **kwargs):
        super().clean(*args, **kwargs)

        if self.protocol.category != Protocol.MSRUN_PROTOCOL:
            raise ValidationError(
                "Protocol category for an MSRun must be of type "
                f"{Protocol.MSRUN_PROTOCOL}"
            )
