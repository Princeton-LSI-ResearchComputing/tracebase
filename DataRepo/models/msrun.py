
from django.db import models

from DataRepo.hier_cached_model import HierCachedModel, cached_function

from .protocol import Protocol
from .sample import Sample

class MSRun(HierCachedModel):
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
    # Don't allow a Protocol to be deleted if an MSRun links to it
    protocol = models.ForeignKey(
        Protocol,
        on_delete=models.RESTRICT,
        limit_choices_to={"category": Protocol.MSRUN_PROTOCOL},
        help_text="The protocol that was used for this mass spectrometer run.",
    )
    # Don't allow a Sample to be deleted if an MSRun links to it
    sample = models.ForeignKey(
        Sample,
        on_delete=models.RESTRICT,
        related_name="msruns",
        help_text="The sample that was run on the mass spectrometer.",
    )

    class Meta:
        verbose_name = "mass spectrometry run"
        verbose_name_plural = "mass spectrometry runs"
        ordering = ["date", "researcher", "sample__name", "protocol__name"]

        """
        MS runs that share researcher, date, protocol, and sample would be
        indistinguishable, thus we restrict the database to ensure that
        combination is unique. Constraint below assumes a researcher runs a
        sample/protocol combo only once a day.
        """
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

    def clean(self):
        super().clean()

        if self.protocol.category != Protocol.MSRUN_PROTOCOL:
            raise ValidationError(
                "Protocol category for an MSRun must be of type "
                f"{Protocol.MSRUN_PROTOCOL}"
            )

