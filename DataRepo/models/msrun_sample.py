from django.db.models import (
    CASCADE,
    RESTRICT,
    AutoField,
    ForeignKey,
    ManyToManyField,
    Model,
    UniqueConstraint,
)


class MSRunSample(Model):
    id = AutoField(primary_key=True)
    msrun_sequence = ForeignKey(
        to="DataRepo.MSRunSequence",
        null=False,
        blank=False,
        # Prevent MSRunSequence deletion unless all MSRunSamples linked to i are deleted via a different field's cascade
        on_delete=RESTRICT,
        related_name="msrun_samples",
        help_text="The series of sample injections in a batch run of the mass spec instrument.",
    )
    sample = ForeignKey(
        to="DataRepo.Sample",
        null=False,
        blank=False,
        on_delete=CASCADE,  # If the linked Sample is deleted, delete this record
        related_name="msrun_samples",
        help_text="A sample that was injected at least once during a mass spectrometer sequence.",
    )
    ms_data_files = ManyToManyField(
        to="DataRepo.ArchiveFile",
        blank=True,
        related_name="msrun_samples",
        help_text="Any mass spectrometry data files (e.g. mzXML files) associated with this sample and sequence, "
        "(potentially associated with multiple different raw files for multiple scans of the same sample).",
    )

    class Meta:
        verbose_name = "mass spectrometry run sample"
        verbose_name_plural = "mass spectrometry samples"
        ordering = ["msrun_sequence", "sample"]
        constraints = [
            UniqueConstraint(
                fields=["msrun_sequence", "sample"],
                name="unique_msrunsample",
            )
        ]

    def __str__(self):
        return f"MS run of sample {self.sample} in {self.msrun_sequence}"
