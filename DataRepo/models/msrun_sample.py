from django.db.models import (
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
        on_delete=RESTRICT,  # Delete an MSRunSequence once all its MSRunSamples are deleted
        related_name="msrun_samples",
        help_text="The sequence of all mass spectrometer runs in a single instrument run.",
    )
    sample = ForeignKey(
        to="DataRepo.Sample",
        null=False,
        blank=False,
        on_delete=RESTRICT,  # Delete a Sample once all its MSRunSamples are deleted
        related_name="msrun_samples",
        help_text="The sample that was run on the mass spectrometer, potentially multiple times/scans (resulting in "
        "multiple raw files), and potentially in different polarity modes.",
    )
    ms_data_files = ManyToManyField(
        to="DataRepo.ArchiveFile",
        null=True,
        blank=True,
        related_name="msrun_samples",
        help_text="All mass spectrometry data files (e.g. mzXML files) associated with this sample and sequence, "
        "potentially associated with multiple different raw files for multiple scans of the same sample.",
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
