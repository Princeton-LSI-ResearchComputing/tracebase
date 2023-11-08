from django.db.models import (
    RESTRICT,
    AutoField,
    CharField,
    ForeignKey,
    Model,
    UniqueConstraint,
)


class MSRunSample(Model):
    POLARITY_CHOICES = [
        ("negative", "negative"),
        ("positive", "positive"),
    ]

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
        help_text="The sample that was scanned in this mass spectrometry run.",
    )
    polarity = CharField(
        max_length=8,
        null=True,
        blank=True,
        choices=POLARITY_CHOICES,
        help_text="The polarity mode of this mass spectrometry run.",
    )
    ms_raw_file = ForeignKey(
        to="DataRepo.ArchiveFile",
        null=True,
        blank=True,
        on_delete=RESTRICT,  # Delete a raw file once all its MSRunSamples are deleted
        related_name="raw_to_msrunsamples",
        help_text="A raw file (generated by an instrument) representing one of potentially multiple mass spectrometry "
        "runs of this sample.",
        # NOTE: Even without the raw file, its sha1 hash can be parsed from the mzXML file.  And if we ever support
        # another ms data format that doesn't have the raw sha1, a sha1 can be generated from a string composed of the
        # sample name, date, researcher, lc_method, and polarity, as we have done in the past before we started saving
        # files.
    )
    ms_data_file = ForeignKey(
        to="DataRepo.ArchiveFile",
        null=True,
        blank=True,
        related_name="mz_to_msrunsamples",
        help_text="A file representing a subset of data extracted from the raw file (e.g. an mzXML file).",
    )

    class Meta:
        verbose_name = "mass spectrometry run sample"
        verbose_name_plural = "mass spectrometry samples"
        ordering = ["msrun_sequence", "sample", "ms_raw_file", "ms_data_file"]
        constraints = [
            UniqueConstraint(
                fields=["ms_data_file"],
                name="unique_ms_data_file",
            ),
            # Since ms_data_file can be null (& null != null), the following prevents duplicate sequence/sample records
            UniqueConstraint(
                fields=["msrun_sequence", "sample", "polarity", "ms_raw_file", "ms_data_file"],
                name="unique_msrunsample",
            )
        ]

    def __str__(self):
        details = ", ".join(
            [
                i
                for i in [self.polarity, self.ms_raw_file, self.ms_data_file]
                if i is not None
            ]
        )
        return f"MS run of sample {self.sample} in {self.msrun_sequence} ({details})"
