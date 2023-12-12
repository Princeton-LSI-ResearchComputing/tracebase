from django.db import models


class MSRun(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    researcher = models.CharField(
        max_length=256,
        help_text="The name of the researcher who ran the mass spectrometer.",
    )
    date = models.DateField(
        help_text="The date that the mass spectrometer was run.",
    )
    # Don't delete an LCMethod if an MSRun that links to it is deleted
    lc_method = models.ForeignKey(
        null=False,
        blank=False,
        to="DataRepo.LCMethod",
        on_delete=models.RESTRICT,
        help_text="The liquid chromatography protocol that was used for this mass spectrometer run.",
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
        ordering = ["date", "researcher", "sample__name", "lc_method__name"]

        # MS runs that share researcher, date, protocol, and sample would be
        # indistinguishable, thus we restrict the database to ensure that
        # combination is unique. Constraint below assumes a researcher runs a
        # sample/protocol combo only once a day.
        constraints = [
            models.UniqueConstraint(
                fields=["researcher", "date", "lc_method", "sample"],
                name="unique_msrun",
            )
        ]

    def __str__(self):
        return str(
            f"MS run of sample {self.sample.name} with {self.lc_method.name} by {self.researcher} on {self.date}"
        )
