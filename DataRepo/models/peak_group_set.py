from django.db import models


class PeakGroupSet(models.Model):
    id = models.AutoField(primary_key=True)
    filename = models.CharField(
        max_length=256,
        unique=True,
        help_text="The unique name of the source-file or dataset containing "
        "a researcher-defined set of peak groups and their associated data",
    )
    imported_timestamp = models.DateTimeField(
        auto_now_add=True,
        help_text="The timestamp for when the source datafile was imported.",
    )

    class Meta:
        verbose_name = "peak group set"
        verbose_name_plural = "peak group sets"
        ordering = ["filename"]

    def __str__(self):
        return str(f"{self.filename} at {self.imported_timestamp}")
