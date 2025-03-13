from django.db import models


class Tissue(models.Model):
    detail_name = "tissue_detail"
    SERUM_TISSUE_PREFIX = "serum"

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text='The laboratory standardized name for this tissue type (e.g. "serum", "brain", "liver").',
    )
    description = models.TextField(
        help_text="Description of this tissue type.",
    )

    class Meta:
        verbose_name = "tissue"
        verbose_name_plural = "tissues"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)

    def is_serum(self):
        return self.name.startswith(Tissue.SERUM_TISSUE_PREFIX)

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})
