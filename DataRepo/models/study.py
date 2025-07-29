from django.db import models


class Study(models.Model):
    detail_name = "study_detail"

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        blank=False,
        null=False,
        unique=True,
        help_text="A succinct name for a study, which is a collection of one or more animals and their data.",
    )
    description = models.TextField(
        blank=True,
        null=True,
        help_text=(
            "A long form description for a study which may include the experimental design process, citations, and "
            "other relevant details."
        ),
    )

    class Meta:
        verbose_name = "study"
        verbose_name_plural = "studies"
        ordering = ["name"]

    def __str__(self):
        return self.name

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})
