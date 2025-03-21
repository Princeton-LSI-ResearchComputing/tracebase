from django.db import models
from django.db.models.functions import Upper


class Protocol(models.Model):
    detail_name = "protocol_detail"
    ANIMAL_TREATMENT = "animal_treatment"
    CATEGORY_CHOICES = [
        (ANIMAL_TREATMENT, "Animal Treatment"),
    ]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="Unique name of the protocol.",
    )
    description = models.TextField(
        blank=True,
        null=True,
        help_text="Full text of the protocol's methods.",
    )
    category = models.CharField(
        max_length=256,
        choices=CATEGORY_CHOICES,
        help_text="Classification of the protocol, " "e.g. an animal treatment.",
    )

    class Meta:
        verbose_name = "protocol"
        verbose_name_plural = "protocols"
        ordering = [Upper("name")]

    def __str__(self):
        return str(self.name)

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})
