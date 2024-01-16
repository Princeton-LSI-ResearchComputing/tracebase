from django.core.validators import MinLengthValidator, RegexValidator
from django.db import models


class Study(models.Model):
    alphanumeric = RegexValidator(
        r"^[0-9a-zA-Z]+$", "Only alphanumeric characters are allowed."
    )
    atleast2chars = MinLengthValidator(2)

    id = models.AutoField(primary_key=True)
    code = models.CharField(
        max_length=6,
        blank=True,
        null=True,
        unique=True,
        help_text=(
            "A 2 to 6 character unique readable alphanumeric code for the study, to be used as a prefix for animal "
            "names, sample names, etc if necessary, to make them unique."
        ),
        validators=[atleast2chars, alphanumeric],
    )
    name = models.CharField(
        max_length=256,
        blank=False,
        null=False,
        unique=True,
        help_text=(
            "A succinct name for the study, which is a collection of one or more series of animals and their "
            "associated data."
        ),
    )
    description = models.TextField(
        blank=True,
        null=True,
        help_text="A long form description for the study which may include "
        "the experimental design process, citations, and other relevant details.",
    )

    class Meta:
        verbose_name = "study"
        verbose_name_plural = "studies"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)
