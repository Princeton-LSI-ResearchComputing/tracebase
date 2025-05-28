from django.db import ProgrammingError
from django.db.models import AutoField, CharField, Model, Q, TextField


class Tissue(Model):
    detail_name = "tissue_detail"
    SERUM_TISSUE_PREFIX = "serum"

    id = AutoField(primary_key=True)
    name = CharField(
        max_length=256,
        unique=True,
        help_text='The laboratory standardized name for this tissue type (e.g. "serum", "brain", "liver").',
    )
    description = TextField(
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

    @classmethod
    def name_is_serum(cls, tissue_name: str):
        return tissue_name.startswith(Tissue.SERUM_TISSUE_PREFIX)

    @classmethod
    def serum_q_expression(cls, field_path: str):
        """Return a Q expression to check that the tissue name in the supplied field path is a serum tissue.

        Args:
            field_path (str): E.g. 'animals__samples__tissue__name'.  Must end in 'name'
        Exceptions:
            ProgrammingError - when the field_path is invalid.
        Returns:
            (Q)
        """
        if not field_path.endswith("name"):
            raise ProgrammingError(
                f"field_path '{field_path}' must end with 'name' (the tissue's name field)."
            )
        return Q(**{f"{field_path}__istartswith": Tissue.SERUM_TISSUE_PREFIX})

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})
