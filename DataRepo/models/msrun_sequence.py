from django.db.models import (
    RESTRICT,
    AutoField,
    CharField,
    DateField,
    ForeignKey,
    Model,
    TextField,
    UniqueConstraint,
)


class MSRunSequence(Model):
    INSTRUMENT_CHOICES = [
        ("HILIC", "HILIC"),
        ("QE", "QE"),
        ("QE2", "QE2"),
        ("QTOF", "QTOF"),
    ]

    id = AutoField(primary_key=True)
    researcher = CharField(
        null=False,
        blank=False,
        max_length=256,
        help_text="The name of the researcher who ran the mass spectrometer.",
    )
    date = DateField(
        null=False,
        blank=False,
        help_text="The date that the mass spectrometer was run.",
    )
    instrument = CharField(
        null=False,
        blank=False,
        max_length=6,
        choices=INSTRUMENT_CHOICES,
        help_text="The name of the mass spectrometer.",
    )
    lc_method = ForeignKey(
        null=False,
        blank=False,
        to="DataRepo.LCMethod",
        on_delete=RESTRICT,  # Delete this MSRunSequence if all other records referencing it are being deleted
        help_text="The liquid chromatography protocol that was used for this mass spectrometer run sequence.",
    )
    notes = TextField(
        unique=True,
        blank=True,
        null=True,
        help_text="Notes on this mass spectrometer run sequence.",
    )

    class Meta:
        verbose_name = "mass spectrometry run sequence"
        verbose_name_plural = "mass spectrometry run sequences"
        ordering = ["date", "researcher", "instrument", "lc_method__name"]
        constraints = [
            UniqueConstraint(
                fields=["researcher", "date", "instrument", "lc_method"],
                name="unique_msrunsequence",
            )
        ]

    def __str__(self):
        return str(
            f"MS run sequence using instrument {self.instrument} with {self.lc_method.name} by {self.researcher} on "
            f"{self.date}"
        )
