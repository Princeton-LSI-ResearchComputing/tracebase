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
        ("unknown", "unknown"),
    ]
    # Note, INSTRUMENT_DEFAULT is not used as a "default" value for loading.  It is used for the following reasons:
    # 1. To allow the validation page to proceed without complaining about a missing instrument value
    # 2. As a placeholder value in order to proceed when a problem with an instrument value is encountered.  Whenever
    #    such a problem is encountered, an error is buffered and eventually raised at the end of a failed load.
    # 3. To avoid hard-coding static "magic" values in multiple places.
    INSTRUMENT_DEFAULT = INSTRUMENT_CHOICES[4][0]

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
        max_length=7,
        choices=INSTRUMENT_CHOICES,
        help_text="The name of the mass spectrometer.",
    )
    lc_method = ForeignKey(
        # Prevent MSRunSequence deletion unless initiated by another field's CASCADE deletion includes all other records
        # referencing that lc_method
        on_delete=RESTRICT,
        null=False,
        blank=False,
        to="DataRepo.LCMethod",
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
            f"MS run sequence on instrument [{self.instrument}] with LC protocol [{self.lc_method.name}], operated by "
            f"{self.researcher} on {self.date}"
        )
