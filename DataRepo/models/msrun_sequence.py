from typing import Optional

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
    detail_name = "msrunsequence_detail"
    INSTRUMENT_CHOICES = [
        ("QE", "Q Exactive"),
        ("QE2", "Q Exactive 2"),
        ("QEPlus", "Q Exactive Plus"),
        ("QEHF", "Q Exactive HF"),
        ("Exploris240", "Exploris 240"),
        ("Exploris480", "Exploris 480"),
        ("ExplorisMX", "Exploris MX"),
        ("unknown", "Unknown"),
    ]
    # Note, INSTRUMENT_DEFAULT is not used as a "default" value for loading.  It is used for the following reasons:
    # 1. To allow the validation page to proceed without complaining about a missing instrument value
    # 2. As a placeholder value in order to proceed when a problem with an instrument value is encountered.  Whenever
    #    such a problem is encountered, an error is buffered and eventually raised at the end of a failed load.
    # 3. To avoid hard-coding static "magic" values in multiple places.
    INSTRUMENT_DEFAULT = INSTRUMENT_CHOICES[-1][0]
    SEQNAME_DELIMITER = ", "

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
        max_length=32,
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

    @classmethod
    def parse_sequence_name(cls, sequence_name: str):
        """Parses an Sequence Name into its parts.

        Args:
            name (str)
        Exceptions:
            None
        Returns:
            operator (Optional[str])
            lc_protocol_name (Optional[str])
            instrument (Optional[str])
            date (Optional[str])
        """
        from DataRepo.utils.exceptions import InvalidMSRunName

        operator = None
        date = None
        lc_protocol_name = None
        instrument = None

        if sequence_name is not None:
            try:
                (
                    operator,
                    lc_protocol_name,
                    instrument,
                    date,
                ) = sequence_name.split(cls.SEQNAME_DELIMITER)
            except ValueError as ve:
                raise InvalidMSRunName(
                    f"Unable to parse Sequence Name '{sequence_name}'.  Must be 4 comma-delimited values of "
                    "[Operator, LC Protocol, Instrument, and Date]."
                ).with_traceback(ve.__traceback__)

            if operator is not None:
                operator = operator.strip()
            if lc_protocol_name is not None:
                lc_protocol_name = lc_protocol_name.strip()
            if instrument is not None:
                instrument = instrument.strip()
            if date is not None:
                date = date.strip()

        return (
            operator,
            lc_protocol_name,
            instrument,
            date,
        )

    @property
    def sequence_name(self):
        from DataRepo.utils.file_utils import date_to_string

        return self.create_sequence_name(
            self.researcher,
            self.lc_method.name,
            self.instrument,
            date_to_string(self.date),
        )

    @classmethod
    def create_sequence_name(
        cls,
        operator: Optional[str] = "",
        protocol: Optional[str] = "",
        instrument: Optional[str] = "",
        date: Optional[str] = "",
    ):
        """Creates the sequence name in the prescribed order.

        This allows None values, which means that the default name would be ",,,".

        Args:
            operator (str)
            protocol (str)
            instrument (str)
            date (str)
        Exceptions:
            None
        Returns:
            seqname (str)
        """
        if operator is None:
            operator = ""
        if protocol is None:
            protocol = ""
        if instrument is None:
            instrument = ""
        if date is None:
            date = ""
        return cls.SEQNAME_DELIMITER.join([operator, protocol, instrument, date])

    @classmethod
    def get_most_used_protocol(cls, default=None):
        """Retrieves the name of the most used LCMethod in the database.

        Args:
            None
        Exceptions:
            None
        Returns:
            LCMethod name (str) [LCMethod.create_name(LCMethod.DEFAULT_TYPE)]
        """
        from django.db.models import Count

        from DataRepo.models import LCMethod

        if default is None:
            default = LCMethod.create_name(LCMethod.DEFAULT_TYPE)

        max_lc_dict = (
            cls.objects.exclude(lc_method__type="unknown")
            .values("lc_method_id")
            .annotate(count=Count("lc_method_id"))
            .order_by("-count")
            .first()
        )
        if max_lc_dict is None:
            return default
        return LCMethod.objects.get(id=max_lc_dict["lc_method_id"]).name

    @classmethod
    def get_most_used_instrument(cls, default=None):
        """Retrieves the name of the most used instrument in the database.

        Args:
            None
        Exceptions:
            None
        Returns:
            instrument (str) [cls.INSTRUMENT_DEFAULT]
        """
        from django.db.models import Count

        if default is None:
            default = cls.INSTRUMENT_DEFAULT

        max_instrument_dict = (
            cls.objects.exclude(instrument="unknown")
            .values("instrument")
            .annotate(count=Count("instrument"))
            .order_by("-count")
            .first()
        )
        if max_instrument_dict is None:
            return default
        return max_instrument_dict["instrument"]

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})
