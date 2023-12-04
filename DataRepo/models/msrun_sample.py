from django.core.exceptions import ValidationError
from django.db.models import (
    CASCADE,
    RESTRICT,
    AutoField,
    CharField,
    ForeignKey,
    UniqueConstraint,
)

from DataRepo.models import HierCachedModel, MaintainedModel


@MaintainedModel.relation(
    generation=2,
    parent_field_name="sample",
    # child_field_names=["peak_groups"],  # Only propagate up
    update_label="fcirc_calcs",
)
class MSRunSample(HierCachedModel, MaintainedModel):
    parent_related_key_name = "sample"
    child_related_key_names = ["peak_groups"]

    POLARITY_CHOICES = [
        ("negative", "negative"),
        ("positive", "positive"),
    ]
    VALID_RAW_FILES = {
        "TYPES": ["ms_data"],
        "FORMATS": ["ms_raw", "unknown"],
    }
    VALID_DATA_FILES = {
        "TYPES": ["ms_data"],
        "FORMATS": ["mzxml", "unknown"],
    }

    id = AutoField(primary_key=True)
    msrun_sequence = ForeignKey(
        to="DataRepo.MSRunSequence",
        null=False,
        blank=False,
        # Block MSRunSequence deletion unless all MSRunSamples linked to it are deleted via a different field's cascade
        on_delete=RESTRICT,
        related_name="msrun_samples",
        help_text="The series of sample injections in a batch run of the mass spec instrument.",
    )
    sample = ForeignKey(
        to="DataRepo.Sample",
        null=False,
        blank=False,
        on_delete=CASCADE,  # If the linked Sample is deleted, delete this record
        related_name="msrun_samples",
        help_text="A sample that was injected at least once during a mass spectrometer sequence.",
    )
    polarity = CharField(
        max_length=8,
        null=True,
        blank=True,
        choices=POLARITY_CHOICES,
        help_text="The polarity mode of this mass spectrometry run.",
    )
    ms_raw_file = ForeignKey(
        to="DataRepo.ArchiveFile",
        null=True,
        blank=True,
        # Block ArchiveFile deletion unless all MSRunSamples linked to it are deleted via a different field's cascade
        on_delete=RESTRICT,
        related_name="raw_to_msrunsamples",
        help_text="A raw file (generated by an instrument) representing one of potentially multiple mass spectrometry "
        "runs of this sample.",
        # NOTE: Even without the raw file, its sha1 hash can be parsed from the mzXML file.  And if we ever support
        # another ms data format that doesn't have the raw sha1, a sha1 can be generated from a string composed of the
        # sample name, date, researcher, lc_method, and polarity, as we have done in the past before we started saving
        # files.
    )
    ms_data_file = ForeignKey(
        to="DataRepo.ArchiveFile",
        null=True,
        blank=True,
        # Block ArchiveFile deletion unless all MSRunSamples linked to it are deleted via a different field's cascade
        on_delete=RESTRICT,
        related_name="mz_to_msrunsamples",
        help_text="A file representing a subset of data extracted from the raw file (e.g. an mzXML file).",
    )

    class Meta:
        verbose_name = "mass spectrometry run sample"
        verbose_name_plural = "mass spectrometry samples"
        ordering = ["msrun_sequence", "sample", "ms_raw_file", "ms_data_file"]
        constraints = [
            UniqueConstraint(
                fields=["ms_data_file"],
                name="unique_ms_data_file",
            ),
            # Since ms_data_file can be null (& null != null), the following prevents duplicate sequence/sample records
            UniqueConstraint(
                fields=[
                    "msrun_sequence",
                    "sample",
                    "polarity",
                    "ms_raw_file",
                    "ms_data_file",
                ],
                name="unique_msrunsample",
            ),
        ]

    def __str__(self):
        details = ", ".join(
            [
                i
                for i in [self.polarity, self.ms_raw_file, self.ms_data_file]
                if i is not None
            ]
        )
        return f"MS run of sample {self.sample} in {self.msrun_sequence} ({details})"

    def clean(self, *args, **kwargs):
        super().clean(*args, **kwargs)

        if self.ms_raw_file is not None:
            if self.ms_raw_file.data_type.code not in self.VALID_RAW_FILES["TYPES"]:
                raise ValidationError(
                    f"Invalid ms_raw_file ({self.ms_raw_file.filename}) data type: "
                    f"[{self.ms_raw_file.data_type.code}], must be one of [{self.VALID_RAW_FILES['TYPES']}]."
                )
            if self.ms_raw_file.data_format.code not in self.VALID_RAW_FILES["FORMATS"]:
                raise ValidationError(
                    f"Invalid ms_raw_file ({self.ms_raw_file.filename}) data format: "
                    f"[{self.ms_raw_file.data_format.code}], must be one of [{self.VALID_RAW_FILES['FORMATS']}]."
                )
        if self.ms_data_file is not None:
            if self.ms_data_file.data_type.code not in self.VALID_DATA_FILES["TYPES"]:
                raise ValidationError(
                    f"Invalid ms_data_file ({self.ms_data_file.filename}) data type: "
                    f"[{self.ms_data_file.data_type.code}], must be one of [{self.VALID_DATA_FILES['TYPES']}]."
                )
            if (
                self.ms_data_file.data_format.code
                not in self.VALID_DATA_FILES["FORMATS"]
            ):
                raise ValidationError(
                    f"Invalid ms_data_file ({self.ms_data_file.filename}) data format: "
                    f"[{self.ms_data_file.data_format.code}], must be one of [{self.VALID_DATA_FILES['FORMATS']}]."
                )
