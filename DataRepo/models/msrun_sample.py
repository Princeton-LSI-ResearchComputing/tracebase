from __future__ import annotations

from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db.models import (
    CASCADE,
    RESTRICT,
    AutoField,
    CharField,
    FloatField,
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
        ("unknown", "unknown"),
        ("positive", "positive"),
        ("negative", "negative"),
    ]
    # The default polarity is used in loading, tests, and the validation interface as placeholders.  During loading,
    # the precedence is: polarity parsed from mzXML file > value recorded in the LCMS Metadata file for the given sample
    # > value provided on the command line > This default value.  An exception will be raised if the mzXML file value
    # and the LCMS Metadata values differ.
    POLARITY_DEFAULT = POLARITY_CHOICES[0][0]
    UNKNOWN_POLARITY = POLARITY_CHOICES[0][0]
    # These polarity values are used to convert the polarity representation parsed from an mzXML file (which records
    # polarity using the symbols "=" and "-")
    POSITIVE_POLARITY = POLARITY_CHOICES[1][0]
    NEGATIVE_POLARITY = POLARITY_CHOICES[2][0]

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
    mz_min = FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text=(
            "Unsigned minimum charge of the scan range.  Only required if a study contains multiple MSRuns with the "
            "same polarity.  Automatically parsed from mzXML.  If unavailable, the minimum medMz value from the "
            "accucor/isocorr file is acceptable."
        ),
    )
    mz_max = FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text=(
            "Unsigned maximum charge of the scan range.  Only required if a study contains multiple MSRuns with the "
            "same polarity.  Automatically parsed from mzXML.  If unavailable, the maximum medMz value from the "
            "accucor/isocorr file is acceptable."
        ),
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
                    "mz_min",
                    "mz_max",
                ],
                name="unique_msrunsample",
            ),
        ]

    def __str__(self):
        details = ", ".join(
            [
                f"{k}: {i}"
                for k, i in {
                    "polarity": self.polarity,
                    "mz min": self.mz_min,
                    "mz max": self.mz_max,
                    "RAW file": self.ms_raw_file,
                    "mzXML": self.ms_data_file,
                }.items()
                if i is not None
            ]
        )
        if details == "":
            details = "using default parameters"
        return f"MS run of sample {self.sample} ({details}) in {self.msrun_sequence}"

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

        # TODO: Create a migration that removes polarity, mz_min, & mz_max from the MSRunSample placeholder records

        # PR REVIEW NOTE: This should be handled in a separate issue, as this causes over 300 tests to fail (though a
        # lot of them are because class setup fails).  This is necessary to keep from associating peak groups with the
        # wrong polarity and/or scan range that are forced to be wrong due to the new null == null unique constraint.

        # TODO: Create an issue to uncomment this code.  A bunch of tests will have to be fixed.  This should probably
        # be addressed during the peak annotation loader refactor.
        # else:
        #     # Since there can only be 1 placeholder record (when we don't have an ms_data_file), and peak groups could
        #     # have been created from scans of different polarities and scan ranges, we do not allow polarity, mz_min,
        #     # or mz_max to be set when there is no ms_data_file.
        #     disallowed = []
        #     if self.polarity is not None and self.polarity != self.UNKNOWN_POLARITY:
        #         disallowed.append(f"polarity ({self.polarity})")
        #     if self.mz_min is not None:
        #         disallowed.append(f"mz_min ({self.mz_min})")
        #     if self.mz_max is not None:
        #         disallowed.append(f"mz_max ({self.mz_max})")
        #     if len(disallowed) > 0:
        #         raise ValidationError(
        #             f"This/these value(s) {disallowed} cannot be defined when ms_data_file is not defined.  Multiple "
        #             "PeakGroup records, originating from different mzXML files with different polarities and scan "
        #             "ranges, could link to this placeholder record."
        #         )

        if (
            self.mz_min is not None
            and self.mz_max is not None
            and self.mz_min > self.mz_max
        ):
            raise ValidationError(
                f"Invalid mz_min [{self.mz_min}] and mz_max [{self.mz_max}]: "
                f"The minimum charge must be less than or equal to the maximum charge in the scan range."
            )
