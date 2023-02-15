import warnings
from datetime import timedelta

from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.maintained_model import (
    MaintainedModel,
    maintained_field_function,
)
from DataRepo.models.utilities import create_is_null_field

from .element_label import ElementLabel
from .protocol import Protocol
from .tissue import Tissue


class Animal(MaintainedModel, HierCachedModel, ElementLabel):
    # No parent_related_key_name, because this is a root
    child_related_key_names = ["samples", "labels"]

    FEMALE = "F"
    MALE = "M"
    SEX_CHOICES = [(FEMALE, "female"), (MALE, "male")]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        null=False,
        help_text="A unique name or lab identifier of the source animal for a series of studied samples.",
    )
    infusate = models.ForeignKey(
        to="DataRepo.Infusate",
        on_delete=models.RESTRICT,
        null=False,
        blank=False,
        related_name="animals",
        help_text="The solution infused into the animal containing 1 or more tracer compounds at specific "
        "concentrations.",
    )
    infusion_rate = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The rate of infusion of the tracer solution in microliters/min/gram of body weight of the animal "
        "(ul/min/g).",
    )
    genotype = models.CharField(
        max_length=256, help_text="The laboratory standardized genotype of the animal."
    )
    body_weight = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The weight (in grams) of the animal at the time of sample collection.",
    )
    age = models.DurationField(
        null=True,
        blank=True,
        validators=[MinValueValidator(timedelta(seconds=0))],
        help_text="The age of the animal at the time of sample collection.",
    )
    sex = models.CharField(
        max_length=1,
        null=True,
        blank=True,
        choices=SEX_CHOICES,
        help_text='The sex of the animal ("male" or "female").',
    )
    diet = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text='The feeding descriptor for the animal [e.g. "LabDiet Rodent 5001"].',
    )
    feeding_status = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text="The laboratory coded dietary state for the animal, "
        'also referred to as "Animal State" (e.g. "fasted").',
    )
    studies = models.ManyToManyField(
        to="DataRepo.Study",
        related_name="animals",
        help_text="The experimental study(ies) the the animal is associated with.",
    )
    treatment = models.ForeignKey(
        to="DataRepo.Protocol",
        on_delete=models.RESTRICT,
        null=True,
        blank=True,
        related_name="animals",
        limit_choices_to={"category": Protocol.ANIMAL_TREATMENT},
        help_text="The laboratory controlled label of the actions taken on an animal.",
    )
    last_serum_sample = models.ForeignKey(
        to="DataRepo.Sample",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        db_column="last_serum_sample_id",  # Necessary because of Sample's link to Animal
        related_name="animals",
        help_text="Automatically maintained field. Shortcut to the last serum sample.",
    )

    @property  # type: ignore
    @cached_function
    def tracers(self):
        if self.infusate.tracers.count() == 0:
            warnings.warn(f"Animal [{self.name}] has no tracers.")
        return self.infusate.tracers.all()

    @maintained_field_function(
        generation=0,
        child_field_names=["samples"],
        update_label="fcirc_calcs",
        update_field_name="last_serum_sample",
    )
    def _last_serum_sample(self):
        """
        last_serum_sample in an instance method that returns the last single serum sample removed from the animal,
        based on the time elapsed/duration from the initiation of infusion or treatment, typically.  If the animal has
        no serum samples or if the retrieved serum sample has no annotated time_collected, a warning will be issued.
        """
        # Create an is_null field for time_collected to be able to sort them
        (extra_args, is_null_field) = create_is_null_field("time_collected")
        last_serum_sample = (
            self.samples.filter(tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX)
            .extra(**extra_args)
            .order_by(f"-{is_null_field}", "time_collected")
            .last()
        )

        if last_serum_sample is None:
            warnings.warn(f"Animal {self.name} has no 'serum' samples.")
        elif not last_serum_sample.time_collected:
            warnings.warn(
                f"The Final serum sample {last_serum_sample} for animal [{self}] is missing a time_collected value."
            )

        return last_serum_sample

    @property  # type: ignore
    @cached_function
    def last_serum_tracer_peak_groups(self):
        """
        Retrieves the last Peak Group for each tracer compound that has this.element
        """
        from DataRepo.models.peak_group import PeakGroup

        # Get every tracer's compound that contains this element
        if self.tracers.count() == 0:
            warnings.warn(f"Animal [{self}] has no tracers.")
            return PeakGroup.objects.none()

        # Get the last peakgroup for each tracer that has this label
        last_serum_peakgroup_ids = []
        (tc_extra_args, tc_is_null_field) = create_is_null_field(
            "msrun__sample__time_collected"
        )
        # Create an is_null field for the date to be able to sort them
        (d_extra_args, d_is_null_field) = create_is_null_field("msrun__date")
        for tracer in self.tracers.all():
            tracer_peak_group = (
                PeakGroup.objects.filter(msrun__sample__animal__id__exact=self.id)
                .filter(compounds__id__exact=tracer.compound.id)
                .filter(
                    msrun__sample__tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
                )
                .extra(**tc_extra_args)
                .extra(**d_extra_args)
                .order_by(
                    f"-{tc_is_null_field}",
                    "msrun__sample__time_collected",
                    f"-{d_is_null_field}",
                    "msrun__date",
                )
                .last()
            )
            if tracer_peak_group:
                last_serum_peakgroup_ids.append(tracer_peak_group.id)
            else:
                warnings.warn(
                    f"Animal {self} has no serum sample peak group for {tracer.compound}."
                )
                return PeakGroup.objects.none()

        return PeakGroup.objects.filter(id__in=last_serum_peakgroup_ids)

    class Meta:
        verbose_name = "animal"
        verbose_name_plural = "animals"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)

    def clean(self, *args, **kwargs):
        super().clean(*args, **kwargs)

        if self.treatment is not None:
            if self.treatment.category != Protocol.ANIMAL_TREATMENT:
                raise ValidationError(
                    "Protocol category for an Animal must be of type "
                    f"{Protocol.ANIMAL_TREATMENT}"
                )
