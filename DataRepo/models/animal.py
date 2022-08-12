import warnings
from datetime import timedelta

from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models
from django.utils.functional import cached_property

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function

from .element_label import ElementLabel
from .protocol import Protocol
from .tissue import Tissue


class Animal(HierCachedModel, ElementLabel):
    # No parent_related_key_name, because this is a root
    child_related_key_names = ["samples", "animal_labels", "animal_tracers"]

    FEMALE = "F"
    MALE = "M"
    SEX_CHOICES = [(FEMALE, "female"), (MALE, "male")]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="A unique name or lab identifier of the source animal for a series of studied samples.",
    )
    infusate = models.ForeignKey(
        to="DataRepo.Infusate",
        on_delete=models.RESTRICT,
        null=True,
        blank=True,
        related_name="animal",
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

    # @cached_function is *slower* than uncached
    @cached_property
    def all_serum_samples(self):
        """
        all_serum_samples() in an instance method that returns all the serum
        samples removed from the calling animal object, ordered by the time they
        were collected from the animal, which is recorded as the time
        elapsed/duration from the initiation of infusion or treatment,
        typically.
        """
        return (
            self.samples.filter(tissue__name__startswith=Tissue.SERUM_TISSUE_PREFIX)
            .order_by("time_collected")
            .all()
        )

    @property  # type: ignore
    @cached_function
    def final_serum_sample(self):
        """
        final_serum_sample in an instance method that returns the last single
        serum sample removed from the animal, based on the time elapsed/duration
        from the initiation of infusion or treatment, typically.  If the animal
        has no serum samples or if the retrieved serum sample has no annotated
        time_collected, a warning will be issued.
        """
        final_serum_sample = (
            self.samples.filter(tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX)
            .order_by("time_collected")
            .last()
        )

        if final_serum_sample is None:
            warnings.warn(f"Animal {self.name} has no 'serum' samples.")
        elif not final_serum_sample.time_collected:
            warnings.warn(
                f"The Final serum sample {final_serum_sample.name} for "
                f"Animal {self.name} is missing a time_collected value."
            )

        return final_serum_sample

    @property  # type: ignore
    @cached_function
    def final_serum_sample_id(self):
        """
        final_serum_sample_id in an instance method that returns the id of the last single
        serum sample removed from the animal, based on the time elapsed/duration from the initiation of infusion or
        treatment.  If the animal has no serum samples, a warning will be issued.
        """
        # Note: calling self.final_serum_sample here ran into linting issues with `fss.id` not "existing". Added
        # fss\..* to this list of generated-members in the pylint config to ignore it.
        id = None
        fss = self.final_serum_sample
        if fss and fss.id:
            id = fss.id
        return id

    class Meta:
        verbose_name = "animal"
        verbose_name_plural = "animals"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)

    def clean(self):
        super().clean()

        if self.treatment is not None:
            if self.treatment.category != Protocol.ANIMAL_TREATMENT:
                raise ValidationError(
                    "Protocol category for an Animal must be of type "
                    f"{Protocol.ANIMAL_TREATMENT}"
                )
