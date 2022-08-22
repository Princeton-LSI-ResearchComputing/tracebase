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
    child_related_key_names = ["samples", "labels"]

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
        # PR REVIEW NOTE: This was previously RESTRICT, but I got errors during the cleanup of the validation
        #       database that it couldn't delete some infusate records because of links to it from animal, which I
        #       didn't expect... I thought it would delete only if there didn't exist any other links to it, and all
        #       the animals were being deleted.  Perhaps the first time, it wouldn't delete, but the last animal that
        #       links to it should delete the infusate...  How do I get that behavior?
        #       Here was the error:
        #       django.db.models.deletion.RestrictedError: ("Cannot delete some instances of model 'Infusate' because
        #       they are referenced through restricted foreign keys: 'Animal.infusate'.", {<Animal: 090320_M1>,
        #       <Animal: 090320_M2>, <Animal: 090320_M3>, ...
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

    @property  # type: ignore
    @cached_function
    def tracers(self):
        if self.infusate.tracers.count() == 0:
            warnings.warn(
                f"Animal [{self.animal}] has no tracers."
            )
        return self.infusate.tracers.all()

    # @cached_function is *slower* than uncached
    @cached_property
    def all_serum_samples(self):
        """
        all_serum_samples() in an instance method that returns all the serum samples removed from the calling animal
        object, ordered by the time they were collected from the animal, which is recorded as the time elapsed/duration
        from the initiation of infusion or treatment, typically.
        """
        return (
            self.samples.filter(tissue__name__startswith=Tissue.SERUM_TISSUE_PREFIX)
            .order_by("time_collected")
            .all()
        )

    @property  # type: ignore
    @cached_function
    def last_serum_sample(self):
        """
        last_serum_sample in an instance method that returns the last single serum sample removed from the animal,
        based on the time elapsed/duration from the initiation of infusion or treatment, typically.  If the animal has
        no serum samples or if the retrieved serum sample has no annotated time_collected, a warning will be issued.
        """
        last_serum_sample = (
            self.samples.filter(tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX)
            .order_by("time_collected")
            .last()
        )

        if last_serum_sample is None:
            warnings.warn(f"Animal {self.name} has no 'serum' samples.")
        elif not last_serum_sample.time_collected:
            warnings.warn(
                f"The Final serum sample {last_serum_sample} for animal [{self}] is missing a time_collected value."
            )

        return last_serum_sample

    def last_serum_sample_peak_group(self, compound):
        """
        Retrieve the latest PeakGroup of this animal for a given compound (whether it's the last serum sample or not -
        just as long as it's the last peakgroup for this compound measured in a serum sample).
        """
        from DataRepo.models import PeakGroup

        # PR REVIEW NOTE: I have noted that it should be possible to calculate all the below values
        # based on the "not last" peak group of a serum sample.  For example, if Lysine was the tracer, and it was
        # included in an msrun twice for the same serum sample, calculating based on it might be worthwhile for the
        # same reason that we show calculations for the "not last" serum sample.  If people think that's worthwhile, I
        # could hang this table off of peakGroup instead of here...
        peakgroups = (
            PeakGroup.objects.filter(msrun__sample__animal__id__exact=self.id)
            .filter(compounds__id__exact=compound.id)
            .filter(msrun__sample__tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX)
            .order_by("msrun__sample__time_collected", "msrun__date")
        )

        if peakgroups.count() == 0:
            warnings.warn(
                f"Animal [{self.name}] has no serum sample peak group for compound {compound}."
            )
            return None

        return peakgroups.last()

    def last_serum_sample_peak_group_label(self, compound, element):
        """
        Retrieve the latest PeakGroup of this animal for a given compound (whether it's the last serum sample or not -
        just as long as it's the last peakgroup for this compound measured in a serum sample).
        """
        from DataRepo.models import PeakGroupLabel

        # PR REVIEW NOTE: I have noted that it should be possible to calculate all the below values
        # based on the "not last" peak group of a serum sample.  For example, if Lysine was the tracer, and it was
        # included in an msrun twice for the same serum sample, calculating based on it might be worthwhile for the
        # same reason that we show calculations for the "not last" serum sample.  If people think that's worthwhile, I
        # could hang this table off of peakGroup instead of here...
        peakgrouplabels = (
            PeakGroupLabel.objects.filter(
                peak_group__msrun__sample__animal__id__exact=self.id
            )
            .filter(peak_group__compounds__id__exact=compound.id)
            .filter(element__exact=element)
            .filter(
                peak_group__msrun__sample__tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
            )
            .order_by(
                "peak_group__msrun__sample__time_collected", "peak_group__msrun__date"
            )
        )

        if peakgrouplabels.count() == 0:
            warnings.warn(
                f"Animal [{self.name}] has no serum sample peak group label for compound [{compound}] and element "
                f"[{element}]."
            )
            return None

        return peakgrouplabels.last()

    @property  # type: ignore
    @cached_function
    def last_serum_tracer_peak_groups(self):
        """
        Retrieves the last Peak Group for each tracer compound that has this.element
        """
        from DataRepo.models.peak_group import PeakGroup

        # Get every tracer's compound that contains this element
        if self.tracers.count() == 0:
            warnings.warn(
                f"Animal [{self.animal}] has no tracers containing labeled element [{self.element}]."
            )
            return PeakGroup.objects.none()

        # Get the last peakgroup for each tracer that has this label
        last_serum_peakgroup_ids = []
        for tracer in self.tracers.all():
            tracer_peak_group = (
                PeakGroup.objects.filter(
                    msrun__sample__animal__id__exact=self.id
                )
                .filter(compounds__id__exact=tracer.compound.id)
                .filter(
                    msrun__sample__tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
                )
                .order_by("msrun__sample__time_collected", "msrun__date")
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

    def clean(self):
        super().clean()

        if self.treatment is not None:
            if self.treatment.category != Protocol.ANIMAL_TREATMENT:
                raise ValidationError(
                    "Protocol category for an Animal must be of type "
                    f"{Protocol.ANIMAL_TREATMENT}"
                )
