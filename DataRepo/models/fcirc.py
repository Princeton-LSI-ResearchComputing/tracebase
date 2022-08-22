import warnings

from django.db import models

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.hier_cached_model import HierCachedModel, cached_function


class FCirc(HierCachedModel):
    parent_related_key_name = "serum_sample"
    # Leaf

    id = models.AutoField(primary_key=True)
    serum_sample = models.ForeignKey(
        "DataRepo.Sample",
        on_delete=models.CASCADE,
        related_name="fcircs",
        null=False,
        blank=False,
    )
    tracer = models.ForeignKey(
        "DataRepo.Tracer",
        on_delete=models.CASCADE,
        related_name="fcircs",
        null=False,
        blank=False,
    )
    element = models.CharField(
        max_length=1,
        null=False,
        blank=False,
        choices=ElementLabel.LABELED_ELEMENT_CHOICES,
        default=ElementLabel.CARBON,
        help_text='An element that is labeled in any of the tracers in this infusate (e.g. "C", "H", "O").',
    )

    class Meta:
        verbose_name = "fcirc"
        verbose_name_plural = "fcircs"
        ordering = ["serum_sample", "tracer", "element"]
        constraints = [
            models.UniqueConstraint(
                fields=["serum_sample", "tracer", "element"],
                name="unique_fcirc",
            )
        ]

    @property  # type: ignore
    @cached_function
    def last_peak_group(self):
        """
        Retrieve the latest PeakGroup for this serum sample, tracer, and label.  This differs from
        Animal.last_serum_sample_peak_group_label in that it forces the calculation for this serum sample specifically
        whereas Animal.last_serum_sample_peak_group_label gets the last peakgroup for the tracer compound in whichever
        serum sample has it (most likely the last one).
        """

        # PR REVIEW NOTE: I have noted that it should be possible to calculate all the below values
        # based on the "not last" peak group of a serum sample.  For example, if Lysine was the tracer, and it was
        # included in an msrun twice for the same serum sample, calculating based on it might be worthwhile for the
        # same reason that we show calculations for the "not last" serum sample.  If people think that's worthwhile, I
        # could hang this table off of peakGroup instead of here...
        peakgroups = (
            self.serum_sample.peak_groups(self.tracer.compound.id)
            .filter(labels__element__exact=self.element)
            .order_by("msrun__date")
        )

        if peakgroups.count() == 0:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None

        return peakgroups.last()

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_gram(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact_g. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.last_peak_group:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group.labels.get(
                element__exact=self.element,
            ).rate_disappearance_intact_per_gram

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_gram(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact_g, or
        sometimes Fcirc_intact. This is calculated on the Animal's
        final serum sample tracer's PeakGroup.
        """
        if not self.last_peak_group:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group.labels.get(
                element__exact=self.element,
            ).rate_appearance_intact_per_gram

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_animal(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.last_peak_group:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group.labels.get(
                element__exact=self.element,
            ).rate_disappearance_intact_per_animal

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_animal(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact, or sometimes
        Fcirc_intact_per_mouse. This is calculated on the Animal's final serum
        sample tracer's PeakGroup.
        """
        if not self.last_peak_group:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group.labels.get(
                element__exact=self.element,
            ).rate_appearance_intact_per_animal

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_gram(self):
        """
        Also referred to as Rd_avg_g = [Infusate] * 'Infusion Rate' / 'Enrichment Fraction' in
        nmol/min/g
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.last_peak_group:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group.labels.get(
                element__exact=self.element,
            ).rate_disappearance_average_per_gram

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_gram(self):
        """
        Also referred to as Ra_avg_g, and sometimes referred to as Fcirc_avg.
        Equivalent to Rd_avg_g - [Infusate] * 'Infusion Rate' in nmol/min/g
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.last_peak_group:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group.labels.get(
                element__exact=self.element,
            ).rate_appearance_average_per_gram

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_animal(self):
        """
        Rate of Disappearance (avg), also referred to as Rd_avg
        Rd_avg = Rd_avg_g * 'Body Weight' in nmol/min
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.last_peak_group:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group.labels.get(
                element__exact=self.element,
            ).rate_disappearance_average_per_animal

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_animal(self):
        """
        Rate of Appearance (avg), also referred to as Ra_avg or sometimes
        Fcirc_avg_per_mouse. Ra_avg = Ra_avg_g * 'Body Weight'' in nmol/min
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.last_peak_group:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group.labels.get(
                element__exact=self.element,
            ).rate_appearance_average_per_animal
