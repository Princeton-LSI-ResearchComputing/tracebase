import warnings

from django.db import models

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.maintained_model import MaintainedModel, maintained_field_function


class FCirc(MaintainedModel, HierCachedModel):
    """
    This class is here to perform rate of appearance/disappearance calculations for every combination of serum sample,
    tracer, and labeled element.  The last peakgroup of the given sample is used for every calculation.
    """
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
    is_last = models.BooleanField(
        default=False,
        help_text=(
            "This field indicates whether the last peak group of this serum sample and this tracer, is the last among "
            "the serum samples/tracers for the associated animal. Maintained field. Do not edit/set."
        ),
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

    def save(self, *args, **kwargs):
        """
        This checks to make sure that self.serum_sample is in fact a serum sample.
        """

        if not self.serum_sample.is_serum_sample:
            raise InvalidSerumSample(
                f"The linked sample [{self.serum_sample}] must be a serum sample."
            )

        # Now save the updated values
        super().save(*args, **kwargs)

    @maintained_field_function(
        generation=2,
        update_field_name="is_last",
        parent_field_name="serum_sample",
        update_label="fcirc_calcs",
    )
    def is_last_serum_peak_group(self):
        """
        Note, there is an FCirc record for every serum sample, tracer, and label combo.  Each such combo represents a
        single "peak group" even though there can exist multiple peak groups from different serum samples and different
        msruns from the same serum sample.  However, multiple msruns from the same sample are ignored - only the last
        one is represented by a record in this table.  Michael and I (Rob) discussed whether it was worthwhile to
        compute values for peak groups from this sample in prior msruns and Michael said no, so:

        This method determines whether the peak group from the last msrun that included this tracer and label is the
        last peakgroup when considered among multiple serum samples.  There could exist last peak groups in prior serum
        samples that would result in a false return here.  There can also be later serum samples that don't include a
        peak group for this tracer which would return false if it was among the peakgroups returned, but they will not
        be among the peakgroups represented in this table.
        """

        if self.last_peak_group_in_sample:
            return self.last_peak_group_in_sample == self.last_peak_group_in_animal
        else:
            warnings.warn(
                f"Serum sample {self.serum_sample} has no peak group for tracer {self.tracer}."
            )
            return False

    @property  # type: ignore
    @cached_function
    def last_peak_group_in_animal(self):
        """
        Retrieve the latest serum sample PeakGroup for this animal and tracer.
        """
        return self.serum_sample.animal.last_serum_tracer_peak_groups.filter(
            compounds__exact=self.tracer.compound
        ).get()

    @property  # type: ignore
    @cached_function
    def last_peak_group_in_sample(self):
        """
        Retrieve the latest PeakGroup for this serum sample and tracer.
        """
        peakgroups = self.serum_sample.last_tracer_peak_groups.filter(
            compounds__exact=self.tracer.compound
        )

        if peakgroups.count() == 0:
            warnings.warn(
                f"Serum sample {self.serum_sample} has no peak group for tracer {self.tracer}."
            )
            return None

        return peakgroups.get()

    @property  # type: ignore
    @cached_function
    def peak_groups(self):
        """
        Retrieve all PeakGroups for this serum sample and tracer, regardless of msrun date.

        Currently unused - see docstring in self.is_last_serum_peak_group
        """
        from DataRepo.models.peak_group import PeakGroup

        peakgroups = (
            PeakGroup.objects.filter(msrun__sample__exact=self.serum_sample)
            .filter(compounds__exact=self.tracer.compound)
            .order_by("msrun__date")
        )

        if peakgroups.count() == 0:
            warnings.warn(
                f"Serum sample {self.serum_sample} has no peak group for tracer {self.tracer}."
            )

        return peakgroups.all()

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_gram(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact_g. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
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
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_appearance_intact_per_gram

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_animal(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
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
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
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
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
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
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
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
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
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
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_appearance_average_per_animal


class InvalidSerumSample(ValueError):
    pass
