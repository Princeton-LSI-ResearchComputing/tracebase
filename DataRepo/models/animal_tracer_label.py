import warnings

from django.db import models

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.peak_group import PeakGroup


class AnimalTracerLabel(HierCachedModel):
    parent_related_key_name = "animal_tracer"
    # Leaf

    id = models.AutoField(primary_key=True)
    animal_tracer = models.ForeignKey(
        "DataRepo.AnimalTracer",
        on_delete=models.CASCADE,
        related_name="animal_tracer_labels",
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
        verbose_name = "animal_tracer_label"
        verbose_name_plural = "animal_tracer_labels"
        ordering = ["animal_tracer", "element"]
        constraints = [
            models.UniqueConstraint(
                fields=["animal_tracer", "element"],
                name="unique_animal_tracer_label",
            )
        ]

    def final_peak_group(self, sample):
        """
        Retrieve the latest PeakGroup for a given sample and this tracer/label.
        """

        sample_peakgroups = (
            PeakGroup.objects
            .filter(msrun__sample_id=sample.id)
            .filter(compounds__id__exact=self.animal_tracer.tracer.compound.id)
            .filter(peak_group_labels__element__exact=self.element)
            .order_by("msrun__date")
        )

        return sample_peakgroups.last()

    @property  # type: ignore
    @cached_function
    def final_serum_sample_tracer_peak_group(self):
        """
        final_serum_sample_tracer_peak_group is an instance method that returns
        the very last recorded PeakGroup obtained from the Animal's final serum
        sample from the last date it was measured/assayed
        """

        return self.final_peak_group(self.animal_tracer.animal.final_serum_sample)

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_disappearance_intact_per_gram(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact_g. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.animal_tracer.animal.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.final_serum_sample_tracer_peak_group.peak_group_labels.get(
                    element__exact=self.element,
                ).rate_disappearance_intact_per_gram
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_appearance_intact_per_gram(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact_g, or
        sometimes Fcirc_intact. This is calculated on the Animal's
        final serum sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.animal.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.final_serum_sample_tracer_peak_group.peak_group_labels.get(
                    element__exact=self.element,
                ).rate_appearance_intact_per_gram
            )









    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_disappearance_intact_per_animal(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.animal.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.animal.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.animal.final_serum_sample_tracer_peak_group.peak_group_labels.get(
                    element__exact=self.element,
                ).rate_disappearance_intact_per_animal
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_appearance_intact_per_animal(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact, or sometimes
        Fcirc_intact_per_mouse. This is calculated on the Animal's final serum
        sample tracer's PeakGroup.
        """
        if not self.animal.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.animal.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.animal.final_serum_sample_tracer_peak_group.peak_group_labels.get(
                    element__exact=self.element,
                ).rate_appearance_intact_per_animal
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_disappearance_average_per_gram(self):
        """
        Also referred to as Rd_avg_g = [Infusate] * 'Infusion Rate' / 'Enrichment Fraction' in
        nmol/min/g
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.animal.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.animal.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.animal.final_serum_sample_tracer_peak_group.peak_group_labels.get(
                    element__exact=self.element,
                ).rate_disappearance_average_per_gram
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_appearance_average_per_gram(self):
        """
        Also referred to as Ra_avg_g, and sometimes referred to as Fcirc_avg.
        Equivalent to Rd_avg_g - [Infusate] * 'Infusion Rate' in nmol/min/g
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.animal.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.animal.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.animal.final_serum_sample_tracer_peak_group.peak_group_labels.get(
                    element__exact=self.element,
                ).rate_appearance_average_per_gram
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_disappearance_average_per_animal(self):
        """
        Rate of Disappearance (avg), also referred to as Rd_avg
        Rd_avg = Rd_avg_g * 'Body Weight' in nmol/min
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.animal.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.animal.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.animal.final_serum_sample_tracer_peak_group.peak_group_labels.get(
                    element__exact=self.element,
                ).rate_disappearance_average_per_animal
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_appearance_average_per_animal(self):
        """
        Rate of Appearance (avg), also referred to as Ra_avg or sometimes
        Fcirc_avg_per_mouse. Ra_avg = Ra_avg_g * 'Body Weight'' in nmol/min
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.animal.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.animal.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.animal.final_serum_sample_tracer_peak_group.peak_group_labels.get(
                    element__exact=self.element,
                ).rate_appearance_average_per_animal
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_appearance_average_atom_turnover(self):
        """
        also referred to as Fcirc_avg_atom.  Originally defined as
        Fcirc_avg * PeakData:label_count in nmol atom / min / gram
        turnover of atoms in this compound, e.g. "nmol carbon / min / g"
        """
        if not self.animal.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.animal.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.animal.final_serum_sample_tracer_peak_group.peak_group_labels.get(
                    element__exact=self.element,
                ).rate_appearance_average_atom_turnover
            )
