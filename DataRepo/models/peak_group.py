import warnings

from django.db import models
from django.db.models import Sum
from django.utils.functional import cached_property

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.utilities import atom_count_in_formula


class PeakGroup(HierCachedModel):

    parent_related_key_name = "msrun"
    child_related_key_names = ["peak_group_labels"]

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        help_text='The compound or isomer group name (e.g. "citrate/isocitrate", "glucose").',
    )
    formula = models.CharField(
        max_length=256,
        help_text='The molecular formula of the compound (e.g. "C6H12O6").',
    )
    msrun = models.ForeignKey(
        to="DataRepo.MSRun",
        on_delete=models.CASCADE,
        null=False,
        related_name="peak_groups",
        help_text="The MS Run this PeakGroup belongs to.",
    )
    compounds = models.ManyToManyField(
        to="DataRepo.Compound",
        related_name="peak_groups",
        help_text="The compound(s) that this PeakGroup is presumed to represent.",
    )
    peak_group_set = models.ForeignKey(
        to="DataRepo.PeakGroupSet",
        on_delete=models.CASCADE,
        null=False,
        related_name="peak_groups",
        help_text="The source file this PeakGroup came from.",
    )

    def atom_count(self, atom):
        return atom_count_in_formula(self.formula, atom)

    # @cached_function is *slower* than uncached
    @cached_property
    def total_abundance(self):
        """
        Total ion counts for this compound.
        Accucor provides this in the tab "pool size".
        Sum of the corrected_abundance of all PeakData for this PeakGroup.
        """
        # Note: If the measured compound does not contain one of the labeled atoms from the tracer compounds, including
        # counts from peakdata records linked to a label of an atom that is not in the measured compound would be
        # invalid.  However, such records should not exist, and if they do, their abundance would be 0, so this code
        # assumes that to be the case.
        return self.peak_data.all().aggregate(
            total_abundance=Sum("corrected_abundance", default=0)
        )["total_abundance"]

    @property  # type: ignore
    @cached_function
    def peak_labeled_elements(self):
        """
        Gets labels present among any of the tracers in the infusate IF the elements are present in the supplied
        (measured) compounds.  Basically, if the supplied compound contains an element that is a labeled element in any
        of the tracers, included in the returned list.
        """
        peak_labeled_elements = []
        compound_recs = self.compounds.all()
        for compound_rec in compound_recs:
            for (
                tracer_labeled_element
            ) in self.msrun.sample.animal.infusate.tracer_labeled_elements():
                if (
                    compound_rec.atom_count(tracer_labeled_element) > 0
                    and tracer_labeled_element not in peak_labeled_elements
                ):
                    peak_labeled_elements.append(tracer_labeled_element)
        return peak_labeled_elements

    # @cached_function is *slower* than uncached
    @cached_property
    def animal(self):
        """Convenient instance method to cache the animal this PeakGroup came from"""
        return self.msrun.sample.animal

    @property  # type: ignore
    @cached_function
    def is_tracer_compound_group(self):
        """
        Instance method which returns True if a compound it is associated with
        is also the tracer compound for the animal it came from.  This is
        primarily a check to prevent tracer appearance/disappearance
        calculations from returning values from non-tracer compounds. Uncertain
        whether this is a true concern.
        """
        if self.animal.tracer_compound in self.compounds.all():
            return True
        else:
            return False

    @property  # type: ignore
    @cached_function
    def from_serum_sample(self):
        """
        Instance method which returns True if a peakgroup was obtained from a
        msrun of a serum sample. Uncertain whether this is a true concern.
        """
        if self.msrun.sample.is_serum_sample:
            return True
        else:
            warnings.warn(f"{self.name} is not from a serum sample msrun.")
            return False

    @property  # type: ignore
    @cached_function
    def can_compute_tracer_rates(self):
        """
        Instance method which returns True if a peak_group can (feasibly)
        calculate rates of appearance and dissapearance of a tracer group
        """
        if not self.is_tracer_compound_group:
            warnings.warn(
                f"{self.name} is not the designated tracer for Animal {self.animal.name}."
            )
            return False
        elif not self.from_serum_sample:
            warnings.warn(f"{self.name} is not from a serum sample msrun.")
            return False
        elif not self.animal.tracer_infusion_concentration:
            warnings.warn(
                f"Animal {self.animal.name} has no annotated tracer_concentration."
            )
            return False
        elif not self.animal.tracer_infusion_rate:
            warnings.warn(
                f"Animal {self.animal.name} has no annotated tracer_infusion_rate."
            )
            return False
        return True

    # @cached_function is *slower* than uncached
    @cached_property
    def can_compute_body_weight_tracer_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can utilize
        the associated animal.body_weight
        """
        if not self.animal.body_weight:
            warnings.warn(f"Animal {self.animal.name} has no annotated body_weight.")
            return False
        else:
            return True

    @property  # type: ignore
    @cached_function
    def can_compute_intact_tracer_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can be
        calculated using fully-labeled/intact measurements of a tracer's
        peakdata.  Returns the peakdata.fraction, if it exists and is greater
        than zero.
        """
        from DataRepo.models.peak_data import PeakData

        if not self.can_compute_tracer_rates:
            warnings.warn(f"{self.name} cannot compute tracer rates.")
            return False
        else:
            try:
                intact_peakdata = self.peak_data.filter(
                    labeled_count=self.animal.tracer_labeled_count
                ).get()
            except PeakData.DoesNotExist:
                warnings.warn(
                    f"PeakGroup {self.name} has no fully labeled/intact peakdata."
                )
                return False

            if (
                intact_peakdata
                and intact_peakdata.fraction
                and intact_peakdata.fraction > 0
            ):
                return True
            else:
                warnings.warn(
                    f"PeakGroup {self.name} has no fully labeled/intact peakdata."
                )
                return False

    @property  # type: ignore
    @cached_function
    def can_compute_average_tracer_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can be
        calculated using averaged enrichment measurements of a tracer's
        peakdata.
        """
        if not self.can_compute_tracer_rates:
            warnings.warn(f"{self.name} cannot compute tracer rates.")
            return False
        else:
            if self.enrichment_fraction and self.enrichment_fraction > 0:
                return True
            else:
                warnings.warn(f"PeakGroup {self.name} has no enrichment_fraction.")
                return False

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_gram(self):
        """Rate of Disappearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        else:
            fraction = (
                self.peak_data.filter(labeled_count=self.animal.tracer_labeled_count)
                .get()
                .fraction
            )

            return (
                self.animal.tracer_infusion_rate
                * self.animal.tracer_infusion_concentration
                / fraction
            )

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_gram(self):
        """Rate of Appearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        else:
            return (
                self.rate_disappearance_intact_per_gram
                - self.animal.tracer_infusion_rate
                * self.animal.tracer_infusion_concentration
            )

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_animal(self):
        """Rate of Disappearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        elif not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        else:
            return self.rate_disappearance_intact_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_animal(self):
        """Rate of Appearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        elif not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        else:
            return self.rate_appearance_intact_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_gram(self):
        """
        Rd_avg_g = [Infusate] * 'Infusion Rate' / 'Enrichment Fraction'
        in nmol/min/g
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        else:
            return (
                self.animal.tracer_infusion_concentration
                * self.animal.tracer_infusion_rate
                / self.enrichment_fraction
            )

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_gram(self):
        """
        Ra_avg_g = Rd_avg_g - [Infusate] * 'Infusion Rate' in nmol/min/g
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        else:
            return (
                self.rate_disappearance_average_per_gram
                - self.animal.tracer_infusion_concentration
                * self.animal.tracer_infusion_rate
            )

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_animal(self):
        """
        Rate of Disappearance (avg)
        Rd_avg = Rd_avg_g * 'Body Weight' in nmol/min
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        elif not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        else:
            return self.rate_disappearance_average_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_animal(self):
        """
        Rate of Appearance (avg)
        Ra_avg = Ra_avg_g * 'Body Weight' in nmol/min
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        elif not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        else:
            return self.rate_appearance_average_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_atom_turnover(self):
        """
        turnover of atoms in this compound in nmol atom / min / gram
        """
        if (
            not self.can_compute_average_tracer_rates
            or not self.animal.tracer_labeled_count
        ):
            warnings.warn(
                f"{self.name} cannot compute average tracer turnover of atoms."
            )
            return None
        else:
            return (
                self.rate_appearance_average_per_gram * self.animal.tracer_labeled_count
            )

    class Meta:
        verbose_name = "peak group"
        verbose_name_plural = "peak groups"
        ordering = ["name"]

        # composite key
        constraints = [
            models.UniqueConstraint(
                fields=["name", "msrun"],
                name="unique_peakgroup",
            ),
        ]

    def __str__(self):
        return str(self.name)
